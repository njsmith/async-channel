import functools
import collections
import itertools
import traceback
import time

from async_generator import asynccontextmanager
import trio

from exceptions import RemoteException
import protocol


# Maximum number of incoming messages that the channel buffers.
# If message queue is full, the channel stops reading bytes from the network
# to apply backpressure.
MAX_INCOMING_MESSAGES = 100


@asynccontextmanager
async def open_connection(nursery, host, port, *, create_connection=None):
    """Return a :class:`Channel` connected to *addr*."""
    if create_connection is None:
        create_connection = protocol.Connection.get_factory()

    stream = await trio.open_tcp_stream(host, port)
    connection = create_connection()
    channel = Channel(nursery, stream, connection)
    try:
        yield channel
    finally:
        await channel.close()


async def start_server(nursery, host, port, client_connected_task, *,
                       create_connection=None):
    """Start a server listening on *addr* and call *client_connected_cb*
    for every client connecting to it.

    """
    if create_connection is None:
        create_connection = protocol.Connection.get_factory()

    async def task(stream):
        connection = create_connection()
        async with Channel(nursery, stream, connection) as channel:
            return await client_connected_task(channel)

    await nursery.start(functools.partial(
        trio.serve_tcp, task, port, host=host))


class Request:
    """Represents a request returned by :meth:`Channel.recv()`.  You shoudn't
    instantiate it yourself.

    *channel* is the :class:`Channel` instance that received the request.

    *content* contains the incoming message.

    *msg_id* is the ID for that message.  It is unique within a channel.

    To reply to that request you can ``await`` :meth:`Request.reply()` or
    :meth:`Request.fail()`.

    """
    def __init__(self, channel, message_id, content):
        self._channel = channel
        self._msg_id = message_id
        self._content = content

    @property
    def content(self):
        """The content of the incoming message."""
        return self._content

    async def reply(self, result):
        """Reply to the request with the provided result."""
        await self._channel._send_result(self._msg_id, result)

    async def fail(self, exception):
        """Indicate a failure described by the *exception* instance.

        This will raise a :exc:`~aiomas.exceptions.RemoteException` on the
        other side of the channel.

        """
        stacktrace = traceback.format_exception(exception.__class__, exception,
                                                exception.__traceback__)
        await self._channel._send_exception(self._msg_id, ''.join(stacktrace))


class Channel:
    """A Channel represents a request-reply channel between two endpoints. An
    instance of it is returned by :func:`open_connection()` or is passed to the
    callback of :func:`start_server()`.

    *protocol* is an instance of :class:`ChannelProtocol`.

    *transport* is an :class:`asyncio.BaseTransport`.

    *loop* is an instance of an :class:`asyncio.AbstractEventLoop`.

    """
    def __init__(self, nursery, stream, connection):
        self._stream = stream
        self._connection = connection

        self._message_id = itertools.count()
        self._waiters = {}  # message_id -> task
        # TODO: In theory, we could use a trio queue to automatically apply
        # backpressure when the queue is full.  However, when we use a queue,
        # we can no longer determine whether another task is already trying
        # to receive a mesage (is this really necessary?).  With a queue, it’s
        # also harder to forward and raise a ConnectionError to the waiting
        # task(s) (it’s already not possible since trio events cannot have
        # values and exceptions – or maybe I just use wait_task_rescheduled()
        # again).
        # self._in_queue = trio.Queue(MAX_INCOMING_MESSAGES)
        self._in_queue = collections.deque()
        self._waiter = None  # A future.

        nursery.start_soon(self._recv_messages)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def send(self, content):
        """Send a request *content* to the other end and return a future which
        is triggered when a reply arrives.

        One of the following exceptions may be raised:

        - :exc:`ValueError` if the message is too long (the length of the
          encoded message does not fit into a *long*, which is ~ 4 GiB).

        - :exc:`~aiomas.exceptions.RemoteException`: The remote site raised an
          exception during the computation of the result.

        - :exc:`ConnectionError` (or its subclass :exc:`ConnectionResetError`):
          The connection was closed during the request.

        - :exc:`RuntimeError`:

          - If an invalid message type was received.

          - If the future returned by this method was already triggered or
            canceled by a third party when an answer to the request arrives
            (e.g., if a task containing the future is cancelled).  You get
            more detailed exception messages if you `enable asyncio's debug
            mode`__

            __ https://docs.python.org/3/library/asyncio-dev.html

        .. code-block:: python

           try:
               result = await channel.send('ohai')
           except RemoteException as exc:
               print(exc)

        """
        message_id = next(self._message_id)
        self._waiters[message_id] = trio.hazmat.current_task()
        await self._send_request(message_id, content)

        def abort_fn(_):
            # Abandon cancelled "send()"s without notifying the peer.  Silent
            # drop the reply when it arrives.
            del self._waiters[message_id]
            return trio.hazmat.Abort.SUCCESS

        return await trio.hazmat.wait_task_rescheduled(abort_fn)

    async def recv(self):
        """Wait for an incoming :class:`Request` and return it.

        May raise one of the following exceptions:


        - :exc:`ConnectionError` (or its subclass :exc:`ConnectionResetError`):
          The connection was closed during the request.

        """
        if not self._in_queue:
            if self._waiter is not None:
                raise RuntimeError('recv() called while another coroutine is '
                                   'already waiting for incoming data.')
            self._waiter = trio.Event()
            # FIXME: _waiter should give me an exception if there was
            # a ConnectionError in "_recv_messages()"
            await self._waiter.wait()

        return self._in_queue.popleft()

    async def close(self):
        await self._stream.aclose()

    async def _send_request(self, msg_id, content):
        return await self._send(protocol.REQUEST, msg_id, content)

    async def _send_result(self, msg_id, content):
        return await self._send(protocol.RESULT, msg_id, content)

    async def _send_exception(self, msg_id, content):
        return await self._send(protocol.EXCEPTION, msg_id, content)

    async def _send(self, msg_type, msg_id, content):
        data = self._connection.produce_bytes(msg_type, msg_id, content)
        await self._stream.send_all(data)

    async def _recv_messages(self):
        """Called by :class:`ChannelProtocol` when a new message arrived."""
        while True:
            try:
                data = await self._stream.receive_some(1024)
            except trio.ClosedStreamError:
                data = b''
            if not data:
                # FIXME: If there is a self._waiter event, we should set it
                # with a connection error so that any task doing an "await
                # channel.recv()" gets this error.
                break

            # We handle messages in-line (and not in a separate method) for
            # performance reasons:
            msgs = self._connection.receive_bytes(data)
            for msg_type, msg_id, content in msgs:
                if msg_type == protocol.REQUEST:
                    # Received new request
                    message = Request(self, msg_id, content)
                    self._in_queue.append(message)

                    waiter = self._waiter
                    if waiter is not None:
                        self._waiter = None
                        waiter.set()

                elif msg_type in (protocol.RESULT, protocol.EXCEPTION):
                    # Received reply to a request
                    try:
                        task = self._waiters.pop(msg_id)
                    except KeyError:
                        # "send()" was cancelled.  We ignore the reply to it.
                        pass

                    if msg_type == protocol.RESULT:
                        result = trio.hazmat.Value(content)
                    else:
                        origin = self._stream.socket.getpeername()
                        exc = RemoteException(origin, content)
                        result = trio.haszmat.Error(exc)
                    trio.hazmat.reschedule(task, result)

                else:
                    raise RuntimeError(f'Invalid message type {msg_type}')


def main(host, port, client_count, msg_count, latencies):
    async def client(nursery, host, port, client_id, msg_count):
        async with open_connection(nursery, host, port) as channel:
            # print('Client: connected')
            for i in range(msg_count):
                start = time.monotonic()
                reply = await channel.send(f'ohai {client_id}-{i}')
                latencies.append(time.monotonic() - start)
                # print('Client: got reply:', reply)
        # print('Client: done')

    async def handle_client(channel):
        # print('Server: new connection')
        try:
            while True:
                request = await channel.recv()
                # print('Server: Got request:', request.content)
                await request.reply(request.content)
        except trio.Cancelled:
            # print('Server: Got cancelled')
            pass

    async def run():
        async with trio.open_nursery() as nursery:
            await start_server(nursery, host, port, handle_client)
            async with trio.open_nursery() as client_nursery:
                for i in range(client_count):
                    client_nursery.start_soon(
                        client, client_nursery, host, port, i, msg_count)
            nursery.cancel_scope.cancel()

    trio.run(run)


if __name__ == '__main__':
    main('localhost', 5555, 1, 1, [])
