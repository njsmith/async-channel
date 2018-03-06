import collections
import itertools
import traceback

import curio

from exceptions import RemoteException
import protocol


async def open_connection(host, port, *, create_connection=None):
    """Return a :class:`Channel` connected to *addr*."""
    if create_connection is None:
        create_connection = protocol.Connection.get_factory()

    async with curio.timeout_after(1):
        while True:
            try:
                socket = await curio.open_connection(host, port)
                break
            except ConnectionRefusedError:
                await curio.sleep(0.1)

    connection = create_connection()
    return await Channel.create(socket, connection)


async def start_server(host, port, client_connected_task, *,
                       create_connection=None):
    """Start a server listening on *addr* and call *client_connected_cb*
    for every client connecting to it.

    """
    if create_connection is None:
        create_connection = protocol.Connection.get_factory()

    async def task(socket, _addr):
        connection = create_connection()
        channel = await Channel.create(socket, connection)
        return await client_connected_task(channel)

    return await curio.spawn(curio.tcp_server(host, port, task))


class Message:
    """Outgoing message that will eventually receive the peer's reply.

    We need this b/c :class:`curio.Event` cannot contain user data.

    """
    __no_value = object()

    def __init__(self):
        self._exception = self.__no_value
        self._value = self.__no_value
        self._event = curio.Event()

    def is_set(self):
        return self._event.is_set()

    async def set_reply(self, value):
        self._exception = False
        self._value = value
        await self._event.set()

    async def set_exception(self, value):
        self._exception = True
        self._value = value
        await self._event.set()

    async def reply(self):
        await self._event.wait()
        if self._exception:
            raise self._value
        else:
            return self._value


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
    def __init__(self, socket, connection):
        self._socket = socket
        self._connection = connection

        self._message_id = itertools.count()
        self._out_messages = {}  # message_id -> message
        self._in_queue = collections.deque()
        self._waiter = None  # A future.
        self._receiver = None

    @classmethod
    async def create(cls, socket, connection):
        channel = cls(socket, connection)
        channel._receiver = await curio.spawn(channel._recv_messages())
        return channel

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
        out_message = Message()
        self._out_messages[message_id] = out_message
        await self._send_request(message_id, content)
        return await out_message.reply()

    async def recv(self):
        """Wait for an incoming :class:`Request` and return it.

        May raise one of the following exceptions:


        - :exc:`ConnectionError` (or its subclass :exc:`ConnectionResetError`):
          The connection was closed during the request.

        - :exc:`RuntimeError`: If two processes try to read from the same
          channel or if an invalid message type was received.

        """
        if not self._in_queue:
            if self._waiter is not None:
                raise RuntimeError('recv() called while another coroutine is '
                                   'already waiting for incoming data.')
            self._waiter = curio.Event()
            try:
                await self._waiter.wait()
            finally:
                # FIXME: Curio events don't have values and exceptions.
                #
                # In case of an exception, "self._waiter" is already set to
                # "None" by "self._set_exception()":
                self._waiter = None

        return self._in_queue.popleft()

    async def close(self):
        await self._socket.close()
        await self._receiver.cancel()

    async def _send_request(self, msg_id, content):
        return await self._send(protocol.REQUEST, msg_id, content)

    async def _send_result(self, msg_id, content):
        return await self._send(protocol.RESULT, msg_id, content)

    async def _send_exception(self, msg_id, content):
        return await self._send(protocol.EXCEPTION, msg_id, content)

    async def _send(self, msg_type, msg_id, content):
        data = self._connection.produce_bytes(msg_type, msg_id, content)
        await self._socket.sendall(data)

    async def _recv_messages(self):
        """Called by :class:`ChannelProtocol` when a new message arrived."""
        while True:
            data = await self._socket.recv(1024)
            if not data:
                break
            msgs = self._connection.receive_bytes(data)
            for msg_type, msg_id, content in msgs:
                if msg_type == protocol.REQUEST:
                    # Received new request
                    message = Request(self, msg_id, content)
                    self._in_queue.append(message)

                    waiter = self._waiter
                    if waiter is not None:
                        self._waiter = None
                        await waiter.set()

                elif msg_type in (protocol.RESULT, protocol.EXCEPTION):
                    # Received reply to a request
                    message = self._out_messages.pop(msg_id)
                    if message.is_set():
                        errmsg = 'Request reply already set.'
                        # if message.cancelled():
                        #     errmsg = 'Request was cancelled.'
                        raise RuntimeError(errmsg)

                    if msg_type == protocol.RESULT:
                        await message.set_reply(content)
                    else:
                        origin = self._socket.getpeername()
                        exc = RemoteException(origin, content)
                        await message.set_exception(exc)

                else:
                    raise RuntimeError(f'Invalid message type {msg_type}')


def main(host, port, client_count, msg_count):
    async def client(host, port, client_id, msg_count):
        channel = await open_connection(host, port)
        for i in range(msg_count):
            reply = await channel.send(f'ohai {client_id}-{i}')
        await channel.close()

    async def handle_client(channel):
        try:
            while True:
                request = await channel.recv()
                # print(request.content)
                await request.reply(request.content)
        except curio.CancelledError:
            await channel.close()

    async def run():
        server = await start_server(host, port, handle_client)
        async with curio.TaskGroup() as g:
            for i in range(client_count):
                await g.spawn(client(host, port, i, msg_count))
        await server.cancel()

    curio.run(run())


if __name__ == '__main__':
    main('localhost', 5555, 1, 1)
