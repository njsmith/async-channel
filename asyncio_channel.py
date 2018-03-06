"""
This module implements and asyncio :class:`asyncio.Protocol` protocol for a
request-reply :class:`Channel`.

"""
import asyncio
import collections
import itertools
import time
import traceback

from exceptions import RemoteException
import protocol


async def open_connection(host, port, *, create_connection=None, loop=None):
    """Return a :class:`Channel` connected to *addr*."""
    if create_connection is None:
        create_connection = protocol.Connection.get_factory()

    if loop is None:
        loop = asyncio.get_event_loop()

    def factory():
        connection = create_connection()
        return ChannelProtocol(connection, loop=loop)

    _t, p = await loop.create_connection(factory, host, port)

    return p.channel


async def start_server(host, port, client_connected_cb, *,
                       create_connection=None, loop=None):
    """Start a server listening on *addr* and call *client_connected_cb*
    for every client connecting to it."""
    if create_connection is None:
        create_connection = protocol.Connection.get_factory()

    if loop is None:
        loop = asyncio.get_event_loop()

    def factory():
        connection = create_connection()
        return ChannelProtocol(connection, client_connected_cb, loop=loop)

    return await loop.create_server(factory, host, port)


class ChannelProtocol(asyncio.Protocol):
    """Asyncio :class:`asyncio.Protocol` which connects the low level transport
    with the high level :class:`Channel` API.

    The *codec* is used to (de)serialize messages.  It should be a sub-class of
    :class:`aiomas.codecs.Codec`.

    Optionally you can also pass a function/coroutine *client_connected_cb*
    that will be executed when a new connection is made (see
    :func:`start_server()`).

    """
    def __init__(self, connection, client_connected_cb=None, *, loop):
        super().__init__()
        self.connection = connection
        self.transport = None
        self.channel = None
        self.channel_receive = None
        self._client_connected_cb = client_connected_cb
        self._loop = loop

        # For flow control
        self._paused = False
        self._drain_waiter = None
        self._connection_lost = None
        self._out_msgs = asyncio.Queue()
        self._task_process_out_msgs = None

    def connection_made(self, transport):
        """Create a new :class:`Channel` instance for a new connection.

        Also call the *client_connected_cb* if one was passed to this class.

        """
        self._task_process_out_msgs = self._loop.create_task(
            self._process_out_msgs())

        self.transport = transport
        self.channel = Channel(self, loop=self._loop)
        self.channel_receive = {
            protocol.REQUEST: self.channel._receive_request,
            protocol.RESULT: self.channel._receive_result,
            protocol.EXCEPTION: self.channel._receive_exception,
        }

        if self._client_connected_cb is not None:
            res = self._client_connected_cb(self.channel)
            if asyncio.iscoroutine(res):
                self._loop.create_task(res)

    def connection_lost(self, exc):
        """Set a :exc:`ConnectionError` to the :class:`Channel` to indicate
        that the connection is closed."""
        if exc is None:  # pragma: no branch
            exc = ConnectionResetError('Connection closed')
        self.channel._set_exception(exc)
        self._connection_lost = exc
        self._task_process_out_msgs.cancel()

        # Wake up the writer if currently paused.
        if not self._paused:
            return
        waiter = self._drain_waiter
        if waiter is None:
            return
        self._drain_waiter = None
        if waiter.done():
            return
        waiter.set_exception(exc)

    def data_received(self, data):
        """Buffer incomming data until we have a complete message and then
        pass it to :class:`Channel`.

        Messages are fixed length.  The first four bytes (in network byte
        order) encode the length of the following payload.  The payload is
        a triple ``(msg_type, msg_id, content)`` encoded with the specified
        *codec*.

        """
        msgs = self.connection.receive_bytes(data)
        for msg_type, msg_id, content in msgs:
            try:
                self.channel_receive[msg_type](msg_id, content)
            except KeyError:
                exc = RuntimeError(f'Invalid message type {msg_type}')
                self.channel._set_exception(exc)

    def eof_received(self):
        """Set a :exc:`ConnectionResetError` to the :class:`Channel`."""
        # In previous revisions, an IncompleteMessage error was raised if we
        # already received the beginning of a new message. However, having
        # to types of exceptions raised by this methods makes things more
        # complicated for the user. The benefit of the IncompleteMessage was
        # not big enough.
        self.channel._set_exception(ConnectionResetError())

    async def write(self, msg_type, msg_id, content):
        """Serialize *content* and write the result to the transport."""
        assert self._connection_lost is None
        content = self.connection.produce_bytes(msg_type, msg_id, content)
        done = self._loop.create_future()
        self._out_msgs.put_nowait((done, content))
        await done

    async def write_request(self, msg_id, content):
        return await self.write(protocol.REQUEST, msg_id, content)

    async def write_result(self, msg_id, content):
        return await self.write(protocol.RESULT, msg_id, content)

    async def write_exception(self, msg_id, content):
        return await self.write(protocol.EXCEPTION, msg_id, content)

    def pause_writing(self):
        """Set the *paused* flag to ``True``.

        Can only be called if we are not already paused.

        """
        assert not self._paused
        self._paused = True

    def resume_writing(self):
        """Set the *paused* flat to ``False`` and trigger the waiter future.

        Can only be called if we are paused.

        """
        assert self._paused
        self._paused = False

        waiter = self._drain_waiter
        if waiter is not None:
            self._drain_waiter = None
            if not waiter.done():
                waiter.set_result(None)

    async def _process_out_msgs(self):
        try:
            while True:
                done, content = await self._out_msgs.get()
                self.transport.write(content)
                await self._drain_helper()
                done.set_result(None)
        except asyncio.CancelledError:
            assert self._connection_lost is not None

    async def _drain_helper(self):
        if self._connection_lost is not None:
            raise self._connection_lost
        if not self._paused:
            return
        waiter = self._drain_waiter
        assert waiter is None or waiter.cancelled()
        waiter = self._loop.create_future()
        self._drain_waiter = waiter
        await waiter


class Request:
    """Represents a request returned by :meth:`Channel.recv()`.  You shoudn't
    instantiate it yourself.

    *content* contains the incoming message.

    *msg_id* is the ID for that message.  It is unique within a channel.

    *protocol* is the channel's :class:`ChannelProtocol` instance that is used
    for writing back the reply.

    To reply to that request you can ``await`` :meth:`Request.reply()` or
    :meth:`Request.fail()`.

    """
    def __init__(self, content, message_id, protocol):
        self._content = content
        self._msg_id = message_id
        self._protocol = protocol

    @property
    def content(self):
        """The content of the incoming message."""
        return self._content

    async def reply(self, result):
        """Reply to the request with the provided result."""
        protocol = self._protocol
        if protocol._connection_lost is not None:
            raise protocol._connection_lost

        await protocol.write_result(self._msg_id, result)

    async def fail(self, exception):
        """Indicate a failure described by the *exception* instance.

        This will raise a :exc:`~aiomas.exceptions.RemoteException` on the
        other side of the channel.

        """
        protocol = self._protocol
        if protocol._connection_lost is not None:
            raise protocol._connection_lost

        stacktrace = traceback.format_exception(exception.__class__, exception,
                                                exception.__traceback__)
        await protocol.write_exception(self._msg_id, ''.join(stacktrace))


class Channel:
    """A Channel represents a request-reply channel between two endpoints. An
    instance of it is returned by :func:`open_connection()` or is passed to the
    callback of :func:`start_server()`.

    *protocol* is an instance of :class:`ChannelProtocol`.

    *transport* is an :class:`asyncio.BaseTransport`.

    *loop* is an instance of an :class:`asyncio.AbstractEventLoop`.

    """
    def __init__(self, protocol, loop):
        self._protocol = protocol
        self._connection = protocol.connection
        self._transport = protocol.transport
        self._loop = loop

        self._message_id = itertools.count()
        self._out_messages = {}  # message_id -> message
        self._in_queue = collections.deque()
        self._waiter = None  # A future.
        self._exception = None

    def send(self, content):
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
               result = await channel.request('ohai')
           except RemoteException as exc:
               print(exc)

        """
        if self._exception is not None:
            raise self._exception

        message_id = next(self._message_id)
        out_message = self._loop.create_future()
        self._out_messages[message_id] = out_message

        self._loop.create_task(
            self._protocol.write_request(message_id, content))

        return out_message

    async def recv(self):
        """Wait for an incoming :class:`Request` and return it.

        May raise one of the following exceptions:


        - :exc:`ConnectionError` (or its subclass :exc:`ConnectionResetError`):
          The connection was closed during the request.

        - :exc:`RuntimeError`: If two processes try to read from the same
          channel or if an invalid message type was received.

        """
        if self._exception is not None:
            raise self._exception

        if not self._in_queue:
            if self._waiter is not None:
                raise RuntimeError('recv() called while another coroutine is '
                                   'already waiting for incoming data.')
            self._waiter = self._loop.create_future()
            try:
                await self._waiter
            finally:
                # In case of an exception, "self._waiter" is already set to
                # "None" by "self._set_exception()":
                self._waiter = None

        return self._in_queue.popleft()

    def _close(self):
        """Close the channel's transport."""
        if self._transport is not None:
            transport = self._transport
            self._transport = None
            return transport.close()

    async def close(self):
        """Close the channel and wait for all sub tasks to finish."""
        self._close()
        try:
            await self._protocol._task_process_out_msgs
            futs = self._out_messages.values()
            await asyncio.gather(*futs, return_exceptions=True)
        except asyncio.CancelledError:
            pass

    def _receive_request(self, msg_id, content):
        message = Request(content, msg_id, self._protocol)
        self._in_queue.append(message)

        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            waiter.set_result(False)

    def _receive_result(self, msg_id, content):
        message = self._out_messages.pop(msg_id)
        if message.done():
            errmsg = 'Request reply already set.'
            if message.cancelled():
                errmsg = 'Request was cancelled.'
            raise RuntimeError(errmsg)

        message.set_result(content)

    def _receive_exception(self, msg_id, content):
        message = self._out_messages.pop(msg_id)
        if message.done():
            errmsg = 'Request reply already set.'
            if message.cancelled():
                errmsg = 'Request was cancelled.'
            raise RuntimeError(errmsg)

        origin = self._transport.get_extra_info('peername')
        message.set_exception(RemoteException(origin, content))

    def _set_exception(self, exc):
        """Set an exception as result for all futures managed by the Channel
        in order to stop all coroutines from reading/writing to the socket."""
        self._exception = exc

        # Set exception to wait-recv future
        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            if not waiter.cancelled():
                waiter.set_exception(exc)

        # Set exception to all message futures which wait for a reply
        for msg in self._out_messages.values():
            if not msg.done():
                msg.set_exception(exc)

        self._close()


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
        except ConnectionResetError:
            await channel.close()

    async def run():
        await start_server(host, port, handle_client)
        tasks = [loop.create_task(client(host, port, i, msg_count))
                 for i in range(client_count)]
        await asyncio.gather(*tasks)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    loop.close()


if __name__ == '__main__':
    main('localhost', 5555, 1, 1)
