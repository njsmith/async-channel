"""
This package imports the codecs that can be used for de- and encoding incoming
and outgoing messages:

- :class:`JSON` uses `JSON <http://www.json.org/>`_
- :class:`MsgPack` uses `msgpack <https://msgpack.org/>`_
- :class:`MsgPackBlosc` uses `msgpack <https://msgpack.org/>`_ and
  `Blosc <http://blosc.org/>`_

All codecs should implement the base class :class:`Codec`.

"""
import inspect
import json
import struct
import sys

from exceptions import SerializationError


__all__ = [
    'Connection', 'Codec', 'JSON', 'MsgPack', 'MsgPackBlosc',
]


# Message types
REQUEST = 0
RESULT = 1
EXCEPTION = 2

TYPESIZE = 8 if sys.maxsize > 2**32 else 4

Header = struct.Struct('!L')


class JSON:
    """Stripped down version of
    https://gitlab.com/sscherfke/aiomas/blob/master/src/aiomas/codecs.py

    """
    def encode(self, data):
        return json.dumps(data).encode()

    def decode(self, data):
        return json.loads(data.decode())


class Connection:
    def __init__(self, codec):
        self._codec = codec
        self._buffer = bytearray()
        self._read_size = None

    @classmethod
    def get_factory(cls, codec_cls=JSON, extra_serializers=()):
        def create_connection():
            codec = codec_cls()
            for s in extra_serializers:
                codec.add_serializer(*s())
            return cls(codec)
        return create_connection

    def receive_bytes(self, data: bytes) -> list:
        """Buffer the incoming *data* and return a list of *msg_type, msg_id,
        content* triplets."""
        self._buffer.extend(data)

        msgs = []
        while True:
            if self._read_size is None and len(self._buffer) >= Header.size:
                # Received the complete header of a new message
                self._read_size = Header.unpack_from(self._buffer)[0]
                # TODO: Check for too large messages?
                self._read_size += Header.size

            if self._read_size and len(self._buffer) >= self._read_size:
                # At least one complete message is in the self._buffer
                data = self._buffer[Header.size:self._read_size]
                self._buffer = self._buffer[self._read_size:]
                self._read_size = None
                msgs.append(self._codec.decode(data))

            else:
                # No complete message in the buffer. We are done.
                break

        return msgs

    def produce_bytes(self, msg_type, msg_id, content) -> bytes:
        """Convert the triplet *msg_type, msg_id, content* into a byte string
        and return it."""
        data = self._codec.encode((msg_type, msg_id, content))
        msg_len = self._get_checked_msg_len(data)
        return msg_len + data

    @staticmethod
    def _get_checked_msg_len(msg):
        """Return the number of bytes/length of *msg*.

        Raise a exc:`ValueError` if the length is to large for :class:`Header`.

        """
        msg_len = len(msg)
        try:
            len_bytes = Header.pack(msg_len)
        except struct.error:
            max_size = 2 ** (Header.size * 8)
            raise ValueError(f'Serialized message is too long: {msg_len}.  '
                             f'Maximum length is {max_size}.') from None

        return len_bytes
