class RemoteException(Exception):
    """Wraps a traceback of an exception on the other side of a channel.

    *origin* is the remote peername.

    *remote_traceback* is the remote exception's traceback.

    """
    def __init__(self, origin, remote_traceback):
        super().__init__(origin, remote_traceback)
        self.origin = origin  #: Peername (producer of the exception)
        self.remote_traceback = remote_traceback  #: Original traceback

    def __str__(self):
        return 'Origin: {}\n{}'.format(self.origin, self.remote_traceback)


class SerializationError(Exception):
    """Raised when an object cannot be serialized."""
