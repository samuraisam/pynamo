class NotFoundError(Exception):
    """
    Thrown during :meth:`get` when the requested object could not be found.
    """
    pass


class ValidationError(Exception):
    """
    Thrown by :class:`Field` subclasses when setting attributes that are 
    invalid.
    """
    pass
