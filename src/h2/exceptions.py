"""
h2/exceptions
~~~~~~~~~~~~~

Exceptions for the HTTP/2 module.
"""
from __future__ import annotations

from .errors import ErrorCodes


class H2Error(Exception):
    """
    The base class for all exceptions for the HTTP/2 module.
    """


class ProtocolError(H2Error):
    """
    An action was attempted in violation of the HTTP/2 protocol.
    """

    #: The error code corresponds to this kind of Protocol Error.
    error_code = ErrorCodes.PROTOCOL_ERROR


class FrameTooLargeError(ProtocolError):
    """
    The frame that we tried to send or that we received was too large.
    """

    #: The error code corresponds to this kind of Protocol Error.
    error_code = ErrorCodes.FRAME_SIZE_ERROR


class FrameDataMissingError(ProtocolError):
    """
    The frame that we received is missing some data.

    .. versionadded:: 2.0.0
    """

    #: The error code corresponds to this kind of Protocol Error.
    error_code = ErrorCodes.FRAME_SIZE_ERROR


class TooManyStreamsError(ProtocolError):
    """
    An attempt was made to open a stream that would lead to too many concurrent
    streams.
    """



class FlowControlError(ProtocolError):
    """
    An attempted action violates flow control constraints.
    """

    #: The error code corresponds to this kind of Protocol Error.
    error_code = ErrorCodes.FLOW_CONTROL_ERROR


class StreamIDTooLowError(ProtocolError):
    """
    An attempt was made to open a stream that had an ID that is lower than the
    highest ID we have seen on this connection.
    """

    def __init__(self, stream_id: int, max_stream_id: int) -> None:
        #: The ID of the stream that we attempted to open.
        self.stream_id = stream_id

        #: The current highest-seen stream ID.
        self.max_stream_id = max_stream_id

    def __str__(self) -> str:
        return f"StreamIDTooLowError: {self.stream_id} is lower than {self.max_stream_id}"


class NoAvailableStreamIDError(ProtocolError):
    """
    There are no available stream IDs left to the connection. All stream IDs
    have been exhausted.

    .. versionadded:: 2.0.0
    """



class NoSuchStreamError(ProtocolError):
    """
    A stream-specific action referenced a stream that does not exist.

    .. versionchanged:: 2.0.0
       Became a subclass of :class:`ProtocolError
       <h2.exceptions.ProtocolError>`
    """

    def __init__(self, stream_id: int) -> None:
        #: The stream ID corresponds to the non-existent stream.
        self.stream_id = stream_id


class StreamClosedError(NoSuchStreamError):
    """
    A more specific form of
    :class:`NoSuchStreamError <h2.exceptions.NoSuchStreamError>`. Indicates
    that the stream has since been closed, and that all state relating to that
    stream has been removed.
    """

    def __init__(self, stream_id: int) -> None:
        #: The stream ID corresponds to the nonexistent stream.
        self.stream_id = stream_id

        #: The relevant HTTP/2 error code.
        self.error_code = ErrorCodes.STREAM_CLOSED

        # Any events that internal code may need to fire. Not relevant to
        # external users that may receive a StreamClosedError.
        self._events = []  # type: ignore


class InvalidSettingsValueError(ProtocolError, ValueError):
    """
    An attempt was made to set an invalid Settings value.

    .. versionadded:: 2.0.0
    """

    def __init__(self, msg: str, error_code: ErrorCodes) -> None:
        super().__init__(msg)
        self.error_code = error_code


class InvalidBodyLengthError(ProtocolError):
    """
    The remote peer sent more or less data that the Content-Length header
    indicated.

    .. versionadded:: 2.0.0

    .. versionchanged:: 4.5.0
       Added the ``stage``, ``kind`` and ``remaining`` attributes, so that a
       body that overruns its declared boundary (``body-overflow``) can be
       told apart from one that ends before it (``body-underflow``) without
       inspecting the exception message.
    """

    #: The receive phase in which the boundary was violated.
    stage = "body-boundary"

    def __init__(self,
                 expected: int,
                 actual: int,
                 discarded: int = 0) -> None:
        self.expected_length = expected
        self.actual_length = actual
        self.discarded_bytes = discarded

    @property
    def kind(self) -> str:
        """
        A stable, machine-readable classification of this boundary breach.
        """
        if self.actual_length > self.expected_length:
            return "body-overflow"
        return "body-underflow"

    @property
    def remaining(self) -> int:
        """
        How many declared bytes are still unaccounted for (negative for an
        overflow).
        """
        return self.expected_length - self.actual_length

    def __str__(self) -> str:
        return f"InvalidBodyLengthError: Expected {self.expected_length} bytes, received {self.actual_length}"


class TruncatedFrameError(ProtocolError):
    """
    The peer's input ended before a frame it had already declared was
    complete.

    This is deliberately a distinct type from
    :class:`FrameDataMissingError <h2.exceptions.FrameDataMissingError>`: a
    frame whose body is missing *inside* an otherwise valid frame is a peer
    protocol error, while input that simply stops mid-frame means the
    connection can never be safely reused. The ``stage`` attribute says
    where the input ran out (``frame-header``, ``frame-body`` or
    ``header-block``), so a diagnostic does not have to infer the phase from
    the final exception.

    .. versionadded:: 4.5.0
    """

    #: The error code corresponds to this kind of Protocol Error.
    error_code = ErrorCodes.PROTOCOL_ERROR

    #: The receive phase in which the input ended.
    stage = "frame-body"

    #: A stable, machine-readable classification.
    kind = "truncated"

    def __init__(self,
                 declared_length: int | None = None,
                 received_length: int = 0,
                 offset: int = 0,
                 stage: str | None = None) -> None:
        self.declared_length = declared_length
        self.received_length = received_length
        self.offset = offset
        if stage is not None:
            self.stage = stage

    def __str__(self) -> str:
        declared = (
            "unknown" if self.declared_length is None
            else str(self.declared_length)
        )
        return (
            f"TruncatedFrameError: input ended during {self.stage} at byte "
            f"offset {self.offset}: declared {declared} bytes, received "
            f"{self.received_length}"
        )


class BoundaryViolationError(H2Error):
    """
    A request-body length boundary, an upstream byte remainder, or a buffer
    ownership hand-off was violated.

    Unlike :class:`ProtocolError <h2.exceptions.ProtocolError>` this does not
    imply that the wire protocol was malformed: it is raised by the
    boundary/ownership guards that sit on top of the protocol stack (for
    example when a gateway tries to reuse a connection whose previous
    request body was not fully consumed). ``stage`` and ``kind`` carry the
    stable classification.

    .. versionadded:: 4.5.0
    """

    def __init__(self,
                 message: str,
                 *,
                 stage: str,
                 kind: str,
                 connection_id: str | None = None,
                 stream_id: int | None = None) -> None:
        super().__init__(message)
        self.stage = stage
        self.kind = kind
        self.connection_id = connection_id
        self.stream_id = stream_id

    def __str__(self) -> str:
        location = ""
        if self.connection_id is not None:
            location = f" on connection {self.connection_id}"
        if self.stream_id is not None:
            location += f" stream {self.stream_id}"
        return f"BoundaryViolationError[{self.stage}/{self.kind}]{location}: {self.args[0]}"


class UnsupportedFrameError(ProtocolError):
    """
    The remote peer sent a frame that is unsupported in this context.

    .. versionadded:: 2.1.0

    .. versionchanged:: 4.0.0
       Removed deprecated KeyError parent class.
    """



class RFC1122Error(H2Error):
    """
    Emitted when users attempt to do something that is literally allowed by the
    relevant RFC, but is sufficiently ill-defined that it's unwise to allow
    users to actually do it.

    While there is some disagreement about whether or not we should be liberal
    in what accept, it is a truth universally acknowledged that we should be
    conservative in what emit.

    .. versionadded:: 2.4.0
    """

    # shazow says I'm going to regret naming the exception this way. If that
    # turns out to be true, TELL HIM NOTHING.


class DenialOfServiceError(ProtocolError):
    """
    Emitted when the remote peer exhibits a behaviour that is likely to be an
    attempt to perform a Denial of Service attack on the implementation. This
    is a form of ProtocolError that carries a different error code, and allows
    more easy detection of this kind of behaviour.

    .. versionadded:: 2.5.0
    """

    #: The error code corresponds to this kind of
    #: :class:`ProtocolError <h2.exceptions.ProtocolError>`
    error_code = ErrorCodes.ENHANCE_YOUR_CALM
