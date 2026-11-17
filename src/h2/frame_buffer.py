"""
h2/frame_buffer
~~~~~~~~~~~~~~~

A data structure that provides a way to iterate over a byte buffer in terms of
frames.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from hyperframe.exceptions import InvalidDataError, InvalidFrameError
from hyperframe.frame import ContinuationFrame, Frame, HeadersFrame, PushPromiseFrame

from .exceptions import (
    FrameDataMissingError,
    FrameTooLargeError,
    ProtocolError,
    TruncatedFrameError,
)

if TYPE_CHECKING:  # pragma: no cover
    from ._typing import Buffer

# To avoid a DOS attack based on sending loads of continuation frames, we limit
# the maximum number we're prepared to receive. In this case, we'll set the
# limit to 64, which means the largest encoded header block we can receive by
# default is 262144 bytes long, and the largest possible *at all* is 1073741760
# bytes long.
#
# This value seems reasonable for now, but in future we may want to evaluate
# making it configurable.
CONTINUATION_BACKLOG = 64

#: Byte offset of the frame body in a serialized frame: a frame header is
#: exactly nine bytes (three length bytes, one type byte, one flags byte and
#: four stream-id bytes).
FRAME_HEADER_LEN = 9


@dataclass(frozen=True, slots=True)
class FrameBufferState:
    """
    An immutable snapshot of a :class:`FrameBuffer`'s receive state.

    Snapshots exist so that a diagnostic can record, at every fragment
    boundary, what the buffer held and what the peer had declared. Every
    field is derived from bytes that have actually arrived: ``declared_length``
    is the peer's claim and is never used to allocate or count anything.

    .. versionadded:: 4.5.0
    """

    #: Bytes currently buffered but not yet consumed.
    buffered_bytes: int

    #: The length declared by the frame header at the head of the buffer, or
    #: ``None`` when fewer than nine bytes are buffered (so there is no
    #: complete header to read a length from).
    declared_length: int | None

    #: Body bytes of the head frame that have already arrived. Zero when
    #: ``declared_length`` is ``None``.
    received_bytes: int

    #: The receive stage the buffer is currently in: ``"frame-header"`` while
    #: waiting for a complete header, ``"frame-body"`` while waiting for the
    #: rest of a declared body, ``"header-block"`` while a HEADERS or
    #: PUSH_PROMISE block is still waiting for CONTINUATION frames, or
    #: ``"idle"`` when the buffer is empty.
    stage: str

    #: The number of fragments (HEADERS plus CONTINUATION frames) buffered
    #: for the in-progress header block.
    header_block_fragments: int

    #: Total bytes successfully parsed and removed from this buffer.
    consumed_bytes: int

    @property
    def pending_body_bytes(self) -> int:
        """
        How many more body bytes the head frame still needs, if any.
        """
        if self.declared_length is None:
            return 0
        return max(0, self.declared_length - self.received_bytes)


class FrameBuffer:
    """
    A buffer data structure for HTTP/2 data that allows iteration in terms of
    H2 frames.
    """

    def __init__(self, server: bool = False) -> None:
        self._data = bytearray()
        self.max_frame_size = 0
        self._preamble = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n" if server else b""
        self._preamble_len = len(self._preamble)
        self._preamble_received = 0
        self._headers_buffer: list[HeadersFrame | ContinuationFrame | PushPromiseFrame] = []

        # Diagnostics state. ``_consumed_bytes`` counts bytes actually parsed
        # out of this buffer; it is what a replay compares when it wants to
        # know whether two fragmentation schedules delivered the same stream.
        self._consumed_bytes = 0
        self._frames_parsed = 0

    def add_data(self, data: Buffer) -> None:
        """
        Add more data to the frame buffer.

        :param data: A bytestring containing the byte buffer.
        """
        data_view = memoryview(data)

        if self._preamble_len:
            data_len = len(data_view)
            of_which_preamble = min(self._preamble_len, data_len)

            if self._preamble[:of_which_preamble] != data_view[:of_which_preamble]:
                msg = "Invalid HTTP/2 preamble."
                raise ProtocolError(msg)

            data_view = data_view[of_which_preamble:]
            self._preamble_len -= of_which_preamble
            self._preamble = self._preamble[of_which_preamble:]
            self._preamble_received += of_which_preamble

        self._data += data_view

    @property
    def buffered_bytes(self) -> int:
        """
        The number of bytes currently buffered and not yet consumed.

        This counts both parsed-frame bytes waiting in the buffer and any
        part of the client connection preface that has not yet arrived, since
        both are input the peer has left outstanding.
        """
        return len(self._data) + self._preamble_len

    @property
    def pending_preamble_bytes(self) -> int:
        """
        The number of bytes of the client connection preface still missing.

        A non-zero value means the peer has not even finished its preface, so
        the connection cannot be considered finished or reusable.
        """
        return self._preamble_len

    @property
    def consumed_bytes(self) -> int:
        """
        The total number of bytes this buffer has successfully parsed and
        removed since it was created.
        """
        return self._consumed_bytes

    @property
    def frames_parsed(self) -> int:
        """
        The total number of frames this buffer has successfully yielded.
        """
        return self._frames_parsed

    def snapshot(self) -> FrameBufferState:
        """
        Return an immutable :class:`FrameBufferState` describing exactly what
        has arrived and what is still outstanding.

        This deliberately re-checks the declared length against the bytes
        received instead of trusting any earlier observation: a snapshot is
        always computed from the buffer's current contents, so calling it at
        every fragment boundary gives a consistent view no matter how the
        peer's bytes were split on the wire.
        """
        declared_length, received_bytes = self._pending_frame_progress()

        if self._preamble_len:
            stage = "preamble"
        elif declared_length is not None:
            stage = "frame-body"
        elif self._headers_buffer:
            stage = "header-block"
        elif self._data:
            stage = "frame-header"
        else:
            stage = "idle"

        if self._headers_buffer:
            # A header block that is still waiting for CONTINUATION frames
            # takes precedence over "waiting for a body": the block cannot
            # legally be interleaved with any other frame.
            stage = "header-block"

        return FrameBufferState(
            buffered_bytes=len(self._data) + self._preamble_len,
            declared_length=declared_length,
            received_bytes=received_bytes,
            stage=stage,
            header_block_fragments=len(self._headers_buffer),
            consumed_bytes=self._consumed_bytes,
        )

    def _pending_frame_progress(self) -> tuple[int | None, int]:
        """
        Return ``(declared_length, received_body_bytes)`` for the frame at the
        head of the buffer, or ``(None, 0)`` when no complete frame header has
        arrived yet.

        The declared length is read from the header only to *compare* it with
        what has arrived; it is never used to reserve space or to decide that
        a frame is complete.
        """
        if len(self._data) < FRAME_HEADER_LEN:
            return None, 0
        try:
            _frame, length = Frame.parse_frame_header(
                memoryview(self._data[:FRAME_HEADER_LEN]),
            )
        except (InvalidDataError, InvalidFrameError):  # pragma: no cover
            # A header that cannot even be parsed has no trustworthy declared
            # length; ``__next__`` will raise the protocol error.
            return None, 0
        received = max(0, len(self._data) - FRAME_HEADER_LEN)
        return length, min(received, length)

    def _validate_frame_length(self, length: int) -> None:
        """
        Confirm that the frame is an appropriate length.
        """
        if length > self.max_frame_size:
            msg = f"Received overlong frame: length {length}, max {self.max_frame_size}"
            raise FrameTooLargeError(msg)


    def _update_header_buffer(self, f: Frame | None) -> Frame | None:
        """
        Updates the internal header buffer. Returns a frame that should replace
        the current one. May throw exceptions if this frame is invalid.
        """
        # Check if we're in the middle of a headers block. If we are, this
        # frame *must* be a CONTINUATION frame with the same stream ID as the
        # leading HEADERS or PUSH_PROMISE frame. Anything else is a
        # ProtocolError. If the frame *is* valid, append it to the header
        # buffer.
        if self._headers_buffer:
            stream_id = self._headers_buffer[0].stream_id
            valid_frame = (
                f is not None and
                isinstance(f, ContinuationFrame) and
                f.stream_id == stream_id
            )
            if not valid_frame:
                msg = "Invalid frame during header block."
                raise ProtocolError(msg)
            assert isinstance(f, ContinuationFrame)

            # Append the frame to the buffer.
            self._headers_buffer.append(f)
            if len(self._headers_buffer) > CONTINUATION_BACKLOG:
                msg = "Too many continuation frames received."
                raise ProtocolError(msg)

            # If this is the end of the header block, then we want to build a
            # mutant HEADERS frame that's massive. Use the original one we got,
            # then set END_HEADERS and set its data appropriately. If it's not
            # the end of the block, lose the current frame: we can't yield it.
            if "END_HEADERS" in f.flags:
                f = self._headers_buffer[0]
                f.flags.add("END_HEADERS")
                f.data = b"".join(x.data for x in self._headers_buffer)
                self._headers_buffer = []
            else:
                f = None
        elif (isinstance(f, (HeadersFrame, PushPromiseFrame)) and
                "END_HEADERS" not in f.flags):
            # This is the start of a headers block! Save the frame off and then
            # act like we didn't receive one.
            self._headers_buffer.append(f)
            f = None

        return f

    # The methods below support the iterator protocol.
    def __iter__(self) -> FrameBuffer:
        return self

    def _truncated(self, declared_length: int | None = None) -> TruncatedFrameError:
        """
        Build a :class:`TruncatedFrameError` describing the frame that input
        ran out on. The consume offset is the absolute byte offset, within
        this buffer's history, of the first byte of the incomplete item, so
        that a report can point at the exact place the stream stopped.
        """
        if self._preamble_len and not self._data:
            return TruncatedFrameError(
                declared_length=declared_length,
                received_length=len(self._preamble),
                offset=0,
                stage="preamble",
            )
        if self._headers_buffer:
            return TruncatedFrameError(
                declared_length=declared_length,
                received_length=max(0, len(self._data) - FRAME_HEADER_LEN),
                offset=self._consumed_bytes,
                stage="header-block",
            )
        if declared_length is None:
            return TruncatedFrameError(
                declared_length=None,
                received_length=0,
                offset=self._consumed_bytes,
                stage="frame-header",
            )
        return TruncatedFrameError(
            declared_length=declared_length,
            received_length=max(0, len(self._data) - FRAME_HEADER_LEN),
            offset=self._consumed_bytes,
            stage="frame-body",
        )

    def finish(self) -> None:
        """
        Signal that the peer's input has ended and no more data will arrive.

        Any bytes still buffered at this point are a half-delivered frame:
        either a partial frame header, or a frame body shorter than the length
        the header declared. :class:`TruncatedFrameError
        <h2.exceptions.TruncatedFrameError>` is raised for that case, with the
        stage it happened in. A buffer that is empty (and whose preface, if
        any, was fully received) finishes cleanly.

        This exists so that callers never have to treat a buffer that merely
        stopped short as a normal end of stream: ``StopIteration`` from
        :meth:`__next__` means "need more data", while a successful
        :meth:`finish` means "the stream was complete".
        """
        if self._preamble_len:
            raise self._truncated()

        if not self._data and not self._headers_buffer:
            return

        declared_length, _received = self._pending_frame_progress()
        raise self._truncated(declared_length)

    def __next__(self) -> Frame:
        # First, check that we have enough data to successfully parse the
        # next frame header. If not, bail. Otherwise, parse it.
        if len(self._data) < FRAME_HEADER_LEN:
            raise StopIteration

        try:
            f, length = Frame.parse_frame_header(memoryview(self._data[:FRAME_HEADER_LEN]))
        except (InvalidDataError, InvalidFrameError) as err:  # pragma: no cover
            msg = f"Received frame with invalid header: {err!s}"
            raise ProtocolError(msg) from err

        # Next, check that we have enough length to parse the frame body. If
        # not, bail, leaving the frame header data in the buffer for next time.
        # The comparison is re-done from the buffer's current size every time
        # this method runs: a single earlier read that happened to contain a
        # whole frame must not stop us from re-checking the declared length
        # against the bytes received at the next fragment boundary.
        if len(self._data) < length + FRAME_HEADER_LEN:
            raise StopIteration

        # Confirm the frame has an appropriate length.
        self._validate_frame_length(length)

        # Try to parse the frame body
        try:
            f.parse_body(memoryview(self._data[9:9+length]))
        except InvalidDataError as err:
            msg = "Received frame with non-compliant data"
            raise ProtocolError(msg) from err
        except InvalidFrameError as err:
            msg = "Frame data missing or invalid"
            raise FrameDataMissingError(msg) from err

        # At this point, as we know we'll use or discard the entire frame, we
        # can update the data.
        # Deleting the consumed prefix mutates the bytearray in place instead
        # of copying the remaining bytes into a new object, as slicing would.
        # ``del s[i:j]`` is documented for mutable sequences in
        # https://docs.python.org/3/library/stdtypes.html#mutable-sequence-types
        # and CPython's bytearray tracks an internal offset (``ob_start`` in
        # Objects/bytearrayobject.c) that makes repeated deletes from the
        # front amortized O(1) per byte rather than O(len) per frame.
        del self._data[:9+length]
        self._consumed_bytes += FRAME_HEADER_LEN + length
        self._frames_parsed += 1

        # Pass the frame through the header buffer.
        new_frame = self._update_header_buffer(f)

        # If we got a frame we didn't understand or shouldn't yield, rather
        # than return None it'd be better if we just tried to get the next
        # frame in the sequence instead. Recurse back into ourselves to do
        # that. This is safe because the amount of work we have to do here is
        # strictly bounded by the length of the buffer.
        return new_frame if new_frame is not None else self.__next__()
