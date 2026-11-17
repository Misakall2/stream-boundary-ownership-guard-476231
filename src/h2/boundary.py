"""
h2/boundary
~~~~~~~~~~~

Boundary and buffer-ownership accounting for HTTP/2 receive paths.

This module supports gateway-style deployments that splice one HTTP/2
connection's traffic into another. It keeps three kinds of information
strictly separate, because conflating them is what allows one request's bytes
to leak into the next connection:

- :class:`BodyBoundary` describes the *declared* length of one request body
  (from ``content-length``) and how many bytes have been *confirmed* on that
  stream. It never contains information about unrelated buffered bytes.
- :class:`UpstreamRemainder` describes bytes that are still physically
  buffered on a connection and have not been handed to anyone yet. It never
  contains information about a declared body length.
- :class:`BoundaryEventLog` is an append-only, replayable record of every
  receive step: bytes read, confirmed length, discarded bytes, buffer sizes
  before and after each step, the consume offset, and the connection that
  owns the buffer at that moment. When something goes wrong the failure stage
  is recorded where it happened instead of being inferred from the final
  exception.

The types here are deliberately independent of :class:`H2Connection
<h2.connection.H2Connection>`: a connection only *feeds* the log through
:meth:`H2Connection.enable_boundary_diagnostics
<h2.connection.H2Connection.enable_boundary_diagnostics>`, and the pool and
the replay harness in :mod:`h2.diagnostics` consume it.

.. versionadded:: 4.5.0
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .exceptions import (
    BoundaryViolationError,
    H2Error,
    InvalidBodyLengthError,
    ProtocolError,
    TruncatedFrameError,
)

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable, Iterator


class ReceiveStage(StrEnum):
    """
    The stable names of the receive steps a connection passes through. Every
    :class:`BoundaryEvent` records the stage it belongs to, so a failure can
    be attributed to where it happened rather than guessed from the exception
    that was finally raised.
    """

    #: Bytes were handed to the connection but not yet attributed to a frame.
    BUFFER_APPEND = "buffer-append"
    #: A complete frame header was parsed from the buffer.
    FRAME_HEADER = "frame-header"
    #: A frame body was received (fully or, at a fragment boundary, partly).
    FRAME_BODY = "frame-body"
    #: A HEADERS/PUSH_PROMISE block is waiting for CONTINUATION frames.
    HEADER_BLOCK = "header-block"
    #: A frame was dispatched to the connection's handler.
    FRAME_DISPATCH = "frame-dispatch"
    #: Bytes were attributed to a stream's request/response body.
    BODY = "body"
    #: The receive step failed.
    FAILURE = "failure"
    #: The connection changed hands (returned to a pool, leased, retired).
    OWNERSHIP = "ownership"


class FailureClass(StrEnum):
    """
    The two failure categories a gateway must never conflate.

    Timeouts are a local/peer liveness judgement and are usually retryable;
    protocol errors mean the peer broke the framing rules and the connection
    can never be trusted again. Keeping them apart is what lets a pool retire
    a connection on the right signal instead of treating a slow read as a
    corrupt stream (or vice versa).
    """

    TIMEOUT = "timeout"
    PROTOCOL_ERROR = "protocol-error"
    BOUNDARY_ERROR = "boundary-error"
    INTERNAL_ERROR = "internal-error"


@dataclass(frozen=True, slots=True)
class BodyBoundary:
    """
    The declared length boundary of one stream's body, kept separate from any
    leftover bytes elsewhere on the connection.

    ``declared`` is the length the peer promised (``None`` when no
    ``content-length`` was sent, which makes the body unbounded but still
    trackable). ``confirmed`` counts only bytes that have been attributed to
    *this* stream and accepted as body data. ``discarded`` counts bytes that
    arrived for this stream after its boundary either overflowed or was
    closed: those are recorded rather than silently mixed back into a buffer.

    .. versionadded:: 4.5.0
    """

    stream_id: int
    declared: int | None
    confirmed: int = 0
    discarded: int = 0

    @property
    def remaining(self) -> int | None:
        """
        Declared bytes still unaccounted for, or ``None`` for an unbounded
        body. Negative values mean the boundary has been overrun.
        """
        if self.declared is None:
            return None
        return self.declared - self.confirmed

    @property
    def complete(self) -> bool:
        """
        Whether the declared length has been exactly confirmed.
        """
        return self.declared is not None and self.confirmed == self.declared

    @property
    def overrun(self) -> bool:
        """
        Whether more bytes were confirmed than the peer declared.
        """
        return self.declared is not None and self.confirmed > self.declared

    def confirm(self, length: int) -> BodyBoundary:
        """
        Return a new boundary with ``length`` more bytes confirmed.
        """
        return BodyBoundary(
            stream_id=self.stream_id,
            declared=self.declared,
            confirmed=self.confirmed + length,
            discarded=self.discarded,
        )

    def discard(self, length: int) -> BodyBoundary:
        """
        Return a new boundary with ``length`` more bytes discarded.
        """
        return BodyBoundary(
            stream_id=self.stream_id,
            declared=self.declared,
            confirmed=self.confirmed,
            discarded=self.discarded + length,
        )


@dataclass(frozen=True, slots=True)
class UpstreamRemainder:
    """
    Bytes still physically buffered on one connection that nobody has
    consumed yet.

    This is the counterweight to :class:`BodyBoundary`: it answers "what is
    sitting in the buffer?" and never "how long is the body supposed to be?".
    A gateway that decides a connection is safe to reuse while
    ``remaining_bytes`` is non-zero is exactly how an unfinished upstream
    response ends up as the first bytes of the next request.

    .. versionadded:: 4.5.0
    """

    connection_id: str
    remaining_bytes: int
    #: The receive stage the leftover bytes belong to.
    stage: str
    #: The stream the leftover bytes belong to, when known.
    stream_id: int | None = None

    @property
    def clean(self) -> bool:
        """
        Whether the connection has no unconsumed bytes at all.
        """
        return self.remaining_bytes == 0


@dataclass(frozen=True, slots=True)
class BoundaryEvent:
    """
    One replayable receive/ownership step.

    The fields deliberately overlap so that a report can reconstruct the
    whole story from the table alone: ``bytes_read`` and ``confirmed_length``
    answer "how much arrived and how much was accepted?", while
    ``buffer_before``/``buffer_after`` and ``consumed_offset`` answer "where
    did it go?". ``discarded_bytes`` catches data that was neither delivered
    nor left in the buffer, so it cannot vanish silently.

    .. versionadded:: 4.5.0
    """

    #: Monotonic sequence number within the log that produced this event.
    seq: int
    #: Wall-clock capture time (Unix seconds).
    timestamp: float
    #: The connection that owned the buffer for this step.
    connection_id: str
    stage: ReceiveStage
    #: The stream this step relates to, if any.
    stream_id: int | None = None
    #: Bytes that arrived in this step.
    bytes_read: int = 0
    #: Declared body length known for the stream at this point, if any.
    declared_length: int | None = None
    #: Body bytes confirmed for the stream in this step.
    confirmed_length: int = 0
    #: Bytes dropped in this step (never delivered, never buffered).
    discarded_bytes: int = 0
    #: Buffer occupancy before this step.
    buffer_before: int = 0
    #: Buffer occupancy after this step.
    buffer_after: int = 0
    #: Absolute consume offset of this step within the stream.
    consumed_offset: int = 0
    #: Outstanding declared length after this step, if any.
    remaining_length: int | None = None
    #: Failure classification, when this step recorded a failure.
    failure: FailureClass | None = None
    #: A short human-readable note (never contains body bytes or tokens).
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        """
        Return a JSON-serializable representation of this event.
        """
        return {
            "seq": self.seq,
            "timestamp": self.timestamp,
            "connection_id": self.connection_id,
            "stage": str(self.stage),
            "stream_id": self.stream_id,
            "bytes_read": self.bytes_read,
            "declared_length": self.declared_length,
            "confirmed_length": self.confirmed_length,
            "discarded_bytes": self.discarded_bytes,
            "buffer_before": self.buffer_before,
            "buffer_after": self.buffer_after,
            "consumed_offset": self.consumed_offset,
            "remaining_length": self.remaining_length,
            "failure": None if self.failure is None else str(self.failure),
            "detail": self.detail,
        }


class BoundaryEventLog:
    """
    An append-only table of :class:`BoundaryEvent` records for one connection.

    Every record is written where the corresponding step happens, so the
    stage is part of the data rather than something a reader has to infer
    from the final exception. The log also keeps the two accounting halves
    apart: per-stream :class:`BodyBoundary` values and the connection's
    :class:`UpstreamRemainder`.

    .. versionadded:: 4.5.0
    """

    def __init__(self, connection_id: str = "unknown") -> None:
        self.connection_id = connection_id
        self._events: list[BoundaryEvent] = []
        self._boundaries: dict[int, BodyBoundary] = {}
        self._consumed_offset = 0
        self._frames = 0
        self._next_seq = 0

    def __len__(self) -> int:
        return len(self._events)

    @property
    def events(self) -> tuple[BoundaryEvent, ...]:
        """
        A read-only view of all recorded events, oldest first.
        """
        return tuple(self._events)

    def boundary(self, stream_id: int) -> BodyBoundary | None:
        """
        Return the recorded body boundary for ``stream_id``, if one exists.
        """
        return self._boundaries.get(stream_id)

    def boundaries(self) -> dict[int, BodyBoundary]:
        """
        Return a copy of every recorded body boundary, keyed by stream ID.
        """
        return dict(self._boundaries)

    @property
    def consumed_offset(self) -> int:
        """
        The absolute number of bytes this log has accounted for.
        """
        return self._consumed_offset

    @property
    def frames(self) -> int:
        """
        The number of frames accounted for so far.
        """
        return self._frames

    def record(self,
               stage: ReceiveStage,
               *,
               stream_id: int | None = None,
               bytes_read: int = 0,
               confirmed_length: int = 0,
               discarded_bytes: int = 0,
               buffer_before: int = 0,
               buffer_after: int = 0,
               declared_length: int | None = None,
               failure: FailureClass | None = None,
               detail: str = "") -> BoundaryEvent:
        """
        Append one event and return it.

        ``consumed_offset`` and ``remaining_length`` are derived from the
        log's own running counters plus the recorded boundary, so callers
        cannot accidentally write an inconsistent offset or promise more
        bytes than were declared. ``remaining_length`` is measured *after*
        this event's confirmed bytes are applied, so it is the number of
        declared bytes the stream still needed when the row was written.
        """
        if stream_id is not None and (
            declared_length is not None
            or confirmed_length
            or discarded_bytes
        ):
            # A boundary is tracked for every stream that actually carries
            # body bytes, even when the peer declared no length: ``declared``
            # is then ``None`` and exactness checks skip it, but the confirmed
            # and discarded byte counts are still recorded. Frames that carry
            # no body accounting (headers, window updates) do not create a
            # boundary. Using exactly the same rule when reconstructing from a
            # serialized log keeps the live and replayed views identical.
            self._ensure_boundary(stream_id, declared_length)

        boundary = (
            self._boundaries.get(stream_id)
            if stream_id is not None else None
        )
        if boundary is not None and stream_id is not None:
            if confirmed_length:
                boundary = boundary.confirm(confirmed_length)
            if discarded_bytes:
                boundary = boundary.discard(discarded_bytes)
            self._boundaries[stream_id] = boundary

        # The consume offset tracks input bytes, so it advances only when
        # bytes actually *enter* the connection (the append step). A later
        # body step confirms some of those same bytes and must not count them
        # a second time: ``consumed_offset`` is therefore directly comparable
        # with an offset into the capture that was replayed.
        if stage is ReceiveStage.BUFFER_APPEND:
            self._consumed_offset += bytes_read
        if stage == ReceiveStage.FRAME_DISPATCH:
            self._frames += 1

        event = BoundaryEvent(
            seq=self._next_seq,
            timestamp=time.time(),
            connection_id=self.connection_id,
            stage=stage,
            stream_id=stream_id,
            bytes_read=bytes_read,
            declared_length=(
                boundary.declared if boundary is not None else declared_length
            ),
            confirmed_length=confirmed_length,
            discarded_bytes=discarded_bytes,
            buffer_before=buffer_before,
            buffer_after=buffer_after,
            consumed_offset=self._consumed_offset,
            remaining_length=(
                boundary.remaining if boundary is not None else None
            ),
            failure=failure,
            detail=detail,
        )
        self._next_seq += 1
        self._events.append(event)
        return event

    def declare_boundary(self, stream_id: int, declared: int | None) -> BodyBoundary:
        """
        Register (or replace) the declared body length for ``stream_id``.

        A boundary that was already recorded keeps any confirmed/discarded
        counts, so re-declaring (for example on trailers) never erases data
        that has already been accounted for.
        """
        existing = self._boundaries.get(stream_id)
        if existing is None:
            boundary = BodyBoundary(stream_id=stream_id, declared=declared)
        else:
            boundary = BodyBoundary(
                stream_id=stream_id,
                declared=declared,
                confirmed=existing.confirmed,
                discarded=existing.discarded,
            )
        self._boundaries[stream_id] = boundary
        return boundary

    def _ensure_boundary(self, stream_id: int, declared: int | None) -> BodyBoundary:
        existing = self._boundaries.get(stream_id)
        if existing is not None:
            return existing
        return self.declare_boundary(stream_id, declared)

    def record_failure(self,
                       stage: ReceiveStage,
                       failure: FailureClass,
                       *,
                       stream_id: int | None = None,
                       detail: str = "") -> BoundaryEvent:
        """
        Record a failure at the stage where it happened.
        """
        return self.record(
            stage,
            stream_id=stream_id,
            failure=failure,
            detail=detail,
        )

    def remainder(self,
                  buffered_bytes: int,
                  stage: str,
                  stream_id: int | None = None) -> UpstreamRemainder:
        """
        Build an :class:`UpstreamRemainder` describing the connection's
        currently buffered bytes.
        """
        return UpstreamRemainder(
            connection_id=self.connection_id,
            remaining_bytes=buffered_bytes,
            stage=stage,
            stream_id=stream_id,
        )

    def to_dicts(self) -> list[dict[str, Any]]:
        """
        Return every event as a JSON-serializable dictionary.
        """
        return [event.as_dict() for event in self._events]

    def to_json(self) -> str:
        """
        Serialize the whole log to a JSON document.
        """
        return json.dumps(
            {
                "connection_id": self.connection_id,
                "consumed_offset": self._consumed_offset,
                "frames": self._frames,
                "boundaries": {
                    str(stream_id): {
                        "declared": boundary.declared,
                        "confirmed": boundary.confirmed,
                        "discarded": boundary.discarded,
                    }
                    for stream_id, boundary in self._boundaries.items()
                },
                "events": self.to_dicts(),
            },
            indent=2,
            sort_keys=True,
        )

    def write_json(self, path: str | Path) -> None:
        """
        Write the serialized log to ``path``.
        """
        Path(path).write_text(self.to_json(), encoding="utf-8")

    @classmethod
    def from_dicts(cls,
                   connection_id: str,
                   events: Iterable[dict[str, Any]]) -> BoundaryEventLog:
        """
        Rebuild a log from previously serialized event dictionaries.
        """
        log = cls(connection_id)
        for raw in events:
            log._append_raw(raw)
        return log

    def _append_raw(self, raw: dict[str, Any]) -> BoundaryEvent:
        event = BoundaryEvent(
            seq=int(raw["seq"]),
            timestamp=float(raw["timestamp"]),
            connection_id=str(raw["connection_id"]),
            stage=ReceiveStage(raw["stage"]),
            stream_id=raw.get("stream_id"),
            bytes_read=int(raw.get("bytes_read", 0)),
            declared_length=raw.get("declared_length"),
            confirmed_length=int(raw.get("confirmed_length", 0)),
            discarded_bytes=int(raw.get("discarded_bytes", 0)),
            buffer_before=int(raw.get("buffer_before", 0)),
            buffer_after=int(raw.get("buffer_after", 0)),
            consumed_offset=int(raw.get("consumed_offset", 0)),
            remaining_length=raw.get("remaining_length"),
            failure=(
                None if raw.get("failure") is None
                else FailureClass(raw["failure"])
            ),
            detail=str(raw.get("detail", "")),
        )
        stream_id = event.stream_id
        if stream_id is not None:
            self._ensure_boundary(stream_id, event.declared_length)
        if stream_id is not None and event.confirmed_length:
            self._boundaries[stream_id] = self._ensure_boundary(
                stream_id, event.declared_length,
            ).confirm(event.confirmed_length)
        if stream_id is not None and event.discarded_bytes:
            self._boundaries[stream_id] = self._boundaries[stream_id].discard(
                event.discarded_bytes,
            )
        self._consumed_offset = event.consumed_offset
        if event.stage == ReceiveStage.FRAME_DISPATCH:
            self._frames += 1
        self._next_seq = event.seq + 1
        self._events.append(event)
        return event

    def __iter__(self) -> Iterator[BoundaryEvent]:
        return iter(self._events)

    def __repr__(self) -> str:
        return (
            f"<BoundaryEventLog connection_id:{self.connection_id!r} "
            f"events:{len(self._events)} boundaries:{len(self._boundaries)}>"
        )


def classify_failure(exc: BaseException) -> FailureClass:
    """
    Classify an exception into a :class:`FailureClass`.

    This is the one place that decides whether a failure is a timeout, a
    protocol error, or an internal error. A ``TimeoutError`` (and the common
    socket timeout aliases, which are subclasses of it) is never folded in
    with protocol errors: a slow peer must not be mistaken for a corrupt one,
    and a corrupt peer must not be retried as though it were merely slow.
    """
    if isinstance(exc, TimeoutError):
        return FailureClass.TIMEOUT

    if isinstance(exc, (TruncatedFrameError, InvalidBodyLengthError,
                        BoundaryViolationError)):
        return FailureClass.BOUNDARY_ERROR
    if isinstance(exc, (ProtocolError, H2Error)):
        return FailureClass.PROTOCOL_ERROR
    return FailureClass.INTERNAL_ERROR
