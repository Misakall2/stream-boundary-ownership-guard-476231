"""
h2/pool
~~~~~~~

A conservative connection reuse pool for HTTP/2 connections.

HTTP/2 multiplexing makes connection reuse attractive, but reuse is only safe
when the previous exchange *finished in both directions*. A connection whose
response body was only partially consumed still holds bytes nobody has read;
handing it to the next requester splices those bytes in front of the next
request's data. That is the "cross-talk" failure: concurrent requests receive
somebody else's body.

This module makes the safety condition explicit and observable:

- :meth:`ConnectionPool.checkin` only returns a connection to the pool when
  both the send and receive directions are closed, the body boundaries are
  exact, and the connection's buffer is empty. Everything else retires the
  connection with a reason code instead.
- :meth:`ConnectionPool.checkout` re-checks a connection before handing it
  out, so a connection mutated while idle is not resurrected.
- Every decision is recorded, so when cross-talk still happens with reuse
  enabled an operator can tell which layer let it through:
  ``version-lock`` (a library/protocol version mismatch), ``timeout-config``
  (a stale liveness judgement) or ``buffer-cleanup`` (bytes left behind).

.. versionadded:: 4.5.0
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from .boundary import BoundaryEventLog, ReceiveStage
from .exceptions import BoundaryViolationError

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable


class ReuseDenial(StrEnum):
    """
    The stable reasons a connection may be refused reuse.

    These are the three layers a gateway operator needs to distinguish when
    cross-talk appears with reuse turned on: a library version mismatch, a
    stale timeout configuration, or a buffer that was not cleaned up. Two
    more describe the exchange itself: a direction that never finished, and a
    body length that did not match its declaration.
    """

    #: The connection's view of the protocol version does not match the
    #: pool's, so the endpoint may not speak the same dialect.
    VERSION_LOCK = "version-lock"
    #: The connection's timeout configuration is stale or inconsistent, so
    #: its notion of "finished" cannot be trusted.
    TIMEOUT_CONFIG = "timeout-config"
    #: The connection still holds bytes that were not consumed.
    BUFFER_CLEANUP = "buffer-cleanup"
    #: The send or receive direction was not closed for the last exchange.
    DIRECTION_OPEN = "direction-open"
    #: The last body's confirmed length did not match its declared length.
    BODY_BOUNDARY = "body-boundary"
    #: The connection was explicitly retired (protocol error, truncation...).
    RETIRED = "retired"
    #: The connection is already checked out to another owner.
    BUSY = "busy"


class PoolState(StrEnum):
    """
    The pool's view of a connection's lifecycle.
    """

    IDLE = "idle"
    CHECKED_OUT = "checked-out"
    RETIRED = "retired"


class PoolEventKind(StrEnum):
    """
    The kinds of decision recorded by :attr:`ConnectionPool.events`.
    """

    CHECKOUT = "checkout"
    CHECKIN = "checkin"
    RETIRE = "retire"
    DENIED = "denied"


@dataclass(frozen=True, slots=True)
class PoolEvent:
    """
    One replayable pool decision.

    ``reason`` is filled in for denials and retirements so that a diagnostic
    report can name the layer that failed rather than only the outcome.
    """

    seq: int
    timestamp: float
    kind: PoolEventKind
    connection_id: str
    reason: ReuseDenial | None = None
    detail: str = ""

    def as_dict(self) -> dict[str, object]:
        """
        Return a JSON-serializable representation.
        """
        return {
            "seq": self.seq,
            "timestamp": self.timestamp,
            "kind": str(self.kind),
            "connection_id": self.connection_id,
            "reason": None if self.reason is None else str(self.reason),
            "detail": self.detail,
        }


class ReusableConnection:
    """
    The structural interface a connection must satisfy to be pooled.

    :class:`H2Connection <h2.connection.H2Connection>` satisfies it without
    changes; tests use the same interface with small fakes.
    """

    config: object

    @property
    def connection_id(self) -> str | None:
        raise NotImplementedError

    @property
    def retired(self) -> bool:
        raise NotImplementedError

    @property
    def retire_reason(self) -> str | None:
        raise NotImplementedError

    @property
    def boundary_log(self) -> BoundaryEventLog | None:
        raise NotImplementedError

    def retire(self, reason: str) -> None:
        raise NotImplementedError

    def upstream_remainder(self) -> object:
        raise NotImplementedError


class ConnectionPool:
    """
    A pool that refuses to reuse a connection that is not provably finished.

    A connection is handed back to the pool only when *all* of the following
    hold:

    - it is not retired;
    - neither its send side nor its receive side is still open;
    - every stream's declared body length was exactly confirmed;
    - its receive buffer holds no unconsumed bytes;
    - its protocol version and timeout configuration still match the pool's.

    Anything else retires the connection with a :class:`ReuseDenial` reason,
    which is recorded as a :class:`PoolEvent`.

    .. versionadded:: 4.5.0
    """

    def __init__(self,
                 *,
                 protocol_version: str = "h2/4",
                 timeout: float | None = None) -> None:
        self.protocol_version = protocol_version
        self.timeout = timeout
        self._entries: dict[str, ReusableConnection] = {}
        self._states: dict[str, PoolState] = {}
        self._retired: dict[str, str] = {}
        self._events: list[PoolEvent] = []

    def __len__(self) -> int:
        return sum(
            1 for state in self._states.values()
            if state is PoolState.IDLE
        )

    @property
    def events(self) -> tuple[PoolEvent, ...]:
        """
        Every decision this pool has made, oldest first.
        """
        return tuple(self._events)

    def state_of(self, connection_id: str) -> PoolState | None:
        """
        Return the pool's state for ``connection_id``, if it knows it.
        """
        return self._states.get(connection_id)

    def retire(self,
               connection_id: str,
               reason: ReuseDenial | str,
               *,
               connection: ReusableConnection | None = None) -> None:
        """
        Retire a pooled connection immediately.

        The connection's own ``retire`` hook is called (when it has one) and
        the entry is removed from the pool, so a later ``checkout`` cannot
        hand it back out. The first retirement reason wins: an earlier
        protocol error is not overwritten by a later cleanup call.

        :param connection_id: The connection to retire.
        :param reason: A :class:`ReuseDenial` or a free-form reason code.
        :param connection: The connection object, when it is not (or no
            longer) held in the pool. Supplying it ensures its ``retire``
            hook runs even for a connection that was refused on check-in.
        """
        entry = self._entries.pop(connection_id, None)
        if connection is None and entry is not None:
            connection = entry
        self._states[connection_id] = PoolState.RETIRED
        self._retired.setdefault(connection_id, str(reason))
        if connection is not None and connection.retire_reason is None:
            connection.retire(str(reason))
        self._record(
            PoolEventKind.RETIRE,
            connection_id,
            reason=self._as_denial(reason),
            detail=str(reason),
        )

    def checkin(self,
                connection: ReusableConnection,
                *,
                connection_id: str | None = None) -> bool:
        """
        Offer a connection back to the pool.

        Returns ``True`` when the connection was accepted for reuse and
        ``False`` when it was retired instead. A refusal is never silent: the
        reason is recorded as a :class:`PoolEvent` and stored on the pool and
        on the connection.

        :param connection: The connection being returned.
        :param connection_id: The identifier to use for it. Defaults to the
            connection's own ``connection_id``.
        """
        cid = self._id_for(connection, connection_id)
        self._states.setdefault(cid, PoolState.CHECKED_OUT)
        self._retired.setdefault(cid, connection.retire_reason or "")
        denial = self._reuse_denial(connection)
        if denial is not None:
            self._record(
                PoolEventKind.DENIED,
                cid,
                reason=denial,
                detail=self._denial_detail(connection, denial),
            )
            self.retire(cid, denial, connection=connection)
            return False

        self._entries[cid] = connection
        self._states[cid] = PoolState.IDLE
        self._record(PoolEventKind.CHECKIN, cid)
        return True

    def checkout(self, connection_id: str | None = None) -> ReusableConnection | None:
        """
        Take a finished connection out of the pool.

        Returns ``None`` when no connection is available. The returned
        connection is never one that failed the reuse checks in
        :meth:`checkin`: those were retired on the way in. A connection
        mutated while idle is re-checked here and retired if it no longer
        qualifies.

        :param connection_id: A specific connection to take, or ``None`` for
            the oldest idle one.
        """
        cid = self._pick_idle(connection_id)
        if cid is None:
            return None
        entry = self._entries[cid]
        denial = self._reuse_denial(entry)
        if denial is not None:
            self._record(
                PoolEventKind.DENIED,
                cid,
                reason=denial,
                detail=self._denial_detail(entry, denial),
            )
            self.retire(cid, denial, connection=entry)
            return None
        self._states[cid] = PoolState.CHECKED_OUT
        self._entries.pop(cid)
        self._record(PoolEventKind.CHECKOUT, cid)
        return entry

    def retire_remaining(self, reason: str = "pool-closed") -> None:
        """
        Retire every pooled connection.

        Useful as an end-of-run cleanup so that a diagnostic report shows
        exactly which connections were thrown away and why.

        :param reason: The reason code to record for each connection.
        """
        for cid in list(self._entries):
            self.retire(cid, reason)

    def _id_for(self,
                connection: ReusableConnection,
                connection_id: str | None) -> str:
        if connection_id is not None:
            return connection_id
        if connection.connection_id is not None:
            return connection.connection_id
        return f"conn-{id(connection):x}"

    def _pick_idle(self, requested: str | None) -> str | None:
        if requested is not None:
            if self._states.get(requested) is PoolState.IDLE:
                return requested
            return None
        for cid, state in self._states.items():
            if state is PoolState.IDLE:
                return cid
        return None

    def _reuse_denial(self, connection: ReusableConnection) -> ReuseDenial | None:
        checks = (
            (ReuseDenial.RETIRED, self._is_retired(connection)),
            (ReuseDenial.VERSION_LOCK, self._version_mismatch(connection)),
            (ReuseDenial.TIMEOUT_CONFIG, self._timeout_mismatch(connection)),
            (ReuseDenial.DIRECTION_OPEN, not self._directions_finished(connection)),
            (ReuseDenial.BODY_BOUNDARY, not self._body_boundaries_exact(connection)),
            (ReuseDenial.BUFFER_CLEANUP, not self._buffer_clean(connection)),
        )
        for denial, failed in checks:
            if failed:
                return denial
        return None

    def _is_retired(self, connection: ReusableConnection) -> bool:
        return bool(connection.retired)

    def _version_mismatch(self, connection: ReusableConnection) -> bool:
        version = getattr(connection, "protocol_version", None)
        return version is not None and version != self.protocol_version

    def _timeout_mismatch(self, connection: ReusableConnection) -> bool:
        timeout = getattr(connection, "timeout", None)
        return timeout is not None and self.timeout != timeout

    def _directions_finished(self, connection: ReusableConnection) -> bool:
        check = getattr(connection, "exchange_finished", None)
        if check is None:
            return True
        return bool(check())

    def _body_boundaries_exact(self, connection: ReusableConnection) -> bool:
        log = connection.boundary_log
        if log is None:
            check = getattr(connection, "body_boundaries_exact", None)
            if check is None:
                return True
            return bool(check())
        for boundary in log.boundaries().values():
            if boundary.declared is None:
                continue
            if boundary.confirmed != boundary.declared:
                return False
            if boundary.discarded:
                return False
        return True

    def _buffer_clean(self, connection: ReusableConnection) -> bool:
        remainder = connection.upstream_remainder()
        remaining = getattr(remainder, "remaining_bytes", 0)
        return remaining == 0

    def _denial_detail(self, connection: ReusableConnection, denial: ReuseDenial) -> str:
        if denial is ReuseDenial.BUFFER_CLEANUP:
            remainder = connection.upstream_remainder()
            return (
                f"{getattr(remainder, 'remaining_bytes', '?')} unconsumed "
                f"byte(s) in stage {getattr(remainder, 'stage', '?')}"
            )
        if denial is ReuseDenial.RETIRED:
            return connection.retire_reason or "retired"
        return ""

    def _as_denial(self, reason: ReuseDenial | str) -> ReuseDenial | None:
        if isinstance(reason, ReuseDenial):
            return reason
        try:
            return ReuseDenial(reason)
        except ValueError:
            return None

    def _record(self,
                kind: PoolEventKind,
                connection_id: str,
                *,
                reason: ReuseDenial | None = None,
                detail: str = "") -> PoolEvent:
        event = PoolEvent(
            seq=len(self._events),
            timestamp=time.time(),
            kind=kind,
            connection_id=connection_id,
            reason=reason,
            detail=detail,
        )
        self._events.append(event)
        return event


def _reuse_violation(message: str,
                     *,
                     connection: ReusableConnection,
                     kind: ReuseDenial,
                     stream_id: int | None) -> BoundaryViolationError:
    return BoundaryViolationError(
        message,
        stage="reuse",
        kind=kind.value,
        connection_id=connection.connection_id,
        stream_id=stream_id,
    )


def assert_reusable(connection: ReusableConnection, *,
                    stream_id: int | None = None) -> None:
    """
    Assert that ``connection`` is safe to reuse, raising
    :class:`BoundaryViolationError <h2.exceptions.BoundaryViolationError>` with
    the failing layer otherwise.

    This is the single check a gateway should run before returning a
    connection to its pool, independent of :class:`ConnectionPool`'s own
    bookkeeping.

    :param connection: The connection to check.
    :param stream_id: Optional stream ID to include in the error.
    """
    if connection.retired:
        msg = f"connection is retired: {connection.retire_reason}"
        raise _reuse_violation(
            msg,
            connection=connection,
            kind=ReuseDenial.RETIRED,
            stream_id=stream_id,
        )

    log = connection.boundary_log
    if log is not None:
        for boundary in log.boundaries().values():
            if boundary.declared is None:
                continue
            if boundary.confirmed != boundary.declared:
                msg = (
                    f"body boundary for stream {boundary.stream_id} is "
                    f"{boundary.confirmed}/{boundary.declared} bytes"
                )
                raise _reuse_violation(
                    msg,
                    connection=connection,
                    kind=ReuseDenial.BODY_BOUNDARY,
                    stream_id=stream_id,
                )
            if boundary.discarded:
                msg = (
                    f"{boundary.discarded} byte(s) were discarded on stream "
                    f"{boundary.stream_id}"
                )
                raise _reuse_violation(
                    msg,
                    connection=connection,
                    kind=ReuseDenial.BODY_BOUNDARY,
                    stream_id=stream_id,
                )

    remainder = connection.upstream_remainder()
    remaining = getattr(remainder, "remaining_bytes", 0)
    if remaining:
        msg = (
            f"{remaining} unconsumed byte(s) remain in the receive buffer"
        )
        raise _reuse_violation(
            msg,
            connection=connection,
            kind=ReuseDenial.BUFFER_CLEANUP,
            stream_id=stream_id,
        )


def iter_pool_events(events: Iterable[PoolEvent]) -> Iterable[dict[str, object]]:
    """
    Yield the serialized form of each pool event.

    :param events: The pool events to serialize.
    """
    for event in events:
        yield event.as_dict()


def record_ownership_handoff(connection: ReusableConnection,
                             previous_owner: str,
                             new_owner: str) -> None:
    """
    Record that a connection's buffer changed hands.

    A hand-off is only recorded: the caller is responsible for having checked
    :func:`assert_reusable` first. Recording it means a later report can show
    who owned the buffer at every point in the exchange.

    :param connection: The connection whose buffer is changing hands.
    :param previous_owner: The owner relinquishing the buffer.
    :param new_owner: The owner receiving the buffer.
    """
    log = connection.boundary_log
    if log is None:
        return
    remainder = connection.upstream_remainder()
    remaining = getattr(remainder, "remaining_bytes", 0)
    log.record(
        ReceiveStage.OWNERSHIP,
        buffer_before=remaining,
        buffer_after=remaining,
        detail=f"{previous_owner}->{new_owner}",
    )
