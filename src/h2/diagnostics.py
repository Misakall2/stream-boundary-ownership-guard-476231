"""
h2/diagnostics
~~~~~~~~~~~~~~

Replay harness for HTTP/2 receive-path fragmentation.

A request that arrives as one ``read()`` and the same bytes arriving one at a
time must be indistinguishable after parsing. When they are not, the bug is
almost always a boundary check that accidentally depended on how much data
happened to be buffered. This module makes that class of bug reproducible:

- :func:`split_whole` feeds an entire capture in one call.
- :func:`split_bytewise` feeds it one byte at a time.
- :func:`split_random` feeds it in random fragments from a seeded RNG, so a
  failure is reproducible from the seed alone.

:func:`replay` runs a capture under a chosen schedule and returns a
:class:`ReplayResult` holding the delivered events, the connection's boundary
log, and where (if anywhere) anything went wrong. :func:`compare_schedules`
runs all three schedules over the same capture and reports the first receive
step at which their results diverge, which is what a report means by "the
inconsistency is in the receive step".

The harness never rewrites malformed input into a form that passes: every
schedule delivers exactly the captured bytes, only the boundaries between
calls differ.

.. versionadded:: 4.5.0
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from .boundary import classify_failure
from .events import DataReceived
from .exceptions import H2Error

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable, Iterator, Sequence

    from .boundary import BoundaryEventLog, FailureClass, ReceiveStage
    from .connection import H2Connection
    from .events import Event


class FeedSchedule(StrEnum):
    """
    The fragmentation strategies the replay harness supports.
    """

    #: The whole capture in a single ``receive_data`` call.
    WHOLE = "whole"
    #: One byte per ``receive_data`` call.
    BYTEWISE = "bytewise"
    #: Random-sized fragments from a seeded RNG.
    RANDOM = "random"


def split_whole(capture: bytes) -> list[bytes]:
    """
    Return the capture as a single fragment.

    :param capture: The raw bytes to split.
    """
    return [capture] if capture else []


def split_bytewise(capture: bytes) -> list[bytes]:
    """
    Return the capture as one-byte fragments.

    :param capture: The raw bytes to split.
    """
    return [capture[i:i + 1] for i in range(len(capture))]


def split_random(capture: bytes, *, seed: int = 0,
                 max_fragment: int = 16) -> list[bytes]:
    """
    Return the capture split into random fragments.

    The split is deterministic for a given ``seed``, so a failing run can be
    reproduced exactly.

    :param capture: The raw bytes to split.
    :param seed: The RNG seed.
    :param max_fragment: The largest fragment size to produce.
    """
    if not capture:
        return []
    if max_fragment < 1:
        msg = "max_fragment must be at least 1"
        raise ValueError(msg)
    # A seeded, non-cryptographic RNG: this only decides fragment sizes,
    # never any secret, and must be reproducible from the seed.
    rng = random.Random(seed)  # noqa: S311
    fragments = []
    position = 0
    while position < len(capture):
        size = rng.randint(1, max_fragment)
        fragments.append(capture[position:position + size])
        position += size
    return fragments


def split_capture(capture: bytes,
                  schedule: FeedSchedule | str,
                  *,
                  seed: int = 0,
                  max_fragment: int = 16) -> list[bytes]:
    """
    Split ``capture`` according to ``schedule``.

    :param capture: The raw bytes to split.
    :param schedule: A :class:`FeedSchedule` or its string value.
    :param seed: The RNG seed, used only by the ``random`` schedule.
    :param max_fragment: The largest fragment size, used only by the
        ``random`` schedule.
    """
    schedule = FeedSchedule(schedule)
    if schedule is FeedSchedule.WHOLE:
        return split_whole(capture)
    if schedule is FeedSchedule.BYTEWISE:
        return split_bytewise(capture)
    return split_random(capture, seed=seed, max_fragment=max_fragment)


@dataclass(frozen=True, slots=True)
class ReplayStep:
    """
    One ``receive_data`` call in a replay: the fragment that was fed, and the
    events it produced. ``stage`` records the buffer stage after the step so
    that a diverging step can be located in the receive pipeline.
    """

    index: int
    fragment_size: int
    event_count: int
    buffered_after: int
    stage: str

    def as_dict(self) -> dict[str, Any]:
        """
        Return a JSON-serializable representation.
        """
        return {
            "index": self.index,
            "fragment_size": self.fragment_size,
            "event_count": self.event_count,
            "buffered_after": self.buffered_after,
            "stage": self.stage,
        }


@dataclass
class ReplayResult:
    """
    The outcome of replaying one capture under one schedule.

    ``events`` is the concatenation of every event delivered across all
    calls. ``error`` is the exception that stopped the replay, if any, and
    ``failure`` its classification. ``boundary_log`` is the connection's
    per-step record, which is what makes a divergence explainable.
    """

    schedule: FeedSchedule
    events: list[Event] = field(default_factory=list)
    steps: list[ReplayStep] = field(default_factory=list)
    error: BaseException | None = None
    failure: FailureClass | None = None
    boundary_log: BoundaryEventLog | None = None

    @property
    def ok(self) -> bool:
        """
        Whether the replay completed without raising.
        """
        return self.error is None

    @property
    def delivered_bytes(self) -> int:
        """
        The number of DATA bytes delivered across all events.
        """
        return sum(
            len(event.data) for event in self.events
            if isinstance(event, DataReceived)
        )

    def event_envelope(self) -> list[tuple[str, int, bytes]]:
        """
        A schedule-independent signature of the delivered events.

        Only the facts that must not depend on fragmentation are included:
        the event type, the stream it belongs to, and the delivered bytes.
        """
        envelope = []
        for event in self.events:
            stream_id = getattr(event, "stream_id", 0) or 0
            payload = event.data if isinstance(event, DataReceived) else b""
            envelope.append((type(event).__name__, stream_id, payload))
        return envelope

    def failure_dict(self) -> dict[str, Any] | None:
        """
        Return a JSON-serializable description of the failure, if any.
        """
        if self.error is None:
            return None
        return {
            "type": type(self.error).__name__,
            "stage": getattr(self.error, "stage", None),
            "kind": getattr(self.error, "kind", None),
            "failure_class": None if self.failure is None else str(self.failure),
            "message": str(self.error),
        }


@dataclass(frozen=True, slots=True)
class Divergence:
    """
    The first receive step at which two replays disagreed.
    """

    step_index: int
    schedule_a: FeedSchedule
    schedule_b: FeedSchedule
    buffered_a: int
    buffered_b: int
    events_a: int
    events_b: int
    stage_a: str
    stage_b: str
    detail: str

    def as_dict(self) -> dict[str, Any]:
        """
        Return a JSON-serializable representation.
        """
        return {
            "step_index": self.step_index,
            "schedule_a": str(self.schedule_a),
            "schedule_b": str(self.schedule_b),
            "buffered_a": self.buffered_a,
            "buffered_b": self.buffered_b,
            "events_a": self.events_a,
            "events_b": self.events_b,
            "stage_a": self.stage_a,
            "stage_b": self.stage_b,
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class ComparisonResult:
    """
    The outcome of comparing every replay schedule for one capture.
    """

    results: tuple[ReplayResult, ...]
    divergences: tuple[Divergence, ...]

    @property
    def consistent(self) -> bool:
        """
        Whether every schedule delivered the same events without error.
        """
        return not self.divergences

    @property
    def failing_schedule(self) -> FeedSchedule | None:
        """
        The first schedule that raised, if any.
        """
        for result in self.results:
            if not result.ok:
                return result.schedule
        return None

    def as_dict(self) -> dict[str, Any]:
        """
        Return a JSON-serializable representation including per-schedule
        results and the located divergences.
        """
        return {
            "consistent": self.consistent,
            "failing_schedule": (
                None if self.failing_schedule is None
                else str(self.failing_schedule)
            ),
            "results": [
                {
                    "schedule": str(result.schedule),
                    "ok": result.ok,
                    "steps": len(result.steps),
                    "events": len(result.events),
                    "delivered_bytes": result.delivered_bytes,
                    "failure": result.failure_dict(),
                }
                for result in self.results
            ],
            "divergences": [d.as_dict() for d in self.divergences],
        }


def replay(capture: bytes,
           *,
           schedule: FeedSchedule | str,
           connection_factory: Callable[[], H2Connection],
           connection_id: str = "replay",
           seed: int = 0,
           max_fragment: int = 16,
           finish: bool = True) -> ReplayResult:
    """
    Feed ``capture`` to a fresh connection under ``schedule``.

    :param capture: The raw bytes to replay.
    :param schedule: The fragmentation schedule to use.
    :param connection_factory: A zero-argument callable returning a fresh,
        configured :class:`H2Connection <h2.connection.H2Connection>`.
    :param connection_id: The identifier recorded in boundary diagnostics.
    :param seed: The RNG seed for the ``random`` schedule.
    :param max_fragment: The maximum fragment size for the ``random``
        schedule.
    :param finish: Whether to call
        :meth:`end_of_input <h2.connection.H2Connection.end_of_input>` after
        the last fragment, so that a truncated capture is reported instead of
        silently accepted.
    """
    schedule = FeedSchedule(schedule)
    fragments = split_capture(
        capture, schedule, seed=seed, max_fragment=max_fragment,
    )
    connection = connection_factory()
    log = connection.enable_boundary_diagnostics(connection_id)
    result = ReplayResult(schedule=schedule, boundary_log=log)

    for index, fragment in enumerate(fragments):
        try:
            events = connection.receive_data(fragment)
        except (H2Error, TimeoutError) as exc:
            result.error = exc
            result.failure = classify_failure(exc)
            result.steps.append(
                ReplayStep(
                    index=index,
                    fragment_size=len(fragment),
                    event_count=0,
                    buffered_after=connection.incoming_buffer.buffered_bytes,
                    stage=connection.incoming_buffer.snapshot().stage,
                ),
            )
            return result

        snapshot = connection.incoming_buffer.snapshot()
        result.events.extend(events)
        result.steps.append(
            ReplayStep(
                index=index,
                fragment_size=len(fragment),
                event_count=len(events),
                buffered_after=snapshot.buffered_bytes,
                stage=snapshot.stage,
            ),
        )

    if finish:
        try:
            connection.end_of_input()
        except H2Error as exc:
            result.error = exc
            result.failure = classify_failure(exc)

    return result


def compare_schedules(capture: bytes,
                      *,
                      connection_factory: Callable[[], H2Connection],
                      connection_id: str = "replay",
                      seed: int = 0,
                      max_fragment: int = 16,
                      finish: bool = True,
                      schedules: Sequence[FeedSchedule] = (
                          FeedSchedule.WHOLE,
                          FeedSchedule.BYTEWISE,
                          FeedSchedule.RANDOM,
                      )) -> ComparisonResult:
    """
    Replay ``capture`` under every schedule and locate any divergence.

    A divergence is reported at the first replay step where two schedules
    disagree about either the events delivered or the buffer state. Because
    the schedules deliberately differ in fragment sizes, the comparison
    first checks the schedule-independent event envelope; only if those
    envelopes match does it walk the per-step records looking for a buffer
    state that disagrees in a way that matters (an error, or a non-empty
    buffer where another schedule drained).

    :param capture: The raw bytes to replay.
    :param connection_factory: A zero-argument callable returning a fresh
        connection for each schedule.
    :param connection_id: The identifier recorded in boundary diagnostics.
    :param seed: The RNG seed for the ``random`` schedule.
    :param max_fragment: The maximum fragment size for the ``random``
        schedule.
    :param finish: Whether to call ``end_of_input`` after the last fragment.
    :param schedules: The schedules to compare.
    """
    results = tuple(
        replay(
            capture,
            schedule=schedule,
            connection_factory=connection_factory,
            connection_id=f"{connection_id}:{schedule}",
            seed=seed,
            max_fragment=max_fragment,
            finish=finish,
        )
        for schedule in schedules
    )
    divergences = _find_divergences(results)
    return ComparisonResult(results=results, divergences=divergences)


def _find_divergences(results: Sequence[ReplayResult]) -> tuple[Divergence, ...]:
    """
    Compare replay results pairwise and return every divergence found.
    """
    divergences: list[Divergence] = []
    for i, first in enumerate(results):
        for second in results[i + 1:]:
            divergence = _divergence_between(first, second)
            if divergence is not None:
                divergences.append(divergence)
    return tuple(divergences)


def _divergence_between(a: ReplayResult, b: ReplayResult) -> Divergence | None:
    """
    Return the first divergence between two replay results, if any.
    """
    stage_a = a.steps[-1].stage if a.steps else "idle"
    stage_b = b.steps[-1].stage if b.steps else "idle"
    buffered_a = a.steps[-1].buffered_after if a.steps else 0
    buffered_b = b.steps[-1].buffered_after if b.steps else 0

    if a.ok != b.ok:
        detail = (
            f"{a.schedule} {'failed' if not a.ok else 'succeeded'} with "
            f"{_describe(a.error)}; {b.schedule} "
            f"{'failed' if not b.ok else 'succeeded'} with "
            f"{_describe(b.error)}"
        )
        return Divergence(
            step_index=min(len(a.steps), len(b.steps)),
            schedule_a=a.schedule,
            schedule_b=b.schedule,
            buffered_a=buffered_a,
            buffered_b=buffered_b,
            events_a=len(a.events),
            events_b=len(b.events),
            stage_a=stage_a,
            stage_b=stage_b,
            detail=detail,
        )

    if a.event_envelope() != b.event_envelope():
        return Divergence(
            step_index=min(len(a.steps), len(b.steps)),
            schedule_a=a.schedule,
            schedule_b=b.schedule,
            buffered_a=buffered_a,
            buffered_b=buffered_b,
            events_a=len(a.events),
            events_b=len(b.events),
            stage_a=stage_a,
            stage_b=stage_b,
            detail="delivered events differ",
        )

    if buffered_a != buffered_b:
        return Divergence(
            step_index=min(len(a.steps), len(b.steps)),
            schedule_a=a.schedule,
            schedule_b=b.schedule,
            buffered_a=buffered_a,
            buffered_b=buffered_b,
            events_a=len(a.events),
            events_b=len(b.events),
            stage_a=stage_a,
            stage_b=stage_b,
            detail="final buffer occupancy differs",
        )

    return None


def _describe(error: BaseException | None) -> str:
    if error is None:
        return "no error"
    return f"{type(error).__name__}: {error}"


@dataclass(frozen=True, slots=True)
class ReplayRecord:
    """
    A durable record of one replay, suitable for writing to disk and feeding
    back into :func:`replay_record`.

    It contains the capture bytes (base64 is deliberately not used: captures
    are small and JSON strings round-trip cleanly via ``latin-1``), the
    schedule, and the boundary log so that the same traffic can be re-run
    with a different fragmentation later.
    """

    capture_hex: str
    schedule: FeedSchedule
    seed: int
    max_fragment: int
    boundary_events: tuple[dict[str, Any], ...]

    @classmethod
    def from_result(cls, capture: bytes, result: ReplayResult,
                    *, seed: int = 0, max_fragment: int = 16) -> ReplayRecord:
        """
        Build a record from a completed replay.
        """
        events = (
            tuple(result.boundary_log.to_dicts())
            if result.boundary_log is not None else ()
        )
        return cls(
            capture_hex=capture.hex(),
            schedule=result.schedule,
            seed=seed,
            max_fragment=max_fragment,
            boundary_events=events,
        )

    @property
    def capture(self) -> bytes:
        """
        The raw bytes this record replays.
        """
        return bytes.fromhex(self.capture_hex)

    def as_dict(self) -> dict[str, Any]:
        """
        Return a JSON-serializable representation.
        """
        return {
            "capture_hex": self.capture_hex,
            "schedule": str(self.schedule),
            "seed": self.seed,
            "max_fragment": self.max_fragment,
            "boundary_events": list(self.boundary_events),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ReplayRecord:
        """
        Rebuild a record from its serialized form.
        """
        return cls(
            capture_hex=str(payload["capture_hex"]),
            schedule=FeedSchedule(payload["schedule"]),
            seed=int(payload.get("seed", 0)),
            max_fragment=int(payload.get("max_fragment", 16)),
            boundary_events=tuple(payload.get("boundary_events", ())),
        )


def replay_record(record: ReplayRecord,
                  *,
                  connection_factory: Callable[[], H2Connection],
                  schedule: FeedSchedule | str | None = None,
                  connection_id: str = "replay") -> ReplayResult:
    """
    Re-run a stored capture, optionally under a different schedule.

    :param record: The stored replay record.
    :param connection_factory: A zero-argument callable returning a fresh
        connection.
    :param schedule: The schedule to use; defaults to the one stored in the
        record.
    :param connection_id: The identifier recorded in boundary diagnostics.
    """
    return replay(
        record.capture,
        schedule=schedule or record.schedule,
        connection_factory=connection_factory,
        connection_id=connection_id,
        seed=record.seed,
        max_fragment=record.max_fragment,
    )


def iter_boundary_events(log: BoundaryEventLog) -> Iterator[dict[str, Any]]:
    """
    Yield each boundary event in ``log`` as a dictionary.

    :param log: The boundary log to serialize.
    """
    yield from log.to_dicts()


def failure_stage(result: ReplayResult) -> ReceiveStage | None:
    """
    Return the receive stage recorded for a replay failure, if any.

    The stage comes from the boundary log written at the moment of failure,
    not from the exception message, so it is available even when the
    exception itself carries no stage.
    """
    log = result.boundary_log
    if log is None:
        return None
    for event in reversed(log.events):
        if event.failure is not None:
            return event.stage
    return None
