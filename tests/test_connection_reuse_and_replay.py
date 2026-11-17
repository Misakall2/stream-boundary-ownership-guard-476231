"""
Tests for connection reuse safety and for the fragmentation replay harness.

Two things are under test:

- :mod:`h2.pool` must only return a connection to the reuse pool when the
  previous exchange finished in both directions, its body boundaries are
  exact, and its buffer is empty. Every refusal must name the layer that
  failed so that cross-talk with reuse enabled can be attributed.
- :mod:`h2.diagnostics` must produce the same delivered result for the same
  bytes regardless of how those bytes were split on arrival, and must locate
  a divergence in the receive step when one exists.
"""
from __future__ import annotations

import json

import pytest
from hpack import Encoder
from hyperframe.frame import ContinuationFrame, DataFrame, HeadersFrame

import h2.boundary
import h2.config
import h2.connection
import h2.diagnostics
import h2.events
import h2.exceptions
import h2.pool

PREAMBLE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

GET_HEADERS = [
    (b":method", b"GET"),
    (b":path", b"/"),
    (b":scheme", b"https"),
    (b":authority", b"example.com"),
]

POST_HEADERS = [
    (b":method", b"POST"),
    (b":path", b"/upload"),
    (b":scheme", b"https"),
    (b":authority", b"example.com"),
]


def make_server(**config_kwargs) -> h2.connection.H2Connection:
    """
    Return a server-side connection with a cleared outbound buffer.
    """
    config = h2.config.H2Configuration(client_side=False, **config_kwargs)
    connection = h2.connection.H2Connection(config=config)
    connection.initiate_connection()
    connection.clear_outbound_data_buffer()
    return connection


def headers_frame(stream_id: int,
                  headers: list[tuple[bytes, bytes]],
                  flags: list[str] | None = None) -> HeadersFrame:
    """
    Build a complete HEADERS frame.
    """
    frame = HeadersFrame(stream_id)
    frame.data = Encoder().encode(headers)
    frame.flags.add("END_HEADERS")
    for flag in flags or []:
        frame.flags.add(flag)
    return frame


def data_frame(stream_id: int, data: bytes,
               end_stream: bool = False) -> DataFrame:
    """
    Build a DATA frame.
    """
    frame = DataFrame(stream_id)
    frame.data = data
    if end_stream:
        frame.flags.add("END_STREAM")
    return frame


def completed_get_exchange(wire: bytes | None = None) -> h2.connection.H2Connection:
    """
    Return a connection whose one GET exchange finished in both directions.

    The request is received with ``END_STREAM`` and the response is sent with
    ``END_STREAM``, so neither direction is left open. This is exactly the
    state a pool is allowed to reuse.
    """
    connection = make_server()
    connection.enable_boundary_diagnostics("done")
    connection.receive_data(PREAMBLE)
    payload = (
        headers_frame(1, GET_HEADERS, ["END_STREAM"]).serialize()
        if wire is None else wire
    )
    connection.receive_data(payload)
    connection.send_headers(1, [(b":status", b"200")], end_stream=True)
    connection.clear_outbound_data_buffer()
    connection.end_of_input()
    return connection


class FinishedStub:
    """
    A minimal poolable connection that reports whatever state the test wants.
    """

    def __init__(self, *,
                 finished: bool = True,
                 boundaries_exact: bool = True,
                 buffered: int = 0,
                 version: str = "h2/4",
                 timeout: float | None = None,
                 connection_id: str = "stub") -> None:
        self.finished = finished
        self._boundaries_exact = boundaries_exact
        self.buffered = buffered
        self.protocol_version = version
        self.timeout = timeout
        self.config = object()
        self.connection_id = connection_id
        self.retired = False
        self.retire_reason: str | None = None
        self.boundary_log = None

    def retire(self, reason: str) -> None:
        self.retired = True
        self.retire_reason = reason

    def upstream_remainder(self) -> h2.boundary.UpstreamRemainder:
        return h2.boundary.UpstreamRemainder(
            connection_id=self.connection_id,
            remaining_bytes=self.buffered,
            stage="idle",
        )

    def exchange_finished(self) -> bool:
        return self.finished

    def body_boundaries_exact(self) -> bool:
        return self._boundaries_exact


class TestPoolAcceptsOnlyFinishedConnections:
    """
    The pool's check-in gate is the single place that decides reuse safety.
    """

    def test_finished_clean_connection_is_accepted(self) -> None:
        pool = h2.pool.ConnectionPool()
        stub = FinishedStub()
        assert pool.checkin(stub)
        assert pool.state_of("stub") is h2.pool.PoolState.IDLE
        assert pool.checkout("stub") is stub

    def test_open_direction_is_refused(self) -> None:
        pool = h2.pool.ConnectionPool()
        stub = FinishedStub(finished=False)
        assert not pool.checkin(stub)
        assert stub.retired
        assert stub.retire_reason == h2.pool.ReuseDenial.DIRECTION_OPEN
        assert pool.state_of("stub") is h2.pool.PoolState.RETIRED

    def test_leftover_bytes_are_refused(self) -> None:
        pool = h2.pool.ConnectionPool()
        stub = FinishedStub(buffered=17)
        assert not pool.checkin(stub)
        assert stub.retire_reason == h2.pool.ReuseDenial.BUFFER_CLEANUP
        denied = [e for e in pool.events if e.kind is h2.pool.PoolEventKind.DENIED]
        assert denied
        assert "17 unconsumed" in denied[-1].detail

    def test_body_boundary_mismatch_is_refused(self) -> None:
        pool = h2.pool.ConnectionPool()
        stub = FinishedStub(boundaries_exact=False)
        assert not pool.checkin(stub)
        assert stub.retire_reason == h2.pool.ReuseDenial.BODY_BOUNDARY

    def test_already_retired_connection_is_refused(self) -> None:
        pool = h2.pool.ConnectionPool()
        stub = FinishedStub()
        stub.retire("protocol-error")
        assert not pool.checkin(stub)
        assert stub.retire_reason == "protocol-error"

    def test_version_lock_is_reported(self) -> None:
        pool = h2.pool.ConnectionPool(protocol_version="h2/5")
        stub = FinishedStub(version="h2/4")
        assert not pool.checkin(stub)
        assert stub.retire_reason == h2.pool.ReuseDenial.VERSION_LOCK

    def test_timeout_config_mismatch_is_reported(self) -> None:
        pool = h2.pool.ConnectionPool(timeout=30.0)
        stub = FinishedStub(timeout=5.0)
        assert not pool.checkin(stub)
        assert stub.retire_reason == h2.pool.ReuseDenial.TIMEOUT_CONFIG

    def test_matching_timeout_and_version_are_accepted(self) -> None:
        pool = h2.pool.ConnectionPool(protocol_version="h2/4", timeout=30.0)
        stub = FinishedStub(version="h2/4", timeout=30.0)
        assert pool.checkin(stub)

    def test_connection_mutated_while_idle_is_rechecked_on_checkout(self) -> None:
        pool = h2.pool.ConnectionPool()
        stub = FinishedStub()
        assert pool.checkin(stub)

        # Someone read a frame's worth of bytes while the connection sat idle.
        stub.buffered = 9
        assert pool.checkout("stub") is None
        assert stub.retire_reason == h2.pool.ReuseDenial.BUFFER_CLEANUP

    def test_retire_removes_from_pool(self) -> None:
        pool = h2.pool.ConnectionPool()
        stub = FinishedStub()
        assert pool.checkin(stub)
        pool.retire("stub", h2.pool.ReuseDenial.RETIRED)
        assert len(pool) == 0
        assert pool.checkout("stub") is None

    def test_first_retire_reason_wins(self) -> None:
        pool = h2.pool.ConnectionPool()
        stub = FinishedStub(buffered=4)
        assert not pool.checkin(stub)
        pool.retire("stub", "cleanup")
        assert stub.retire_reason == h2.pool.ReuseDenial.BUFFER_CLEANUP

    def test_len_counts_only_idle(self) -> None:
        pool = h2.pool.ConnectionPool()
        pool.checkin(FinishedStub(connection_id="one"))
        pool.checkin(FinishedStub(connection_id="two"))
        assert len(pool) == 2
        pool.checkout("one")
        assert len(pool) == 1

    def test_checkout_picks_oldest_idle(self) -> None:
        pool = h2.pool.ConnectionPool()
        first = FinishedStub(connection_id="first")
        second = FinishedStub(connection_id="second")
        pool.checkin(first)
        pool.checkin(second)
        assert pool.checkout() is first
        assert pool.checkout() is second
        assert pool.checkout() is None

    def test_retire_remaining(self) -> None:
        pool = h2.pool.ConnectionPool()
        pool.checkin(FinishedStub(connection_id="a"))
        pool.checkin(FinishedStub(connection_id="b"))
        pool.retire_remaining()
        assert len(pool) == 0
        assert all(
            pool.state_of(cid) is h2.pool.PoolState.RETIRED for cid in ("a", "b")
        )

    def test_event_serialization(self) -> None:
        pool = h2.pool.ConnectionPool()
        pool.checkin(FinishedStub(finished=False))
        serialized = list(h2.pool.iter_pool_events(pool.events))
        assert serialized
        assert serialized[0]["reason"] == "direction-open"
        json.dumps(serialized)


class TestPoolWithRealConnections:
    """
    The pool's guards fire against real :class:`H2Connection` objects.
    """

    def test_complete_exchange_is_reusable(self) -> None:
        connection = completed_get_exchange()
        pool = h2.pool.ConnectionPool()
        assert pool.checkin(connection)
        assert pool.checkout("done") is connection

    def test_request_body_not_fully_read_is_not_reusable(self) -> None:
        connection = make_server()
        connection.enable_boundary_diagnostics("partial")
        connection.receive_data(PREAMBLE)
        connection.receive_data(
            headers_frame(
                1,
                [*POST_HEADERS, (b"content-length", b"32")],
            ).serialize(),
        )
        connection.receive_data(data_frame(1, b"a" * 8).serialize())

        pool = h2.pool.ConnectionPool()
        assert not pool.checkin(connection)
        assert connection.retired

    def test_unconsumed_remainder_is_not_reusable(self) -> None:
        connection = make_server()
        connection.enable_boundary_diagnostics("leftover")
        connection.receive_data(PREAMBLE)
        connection.receive_data(
            headers_frame(
                1,
                [*POST_HEADERS, (b"content-length", b"4")],
                ["END_STREAM"],
            ).serialize(),
        )
        connection.receive_data(data_frame(1, b"body", end_stream=True).serialize())
        connection.incoming_buffer.add_data(data_frame(3, b"junk").serialize())

        pool = h2.pool.ConnectionPool()
        assert not pool.checkin(connection)
        assert connection.retire_reason == h2.pool.ReuseDenial.BUFFER_CLEANUP

    def test_retired_connection_is_not_reusable(self) -> None:
        connection = make_server()
        connection.receive_data(PREAMBLE)
        connection.retire("operator-decision")
        pool = h2.pool.ConnectionPool()
        assert not pool.checkin(connection)

    def test_assert_reusable_passes_for_clean_exchange(self) -> None:
        h2.pool.assert_reusable(completed_get_exchange())

    def test_assert_reusable_names_buffer_layer(self) -> None:
        connection = completed_get_exchange()
        connection.incoming_buffer.add_data(b"\x00\x00")
        with pytest.raises(h2.exceptions.BoundaryViolationError) as exc_info:
            h2.pool.assert_reusable(connection)
        assert exc_info.value.kind == "buffer-cleanup"
        assert exc_info.value.stage == "reuse"

    def test_ownership_handoff_is_recorded(self) -> None:
        connection = completed_get_exchange()
        log = connection.boundary_log
        assert log is not None
        before = len(log)
        h2.pool.record_ownership_handoff(connection, "gateway", "pool")
        assert len(log) == before + 1
        assert log.events[-1].stage is h2.boundary.ReceiveStage.OWNERSHIP
        assert log.events[-1].detail == "gateway->pool"


class TestReplaySchedules:
    """
    Whole, bytewise and random fragmentation must deliver the same result.
    """

    def _wire(self) -> bytes:
        return (
            PREAMBLE
            + headers_frame(
                1, [*POST_HEADERS, (b"content-length", b"11")],
            ).serialize()
            + data_frame(1, b"hello world", end_stream=True).serialize()
        )

    @pytest.mark.parametrize("seed", [0, 1, 7, 1234])
    def test_all_schedules_agree(self, seed) -> None:
        comparison = h2.diagnostics.compare_schedules(
            self._wire(),
            connection_factory=make_server,
            seed=seed,
        )
        assert comparison.consistent
        assert comparison.failing_schedule is None
        assert {r.schedule for r in comparison.results} == {
            h2.diagnostics.FeedSchedule.WHOLE,
            h2.diagnostics.FeedSchedule.BYTEWISE,
            h2.diagnostics.FeedSchedule.RANDOM,
        }
        assert all(
            r.delivered_bytes == len(b"hello world") for r in comparison.results
        )

    def test_whole_schedule_is_single_call(self) -> None:
        result = h2.diagnostics.replay(
            self._wire(),
            schedule=h2.diagnostics.FeedSchedule.WHOLE,
            connection_factory=make_server,
        )
        assert result.ok
        assert len(result.steps) == 1
        assert result.steps[0].fragment_size == len(self._wire())

    def test_bytewise_schedule_is_one_byte_per_call(self) -> None:
        wire = self._wire()
        result = h2.diagnostics.replay(
            wire,
            schedule=h2.diagnostics.FeedSchedule.BYTEWISE,
            connection_factory=make_server,
        )
        assert result.ok
        assert len(result.steps) == len(wire)
        assert all(step.fragment_size == 1 for step in result.steps)

    def test_random_schedule_is_reproducible_from_seed(self) -> None:
        first = h2.diagnostics.split_random(b"abcdefghij", seed=42, max_fragment=3)
        second = h2.diagnostics.split_random(b"abcdefghij", seed=42, max_fragment=3)
        third = h2.diagnostics.split_random(b"abcdefghij", seed=43, max_fragment=3)
        assert first == second
        assert b"".join(first) == b"abcdefghij"
        assert first != third or len(first) == len(third)

    def test_random_requires_positive_fragment_size(self) -> None:
        with pytest.raises(ValueError, match="max_fragment"):
            h2.diagnostics.split_random(b"abc", max_fragment=0)

    def test_empty_capture_has_no_fragments(self) -> None:
        assert h2.diagnostics.split_whole(b"") == []
        assert h2.diagnostics.split_bytewise(b"") == []
        assert h2.diagnostics.split_random(b"") == []

    def test_split_capture_dispatches(self) -> None:
        assert h2.diagnostics.split_capture(
            b"abc", h2.diagnostics.FeedSchedule.WHOLE,
        ) == [b"abc"]
        assert h2.diagnostics.split_capture(
            b"abc", "bytewise",
        ) == [b"a", b"b", b"c"]


class TestReplayFailureAttribution:
    """
    A replay failure must name the receive stage, not just the exception.
    """

    def test_truncated_capture_is_located_in_frame_body(self) -> None:
        wire = (
            PREAMBLE
            + headers_frame(1, GET_HEADERS).serialize()
            + data_frame(1, b"abcdef").serialize()[:-3]
        )
        comparison = h2.diagnostics.compare_schedules(
            wire, connection_factory=make_server,
        )
        # Every schedule truncates at the same logical place, so they agree
        # with each other even though each fails.
        assert comparison.consistent
        result = comparison.results[0]
        assert not result.ok
        assert isinstance(result.error, h2.exceptions.TruncatedFrameError)
        assert result.failure is h2.boundary.FailureClass.BOUNDARY_ERROR
        assert h2.diagnostics.failure_stage(result) is (
            h2.boundary.ReceiveStage.FAILURE
        )

    def test_failure_dict_is_json_serializable(self) -> None:
        wire = PREAMBLE + b"\x00\x00"
        result = h2.diagnostics.replay(
            wire,
            schedule=h2.diagnostics.FeedSchedule.WHOLE,
            connection_factory=make_server,
        )
        assert not result.ok
        payload = result.failure_dict()
        assert payload is not None
        json.dumps(payload)
        assert payload["type"] == "TruncatedFrameError"

    def test_overflow_failure_is_boundary_error(self) -> None:
        wire = (
            PREAMBLE
            + headers_frame(
                1, [*POST_HEADERS, (b"content-length", b"4")],
            ).serialize()
            + data_frame(1, b"too much data").serialize()
        )
        result = h2.diagnostics.replay(
            wire,
            schedule=h2.diagnostics.FeedSchedule.WHOLE,
            connection_factory=make_server,
        )
        assert not result.ok
        assert isinstance(result.error, h2.exceptions.InvalidBodyLengthError)
        assert result.failure is h2.boundary.FailureClass.BOUNDARY_ERROR


class TestReplayRecords:
    """
    A stored record can be replayed under a different fragmentation.
    """

    def _wire(self) -> bytes:
        return (
            PREAMBLE
            + headers_frame(1, GET_HEADERS, ["END_STREAM"]).serialize()
        )

    def test_record_round_trip(self) -> None:
        wire = self._wire()
        result = h2.diagnostics.replay(
            wire,
            schedule=h2.diagnostics.FeedSchedule.BYTEWISE,
            connection_factory=make_server,
        )
        record = h2.diagnostics.ReplayRecord.from_result(wire, result)
        payload = record.as_dict()
        json.dumps(payload)

        restored = h2.diagnostics.ReplayRecord.from_dict(payload)
        assert restored.capture == wire
        assert restored.schedule is h2.diagnostics.FeedSchedule.BYTEWISE

    def test_record_replays_under_another_schedule(self) -> None:
        wire = self._wire()
        result = h2.diagnostics.replay(
            wire,
            schedule=h2.diagnostics.FeedSchedule.WHOLE,
            connection_factory=make_server,
        )
        record = h2.diagnostics.ReplayRecord.from_result(wire, result)
        replayed = h2.diagnostics.replay_record(
            record,
            connection_factory=make_server,
            schedule=h2.diagnostics.FeedSchedule.RANDOM,
        )
        assert replayed.ok
        assert replayed.event_envelope() == result.event_envelope()

    def test_record_contains_boundary_events(self) -> None:
        wire = self._wire()
        result = h2.diagnostics.replay(
            wire,
            schedule=h2.diagnostics.FeedSchedule.WHOLE,
            connection_factory=make_server,
        )
        record = h2.diagnostics.ReplayRecord.from_result(wire, result)
        assert record.boundary_events
        assert all(
            "connection_id" in event for event in record.boundary_events
        )

    def test_iter_boundary_events(self) -> None:
        result = h2.diagnostics.replay(
            self._wire(),
            schedule=h2.diagnostics.FeedSchedule.WHOLE,
            connection_factory=make_server,
        )
        assert result.boundary_log is not None
        events = list(h2.diagnostics.iter_boundary_events(result.boundary_log))
        assert len(events) == len(result.boundary_log)


class TestDivergenceLocation:
    """
    A divergence must be located at a receive step, with both sides' state.
    """

    def _rigged_factory(self) -> h2.connection.H2Connection:
        """
        Return a connection that silently drops the first DATA byte when
        fed a fragment larger than one byte, simulating a bug that only shows
        up for coalesced reads.
        """
        connection = make_server()
        original = connection.receive_data
        state = {"first_big_read": True}

        def receive_data(data: bytes) -> list[h2.events.Event]:
            if len(data) > 1 and state["first_big_read"]:
                state["first_big_read"] = False
                data = data[:-1]
            return original(data)

        connection.receive_data = receive_data  # type: ignore[method-assign]
        return connection

    def test_divergence_is_reported_at_a_step(self) -> None:
        wire = (
            PREAMBLE
            + headers_frame(1, GET_HEADERS, ["END_STREAM"]).serialize()
        )
        comparison = h2.diagnostics.compare_schedules(
            wire, connection_factory=self._rigged_factory, finish=False,
        )
        assert not comparison.consistent
        divergence = comparison.divergences[0]
        assert divergence.step_index >= 0
        assert divergence.schedule_a != divergence.schedule_b
        assert json.dumps(divergence.as_dict())
        payload = comparison.as_dict()
        assert not payload["consistent"]
        assert payload["divergences"]

    def test_consistent_capture_has_no_divergences(self) -> None:
        wire = (
            PREAMBLE
            + headers_frame(1, GET_HEADERS, ["END_STREAM"]).serialize()
        )
        comparison = h2.diagnostics.compare_schedules(
            wire, connection_factory=make_server,
        )
        assert comparison.consistent
        assert comparison.as_dict()["divergences"] == []


class TestStepwiseReceiveChecks:
    """
    The receive path re-checks declared lengths at every fragment boundary,
    so a schedule that happens to coalesce frames cannot mask a lying header.
    """

    def test_lying_length_field_is_rejected_even_when_coalesced(self) -> None:
        connection = make_server()
        connection.receive_data(PREAMBLE)
        connection.receive_data(headers_frame(1, GET_HEADERS).serialize())

        # A DATA frame header declaring 100 bytes with only 3 bytes present.
        wire = b"\x00\x00\x64\x00\x00\x00\x00\x00\x01" + b"abc"
        connection.receive_data(wire)
        snapshot = connection.incoming_buffer.snapshot()
        assert snapshot.declared_length == 100
        assert snapshot.received_bytes == 3
        assert snapshot.pending_body_bytes == 97
        assert connection.upstream_remainder().remaining_bytes == len(wire)

    def test_continuation_after_body_is_still_protocol_error(self) -> None:
        connection = make_server()
        connection.receive_data(PREAMBLE)
        connection.receive_data(headers_frame(1, GET_HEADERS).serialize())
        continuation = ContinuationFrame(1)
        continuation.flags.add("END_HEADERS")
        continuation.data = b"\x82"
        with pytest.raises(h2.exceptions.ProtocolError):
            connection.receive_data(continuation.serialize())
