"""
Additional coverage for the pool's boundary-log paths and the replay
diagnostics branches.

These complement ``test_connection_reuse_and_replay.py``: they exercise the
less common decisions (a boundary that was confirmed *and* had bytes
discarded, an unbounded body, poolable objects that omit optional hooks) and
the divergence-reporting branches, so that every reuse decision has an
explicit test rather than relying on the happy path.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest
from hpack import Encoder
from hyperframe.frame import DataFrame, HeadersFrame

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


class Bare:
    """
    A poolable object that implements only the mandatory hooks.
    """

    connection_id: str | None = "bare"
    retired = False
    retire_reason: str | None = None
    boundary_log = None
    config = object()

    def retire(self, reason: str) -> None:
        self.retired = True
        self.retire_reason = reason

    def upstream_remainder(self) -> h2.boundary.UpstreamRemainder:
        return h2.boundary.UpstreamRemainder(
            connection_id=str(self.connection_id),
            remaining_bytes=0,
            stage="idle",
        )


class TestPoolBoundaryLogPaths:
    """
    The pool's reuse decision reads a real boundary log when diagnostics are
    enabled, and must catch a boundary that was confirmed but also had bytes
    discarded.
    """

    def _connection_with_discard(self) -> h2.connection.H2Connection:
        connection = make_server()
        log = connection.enable_boundary_diagnostics("discard")
        connection.receive_data(PREAMBLE)
        log.declare_boundary(1, 4)
        log.record(
            h2.boundary.ReceiveStage.BODY,
            stream_id=1,
            bytes_read=4,
            confirmed_length=4,
            declared_length=4,
            discarded_bytes=2,
        )
        return connection

    def test_discarded_bytes_deny_reuse(self) -> None:
        connection = self._connection_with_discard()
        pool = h2.pool.ConnectionPool()
        assert not pool.checkin(connection)
        assert connection.retire_reason == h2.pool.ReuseDenial.BODY_BOUNDARY

    def test_discarded_bytes_fail_assert_reusable(self) -> None:
        connection = self._connection_with_discard()
        with pytest.raises(h2.exceptions.BoundaryViolationError) as exc_info:
            h2.pool.assert_reusable(connection, stream_id=1)
        assert "discarded" in str(exc_info.value)
        assert exc_info.value.stream_id == 1

    def test_unbounded_boundary_is_skipped(self) -> None:
        connection = make_server()
        log = connection.enable_boundary_diagnostics("unbounded")
        connection.receive_data(PREAMBLE)
        log.declare_boundary(1, None)
        log.record(
            h2.boundary.ReceiveStage.BODY,
            stream_id=1,
            bytes_read=100,
            confirmed_length=100,
            declared_length=None,
        )
        pool = h2.pool.ConnectionPool()
        # No content-length means the boundary cannot be compared; the pool
        # falls through to the buffer and direction checks.
        assert pool._body_boundaries_exact(connection)
        h2.pool.assert_reusable(connection, stream_id=1)

    def test_retired_connection_fails_assert_reusable(self) -> None:
        connection = make_server()
        connection.receive_data(PREAMBLE)
        connection.retire("operator")
        with pytest.raises(h2.exceptions.BoundaryViolationError) as exc_info:
            h2.pool.assert_reusable(connection)
        assert exc_info.value.kind == "retired"
        assert "operator" in str(exc_info.value)

    def test_mismatched_boundary_fails_assert_reusable(self) -> None:
        connection = make_server()
        log = connection.enable_boundary_diagnostics("mismatch")
        connection.receive_data(PREAMBLE)
        log.declare_boundary(1, 10)
        log.record(
            h2.boundary.ReceiveStage.BODY,
            stream_id=1,
            bytes_read=3,
            confirmed_length=3,
            declared_length=10,
        )
        with pytest.raises(h2.exceptions.BoundaryViolationError):
            h2.pool.assert_reusable(connection)

    def test_connection_boundary_check_without_log_uses_streams(self) -> None:
        connection = make_server()
        connection.receive_data(PREAMBLE)
        connection.receive_data(
            headers_frame(
                1, [*GET_HEADERS, (b"content-length", b"5")],
            ).serialize(),
        )
        connection.receive_data(data_frame(1, b"abc").serialize())
        assert not connection.body_boundaries_exact()

    def test_connection_boundary_check_without_log_without_length(self) -> None:
        connection = make_server()
        connection.receive_data(PREAMBLE)
        connection.receive_data(headers_frame(1, GET_HEADERS).serialize())
        connection.receive_data(data_frame(1, b"abc").serialize())
        assert connection.body_boundaries_exact()


class TestPoolIncompleteInterface:
    """
    A poolable object that omits the optional hooks is treated permissively on
    those hooks, never as a crash.
    """

    def test_missing_optional_hooks(self) -> None:
        pool = h2.pool.ConnectionPool()
        assert pool.checkin(Bare())

    def test_explicit_connection_id_when_missing(self) -> None:
        class Anonymous(Bare):
            connection_id = None

        pool = h2.pool.ConnectionPool()
        assert pool.checkin(Anonymous(), connection_id="given")
        assert pool.state_of("given") is h2.pool.PoolState.IDLE

    def test_generated_connection_id(self) -> None:
        class Anonymous(Bare):
            connection_id = None

        pool = h2.pool.ConnectionPool()
        anonymous = Anonymous()
        assert pool.checkin(anonymous)
        assert len(pool) == 1
        assert pool.checkout() is anonymous

    def test_pool_event_reason_serialization(self) -> None:
        pool = h2.pool.ConnectionPool()
        pool.retire("ghost", "custom-reason")
        event = pool.events[-1]
        assert event.reason is None
        assert event.detail == "custom-reason"
        assert event.as_dict()["reason"] is None
        assert pool.state_of("ghost") is h2.pool.PoolState.RETIRED

    def test_ownership_handoff_without_log(self) -> None:
        h2.pool.record_ownership_handoff(Bare(), "a", "b")

    def test_version_and_timeout_property_absent(self) -> None:
        pool = h2.pool.ConnectionPool(protocol_version="h2/9", timeout=1.0)
        bare = Bare()
        assert pool._version_mismatch(bare) is False
        assert pool._timeout_mismatch(bare) is False
        assert pool._body_boundaries_exact(bare) is True


class TestDivergenceBranches:
    """
    Every divergence kind is reported with the step it was detected at.
    """

    def test_final_buffer_occupancy_diverges(self) -> None:
        def factory() -> h2.connection.H2Connection:
            connection = make_server()
            original = connection.receive_data

            def receive_data(data: bytes) -> list[h2.events.Event]:
                if len(data) > 1:
                    # Leave the last two bytes in the buffer instead of
                    # letting them complete the frame.
                    return original(data[:-2])
                return original(data)

            connection.receive_data = receive_data  # type: ignore[method-assign]
            return connection

        wire = PREAMBLE + headers_frame(1, GET_HEADERS, ["END_STREAM"]).serialize()
        first = h2.diagnostics.replay(
            wire,
            schedule=h2.diagnostics.FeedSchedule.WHOLE,
            connection_factory=factory,
            finish=False,
        )
        second = h2.diagnostics.replay(
            wire,
            schedule=h2.diagnostics.FeedSchedule.BYTEWISE,
            connection_factory=make_server,
            finish=False,
        )
        divergence = h2.diagnostics._divergence_between(first, second)
        assert divergence is not None
        assert "occupancy" in divergence.detail or "events" in divergence.detail

    def test_failure_vs_success_detail(self) -> None:
        wire = PREAMBLE + headers_frame(1, GET_HEADERS, ["END_STREAM"]).serialize()
        success = h2.diagnostics.replay(
            wire,
            schedule=h2.diagnostics.FeedSchedule.WHOLE,
            connection_factory=make_server,
            finish=False,
        )
        failure = h2.diagnostics.replay(
            wire + b"\x00\x00",
            schedule=h2.diagnostics.FeedSchedule.WHOLE,
            connection_factory=make_server,
        )
        divergence = h2.diagnostics._divergence_between(success, failure)
        assert divergence is not None
        assert "failed" in divergence.detail

    def test_describe_no_error(self) -> None:
        assert h2.diagnostics._describe(None) == "no error"

    def test_failure_stage_without_log(self) -> None:
        result = h2.diagnostics.ReplayResult(
            schedule=h2.diagnostics.FeedSchedule.WHOLE,
        )
        assert h2.diagnostics.failure_stage(result) is None

    def test_failure_stage_when_no_failure_recorded(self) -> None:
        result = h2.diagnostics.replay(
            PREAMBLE,
            schedule=h2.diagnostics.FeedSchedule.WHOLE,
            connection_factory=make_server,
        )
        assert result.ok
        assert h2.diagnostics.failure_stage(result) is None

    def test_replay_step_as_dict(self) -> None:
        result = h2.diagnostics.replay(
            PREAMBLE,
            schedule=h2.diagnostics.FeedSchedule.WHOLE,
            connection_factory=make_server,
        )
        payload = result.steps[0].as_dict()
        assert payload["index"] == 0
        assert json.dumps(payload)

    def test_empty_capture_replay(self) -> None:
        # A client connection has no peer preface to wait for, so an empty
        # capture genuinely is a clean, complete (empty) stream.
        def client_factory() -> h2.connection.H2Connection:
            connection = h2.connection.H2Connection(
                config=h2.config.H2Configuration(client_side=True),
            )
            connection.initiate_connection()
            connection.clear_outbound_data_buffer()
            return connection

        result = h2.diagnostics.replay(
            b"",
            schedule=h2.diagnostics.FeedSchedule.RANDOM,
            connection_factory=client_factory,
        )
        assert result.ok
        assert result.steps == []
        assert result.delivered_bytes == 0

    def test_empty_capture_on_server_reports_missing_preface(self) -> None:
        # A server-side connection *does* wait for the client preface, so an
        # empty capture is truncated input rather than a clean finish.
        result = h2.diagnostics.replay(
            b"",
            schedule=h2.diagnostics.FeedSchedule.RANDOM,
            connection_factory=make_server,
        )
        assert not result.ok
        assert isinstance(result.error, h2.exceptions.TruncatedFrameError)
        assert result.error.stage == "preamble"


class TestRemainingReuseBranches:
    """
    Boundary-log paths for unbounded bodies and discarded bytes are covered
    explicitly, so every reuse denial layer has a test.
    """

    def test_unbounded_boundary_continues_past_declared_none(self) -> None:
        connection = make_server()
        log = connection.enable_boundary_diagnostics("unbounded-branch")
        connection.receive_data(PREAMBLE)
        log.declare_boundary(1, None)
        log.record(
            h2.boundary.ReceiveStage.BODY,
            stream_id=1,
            bytes_read=1,
            confirmed_length=1,
            declared_length=None,
        )
        assert connection.body_boundaries_exact()

    def test_discarded_bytes_fail_body_boundaries_exact(self) -> None:
        connection = make_server()
        log = connection.enable_boundary_diagnostics("discarded-branch")
        connection.receive_data(PREAMBLE)
        log.declare_boundary(1, 4)
        log.record(
            h2.boundary.ReceiveStage.BODY,
            stream_id=1,
            bytes_read=4,
            confirmed_length=4,
            declared_length=4,
            discarded_bytes=1,
        )
        assert not connection.body_boundaries_exact()

    def test_final_buffer_occupancy_branch(self) -> None:
        """
        Two replays that both succeed but drain different amounts of the
        buffer are reported as a final-occupancy divergence.
        """
        def factory(tail: bytes) -> object:
            def build() -> h2.connection.H2Connection:
                connection = make_server()
                original = connection.receive_data

                def receive_data(data: bytes) -> list[h2.events.Event]:
                    return original(data + tail)

                connection.receive_data = receive_data  # type: ignore[method-assign]
                return connection

            return build

        wire = PREAMBLE + headers_frame(1, GET_HEADERS, ["END_STREAM"]).serialize()
        first = h2.diagnostics.replay(
            wire,
            schedule=h2.diagnostics.FeedSchedule.WHOLE,
            connection_factory=factory(b""),
            finish=False,
        )
        second = h2.diagnostics.replay(
            wire,
            schedule=h2.diagnostics.FeedSchedule.WHOLE,
            connection_factory=factory(b"\x00\x00"),
            finish=False,
        )
        divergence = h2.diagnostics._divergence_between(first, second)
        assert divergence is not None
        assert divergence.buffered_a != divergence.buffered_b


class TestExactBoundaryIteration:
    """
    Multi-boundary and stream-less records are iterated without short-circuit
    mistakes.
    """

    def test_connection_iterates_past_an_exact_boundary(self) -> None:
        connection = make_server()
        log = connection.enable_boundary_diagnostics("multi")
        connection.receive_data(PREAMBLE)
        # Stream 1 is exact and clean; stream 3 is exact but had bytes
        # discarded. The loop must inspect the second entry.
        log.declare_boundary(1, 4)
        log.record(
            h2.boundary.ReceiveStage.BODY,
            stream_id=1,
            bytes_read=4,
            confirmed_length=4,
            declared_length=4,
        )
        log.declare_boundary(3, 2)
        log.record(
            h2.boundary.ReceiveStage.BODY,
            stream_id=3,
            bytes_read=2,
            confirmed_length=2,
            declared_length=2,
            discarded_bytes=1,
        )
        assert not connection.body_boundaries_exact()

    def test_pool_iterates_past_an_exact_boundary(self) -> None:
        connection = make_server()
        log = connection.enable_boundary_diagnostics("multi-pool")
        connection.receive_data(PREAMBLE)
        log.declare_boundary(1, 4)
        log.record(
            h2.boundary.ReceiveStage.BODY,
            stream_id=1,
            bytes_read=4,
            confirmed_length=4,
            declared_length=4,
        )
        log.declare_boundary(3, 2)
        log.record(
            h2.boundary.ReceiveStage.BODY,
            stream_id=3,
            bytes_read=2,
            confirmed_length=2,
            declared_length=2,
            discarded_bytes=1,
        )
        pool = h2.pool.ConnectionPool()
        assert not pool._body_boundaries_exact(connection)
        assert not pool.checkin(connection)

    def test_pool_log_with_only_unbounded_boundaries_is_reusable(self) -> None:
        connection = make_server()
        log = connection.enable_boundary_diagnostics("unbounded-only")
        connection.receive_data(PREAMBLE)
        log.declare_boundary(1, None)
        pool = h2.pool.ConnectionPool()
        assert pool._body_boundaries_exact(connection)

    def test_pool_log_with_no_boundaries_is_reusable(self) -> None:
        connection = make_server()
        connection.enable_boundary_diagnostics("no-boundaries")
        connection.receive_data(PREAMBLE)
        pool = h2.pool.ConnectionPool()
        assert pool._body_boundaries_exact(connection)

    def test_assert_reusable_with_log_and_passing_boundary(self) -> None:
        connection = make_server()
        log = connection.enable_boundary_diagnostics("passing")
        connection.receive_data(PREAMBLE)
        log.declare_boundary(1, 4)
        log.record(
            h2.boundary.ReceiveStage.BODY,
            stream_id=1,
            bytes_read=4,
            confirmed_length=4,
            declared_length=4,
        )
        h2.pool.assert_reusable(connection)

    def test_raw_reconstruction_with_streamless_and_unbounded_events(self) -> None:
        log = h2.boundary.BoundaryEventLog("raw-mixed")
        log.record(h2.boundary.ReceiveStage.FRAME_DISPATCH)  # no stream
        log.record(
            h2.boundary.ReceiveStage.BODY,
            stream_id=2,
            bytes_read=5,
            confirmed_length=5,
            declared_length=None,  # stream but no declaration
        )
        restored = h2.boundary.BoundaryEventLog.from_dicts(
            "raw-mixed", log.to_dicts(),
        )
        assert len(restored) == 2
        # A stream that received body bytes gets a boundary even without a
        # declared length, in both the live log and the reconstruction.
        restored_boundary = restored.boundary(2)
        assert restored_boundary is not None
        assert restored_boundary.declared is None
        assert restored_boundary.confirmed == 5
        assert restored.events[1].declared_length is None


def test_assert_reusable_passes_without_diagnostics_log() -> None:
    """
    A connection with no boundary log is still checked against its buffer.
    """
    connection = make_server()
    connection.receive_data(PREAMBLE)
    connection.receive_data(
        headers_frame(1, GET_HEADERS, ["END_STREAM"]).serialize(),
    )
    h2.pool.assert_reusable(connection)


def _load_example_module() -> object:
    """
    Import the example script as a module, without executing ``main``.
    """
    path = (
        Path(__file__).resolve().parents[1]
        / "examples" / "stream_boundary" / "stream_boundary_replay.py"
    )
    spec = importlib.util.spec_from_file_location(
        "stream_boundary_replay_example", path,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestExampleReproduction:
    """
    The shipped reproduction script must keep demonstrating the guards it
    documents.
    """

    def test_cross_talk_is_refused(self) -> None:
        module = _load_example_module()
        report = module.demonstrate_cross_talk()
        assert report["reuse_accepted"] is False
        assert report["retire_reason"] == "buffer-cleanup"
        assert report["leftover_bytes"] > 0
        # The unguarded path really would have interpreted the stray bytes.
        assert report["unguarded_interpreted_stream_3"] is True

    def test_body_overflow_records_discarded_bytes(self) -> None:
        module = _load_example_module()
        report = module.demonstrate_body_boundaries()
        assert report["error"] == "InvalidBodyLengthError"
        assert report["retire_reason"] == "body-boundary"
        discarded = [
            event["discarded_bytes"] for event in report["events_table"]
            if event["failure"] == "boundary-error"
            and event["discarded_bytes"]
        ]
        assert discarded == [8]

    def test_replay_schedules_agree(self) -> None:
        module = _load_example_module()
        report = module.demonstrate_replay_equivalence()
        assert report["consistent"] is True
        assert report["divergences"] == []

    def test_replay_record_matches_original(self) -> None:
        module = _load_example_module()
        report = module.demonstrate_replay_record()
        assert report["same_envelope"] is True
        assert report["delivered_bytes"] == 16
