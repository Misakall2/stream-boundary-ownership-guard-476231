"""
Regression tests for stream boundary accounting and buffer ownership.

The scenario these tests model is a gateway that reuses HTTP/2 connections.
The failure mode is that an unfinished exchange (a request body not fully
read, or an upstream response body not fully delivered) leaves bytes in a
connection's buffer; if the connection is reused, those bytes become the
first bytes of the next exchange and a concurrent request receives somebody
else's body.

Everything here is offline and deterministic: the "peer" is a
:class:`helpers.FrameFactory <tests.helpers.FrameFactory>` (or a hand-built
serialized frame) and the "network" is ``receive_data``.
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

POST_HEADERS = [
    (b":method", b"POST"),
    (b":path", b"/upload"),
    (b":scheme", b"https"),
    (b":authority", b"example.com"),
]

GET_HEADERS = [
    (b":method", b"GET"),
    (b":path", b"/"),
    (b":scheme", b"https"),
    (b":authority", b"example.com"),
]


def make_server(**config_kwargs) -> h2.connection.H2Connection:
    """
    Return a server-side connection that has already sent its settings.
    """
    config = h2.config.H2Configuration(client_side=False, **config_kwargs)
    connection = h2.connection.H2Connection(config=config)
    connection.initiate_connection()
    connection.clear_outbound_data_buffer()
    return connection


def make_frames() -> h2.boundary.BoundaryEventLog:
    """
    Return an empty diagnostics log for direct boundary-bookkeeping tests.
    """
    return h2.boundary.BoundaryEventLog("unit")


def headers_frame(stream_id: int,
                  headers: list[tuple[bytes, bytes]],
                  flags: list[str] | None = None) -> HeadersFrame:
    """
    Build a complete, single-frame HEADERS block.
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


def post_with_content_length(stream_id: int, length: int,
                             end_stream: bool = False) -> HeadersFrame:
    """
    Build a POST HEADERS block that declares a ``content-length``.
    """
    headers = [
        *POST_HEADERS,
        (b"content-length", str(length).encode("ascii")),
    ]
    return headers_frame(stream_id, headers, ["END_STREAM"] if end_stream else None)


class TestBodyBoundaryVersusRemainder:
    """
    The declared body length and the buffered upstream remainder are tracked
    separately and never borrow from each other.
    """

    def test_boundary_is_declared_and_confirmed_independently(self) -> None:
        log = make_frames()
        log.record(
            h2.boundary.ReceiveStage.BODY,
            stream_id=1,
            bytes_read=10,
            confirmed_length=10,
            declared_length=15,
        )

        boundary = log.boundary(1)
        assert boundary is not None
        assert boundary.declared == 15
        assert boundary.confirmed == 10
        assert boundary.remaining == 5
        assert not boundary.complete
        assert not boundary.overrun

    def test_remainder_is_independent_of_declared_length(self) -> None:
        log = make_frames()
        log.record(
            h2.boundary.ReceiveStage.BODY,
            stream_id=1,
            bytes_read=4,
            confirmed_length=4,
            declared_length=100,
        )
        # The remainder counts physical buffer bytes only: it must not be
        # derived from, or reduced by, the declared length.
        remainder = log.remainder(9, "frame-body", stream_id=1)
        assert remainder.remaining_bytes == 9
        assert remainder.connection_id == "unit"
        assert not remainder.clean
        assert log.boundary(1).remaining == 96

    def test_overflow_marks_boundary_overrun(self) -> None:
        boundary = h2.boundary.BodyBoundary(stream_id=1, declared=5)
        overrun = boundary.confirm(7)
        assert overrun.overrun
        assert overrun.remaining == -2
        assert not overrun.complete

    def test_discard_is_recorded_separately_from_confirmation(self) -> None:
        boundary = h2.boundary.BodyBoundary(stream_id=1, declared=5)
        boundary = boundary.confirm(5).discard(3)
        assert boundary.confirmed == 5
        assert boundary.discarded == 3
        assert boundary.complete

    def test_completed_boundary(self) -> None:
        boundary = h2.boundary.BodyBoundary(stream_id=3, declared=0)
        assert boundary.complete
        assert boundary.remaining == 0
        assert not boundary.overrun

    def test_unbounded_body_has_no_remaining(self) -> None:
        boundary = h2.boundary.BodyBoundary(stream_id=1, declared=None)
        boundary = boundary.confirm(50)
        assert boundary.remaining is None
        assert not boundary.complete
        assert not boundary.overrun


class TestEventLog:
    """
    The event table records the facts needed to locate a failure phase
    without inferring it from the final exception.
    """

    def test_event_carries_read_confirmed_discarded_and_offsets(self) -> None:
        log = h2.boundary.BoundaryEventLog("conn-a")
        log.record(
            h2.boundary.ReceiveStage.BUFFER_APPEND,
            bytes_read=20,
            buffer_before=0,
            buffer_after=20,
        )
        event = log.record(
            h2.boundary.ReceiveStage.BODY,
            stream_id=1,
            bytes_read=15,
            confirmed_length=15,
            declared_length=15,
            buffer_before=20,
            buffer_after=5,
            detail="complete",
        )

        assert event.seq == 1
        assert event.connection_id == "conn-a"
        assert event.bytes_read == 15
        assert event.confirmed_length == 15
        assert event.declared_length == 15
        assert event.discarded_bytes == 0
        assert event.buffer_before == 20
        assert event.buffer_after == 5
        # The body step confirms bytes that already entered the connection in
        # the append step, so the consume offset does not advance again.
        assert event.consumed_offset == 20
        assert event.remaining_length == 0
        assert event.stage is h2.boundary.ReceiveStage.BODY

    def test_consumed_offset_accumulates_on_append(self) -> None:
        log = h2.boundary.BoundaryEventLog()
        for size in (9, 4, 9):
            log.record(h2.boundary.ReceiveStage.BUFFER_APPEND, bytes_read=size)
        assert log.consumed_offset == 22

    def test_failure_is_recorded_at_its_stage(self) -> None:
        log = h2.boundary.BoundaryEventLog()
        log.record(h2.boundary.ReceiveStage.BUFFER_APPEND, bytes_read=4)
        event = log.record_failure(
            h2.boundary.ReceiveStage.FRAME_BODY,
            h2.boundary.FailureClass.PROTOCOL_ERROR,
            detail="bad padding",
        )
        assert event.stage is h2.boundary.ReceiveStage.FRAME_BODY
        assert event.failure is h2.boundary.FailureClass.PROTOCOL_ERROR

    def test_json_round_trip_preserves_boundaries(self) -> None:
        log = h2.boundary.BoundaryEventLog("conn-b")
        log.record(
            h2.boundary.ReceiveStage.BODY,
            stream_id=7,
            bytes_read=3,
            confirmed_length=3,
            declared_length=9,
        )
        payload = json.loads(log.to_json())
        restored = h2.boundary.BoundaryEventLog.from_dicts(
            "conn-b", payload["events"],
        )

        assert len(restored) == len(log)
        assert restored.boundary(7) == log.boundary(7)
        assert restored.consumed_offset == log.consumed_offset
        assert restored.events[0].as_dict() == log.events[0].as_dict()

    def test_write_json(self, tmp_path) -> None:
        log = h2.boundary.BoundaryEventLog("conn-write")
        log.record(h2.boundary.ReceiveStage.BUFFER_APPEND, bytes_read=1)
        target = tmp_path / "log.json"
        log.write_json(target)
        payload = json.loads(target.read_text(encoding="utf-8"))
        assert payload["connection_id"] == "conn-write"
        assert len(payload["events"]) == 1

    def test_redeclare_keeps_confirmed_counts(self) -> None:
        boundary = h2.boundary.BodyBoundary(stream_id=1, declared=10)
        boundary = boundary.confirm(4)
        assert boundary.confirmed == 4

    def test_classify_failure_separates_timeout_from_protocol(self) -> None:
        assert h2.boundary.classify_failure(TimeoutError()) is (
            h2.boundary.FailureClass.TIMEOUT
        )
        assert h2.boundary.classify_failure(
            h2.exceptions.ProtocolError("x"),
        ) is h2.boundary.FailureClass.PROTOCOL_ERROR
        assert h2.boundary.classify_failure(
            h2.exceptions.InvalidBodyLengthError(1, 2),
        ) is h2.boundary.FailureClass.BOUNDARY_ERROR
        assert h2.boundary.classify_failure(ValueError("x")) is (
            h2.boundary.FailureClass.INTERNAL_ERROR
        )


class TestFragmentationBoundaries:
    """
    The receive path must re-check declared lengths at every fragment
    boundary. A single read that happened to contain a whole frame must not
    relax the checks applied to later fragments.
    """

    @pytest.mark.parametrize("chunk", [1, 2, 5, 9, 10, 13])
    def test_data_frame_split_at_arbitrary_boundaries(self, chunk) -> None:
        connection = make_server()
        connection.enable_boundary_diagnostics("split")
        wire = (
            headers_frame(1, [*GET_HEADERS, (b"content-length", b"5")]).serialize()
            + data_frame(1, b"hello").serialize()
        )
        connection.receive_data(PREAMBLE)

        events = []
        for start in range(0, len(wire), chunk):
            events.extend(connection.receive_data(wire[start:start + chunk]))

        data_events = [e for e in events if isinstance(e, h2.events.DataReceived)]
        assert len(data_events) == 1
        assert data_events[0].data == b"hello"
        assert connection.body_boundary(1).confirmed == 5
        assert connection.body_boundary(1).declared == 5
        connection.end_of_input()

    def test_header_split_exactly_inside_length_digits(self) -> None:
        """
        A fragment boundary landing in the middle of a frame's length field
        must not be mistaken for a complete (zero-length) frame.
        """
        connection = make_server()
        connection.enable_boundary_diagnostics("digits")
        connection.receive_data(PREAMBLE)
        connection.receive_data(headers_frame(1, GET_HEADERS).serialize())

        frame = data_frame(1, b"payload")
        wire = frame.serialize()

        # Split inside the three-byte length field (bytes 1..2).
        events = connection.receive_data(wire[:1])
        assert not [e for e in events if isinstance(e, h2.events.DataReceived)]
        snapshot = connection.incoming_buffer.snapshot()
        assert snapshot.stage == "frame-header"
        assert snapshot.declared_length is None

        events = connection.receive_data(wire[1:2])
        assert not [e for e in events if isinstance(e, h2.events.DataReceived)]

        events = connection.receive_data(wire[2:])
        data_events = [e for e in events if isinstance(e, h2.events.DataReceived)]
        assert len(data_events) == 1
        assert data_events[0].data == b"payload"

    def test_truncated_body_is_distinguishable_from_clean_end(self) -> None:
        connection = make_server()
        connection.receive_data(PREAMBLE)
        wire = data_frame(1, b"abcdef").serialize()
        # Feed the full header and only part of the body.
        connection.receive_data(headers_frame(1, GET_HEADERS).serialize())
        connection.receive_data(wire[:11])

        snapshot = connection.incoming_buffer.snapshot()
        assert snapshot.declared_length == 6
        assert snapshot.received_bytes == 2
        assert snapshot.pending_body_bytes == 4

        with pytest.raises(h2.exceptions.TruncatedFrameError) as exc_info:
            connection.end_of_input()
        assert exc_info.value.stage == "frame-body"
        assert exc_info.value.declared_length == 6
        assert connection.retired
        assert connection.retire_reason == "truncated-frame"

    def test_truncated_header_reports_header_stage(self) -> None:
        connection = make_server()
        connection.receive_data(PREAMBLE)
        connection.receive_data(b"\x00\x00")
        with pytest.raises(h2.exceptions.TruncatedFrameError) as exc_info:
            connection.end_of_input()
        assert exc_info.value.stage == "frame-header"

    def test_clean_end_of_input_does_not_raise(self) -> None:
        connection = make_server()
        connection.receive_data(PREAMBLE)
        connection.receive_data(headers_frame(1, GET_HEADERS).serialize())
        connection.end_of_input()
        assert not connection.retired

    def test_truncated_header_block_reports_header_block_stage(self) -> None:
        connection = make_server()
        connection.receive_data(PREAMBLE)
        block = Encoder().encode(GET_HEADERS)
        head = HeadersFrame(1)
        head.data = block[:2]
        connection.receive_data(head.serialize())
        assert connection.incoming_buffer.snapshot().stage == "header-block"

        with pytest.raises(h2.exceptions.TruncatedFrameError) as exc_info:
            connection.end_of_input()
        assert exc_info.value.stage == "header-block"

    def test_truncated_preamble_is_reported(self) -> None:
        connection = make_server()
        connection.incoming_buffer.add_data(PREAMBLE[:10])
        with pytest.raises(h2.exceptions.TruncatedFrameError) as exc_info:
            connection.incoming_buffer.finish()
        assert exc_info.value.stage == "preamble"

    def test_oversize_frame_is_a_size_error_not_truncation(self) -> None:
        connection = make_server()
        connection.receive_data(PREAMBLE)
        connection.max_inbound_frame_size = 10
        connection.incoming_buffer.max_frame_size = 10
        wire = data_frame(1, b"x" * 32).serialize()
        with pytest.raises(h2.exceptions.ProtocolError) as exc_info:
            connection.receive_data(wire)
        assert isinstance(exc_info.value, h2.exceptions.FrameTooLargeError)


class TestBodyBoundaryOnTheWire:
    """
    End-to-end body boundary behaviour: overflow is recorded, nothing past
    the boundary is delivered, and the connection is retired.
    """

    def test_overflow_records_discarded_bytes_and_retires(self) -> None:
        connection = make_server()
        log = connection.enable_boundary_diagnostics("overflow")
        connection.receive_data(PREAMBLE)
        connection.receive_data(post_with_content_length(1, 15).serialize())
        connection.receive_data(data_frame(1, b"a" * 10).serialize())

        with pytest.raises(h2.exceptions.InvalidBodyLengthError) as exc_info:
            connection.receive_data(data_frame(1, b"b" * 10).serialize())

        assert exc_info.value.kind == "body-overflow"
        assert exc_info.value.discarded_bytes == 5
        assert connection.retired
        assert connection.retire_reason == "body-boundary"

        boundary_events = [
            e for e in log.events
            if e.stage is h2.boundary.ReceiveStage.BODY and e.failure
        ]
        assert boundary_events
        last = boundary_events[-1]
        assert last.discarded_bytes == 5
        assert last.failure is h2.boundary.FailureClass.BOUNDARY_ERROR

    def test_underflow_at_end_stream_is_body_underflow(self) -> None:
        connection = make_server()
        connection.receive_data(PREAMBLE)
        connection.receive_data(post_with_content_length(1, 15).serialize())
        with pytest.raises(h2.exceptions.InvalidBodyLengthError) as exc_info:
            connection.receive_data(
                data_frame(1, b"a" * 14, end_stream=True).serialize(),
            )
        assert exc_info.value.kind == "body-underflow"
        assert exc_info.value.remaining == 1

    def test_complete_body_is_confirmed_exactly(self) -> None:
        connection = make_server()
        connection.enable_boundary_diagnostics("exact")
        connection.receive_data(PREAMBLE)
        connection.receive_data(post_with_content_length(1, 15).serialize())
        events = connection.receive_data(
            data_frame(1, b"a" * 15, end_stream=True).serialize(),
        )
        assert [e for e in events if isinstance(e, h2.events.StreamEnded)]
        boundary = connection.body_boundary(1)
        assert boundary.declared == 15
        assert boundary.confirmed == 15
        assert boundary.discarded == 0


class TestRequestNotFinishedBeforeNextConnection:
    """
    An exchange whose request body was not fully read may not be treated as
    finished, and its connection may not be reused.
    """

    def test_incomplete_request_body_blocks_reuse(self) -> None:
        connection = make_server()
        connection.enable_boundary_diagnostics("partial")
        connection.receive_data(PREAMBLE)
        connection.receive_data(post_with_content_length(1, 20).serialize())
        connection.receive_data(data_frame(1, b"a" * 8).serialize())

        assert not connection.exchange_finished()
        assert not connection.body_boundaries_exact()
        with pytest.raises(h2.exceptions.BoundaryViolationError):
            h2.pool.assert_reusable(connection)

    def test_unread_upstream_bytes_block_reuse_even_when_body_complete(self) -> None:
        connection = make_server()
        connection.enable_boundary_diagnostics("leftover")
        connection.receive_data(PREAMBLE)
        connection.receive_data(post_with_content_length(1, 4, end_stream=True).serialize())
        connection.receive_data(data_frame(1, b"body", end_stream=True).serialize())

        # A whole extra frame arrives that has not been interpreted yet: this
        # models the head of the next response waiting in the socket buffer.
        trailing = data_frame(3, b"stray").serialize()
        connection.incoming_buffer.add_data(trailing)

        assert connection.body_boundaries_exact()
        assert connection.upstream_remainder().remaining_bytes == len(trailing)
        with pytest.raises(h2.exceptions.BoundaryViolationError) as exc_info:
            h2.pool.assert_reusable(connection)
        assert exc_info.value.kind == "buffer-cleanup"


class TestConcurrentConnectionsIsolation:
    """
    Two connections parsing interleaved traffic must not read each other's
    bodies, even when their reads are interleaved byte by byte.
    """

    def test_interleaved_connections_do_not_cross_deliver(self) -> None:
        first = make_server()
        second = make_server()
        first.enable_boundary_diagnostics("conn-1")
        second.enable_boundary_diagnostics("conn-2")

        first_wire = (
            PREAMBLE
            + headers_frame(1, [*POST_HEADERS, (b"content-length", b"10")]).serialize()
            + data_frame(1, b"AAAAABBBBB").serialize()
        )
        second_wire = (
            PREAMBLE
            + headers_frame(1, [*POST_HEADERS, (b"content-length", b"10")]).serialize()
            + data_frame(1, b"CCCCCDDDDD").serialize()
        )

        first_events: list[h2.events.Event] = []
        second_events: list[h2.events.Event] = []
        for index in range(max(len(first_wire), len(second_wire))):
            if index < len(first_wire):
                first_events.extend(first.receive_data(first_wire[index:index + 1]))
            if index < len(second_wire):
                second_events.extend(second.receive_data(second_wire[index:index + 1]))

        first_body = b"".join(
            e.data for e in first_events if isinstance(e, h2.events.DataReceived)
        )
        second_body = b"".join(
            e.data for e in second_events if isinstance(e, h2.events.DataReceived)
        )
        assert first_body == b"AAAAABBBBB"
        assert second_body == b"CCCCCDDDDD"
        assert first.body_boundary(1).confirmed == 10
        assert second.body_boundary(1).confirmed == 10

    def test_each_connection_records_its_own_id(self) -> None:
        first = make_server()
        second = make_server()
        first_log = first.enable_boundary_diagnostics("alpha")
        second_log = second.enable_boundary_diagnostics("beta")
        first.receive_data(PREAMBLE)
        second.receive_data(PREAMBLE)

        assert all(e.connection_id == "alpha" for e in first_log.events)
        assert all(e.connection_id == "beta" for e in second_log.events)
        assert first.connection_id == "alpha"
        assert second.connection_id == "beta"


class TestDiagnosticsAreOptIn:
    """
    Diagnostics must not change public behaviour or cost when disabled.
    """

    def test_disabled_by_default(self) -> None:
        connection = make_server()
        assert connection.boundary_log is None
        assert connection.connection_id is None
        connection.receive_data(PREAMBLE)
        assert connection.body_boundary(1) is None
        assert connection.upstream_remainder().remaining_bytes == 0

    def test_enable_returns_log_with_default_id(self) -> None:
        connection = make_server()
        log = connection.enable_boundary_diagnostics()
        assert connection.boundary_log is log
        assert connection.connection_id is not None
        assert log.connection_id == connection.connection_id


class TestFrameBufferSnapshots:
    """
    FrameBuffer exposes its state at fragment boundaries without trusting a
    declared length.
    """

    def test_empty_buffer_snapshot(self) -> None:
        buffer = h2.frame_buffer.FrameBuffer()
        snapshot = buffer.snapshot()
        assert snapshot.stage == "idle"
        assert snapshot.buffered_bytes == 0
        assert snapshot.declared_length is None
        assert snapshot.consumed_bytes == 0

    def test_partial_header_snapshot(self) -> None:
        buffer = h2.frame_buffer.FrameBuffer()
        buffer.add_data(b"\x00\x00\x00")
        snapshot = buffer.snapshot()
        assert snapshot.stage == "frame-header"
        assert snapshot.buffered_bytes == 3

    def test_partial_body_snapshot(self) -> None:
        buffer = h2.frame_buffer.FrameBuffer()
        buffer.max_frame_size = 16384
        buffer.add_data(data_frame(1, b"abcdef").serialize()[:12])
        snapshot = buffer.snapshot()
        assert snapshot.declared_length == 6
        assert snapshot.received_bytes == 3
        assert snapshot.pending_body_bytes == 3
        assert snapshot.stage == "frame-body"

    def test_consumed_bytes_track_parsing(self) -> None:
        buffer = h2.frame_buffer.FrameBuffer()
        buffer.max_frame_size = 16384
        wire = data_frame(1, b"hi").serialize()
        buffer.add_data(wire)
        next(iter(buffer))
        assert buffer.consumed_bytes == len(wire)
        assert buffer.frames_parsed == 1

    def test_finish_on_pristine_buffer_is_clean(self) -> None:
        h2.frame_buffer.FrameBuffer().finish()

    def test_header_block_fragment_count(self) -> None:
        buffer = h2.frame_buffer.FrameBuffer()
        buffer.max_frame_size = 16384
        head = HeadersFrame(1)
        head.data = b"\x82"
        buffer.add_data(head.serialize())
        list(buffer)
        snapshot = buffer.snapshot()
        assert snapshot.stage == "header-block"
        assert snapshot.header_block_fragments == 1

        continuation = ContinuationFrame(1)
        continuation.flags.add("END_HEADERS")
        continuation.data = b"\x84"
        buffer.add_data(continuation.serialize())
        frames = list(buffer)
        assert len(frames) == 1
        assert buffer.snapshot().stage == "idle"


class TestDiagnosticRecordingEdges:
    """
    The optional diagnostics hooks must behave correctly for the less common
    paths: explicit retirement, transport failures, and frames that arrive
    while an overrun boundary is being recorded.
    """

    def test_retire_records_ownership_event(self) -> None:
        connection = make_server()
        log = connection.enable_boundary_diagnostics("retire")
        connection.receive_data(PREAMBLE)
        connection.retire("operator-decision")
        assert connection.retired
        assert connection.retire_reason == "operator-decision"
        assert log.events[-1].stage is h2.boundary.ReceiveStage.OWNERSHIP
        assert log.events[-1].detail == "operator-decision"

    def test_retire_is_idempotent_and_keeps_first_reason(self) -> None:
        connection = make_server()
        connection.enable_boundary_diagnostics("retire-twice")
        connection.receive_data(PREAMBLE)
        connection.retire("first")
        connection.retire("second")
        assert connection.retire_reason == "first"

    def test_retire_without_diagnostics_is_harmless(self) -> None:
        connection = make_server()
        connection.retire("no-log")
        assert connection.retired

    def test_record_transport_failure_classifies_timeout(self) -> None:
        connection = make_server()
        log = connection.enable_boundary_diagnostics("timeout")
        connection.receive_data(PREAMBLE)
        failure = connection.record_transport_failure(TimeoutError("slow"))
        assert failure is h2.boundary.FailureClass.TIMEOUT
        assert connection.retired
        assert connection.retire_reason == "timeout"
        failure_events = [e for e in log.events if e.failure is not None]
        assert failure_events
        assert failure_events[0].failure is h2.boundary.FailureClass.TIMEOUT
        assert log.events[-1].detail == "retired-after-transport-failure"

    def test_record_transport_failure_classifies_other_errors(self) -> None:
        connection = make_server()
        connection.enable_boundary_diagnostics("transport")
        failure = connection.record_transport_failure(ConnectionResetError())
        assert failure is h2.boundary.FailureClass.INTERNAL_ERROR
        assert connection.retire_reason == "transport-error"

    def test_record_transport_failure_without_diagnostics(self) -> None:
        connection = make_server()
        failure = connection.record_transport_failure(TimeoutError())
        assert failure is h2.boundary.FailureClass.TIMEOUT
        assert connection.retired

    def test_failure_stage_is_recorded_for_invalid_padding(self) -> None:
        connection = make_server()
        log = connection.enable_boundary_diagnostics("padding")
        connection.receive_data(PREAMBLE)
        connection.receive_data(headers_frame(1, GET_HEADERS).serialize())

        # A PADDED DATA frame whose pad length exceeds its body.
        frame = DataFrame(1)
        frame.data = b""
        frame.flags.add("PADDED")
        frame.pad_length = 5
        wire = bytearray(frame.serialize())
        # Force a pad length larger than the remaining body.
        wire[9] = 200

        with pytest.raises(h2.exceptions.ProtocolError):
            connection.receive_data(bytes(wire))

        failure_events = [e for e in log.events if e.failure is not None]
        assert failure_events
        assert connection.retired

    def test_dispatch_complete_records_stage(self) -> None:
        connection = make_server()
        log = connection.enable_boundary_diagnostics("dispatch")
        connection.receive_data(PREAMBLE)
        connection.receive_data(headers_frame(1, GET_HEADERS).serialize())
        dispatch = [
            e for e in log.events
            if e.stage is h2.boundary.ReceiveStage.FRAME_DISPATCH
        ]
        assert dispatch
        assert "event(s)" in dispatch[-1].detail


class TestBoundaryLogFormattingAndIteration:
    """
    Log iteration and error formatting expose the recorded facts.
    """

    def test_iteration_and_repr(self) -> None:
        log = h2.boundary.BoundaryEventLog("iter")
        log.record(h2.boundary.ReceiveStage.BUFFER_APPEND, bytes_read=1)
        assert list(iter(log)) == list(log.events)
        assert "events:1" in repr(log)

    def test_frames_counter(self) -> None:
        log = h2.boundary.BoundaryEventLog()
        log.record(h2.boundary.ReceiveStage.FRAME_DISPATCH)
        log.record(h2.boundary.ReceiveStage.FRAME_DISPATCH)
        assert log.frames == 2

    def test_redeclare_existing_boundary_keeps_counts(self) -> None:
        log = h2.boundary.BoundaryEventLog()
        log.declare_boundary(1, 10)
        log.record(
            h2.boundary.ReceiveStage.BODY,
            stream_id=1,
            bytes_read=4,
            confirmed_length=4,
            declared_length=10,
        )
        redeclared = log.declare_boundary(1, 10)
        assert redeclared.confirmed == 4

    def test_raw_reconstruction_handles_discarded_bytes(self) -> None:
        log = h2.boundary.BoundaryEventLog.from_dicts("raw", [
            {
                "seq": 0,
                "timestamp": 0.0,
                "connection_id": "raw",
                "stage": "body",
                "stream_id": 1,
                "bytes_read": 8,
                "declared_length": 4,
                "confirmed_length": 4,
                "discarded_bytes": 4,
                "buffer_before": 0,
                "buffer_after": 0,
                "consumed_offset": 8,
                "remaining_length": 0,
                "failure": "boundary-error",
                "detail": "body-overflow",
            },
        ])
        boundary = log.boundary(1)
        assert boundary is not None
        assert boundary.discarded == 4
        assert log.events[0].failure is h2.boundary.FailureClass.BOUNDARY_ERROR

    def test_boundary_violation_message_includes_location(self) -> None:
        error = h2.exceptions.BoundaryViolationError(
            "boom",
            stage="reuse",
            kind="buffer-cleanup",
            connection_id="conn-7",
            stream_id=3,
        )
        message = str(error)
        assert "conn-7" in message
        assert "stream 3" in message
        assert "reuse" in message
        assert "buffer-cleanup" in message

    def test_boundary_violation_message_without_location(self) -> None:
        error = h2.exceptions.BoundaryViolationError(
            "boom", stage="reuse", kind="retired",
        )
        assert "on connection" not in str(error)

    def test_truncated_frame_message_and_stage_override(self) -> None:
        error = h2.exceptions.TruncatedFrameError(
            declared_length=4, received_length=2, offset=9, stage="header-block",
        )
        assert error.stage == "header-block"
        assert "4" in str(error)
        assert "2" in str(error)

    def test_truncated_frame_unknown_declared_length(self) -> None:
        error = h2.exceptions.TruncatedFrameError()
        assert "unknown" in str(error)


class TestSnapshotPendingBytes:
    """
    A snapshot with no declared length needs no pending body bytes.
    """

    def test_pending_body_bytes_zero_without_declaration(self) -> None:
        state = h2.frame_buffer.FrameBufferState(
            buffered_bytes=3,
            declared_length=None,
            received_bytes=0,
            stage="frame-header",
            header_block_fragments=0,
            consumed_bytes=0,
        )
        assert state.pending_body_bytes == 0

    def test_exchange_finished_requires_end_stream_in_both_directions(self) -> None:
        connection = make_server()
        connection.receive_data(PREAMBLE)
        connection.receive_data(
            headers_frame(1, GET_HEADERS, ["END_STREAM"]).serialize(),
        )
        # The request ended, but we have not sent a response at all.
        assert not connection.exchange_finished()


class TestPreambleAccounting:
    """
    A connection that has not finished receiving the client preface is not
    finished, even though nothing has been parsed into the buffer yet.
    """

    def test_partial_preamble_is_outstanding_input(self) -> None:
        connection = make_server()
        connection.receive_data(PREAMBLE[:11])
        remainder = connection.upstream_remainder()
        assert remainder.remaining_bytes == len(PREAMBLE) - 11
        assert remainder.stage == "preamble"
        assert connection.incoming_buffer.pending_preamble_bytes == (
            len(PREAMBLE) - 11
        )

    def test_complete_preamble_leaves_no_remainder(self) -> None:
        connection = make_server()
        connection.receive_data(PREAMBLE)
        assert connection.incoming_buffer.pending_preamble_bytes == 0
        assert connection.upstream_remainder().remaining_bytes == 0

    def test_partial_preamble_blocks_reuse(self) -> None:
        connection = make_server()
        connection.receive_data(PREAMBLE[:11])
        with pytest.raises(h2.exceptions.BoundaryViolationError):
            h2.pool.assert_reusable(connection)


class TestConsumedOffsetAccounting:
    """
    The log's consume offset counts input bytes exactly once, so it can be
    compared with an offset into the raw capture during a before/after
    comparison. A DATA frame that is both appended and confirmed must not
    advance the offset twice.
    """

    def test_offset_matches_wire_bytes(self) -> None:
        connection = make_server()
        log = connection.enable_boundary_diagnostics("offset")
        wire = PREAMBLE + headers_frame(1, GET_HEADERS).serialize() + (
            data_frame(1, b"hello", end_stream=True).serialize()
        )
        connection.receive_data(wire)
        assert log.consumed_offset == len(wire)

    def test_offset_matches_wire_bytes_when_fragmented(self) -> None:
        connection = make_server()
        log = connection.enable_boundary_diagnostics("offset-split")
        connection.receive_data(PREAMBLE)
        wire = headers_frame(1, GET_HEADERS).serialize() + (
            data_frame(1, b"hello", end_stream=True).serialize()
        )
        for index in range(len(wire)):
            connection.receive_data(wire[index:index + 1])
        assert log.consumed_offset == len(PREAMBLE) + len(wire)
