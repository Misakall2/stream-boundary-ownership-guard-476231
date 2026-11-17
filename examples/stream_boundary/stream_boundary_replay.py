#!/usr/bin/env python3
"""
stream_boundary_replay.py
~~~~~~~~~~~~~~~~~~~~~~~~~

An offline, deterministic reproduction of the "unfinished upstream body gets
spliced into the next request" failure, with the boundary/ownership
diagnostics that make it attributable.

Nothing here touches a socket, a subprocess, or the network. A "gateway" is
modelled as a pair of :class:`H2Connection <h2.connection.H2Connection>`
objects whose byte streams are moved around by hand, so every step is
inspectable and replayable.

The script demonstrates three things:

1. **Cross-talk.** An upstream response whose body was only partially
   consumed is offered back to the pool. The pool refuses it and records the
   layer that blocked reuse (``buffer-cleanup``), instead of handing the
   connection to the next requester.
2. **Boundary accounting.** The request-body length boundary and the
   remaining buffered bytes are recorded separately, with the bytes read,
   the confirmed length and the discarded bytes in one event table.
3. **Replay equivalence.** The same capture is fed whole, byte by byte and in
   seeded random fragments; the delivered result must be identical, and any
   divergence is reported at the receive step where it appears.

Run it with::

    python examples/stream_boundary/stream_boundary_replay.py

It prints a reproduction report listing connection IDs, request order and
byte offsets, which can be compared before and after a fix.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

if __package__ in {None, ""}:  # pragma: no cover - script-style execution
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from hpack import Encoder
from hyperframe.frame import DataFrame, HeadersFrame

import h2.config
import h2.connection
import h2.diagnostics
import h2.events
import h2.pool

PREAMBLE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"


def server_connection(connection_id: str) -> h2.connection.H2Connection:
    """
    Build a server-side connection with diagnostics turned on.
    """
    config = h2.config.H2Configuration(client_side=False)
    connection = h2.connection.H2Connection(config=config)
    connection.initiate_connection()
    connection.clear_outbound_data_buffer()
    connection.enable_boundary_diagnostics(connection_id)
    return connection


def replay_connection() -> h2.connection.H2Connection:
    """
    Build a fresh server-side connection for the replay harness.
    """
    config = h2.config.H2Configuration(client_side=False)
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


def upload_capture() -> bytes:
    """
    The wire bytes of one complete POST with a declared body length.
    """
    return (
        PREAMBLE
        + headers_frame(
            1,
            [
                (b":method", b"POST"),
                (b":path", b"/upload"),
                (b":scheme", b"https"),
                (b":authority", b"example.com"),
                (b"content-length", b"16"),
            ],
        ).serialize()
        + data_frame(1, b"0123456789abcdef", end_stream=True).serialize()
    )


def _prime_unfinished_upstream() -> h2.connection.H2Connection:
    """
    Build an upstream connection with one finished request and a response
    whose body has only partly been consumed.

    The request is received with ``END_STREAM``; the response is sent with
    ``END_STREAM``; the beginning of the *next* request's HEADERS block is
    appended to the receive buffer without being interpreted. That leftover
    is what must never reach the next requester: on the wire it belongs to a
    different exchange, but physically it is sitting in this connection's
    buffer.
    """
    upstream = server_connection("upstream-1")
    upstream.receive_data(PREAMBLE)
    upstream.receive_data(
        headers_frame(
            1,
            [
                (b":method", b"GET"),
                (b":path", b"/large"),
                (b":scheme", b"https"),
                (b":authority", b"example.com"),
            ],
            ["END_STREAM"],
        ).serialize(),
    )
    upstream.send_headers(
        1,
        [(b":status", b"200"), (b"content-length", b"16384")],
    )
    upstream.send_data(1, b"A" * 12000)
    upstream.send_data(1, b"A" * 4384, end_stream=True)
    upstream.clear_outbound_data_buffer()

    # Bytes belonging to the next exchange, already read off the socket but
    # not yet interpreted.
    stray_request = headers_frame(
        3,
        [
            (b":method", b"GET"),
            (b":path", b"/someone-elses-body"),
            (b":scheme", b"https"),
            (b":authority", b"example.com"),
        ],
        ["END_STREAM"],
    ).serialize()
    upstream.incoming_buffer.add_data(stray_request)
    return upstream


def demonstrate_cross_talk() -> dict[str, object]:
    """
    Show an unfinished exchange being refused reuse instead of leaking.

    Two runs are shown side by side:

    - the guarded one, where the pool refuses the connection and records
      the ``buffer-cleanup`` layer;
    - the unguarded one, where the leftover bytes are handed straight to the
      connection and are interpreted as a request on this connection.
    """
    guarded = _prime_unfinished_upstream()
    leftover = guarded.upstream_remainder()
    pool = h2.pool.ConnectionPool()
    accepted = pool.checkin(guarded)

    # What the unguarded run would have done: interpret the leftover bytes
    # as this connection's own traffic.
    unguarded = _prime_unfinished_upstream()
    stray_events = unguarded.receive_data(b"")

    return {
        "scenario": "cross-talk refused",
        "connection_id": guarded.connection_id,
        "request_order": [1, 3],
        "consumed_offset_at_checkin": guarded.boundary_log.consumed_offset,
        "leftover_bytes": leftover.remaining_bytes,
        "leftover_stage": leftover.stage,
        "reuse_accepted": accepted,
        "retire_reason": guarded.retire_reason,
        "pool_events": [event.as_dict() for event in pool.events],
        "unguarded_stray_events": [
            {
                "event": type(event).__name__,
                "stream_id": getattr(event, "stream_id", None),
            }
            for event in stray_events
        ],
        "unguarded_interpreted_stream_3": any(
            getattr(event, "stream_id", None) == 3 for event in stray_events
        ),
    }


def demonstrate_body_boundaries() -> dict[str, object]:
    """
    Show the request-body boundary and the buffered remainder kept apart.

    The peer declares a 16-byte body but sends 24. The boundary records 16
    confirmed bytes and 8 discarded ones; the discard count is what makes the
    overrun visible without reading the exception message.
    """
    connection = server_connection("boundary-1")
    connection.receive_data(PREAMBLE)
    connection.receive_data(
        headers_frame(
            1,
            [
                (b":method", b"POST"),
                (b":path", b"/upload"),
                (b":scheme", b"https"),
                (b":authority", b"example.com"),
                (b"content-length", b"16"),
            ],
        ).serialize(),
    )
    connection.receive_data(data_frame(1, b"x" * 10).serialize())

    before = connection.body_boundary(1)
    error_name = None
    try:
        connection.receive_data(data_frame(1, b"y" * 14).serialize())
    except h2.exceptions.H2Error as exc:  # type: ignore[attr-defined]
        error_name = type(exc).__name__

    log = connection.boundary_log
    assert log is not None
    return {
        "scenario": "body boundary overflow",
        "connection_id": connection.connection_id,
        "declared": None if before is None else before.declared,
        "confirmed_before_overflow": None if before is None else before.confirmed,
        "error": error_name,
        "retire_reason": connection.retire_reason,
        "events_table": [
            event.as_dict()
            for event in log.events
            if event.stage in {"body", "failure"}
        ],
    }


def demonstrate_replay_equivalence() -> dict[str, object]:
    """
    Feed one capture three ways and compare the delivered result.
    """
    capture = upload_capture()
    comparison = h2.diagnostics.compare_schedules(
        capture,
        connection_factory=replay_connection,
        connection_id="replay-1",
        seed=1234,
    )
    return {
        "scenario": "replay equivalence",
        "capture_bytes": len(capture),
        "consistent": comparison.consistent,
        "results": comparison.as_dict()["results"],
        "divergences": comparison.as_dict()["divergences"],
    }


def demonstrate_replay_record() -> dict[str, object]:
    """
    Store one replay and re-run it under a different fragmentation.
    """
    capture = upload_capture()
    result = h2.diagnostics.replay(
        capture,
        schedule=h2.diagnostics.FeedSchedule.WHOLE,
        connection_factory=replay_connection,
        connection_id="record-1",
    )
    record = h2.diagnostics.ReplayRecord.from_result(capture, result)
    replayed = h2.diagnostics.replay_record(
        record,
        connection_factory=replay_connection,
        schedule=h2.diagnostics.FeedSchedule.BYTEWISE,
        connection_id="record-1-replayed",
    )

    log = replayed.boundary_log
    assert log is not None
    return {
        "scenario": "replay record",
        "original_schedule": str(record.schedule),
        "replayed_schedule": str(replayed.schedule),
        "same_envelope": replayed.event_envelope() == result.event_envelope(),
        "record_events": len(record.boundary_events),
        "replayed_events": len(log.events),
        "delivered_bytes": replayed.delivered_bytes,
    }


def main() -> None:
    """
    Run every demonstration and print a reproduction report.
    """
    reports = [
        demonstrate_cross_talk(),
        demonstrate_body_boundaries(),
        demonstrate_replay_equivalence(),
        demonstrate_replay_record(),
    ]
    print(json.dumps(reports, indent=2, default=str))  # noqa: T201


if __name__ == "__main__":
    main()
