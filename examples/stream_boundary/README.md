# Stream boundary & buffer ownership diagnostics

This directory contains an **offline, deterministic** reproduction of the
failure where a gateway splices one exchange's unfinished bytes into the next
one, plus the diagnostics that make the failure attributable.

Nothing here opens a socket or starts a process: a "gateway" is modelled with
`h2.connection.H2Connection` objects whose byte streams are moved by hand, so
every step is inspectable and replayable.

## Run it

```bash
# from the repository root
python examples/stream_boundary/stream_boundary_replay.py
```

The script prints a JSON reproduction report with one section per scenario:

| Section | What it shows |
| --- | --- |
| `cross-talk refused` | An upstream connection holding 37 unconsumed bytes is offered back to the pool. The pool refuses it, records the `buffer-cleanup` layer, and retires the connection. The report also shows what the *unguarded* run would have done: interpret those bytes as a `RequestReceived` on stream 3. |
| `body boundary overflow` | A 16-byte declared body receives 24 bytes. The event table records 10 confirmed bytes, then 8 discarded bytes at the `body` stage with a `boundary-error` failure — before the exception ever reaches the caller. |
| `replay equivalence` | One capture is fed whole, byte by byte, and in seeded random fragments. All three deliver identical events; any divergence would be reported with the receive step it appeared at. |
| `replay record` | A capture is stored with its boundary event table and re-run under a different fragmentation, confirming the delivered envelope is unchanged. |

## The accounting split

The two numbers that must never be conflated are kept in separate types:

- `h2.boundary.BodyBoundary` — the *declared* body length for one stream
  (`content-length`), how many bytes were *confirmed* against it, and how many
  were *discarded*. It never contains information about unrelated buffered
  bytes.
- `h2.boundary.UpstreamRemainder` — bytes physically buffered on a connection
  that nobody has consumed yet. It never contains a declared length.

A gateway that decides reuse safety from either one alone is exactly how one
request's body ends up in front of the next request's data. `h2.pool` checks
both, plus both directions and the version/timeout configuration.

## The event table

`h2.boundary.BoundaryEventLog` records every receive step as one row:
bytes read, declared and confirmed length, discarded bytes, buffer occupancy
before and after, the absolute consume offset, the stage, and the connection
that owned the buffer. `H2Connection.enable_boundary_diagnostics(id)` turns it
on. The table is written where a failure happens, so the phase is part of the
data rather than something inferred from the final exception.

## Replay

`h2.diagnostics` provides three feeding schedules (`whole`, `bytewise`,
`random`) and `compare_schedules`, which reports the first receive step at
which two schedules disagree. Divergences include the step index, both
schedules' buffer occupancies and stages, and the event counts, so a report can
name the receive step instead of the symptom.

## Related tests

- `tests/test_stream_boundary_ownership.py` — boundary accounting, fragment
  boundaries (including a boundary exactly inside a frame's length field),
  truncation, overflow, and concurrent-connection isolation.
- `tests/test_connection_reuse_and_replay.py` — pool reuse decisions and the
  replay harness.
- `tests/test_boundary_diagnostics_coverage.py` — the less common pool and
  divergence paths.
