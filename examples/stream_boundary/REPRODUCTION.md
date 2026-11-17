# Stream boundary ownership: reproduction report

This document is the before/after comparison sheet for the cross-talk defect
and the regression fixtures that pin it down. It lists connection IDs, request
order and byte offsets so that a run can be compared directly with a run of the
fixed code.

Everything below is produced offline by `examples/stream_boundary/stream_boundary_replay.py`
and asserted by the test suite; no real network traffic is involved.

## Defect summary

A connection whose previous exchange was not finished in *both* directions can
be offered back for reuse. Its receive buffer still holds bytes belonging to
the unfinished exchange. The next requester then reads those bytes as if they
were its own input, so a concurrent request receives another request's data.

Two independent quantities were being conflated:

- the declared length of a body (`content-length`) and how much of it was
  confirmed;
- the number of bytes physically still in the connection's buffer.

A connection is only reusable when **both** say it is: every declared body
length exactly confirmed with nothing discarded, *and* nothing left in the
buffer, *and* neither direction still open.

## Reproduction 1 — cross-talk refused

| Field | Value |
| --- | --- |
| Connection ID | `upstream-1` |
| Request order | stream 1 (finished), stream 3 (leftover bytes) |
| Bytes consumed before check-in | 52 |
| Leftover bytes in buffer | 37 |
| Leftover stage | `frame-body` |
| Reuse accepted | **no** |
| Retire reason | `buffer-cleanup` |

Pool events:

| seq | kind | reason | detail |
| --- | --- | --- | --- |
| 0 | `denied` | `buffer-cleanup` | 37 unconsumed byte(s) in stage `frame-body` |
| 1 | `retire` | `buffer-cleanup` | `buffer-cleanup` |

The same bytes handed to the unguarded connection are interpreted as a
`RequestReceived` on stream 3 (with a following `StreamEnded`). That is the
leak: on this connection the streams belong to an exchange that already
completed, so the stray request must never be interpreted here.

## Reproduction 2 — body boundary overflow

| Field | Value |
| --- | --- |
| Connection ID | `boundary-1` |
| Declared body length | 16 |
| Confirmed before overflow | 10 |
| Error | `InvalidBodyLengthError` (`kind=body-overflow`) |
| Discarded bytes recorded | 8 |
| Retire reason | `body-boundary` |

Event rows (extract):

| seq | stage | stream | declared | confirmed | discarded | offset | failure |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 7 | `body` | 1 | 16 | 10 | 0 | 75 | – |
| 11 | `body` | 1 | 16 | 0 | 8 | 98 | `boundary-error` |
| 12 | `failure` | – | – | 0 | 0 | 98 | `boundary-error` |

The discarded count is recorded in the row where the boundary was crossed, so
the phase is known without reading the exception message.

## Reproduction 3 — fragmentation equivalence

Capture: one POST with a 16-byte declared body (81 bytes of wire data).

| Schedule | Calls | Events | Delivered bytes | Result |
| --- | --- | --- | --- | --- |
| `whole` | 1 | 3 | 16 | ok |
| `bytewise` | 81 | 3 | 16 | ok |
| `random` (seed 1234) | 14 | 3 | 16 | ok |

All three schedules deliver the same event envelope. A divergence would name
the schedule pair, the receive step index, both buffer occupancies and both
stages.

## Regression fixtures

| File | Covers |
| --- | --- |
| `tests/test_stream_boundary_ownership.py` | declared-vs-confirmed accounting, buffer remainder, event table round-trip, fragment boundaries including one exactly inside a frame's length field, truncation stages, body overflow/underflow, request body unfinished before the next connection, concurrent connections not cross-delivering, preamble accounting. |
| `tests/test_connection_reuse_and_replay.py` | pool accept/refuse for each layer (`version-lock`, `timeout-config`, `buffer-cleanup`, `direction-open`, `body-boundary`, `retired`), re-check on checkout, replay schedules, failure attribution, divergence location, replay records. |
| `tests/test_boundary_diagnostics_coverage.py` | less common pool paths (discarded bytes, unbounded bodies), divergence branches, and the behaviour of the shipped example script. |

## How to compare before and after a fix

1. Run `python examples/stream_boundary/stream_boundary_replay.py` and keep the
   JSON output.
2. Apply the fix.
3. Re-run and compare the `connection_id`, `request_order`, `leftover_bytes`,
   `consumed_offset_at_checkin` and the per-event `consumed_offset` values.
   The offsets are absolute byte offsets into the capture, so they line up
   exactly with a byte-level diff of the wire traffic.
