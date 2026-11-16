# consumgrp

`consumgrp` is a standard-library-only Python simulation of a Kafka-like
consumer group. It models per-partition logs, contiguous offset commits,
rebalance replay, single-owner auditing, lag, graceful shutdown and reports.

## Offset semantics

Each partition owns an independent `OffsetTracker`.

- Offsets are zero-based and identify one message.
- `committed_offset` uses the next-offset convention: offsets `0` through
  `committed_offset - 1` are the committed contiguous prefix.
- `consumed_to` is the end of the in-memory consumed prefix. A message at
  offset `3` extends it from `3` to `4`.
- `lag = log_end_offset - committed_offset`.
- Rebalance moves the durable partition state to the new owner, then seeks the
  read position back to `committed_offset`. Messages at and after that offset
  are eligible for replay.

Example: after consuming offsets `0,1,2,3` and committing offset `3`, offsets
`0,1,2` are committed and offset `3` is consumed but uncommitted. If the owner
then leaves, the next owner starts at offset `3`, producing replay id `p0#3`.

## Commit rules

A commit to offset `n` is accepted only when:

- `n >= committed_offset`, so commits cannot move backwards; and
- `n <= consumed_to`, so every offset below `n` has been consumed and there is
  no hole in the consumed prefix.

A rejected commit returns `CommitResult(accepted=False, ...)` and leaves the
old committed offset unchanged. Synchronous commits apply immediately.
Asynchronous commits are submitted through `ThreadPoolExecutor`; call
`wait_for_commits()` before shutdown to ensure every submitted commit is
durable.

## Rebalance flow

1. The group verifies that the requesting consumer currently owns a partition.
2. An audit `revoke` event is appended and ownership is removed.
3. The partition read position is reset to its committed offset.
4. Surviving consumers receive a stable contiguous assignment.
5. An audit `assign` event is appended before the new owner can consume.

Committed offsets are stored with the partition rather than the consumer, so
they transfer automatically. Uncommitted consumed messages are not marked
committed and therefore replay exactly once when the new owner reads them.

## Audit log fields

Each event contains:

- `seq`: monotonic event number.
- `timestamp`: monotonic event time.
- `event`: `assign` or `revoke`.
- `partition_id`: affected partition.
- `consumer_id`: owner receiving or losing the partition.
- `reason`: why the ownership transition happened.

An `assign` opens an ownership interval and a matching `revoke` closes it.
`audit_single_owner()` reports an assignment while another interval is open,
or a revoke without a matching active owner. Open intervals at the end simply
represent the current valid ownership snapshot.

## Graceful shutdown

`ConsumerGroup.shutdown(consumer_id)` waits for asynchronous commits, commits
each currently consumed prefix, revokes the consumer's partitions and
reassigns them to surviving consumers. The CLI installs `SIGINT` and
`SIGTERM` handlers that run this shutdown path before writing the report.

## Demo

Run:

```bash
python -m consumgrp.cli --partitions demo/offsets.json --consumers 2 --out out/report.json
```

The demo has four partitions with 20 messages each. Consumer A owns `p0` and
`p1`; consumer B owns `p2` and `p3`. A consumes `p0#0` through `p0#3`, commits
offset `3`, then leaves. B receives `p0` and `p1` and replays only `p0#3`.

The stdout summary is one line containing partition count, total deliveries,
duplicate count and every partition's committed offset. The JSON report contains
`committed_offset`, `lag` and `replayed` for every partition.

## Tests

```bash
cd tests
python -m unittest test_consumgrp -v
```
