from __future__ import annotations

import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from .consumer import Consumer
from .offset import CommitResult
from .partition import Message, Partition
from .rebalance import AuditEvent, assign_partitions, audit_single_owner


class ConsumerGroup:
    def __init__(
        self,
        group_id: str,
        partitions: dict[str, int] | list[Partition],
        consumer_ids: list[str],
    ) -> None:
        self.group_id = group_id
        if isinstance(partitions, dict):
            self.partitions: dict[str, Partition] = {
                partition_id: Partition(partition_id, count)
                for partition_id, count in partitions.items()
            }
        else:
            self.partitions = {
                partition.partition_id: partition for partition in partitions
            }
        self._consumers: dict[str, Consumer] = {}
        self._ownership: dict[str, str] = {}
        self._audit: list[AuditEvent] = []
        self._seq = 0
        self._lock = threading.RLock()
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._pending_commits: list[Future[CommitResult]] = []
        self._closed = False
        for consumer_id in consumer_ids:
            self.add_consumer(consumer_id)
        initial = assign_partitions(list(self.partitions), consumer_ids)
        for consumer_id, owned in initial.items():
            for partition_id in owned:
                self._assign(consumer_id, partition_id, "initial assignment")

    @property
    def audit_log(self) -> list[AuditEvent]:
        with self._lock:
            return list(self._audit)

    @property
    def total_deliveries(self) -> int:
        return sum(
            len(partition.delivered_offsets()) + len(partition.replayed)
            for partition in self.partitions.values()
        )

    @property
    def total_replays(self) -> int:
        return sum(len(partition.replayed) for partition in self.partitions.values())

    def partition(self, partition_id: str) -> Partition:
        return self.partitions[partition_id]

    def consumer(self, consumer_id: str) -> Consumer:
        return self._consumers[consumer_id]

    def add_consumer(self, consumer_id: str) -> Consumer:
        with self._lock:
            if consumer_id in self._consumers:
                raise ValueError(f"consumer {consumer_id} already exists")
            consumer = Consumer(consumer_id, self)
            self._consumers[consumer_id] = consumer
            return consumer

    def assignment_for(self, consumer_id: str) -> list[str]:
        with self._lock:
            return [
                partition_id
                for partition_id, owner in self._ownership.items()
                if owner == consumer_id
            ]

    def owner_of(self, partition_id: str) -> str | None:
        with self._lock:
            return self._ownership.get(partition_id)

    def assignment_snapshot(self) -> dict[str, str | None]:
        with self._lock:
            return dict(self._ownership)

    def _emit(
        self,
        event: str,
        partition_id: str,
        consumer_id: str | None,
        reason: str,
    ) -> AuditEvent:
        self._seq += 1
        audit_event = AuditEvent(
            seq=self._seq,
            timestamp=time.monotonic(),
            event=event,
            partition_id=partition_id,
            consumer_id=consumer_id,
            reason=reason,
        )
        self._audit.append(audit_event)
        return audit_event

    def _require_owner(self, consumer_id: str, partition_id: str) -> None:
        owner = self._ownership.get(partition_id)
        if owner is None:
            raise RuntimeError(f"partition {partition_id} has no owner")
        if owner != consumer_id:
            raise PermissionError(
                f"consumer {consumer_id} does not own partition "
                f"{partition_id}; owner is {owner}"
            )

    def _assign(
        self, consumer_id: str, partition_id: str, reason: str
    ) -> None:
        with self._lock:
            if partition_id in self._ownership:
                raise RuntimeError(
                    f"partition {partition_id} is already owned by "
                    f"{self._ownership[partition_id]}"
                )
            self._ownership[partition_id] = consumer_id
            self._emit("assign", partition_id, consumer_id, reason)

    def _revoke(
        self, consumer_id: str, partition_id: str, reason: str
    ) -> None:
        with self._lock:
            self._require_owner(consumer_id, partition_id)
            del self._ownership[partition_id]
            self._emit("revoke", partition_id, consumer_id, reason)

    def deliver(self, consumer_id: str, partition_id: str) -> Message:
        with self._lock:
            self._require_owner(consumer_id, partition_id)
            return self.partitions[partition_id].consume_next()

    def commit(
        self,
        consumer_id: str,
        partition_id: str,
        offset: int,
        asynchronous: bool = False,
    ) -> CommitResult:
        with self._lock:
            self._require_owner(consumer_id, partition_id)
            partition = self.partitions[partition_id]
            if not asynchronous:
                return partition.tracker.commit(offset)

            def apply_commit() -> CommitResult:
                with self._lock:
                    self._require_owner(consumer_id, partition_id)
                    return partition.tracker.commit(offset)

            future = self._executor.submit(apply_commit)
        with self._lock:
            self._pending_commits.append(future)
        return future.result()

    def wait_for_commits(self) -> list[CommitResult]:
        with self._lock:
            futures = list(self._pending_commits)
            self._pending_commits.clear()
        return [future.result() for future in futures]

    def rebalance(self, reason: str = "rebalance") -> None:
        with self._lock:
            active_consumers = list(self._consumers)
            if not active_consumers:
                raise RuntimeError("cannot rebalance without active consumers")
            for partition_id, consumer_id in list(self._ownership.items()):
                self._revoke(consumer_id, partition_id, reason)
                self.partitions[partition_id].seek_to_committed()
            new_assignment = assign_partitions(
                list(self.partitions), active_consumers
            )
            for consumer_id, owned in new_assignment.items():
                for partition_id in owned:
                    self._assign(consumer_id, partition_id, reason)

    def leave(self, consumer_id: str, graceful: bool = False) -> None:
        with self._lock:
            if consumer_id not in self._consumers:
                raise ValueError(f"unknown consumer {consumer_id}")
            owned = self.assignment_for(consumer_id)
            if graceful:
                for partition_id in owned:
                    partition = self.partitions[partition_id]
                    partition.tracker.commit(partition.tracker.consumed_to)
            for partition_id in owned:
                self._revoke(consumer_id, partition_id, "consumer leaving")
                self.partitions[partition_id].seek_to_committed()
            del self._consumers[consumer_id]
            if self._consumers:
                reassignment = assign_partitions(owned, list(self._consumers))
                for new_owner, partition_ids in reassignment.items():
                    for partition_id in partition_ids:
                        self._assign(
                            new_owner,
                            partition_id,
                            f"reassigned after {consumer_id} left",
                        )

    def shutdown(self, consumer_id: str) -> None:
        self.wait_for_commits()
        self.leave(consumer_id, graceful=True)

    def shutdown_all(self) -> None:
        if self._closed:
            return
        self.wait_for_commits()
        with self._lock:
            consumer_ids = list(self._consumers)
        for consumer_id in consumer_ids:
            if consumer_id in self._consumers:
                self.shutdown(consumer_id)
        self._executor.shutdown(wait=True)
        self._closed = True

    def lag(self, partition_id: str) -> int:
        return self.partitions[partition_id].lag()

    def audit_violations(self) -> list[str]:
        return audit_single_owner(self.audit_log)

    def run_script(self, steps: list[dict[str, Any]]) -> dict[str, Any]:
        for index, step in enumerate(steps):
            action = step.get("action")
            consumer_id = step.get("consumer")
            partition_id = step.get("partition")
            if action in {"consume", "consume_next"}:
                self.consumer(consumer_id).consume_next(partition_id)
            elif action == "consume_rest":
                self.consumer(consumer_id).consume_rest(partition_id)
            elif action == "commit":
                result = self.consumer(consumer_id).commit(
                    partition_id,
                    step["offset"],
                    asynchronous=bool(step.get("async", False)),
                )
                if not result.accepted and step.get("require_accepted", True):
                    raise RuntimeError(
                        f"script step {index} rejected: {result.reason}"
                    )
            elif action == "commit_rest":
                result = self.consumer(consumer_id).commit_rest(
                    partition_id,
                    asynchronous=bool(step.get("async", False)),
                )
                if not result.accepted and step.get("require_accepted", True):
                    raise RuntimeError(
                        f"script step {index} rejected: {result.reason}"
                    )
            elif action == "leave":
                self.leave(
                    consumer_id,
                    graceful=bool(step.get("graceful", False)),
                )
            elif action == "shutdown":
                self.shutdown(consumer_id)
            elif action == "rebalance":
                self.rebalance(step.get("reason", "manual rebalance"))
            elif action == "wait_commits":
                self.wait_for_commits()
            else:
                raise ValueError(f"unknown script action: {action!r}")
        self.wait_for_commits()
        return {
            "deliveries": self.total_deliveries,
            "replays": self.total_replays,
            "violations": self.audit_violations(),
        }
