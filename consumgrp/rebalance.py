from __future__ import annotations

from dataclasses import dataclass
@dataclass(frozen=True)
class AuditEvent:
    seq: int
    timestamp: float
    event: str
    partition_id: str
    consumer_id: str | None
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {
            "seq": self.seq,
            "timestamp": self.timestamp,
            "event": self.event,
            "partition_id": self.partition_id,
            "consumer_id": self.consumer_id,
            "reason": self.reason,
        }


def assign_partitions(
    partition_ids: list[str], consumer_ids: list[str]
) -> dict[str, list[str]]:
    if not consumer_ids:
        raise ValueError("at least one consumer is required")
    assignment = {consumer_id: [] for consumer_id in consumer_ids}
    consumer_count = len(consumer_ids)
    base_size, remainder = divmod(len(partition_ids), consumer_count)
    start = 0
    for index, consumer_id in enumerate(consumer_ids):
        size = base_size + (1 if index < remainder else 0)
        assignment[consumer_id] = list(partition_ids[start:start + size])
        start += size
    return assignment


def audit_single_owner(events: list[AuditEvent]) -> list[str]:
    owners: dict[str, str] = {}
    violations: list[str] = []
    for event in sorted(events, key=lambda item: (item.seq, item.timestamp)):
        if event.event == "assign":
            if event.partition_id in owners:
                violations.append(
                    f"partition {event.partition_id} assigned to "
                    f"{event.consumer_id} while owned by "
                    f"{owners[event.partition_id]} at seq {event.seq}"
                )
            else:
                owners[event.partition_id] = event.consumer_id or ""
        elif event.event == "revoke":
            current = owners.get(event.partition_id)
            if current is None:
                violations.append(
                    f"partition {event.partition_id} revoked without an owner "
                    f"at seq {event.seq}"
                )
            elif current != event.consumer_id:
                violations.append(
                    f"partition {event.partition_id} revoke by "
                    f"{event.consumer_id} but owner is {current} at seq "
                    f"{event.seq}"
                )
            else:
                del owners[event.partition_id]
    return violations
