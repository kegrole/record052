from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .group import ConsumerGroup


def build_report(group: "ConsumerGroup") -> dict[str, object]:
    partitions = {}
    for partition_id in sorted(group.partitions):
        partition = group.partition(partition_id)
        partitions[partition_id] = {
            "committed_offset": partition.tracker.committed_offset,
            "lag": partition.lag(),
            "replayed": list(partition.replayed),
            "owner": group.owner_of(partition_id),
        }
    return {
        "group_id": group.group_id,
        "partitions": partitions,
        "totals": {
            "deliveries": group.total_deliveries,
            "replays": group.total_replays,
            "partition_count": len(group.partitions),
            "consumer_count": len(group._consumers),
        },
        "audit": [event.as_dict() for event in group.audit_log],
        "audit_violations": group.audit_violations(),
        "ownership": group.assignment_snapshot(),
    }


def render_summary(report: dict[str, object]) -> str:
    totals = report["totals"]
    partitions = report["partitions"]
    offsets = ",".join(
        f"{partition_id}={details['committed_offset']}"
        for partition_id, details in partitions.items()
    )
    return (
        f"partitions={totals['partition_count']} "
        f"consumed={totals['deliveries']} "
        f"duplicates={totals['replays']} "
        f"committed_offsets={offsets}"
    )


def write_report(report: dict[str, object], path: str | Path) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
