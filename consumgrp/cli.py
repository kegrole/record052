from __future__ import annotations

import argparse
import json
import signal
import sys
from pathlib import Path

from .group import ConsumerGroup
from .report import build_report, render_summary, write_report


def load_scenario(path: str | Path) -> dict:
    scenario_path = Path(path)
    with scenario_path.open("r", encoding="utf-8") as stream:
        scenario = json.load(stream)
    missing = {"partitions", "consumers", "steps"}.difference(scenario)
    if missing:
        raise ValueError(f"scenario missing keys: {sorted(missing)}")
    return scenario


def build_group(scenario: dict) -> ConsumerGroup:
    return ConsumerGroup(
        group_id=scenario.get("group_id", "demo-consumer-group"),
        partitions=dict(scenario["partitions"]),
        consumer_ids=list(scenario["consumers"]),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Simulate a partition-based consumer group."
    )
    parser.add_argument("--partitions", required=True, help="Scenario JSON path")
    parser.add_argument("--consumers", required=True, type=int)
    parser.add_argument("--out", required=True, help="Report JSON output path")
    args = parser.parse_args(argv)

    scenario = load_scenario(args.partitions)
    if len(scenario["consumers"]) != args.consumers:
        raise ValueError(
            f"scenario defines {len(scenario['consumers'])} consumers but "
            f"--consumers is {args.consumers}"
        )

    group = build_group(scenario)

    def stop_on_signal(signum: int, _frame: object) -> None:
        group.shutdown_all()
        raise KeyboardInterrupt(signum)

    previous_handlers = {}
    for signum in (signal.SIGTERM, signal.SIGINT):
        previous_handlers[signum] = signal.signal(signum, stop_on_signal)

    try:
        group.run_script(scenario["steps"])
        group.shutdown_all()
    except KeyboardInterrupt:
        pass
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)

    report = build_report(group)
    write_report(report, args.out)
    print(render_summary(report))
    if report["audit_violations"]:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
