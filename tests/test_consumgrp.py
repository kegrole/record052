import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from consumgrp.cli import build_group, load_scenario, main
from consumgrp.group import ConsumerGroup
from consumgrp.offset import OffsetTracker
from consumgrp.report import build_report


PARTITIONS = {"p0": 20, "p1": 20, "p2": 20, "p3": 20}
CONSUMERS = ["A", "B"]


class ConsumerGroupFixtureTest(unittest.TestCase):
    def make_group(self) -> ConsumerGroup:
        return ConsumerGroup("fixture-group", PARTITIONS, CONSUMERS)

    def prime_before_rebalance(self, group: ConsumerGroup) -> None:
        for partition_id in ("p0", "p1"):
            for _ in range(4):
                group.consumer("A").consume_next(partition_id)
        for partition_id in ("p2", "p3"):
            for _ in range(4):
                group.consumer("B").consume_next(partition_id)
        group.consumer("A").commit("p0", 3)
        group.consumer("A").commit("p1", 4)
        group.consumer("B").commit("p2", 4)
        group.consumer("B").commit("p3", 4)

    def complete_after_rebalance(self, group: ConsumerGroup) -> None:
        group.leave("A")
        owner_b = group.consumer("B")
        owner_b.consume_next("p0")
        for partition_id in ("p0", "p1", "p2", "p3"):
            owner_b.consume_rest(partition_id)
        for partition_id in ("p0", "p1", "p2", "p3"):
            owner_b.commit_rest(partition_id, asynchronous=True)
        group.wait_for_commits()

    def run_fixture(self) -> tuple[ConsumerGroup, dict]:
        group = self.make_group()
        self.prime_before_rebalance(group)
        self.complete_after_rebalance(group)
        return group, build_report(group)

    def test_01_fixture_topology_and_rebalance(self) -> None:
        group = self.make_group()
        self.assertEqual(group.assignment_for("A"), ["p0", "p1"])
        self.assertEqual(group.assignment_for("B"), ["p2", "p3"])
        group.leave("A")
        self.assertNotIn("A", group.assignment_snapshot().values())
        self.assertEqual(
            sorted(group.assignment_for("B")), ["p0", "p1", "p2", "p3"]
        )
        self.assertEqual(group.owner_of("p0"), "B")
        self.assertEqual(group.owner_of("p1"), "B")

    def test_02_every_message_consumed_and_total_is_81(self) -> None:
        group, report = self.run_fixture()
        for partition in group.partitions.values():
            self.assertTrue(partition.all_messages_delivered())
        self.assertEqual(group.total_deliveries, 81)
        self.assertEqual(report["totals"]["deliveries"], 81)
        self.assertLessEqual(group.total_deliveries, 84)

    def test_03_exactly_p0_offset_3_is_replayed(self) -> None:
        group, report = self.run_fixture()
        self.assertEqual(group.partition("p0").replayed, ["p0#3"])
        self.assertEqual(report["partitions"]["p0"]["replayed"], ["p0#3"])
        for partition_id in ("p1", "p2", "p3"):
            self.assertEqual(report["partitions"][partition_id]["replayed"], [])

    def test_04_committed_offset_and_lag_at_rebalance(self) -> None:
        group = self.make_group()
        self.prime_before_rebalance(group)
        group.leave("A")
        self.assertEqual(group.partition("p0").tracker.committed_offset, 3)
        self.assertEqual(group.lag("p0"), 17)
        self.assertEqual(build_report(group)["partitions"]["p0"]["lag"], 17)

    def test_05_audit_log_proves_single_owner_at_every_moment(self) -> None:
        group, _ = self.run_fixture()
        self.assertEqual(group.audit_violations(), [])
        events = [event.as_dict() for event in group.audit_log]
        self.assertTrue(events)
        for event in events:
            self.assertIn(event["event"], {"assign", "revoke"})
            self.assertIn("partition_id", event)
            self.assertIn("consumer_id", event)
            self.assertIn("timestamp", event)
            self.assertIn("reason", event)
        open_owner: dict[str, str] = {}
        for event in events:
            partition_id = event["partition_id"]
            if event["event"] == "assign":
                self.assertNotIn(partition_id, open_owner)
                open_owner[partition_id] = event["consumer_id"]
            else:
                self.assertEqual(
                    open_owner.pop(partition_id), event["consumer_id"]
                )
        self.assertEqual(open_owner, group.assignment_snapshot())

    def test_06_commit_cannot_jump_over_a_consumption_hole(self) -> None:
        tracker = OffsetTracker("p0")
        tracker.record_consumed(0)
        tracker.record_consumed(1)
        self.assertTrue(tracker.commit(2).accepted)
        tracker.record_consumed(2)
        rejected = tracker.commit(5)
        self.assertFalse(rejected.accepted)
        self.assertEqual(tracker.committed_offset, 2)
        self.assertEqual(tracker.consumed_to, 3)

    def test_07_restart_from_committed_offsets_has_no_replay(self) -> None:
        first_group = ConsumerGroup("restart", {"p0": 10}, ["A"])
        consumer = first_group.consumer("A")
        for _ in range(5):
            consumer.consume_next("p0")
        consumer.commit("p0", 5)
        first_group.shutdown_all()
        restarted_group = ConsumerGroup(
            "restart", list(first_group.partitions.values()), ["C"]
        )
        first_message = restarted_group.consumer("C").consume_next("p0")
        self.assertEqual(first_message.offset, 5)
        self.assertEqual(restarted_group.partition("p0").replayed, [])
        self.assertEqual(restarted_group.total_replays, 0)

    def test_08_final_offsets_lag_and_report_fields(self) -> None:
        _, report = self.run_fixture()
        for partition_id, details in report["partitions"].items():
            self.assertEqual(details["committed_offset"], 20)
            self.assertEqual(details["lag"], 0)
            self.assertEqual(
                set(details),
                {"committed_offset", "lag", "replayed", "owner"},
            )
        self.assertEqual(report["totals"]["replays"], 1)

    def test_synchronous_and_asynchronous_commits_share_prefix_rule(self) -> None:
        sync_group = ConsumerGroup("sync", {"p0": 6}, ["A"])
        async_group = ConsumerGroup("async", {"p0": 6}, ["B"])
        for group, consumer_id in ((sync_group, "A"), (async_group, "B")):
            consumer = group.consumer(consumer_id)
            consumer.consume_next("p0")
            rejected = consumer.commit(
                "p0", 3, asynchronous=(group is async_group)
            )
            self.assertFalse(rejected.accepted)
            self.assertEqual(group.partition("p0").tracker.committed_offset, 0)
            consumer.consume_rest("p0")
            accepted = consumer.commit(
                "p0", 6, asynchronous=(group is async_group)
            )
            self.assertTrue(accepted.accepted)
            self.assertEqual(accepted.offset, 6)
        async_group.wait_for_commits()

    def test_graceful_shutdown_commits_consumed_prefix(self) -> None:
        group = ConsumerGroup("shutdown", {"p0": 4}, ["A"])
        consumer = group.consumer("A")
        for _ in range(3):
            consumer.consume_next("p0")
        group.shutdown("A")
        self.assertEqual(group.partition("p0").tracker.committed_offset, 3)
        self.assertEqual(group.lag("p0"), 1)

    def test_demo_scenario_matches_same_generic_rules(self) -> None:
        scenario = load_scenario(ROOT / "demo" / "offsets.json")
        group = build_group(scenario)
        result = group.run_script(scenario["steps"])
        group.shutdown_all()
        report = build_report(group)
        self.assertEqual(result["deliveries"], 81)
        self.assertEqual(result["replays"], 1)
        self.assertEqual(report["partitions"]["p0"]["replayed"], ["p0#3"])

    def test_cli_writes_report_and_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "report.json"
            exit_code = main([
                "--partitions", str(ROOT / "demo" / "offsets.json"),
                "--consumers", "2",
                "--out", str(output),
            ])
            self.assertEqual(exit_code, 0)
            report = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(report["totals"]["deliveries"], 81)

    def test_demo_module_command(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "report.json"
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "consumgrp.cli",
                    "--partitions",
                    "demo/offsets.json",
                    "--consumers",
                    "2",
                    "--out",
                    str(output),
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )
            self.assertEqual(len(completed.stdout.strip().splitlines()), 1)
            self.assertIn("partitions=4", completed.stdout)
            self.assertIn("consumed=81", completed.stdout)
            self.assertIn("duplicates=1", completed.stdout)


if __name__ == "__main__":
    unittest.main()
