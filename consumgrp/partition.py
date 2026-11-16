from __future__ import annotations

from dataclasses import dataclass

from .offset import OffsetTracker


@dataclass(frozen=True)
class Message:
    partition_id: str
    offset: int
    payload: str

    @property
    def message_id(self) -> str:
        return f"{self.partition_id}#{self.offset}"


class Partition:
    """A partition: its message log, offsets and replay bookkeeping."""

    def __init__(self, partition_id: str, message_count: int) -> None:
        if message_count < 0:
            raise ValueError("message_count must be non-negative")
        self.partition_id = partition_id
        self.message_count = message_count
        self.tracker = OffsetTracker(partition_id)
        self._delivered: set[int] = set()
        self.replayed: list[str] = []

    def log_end_offset(self) -> int:
        return self.message_count

    def lag(self) -> int:
        return self.message_count - self.tracker.committed_offset

    def has_next(self) -> bool:
        return self.tracker.consumed_to < self.message_count

    def consume_next(self) -> Message:
        offset = self.tracker.consumed_to
        if offset >= self.message_count:
            raise IndexError(
                f"partition {self.partition_id} has no message at offset {offset}"
            )
        is_replay = offset in self._delivered
        self._delivered.add(offset)
        if is_replay:
            self.replayed.append(self.message_id_for(offset))
        self.tracker.record_consumed(offset)
        return Message(
            partition_id=self.partition_id,
            offset=offset,
            payload=f"{self.partition_id}-payload-{offset}",
        )

    def consume_all(self) -> list[Message]:
        messages: list[Message] = []
        while self.has_next():
            messages.append(self.consume_next())
        return messages

    def seek_to_committed(self) -> None:
        """Reset the read position so a new owner replays uncommitted work."""
        self.tracker.seek(self.tracker.committed_offset)

    def delivered_offsets(self) -> list[int]:
        return sorted(self._delivered)

    def all_messages_delivered(self) -> bool:
        return self._delivered == set(range(self.message_count))

    def message_id_for(self, offset: int) -> str:
        return f"{self.partition_id}#{offset}"
