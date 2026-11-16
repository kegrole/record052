from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CommitResult:
    accepted: bool
    offset: int
    reason: str = ""

    def __bool__(self) -> bool:
        return self.accepted


class OffsetTracker:
    """Per-partition offset state.

    Committed offsets use the Kafka-style *next offset* convention:
    ``committed_offset`` is the first offset that has not been durably
    committed.  Offsets ``0 .. committed_offset - 1`` therefore form the
    committed contiguous prefix.

    A commit is only accepted when every offset below the requested offset
    has been consumed, so a hole in the consumed prefix can never be skipped.
    """

    def __init__(self, partition_id: str, initial_offset: int = 0) -> None:
        if initial_offset < 0:
            raise ValueError("initial_offset must be non-negative")
        self.partition_id = partition_id
        self._committed_offset = initial_offset
        self._consumed_to = initial_offset

    @property
    def committed_offset(self) -> int:
        return self._committed_offset

    @property
    def consumed_to(self) -> int:
        return self._consumed_to

    def record_consumed(self, offset: int) -> None:
        if offset < 0:
            raise ValueError("offset must be non-negative")
        end_offset = offset + 1
        if end_offset > self._consumed_to:
            self._consumed_to = end_offset

    def seek(self, offset: int) -> None:
        """Move the in-memory consumption position back to a safe offset.

        Used during rebalance so the new owner replays from the last durable
        commit.  It never changes the committed prefix.
        """
        if offset < 0:
            raise ValueError("offset must be non-negative")
        self._consumed_to = offset

    def can_commit(self, offset: int) -> bool:
        return self.commit(offset).accepted

    def commit(self, offset: int) -> CommitResult:
        if offset < self._committed_offset:
            return CommitResult(
                False,
                self._committed_offset,
                f"offset {offset} is behind committed offset "
                f"{self._committed_offset}",
            )
        if offset > self._consumed_to:
            return CommitResult(
                False,
                self._committed_offset,
                f"cannot commit offset {offset}: consumed prefix ends at "
                f"{self._consumed_to}, this would skip a hole",
            )
        if offset > self._committed_offset:
            self._committed_offset = offset
        return CommitResult(True, self._committed_offset)

    def snapshot(self) -> dict[str, int]:
        return {
            "committed_offset": self._committed_offset,
            "consumed_to": self._consumed_to,
        }
