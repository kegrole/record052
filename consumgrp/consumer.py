from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .group import ConsumerGroup
    from .offset import CommitResult
    from .partition import Message


class Consumer:
    def __init__(self, consumer_id: str, group: "ConsumerGroup") -> None:
        self.consumer_id = consumer_id
        self._group = group

    @property
    def partitions(self) -> list[str]:
        return list(self._group.assignment_for(self.consumer_id))

    def consume_next(self, partition_id: str) -> "Message":
        return self._group.deliver(self.consumer_id, partition_id)

    def consume_rest(self, partition_id: str) -> list["Message"]:
        messages = []
        while self._group.partition(partition_id).has_next():
            messages.append(
                self._group.deliver(self.consumer_id, partition_id)
            )
        return messages

    def commit(
        self, partition_id: str, offset: int, asynchronous: bool = False
    ) -> "CommitResult":
        return self._group.commit(
            self.consumer_id,
            partition_id,
            offset,
            asynchronous=asynchronous,
        )

    def commit_rest(
        self, partition_id: str, asynchronous: bool = False
    ) -> "CommitResult":
        partition = self._group.partition(partition_id)
        return self.commit(
            partition_id,
            partition.log_end_offset(),
            asynchronous=asynchronous,
        )

    def close(self) -> None:
        self._group.leave(self.consumer_id)
