
import json
import re

from pathlib import Path


class ConsumerGroupManager:

    def __init__(self, data_dir="data"):

        self.file_path = (
            Path(data_dir) / "consumer_offsets.json"
        )

        self.file_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        self.offsets = {}

        self.load()

    def load(self):

        if not self.file_path.exists():
            self.offsets = {}
            return

        try:
            self.offsets = json.loads(
                self.file_path.read_text(encoding="utf-8")
            )
        except (json.JSONDecodeError, OSError):
            self.offsets = {}

    def get_offset(
        self,
        group_id: str,
        topic: str,
        partition: int
    ):

        return (
            self.offsets
            .get(group_id, {})
            .get(topic, {})
            .get(str(partition), 0)
        )

    def commit_offset(
        self,
        group_id: str,
        topic: str,
        partition: int,
        offset: int
    ):

        if not re.fullmatch(r"[a-zA-Z0-9_-]+", group_id):
            raise ValueError("Invalid consumer group ID")

        if partition < 0 or offset < 0:
            raise ValueError("Partition and offset must be non-negative")

        group = self.offsets.setdefault(group_id, {})
        topic_offsets = group.setdefault(topic, {})

        topic_offsets[str(partition)] = offset

        temporary_file = self.file_path.with_suffix(".tmp")

        temporary_file.write_text(
            json.dumps(self.offsets, indent=4),
            encoding="utf-8"
        )

        temporary_file.replace(self.file_path)

        return {
            "group_id": group_id,
            "topic": topic,
            "partition": partition,
            "committed_offset": offset
        }

    def get_group_offsets(self, group_id: str):

        return self.offsets.get(group_id, {})
