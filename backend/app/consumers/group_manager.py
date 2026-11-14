import re
import time

from pathlib import Path

from app.utils.atomic import atomic_write_json, read_json

META_KEY = "__meta__"


class ConsumerGroupManager:
    """Consumer group offsets.

    Reliability additions:
    * every commit carries a timestamp, so brokers can replicate commits to each
      other and resolve conflicts "last write wins";
    * the file is written atomically (temp file + fsync + replace).
    The on-disk format stays backwards compatible: {group: {topic: {partition: offset}}}
    plus one extra "__meta__" key holding commit timestamps.
    """

    def __init__(self, data_dir="data"):
        self.file_path = Path(data_dir) / "consumer_offsets.json"
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.offsets = {}
        self.stamps = {}
        self.load()

    def load(self):
        data = read_json(self.file_path, {})
        if not isinstance(data, dict):
            data = {}
        self.stamps = data.pop(META_KEY, {}) if isinstance(data.get(META_KEY, {}), dict) else {}
        self.offsets = data

    def _persist(self):
        payload = dict(self.offsets)
        payload[META_KEY] = self.stamps
        atomic_write_json(self.file_path, payload)

    @staticmethod
    def _stamp_key(group_id, topic, partition):
        return f"{group_id}|{topic}|{partition}"

    def get_offset(self, group_id: str, topic: str, partition: int):
        return self.offsets.get(group_id, {}).get(topic, {}).get(str(partition), 0)

    def commit_offset(self, group_id: str, topic: str, partition: int, offset: int, stamp: float | None = None):
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", group_id):
            raise ValueError("Invalid consumer group ID")
        if partition < 0 or offset < 0:
            raise ValueError("Partition and offset must be non-negative")
        stamp = time.time() if stamp is None else stamp
        group = self.offsets.setdefault(group_id, {})
        group.setdefault(topic, {})[str(partition)] = offset
        self.stamps[self._stamp_key(group_id, topic, partition)] = stamp
        self._persist()
        return {
            "group_id": group_id, "topic": topic, "partition": partition,
            "committed_offset": offset, "stamp": stamp,
        }

    def apply_remote(self, group_id, topic, partition, offset, stamp) -> bool:
        """Merge a commit replicated from another broker (newest stamp wins)."""
        key = self._stamp_key(group_id, topic, partition)
        if stamp <= self.stamps.get(key, 0):
            return False
        self.commit_offset(group_id, topic, int(partition), int(offset), stamp=float(stamp))
        return True

    def export_commits(self):
        commits = []
        for group_id, topics in self.offsets.items():
            for topic, partitions in topics.items():
                for partition, offset in partitions.items():
                    stamp = self.stamps.get(self._stamp_key(group_id, topic, partition), 0)
                    commits.append({"group": group_id, "topic": topic, "partition": int(partition),
                                    "offset": offset, "stamp": stamp})
        return commits

    def get_group_offsets(self, group_id: str):
        return self.offsets.get(group_id, {})
