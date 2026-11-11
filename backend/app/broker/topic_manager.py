
import json
import re

from pathlib import Path

from app.storage.partition_log import PartitionLog


class TopicManager:

    def __init__(self, data_dir="data"):

        self.data_dir = Path(data_dir)
        self.topics = {}

        self.data_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        self.load_topics()

    def create_topic(
        self,
        name: str,
        partitions: int,
        segment_max_bytes: int = 1048576
    ):

        if not re.fullmatch(r"[a-zA-Z0-9_-]+", name):
            raise ValueError("Invalid topic name")

        if not 1 <= partitions <= 32:
            raise ValueError("Partitions must be between 1 and 32")

        if segment_max_bytes < 1024:
            raise ValueError("Segment size must be at least 1024 bytes")

        if name in self.topics:
            raise ValueError("Topic already exists")

        topic_dir = self.data_dir / "topics" / name

        topic_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        metadata = {
            "name": name,
            "partitions": partitions,
            "segment_max_bytes": segment_max_bytes
        }

        metadata_path = topic_dir / "topic.json"

        metadata_path.write_text(
            json.dumps(metadata, indent=4),
            encoding="utf-8"
        )

        partition_logs = []

        for partition_id in range(partitions):

            partition_log = PartitionLog(
                topic=name,
                partition=partition_id,
                data_dir=str(self.data_dir),
                segment_max_bytes=segment_max_bytes
            )

            partition_logs.append(partition_log)

        self.topics[name] = {
            "metadata": metadata,
            "partitions": partition_logs
        }

        return metadata

    def load_topics(self):

        topics_dir = self.data_dir / "topics"

        if not topics_dir.exists():
            return

        for topic_dir in topics_dir.iterdir():

            metadata_path = topic_dir / "topic.json"

            if not metadata_path.exists():
                continue

            try:

                metadata = json.loads(
                    metadata_path.read_text(
                        encoding="utf-8"
                    )
                )

                name = metadata["name"]
                count = metadata["partitions"]

                segment_max_bytes = metadata.get(
                    "segment_max_bytes",
                    1048576
                )

                partition_logs = []

                for partition_id in range(count):

                    partition_logs.append(
                        PartitionLog(
                            topic=name,
                            partition=partition_id,
                            data_dir=str(self.data_dir),
                            segment_max_bytes=segment_max_bytes
                        )
                    )

                self.topics[name] = {
                    "metadata": metadata,
                    "partitions": partition_logs
                }

            except (
                ValueError,
                KeyError,
                OSError,
                TypeError
            ):
                continue

    def get_topic(self, name: str):

        return self.topics.get(name)

    def list_topics(self):

        return [
            topic["metadata"]
            for topic in self.topics.values()
        ]
