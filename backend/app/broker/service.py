import hashlib

from app.broker.partitioner import Partitioner
from app.broker.topic_manager import TopicManager
from app.models.record import Record


class BrokerService:
    """Everything a single broker does with its OWN local logs."""

    def __init__(self, data_dir="data", event_manager=None):
        self.topic_manager = TopicManager(data_dir)
        self.round_robin_counters = {}
        self.event_manager = event_manager

    async def publish_event(self, event):
        if self.event_manager:
            await self.event_manager.broadcast(event)

    def create_topic(self, name: str, partitions: int, segment_max_bytes: int = 1048576):
        return self.topic_manager.create_topic(name, partitions, segment_max_bytes)

    def list_topics(self):
        return self.topic_manager.list_topics()

    def get_log(self, topic: str, partition: int):
        topic_data = self.topic_manager.get_topic(topic)
        if topic_data is None:
            raise ValueError("Topic does not exist")
        partitions = topic_data["partitions"]
        if partition < 0 or partition >= len(partitions):
            raise ValueError("Invalid partition")
        return partitions[partition]

    def choose_partition(self, topic: str, key: str | None, idempotency_key: str | None = None):
        topic_data = self.topic_manager.get_topic(topic)
        if topic_data is None:
            raise ValueError("Topic does not exist")
        count = len(topic_data["partitions"])
        if key is not None:
            return Partitioner.get_partition(key, count)
        if idempotency_key is not None:
            # A retry of a key-less message must land on the same partition,
            # otherwise round-robin would defeat idempotency.
            return Partitioner.get_partition(idempotency_key, count)
        current = self.round_robin_counters.get(topic, 0)
        self.round_robin_counters[topic] = current + 1
        return current % count

    async def produce_to_partition(self, topic: str, partition_id: int, key: str | None, value: dict,
                                   headers: dict | None = None, idempotency_key: str | None = None,
                                   txn_id: str | None = None, record_type: str = "DATA"):
        """Append to the LOCAL log only. Replication / commit is the cluster layer's job."""
        partition_log = self.get_log(topic, partition_id)
        old_segment = partition_log.active_segment.file_path.name
        record = await partition_log.append(
            key=key, value=value, headers=headers,
            idempotency_key=idempotency_key, txn_id=txn_id, record_type=record_type,
        )
        new_segment = partition_log.active_segment.file_path.name
        if old_segment != new_segment:
            await self.publish_event({"type": "SEGMENT_ROLLED_OVER", "topic": topic, "partition": partition_id,
                                      "old_segment": old_segment, "new_segment": new_segment})
        return ({
            "topic": topic, "partition": partition_id, "offset": record.offset,
            "timestamp": record.timestamp, "key": record.key, "value": record.value,
            "headers": record.headers, "idempotency_key": record.idempotency_key,
            "txn_id": record.txn_id, "record_type": record.record_type,
            "acknowledged": True,
        }, record)

    async def append_replica(self, record_data: dict):
        record = Record.model_validate(record_data)
        topic_data = self.topic_manager.get_topic(record.topic)
        if topic_data is None:
            raise ValueError(f"Topic '{record.topic}' has not been created on this broker")
        if record.partition >= len(topic_data["partitions"]):
            raise ValueError("Replica partition does not exist")
        result = await topic_data["partitions"][record.partition].append_existing(record)
        if not result.get("duplicate"):
            await self.publish_event({
                "type": "REPLICA_APPENDED", "topic": record.topic,
                "partition": record.partition, "offset": record.offset,
            })
        return result

    async def fetch(self, topic: str, partition: int, offset: int = 0, limit: int = 20,
                    isolation: str = "read_committed"):
        log = self.get_log(topic, partition)
        if isolation == "read_uncommitted":
            records = await log.read_raw(offset, limit)
            next_offset = records[-1].offset + 1 if records else offset
            boundary = log.next_offset
        else:
            records, next_offset = await log.read_committed(offset, limit)
            boundary = log.last_stable_offset()
        dumped = []
        for record in records:
            item = record.model_dump(mode="json")
            item["status"] = log.status_of(record)
            dumped.append(item)
        return {
            "topic": topic, "partition": partition, "records": dumped,
            "isolation": isolation, "requested_offset": offset,
            "next_offset": next_offset,
            "high_watermark": log.high_watermark,
            "last_stable_offset": log.last_stable_offset(),
            "log_end_offset": log.next_offset,
            "has_more": next_offset < boundary,
        }

    async def fetch_raw_log(self, topic: str, partition: int, offset: int, limit: int = 200):
        """Replication / catch-up read: every physical record up to the log end."""
        log = self.get_log(topic, partition)
        records = await log.read_raw(offset, limit)
        next_offset = records[-1].offset + 1 if records else offset
        return {
            "topic": topic, "partition": partition,
            "records": [record.model_dump(mode="json") for record in records],
            "high_watermark": log.high_watermark,
            "log_end_offset": log.next_offset,
            "has_more": next_offset < log.next_offset,
        }

    def get_partition_states(self, topic: str):
        topic_data = self.topic_manager.get_topic(topic)
        if topic_data is None:
            raise ValueError("Topic does not exist")
        return [partition.metadata() for partition in topic_data["partitions"]]

    def partition_offsets(self):
        """Compact per-partition summary: used for heartbeats and elections."""
        summary = {}
        for name, topic_data in self.topic_manager.topics.items():
            for log in topic_data["partitions"]:
                summary[f"{name}:{log.partition}"] = {
                    "log_end": log.next_offset,
                    "hw": log.high_watermark,
                    "lso": log.last_stable_offset(),
                    "pending_txns": len(log.pending_txns()),
                }
        return summary

    @staticmethod
    def hash_text(text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()
