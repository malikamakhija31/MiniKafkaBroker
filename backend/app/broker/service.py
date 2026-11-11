from app.broker.partitioner import Partitioner
from app.broker.topic_manager import TopicManager
from app.models.record import Record


class BrokerService:
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

    def choose_partition(self, topic: str, key: str | None):
        topic_data = self.topic_manager.get_topic(topic)
        if topic_data is None:
            raise ValueError("Topic does not exist")
        count = len(topic_data["partitions"])
        if key is not None:
            return Partitioner.get_partition(key, count)
        current = self.round_robin_counters.get(topic, 0)
        self.round_robin_counters[topic] = current + 1
        return current % count

    async def produce(self, topic: str, key: str | None, value: dict, headers: dict | None = None):
        partition_id = self.choose_partition(topic, key)
        result, _ = await self.produce_to_partition(topic, partition_id, key, value, headers)
        return result

    async def produce_to_partition(self, topic: str, partition_id: int, key: str | None, value: dict, headers: dict | None = None):
        topic_data = self.topic_manager.get_topic(topic)
        if topic_data is None:
            raise ValueError("Topic does not exist")
        partitions = topic_data["partitions"]
        if partition_id < 0 or partition_id >= len(partitions):
            raise ValueError("Invalid partition")
        partition_log = partitions[partition_id]
        old_segment = partition_log.active_segment.file_path.name
        record = await partition_log.append(key=key, value=value, headers=headers)
        new_segment = partition_log.active_segment.file_path.name
        if old_segment != new_segment:
            await self.publish_event({"type": "SEGMENT_ROLLED_OVER", "topic": topic, "partition": partition_id, "old_segment": old_segment, "new_segment": new_segment})
        await self.publish_event({
            "type": "MESSAGE_APPENDED", "topic": topic, "partition": partition_id,
            "offset": record.offset, "key": record.key, "value": record.value,
            "timestamp": record.timestamp.isoformat(), "segment": new_segment,
        })
        return ({
            "topic": topic, "partition": partition_id, "offset": record.offset,
            "timestamp": record.timestamp, "key": record.key, "value": record.value,
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
        await self.publish_event({
            "type": "REPLICA_APPENDED", "topic": record.topic,
            "partition": record.partition, "offset": record.offset,
        })
        return result

    async def fetch(self, topic: str, partition: int, offset: int = 0, limit: int = 20):
        topic_data = self.topic_manager.get_topic(topic)
        if topic_data is None:
            raise ValueError("Topic does not exist")
        partitions = topic_data["partitions"]
        if partition < 0 or partition >= len(partitions):
            raise ValueError("Invalid partition")
        partition_log = partitions[partition]
        records = await partition_log.read(offset, limit)
        return {
            "topic": topic, "partition": partition,
            "records": [record.model_dump(mode="json") for record in records],
            "requested_offset": offset,
            "next_offset": records[-1].offset + 1 if records else offset,
            "log_end_offset": partition_log.next_offset,
            "has_more": bool(records) and records[-1].offset + 1 < partition_log.next_offset,
        }

    def get_partition_states(self, topic: str):
        topic_data = self.topic_manager.get_topic(topic)
        if topic_data is None:
            raise ValueError("Topic does not exist")
        return [partition.metadata() for partition in topic_data["partitions"]]
