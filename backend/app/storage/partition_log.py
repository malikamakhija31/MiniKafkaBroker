import asyncio
from pathlib import Path

from app.models.record import Record
from app.storage.segment import Segment


class PartitionLog:
    def __init__(self, topic: str, partition: int, data_dir: str = "data", segment_max_bytes: int = 1048576):
        self.topic = topic
        self.partition = partition
        self.segment_max_bytes = segment_max_bytes
        self.directory = Path(data_dir) / "topics" / topic / f"partition-{partition}"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.segments = []
        log_files = sorted(self.directory.glob("*.log"), key=lambda path: int(path.stem))
        for log_file in log_files:
            self.segments.append(Segment(str(log_file)))
        if not self.segments:
            self.segments.append(Segment(str(self.directory / "00000000.log")))
        self.active_segment = self.segments[-1]
        all_entries = [entry for segment in self.segments for entry in segment.index.entries]
        self.next_offset = max((entry["offset"] for entry in all_entries), default=-1) + 1
        self.lock = asyncio.Lock()

    def _create_new_segment(self):
        filename = f"{self.next_offset:08d}.log"
        new_segment = Segment(str(self.directory / filename))
        self.segments.append(new_segment)
        self.active_segment = new_segment
        return new_segment

    def _ensure_segment_capacity(self, record: Record):
        payload_size = len(record.model_dump_json().encode("utf-8"))
        record_size = payload_size + 8
        current_size = self.active_segment.file_path.stat().st_size
        if self.active_segment.index.entries and current_size + record_size > self.segment_max_bytes:
            self._create_new_segment()

    async def append(self, key, value, headers=None):
        async with self.lock:
            record = Record(topic=self.topic, partition=self.partition, offset=self.next_offset, key=key, value=value, headers=headers or {})
            self._ensure_segment_capacity(record)
            await self.active_segment.append(record)
            self.next_offset += 1
            return record

    async def append_existing(self, record: Record):
        """Append a leader's exact record to a follower log; safe to retry."""
        async with self.lock:
            if record.topic != self.topic or record.partition != self.partition:
                raise ValueError("Replica record does not match this partition")
            if record.offset < self.next_offset:
                return {"replicated": True, "duplicate": True, "offset": record.offset}
            if record.offset != self.next_offset:
                raise ValueError(f"Replica offset gap: expected {self.next_offset}, received {record.offset}")
            self._ensure_segment_capacity(record)
            await self.active_segment.append(record)
            self.next_offset += 1
            return {"replicated": True, "duplicate": False, "offset": record.offset}

    async def read(self, offset=0, limit=20):
        async with self.lock:
            records = []
            for segment in self.segments:
                if len(records) >= limit:
                    break
                records.extend(await segment.read_from(offset, limit - len(records)))
            records.sort(key=lambda record: record.offset)
            return records[:limit]

    def metadata(self):
        segment_data = []
        for segment in self.segments:
            details = segment.metadata()
            details["active"] = segment is self.active_segment
            details["segment_id"] = segment.file_path.stem
            segment_data.append(details)
        total_records = sum(len(segment.index.entries) for segment in self.segments)
        return {
            "topic": self.topic,
            "partition": self.partition,
            "log_end_offset": self.next_offset,
            "record_count": total_records,
            "segment_count": len(self.segments),
            "active_segment": self.active_segment.file_path.name,
            "segments": segment_data,
        }
