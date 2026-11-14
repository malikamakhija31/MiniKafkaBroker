import asyncio
from pathlib import Path

from app.errors import LogDivergence, OffsetGap
from app.models.record import Record
from app.storage.segment import Segment, record_meta
from app.utils.atomic import atomic_write_json, read_json

DATA = "DATA"
COMMIT = "COMMIT"
ABORT = "ABORT"


class PartitionLog:
    """Append-only partition log with the reliability bookkeeping on top.

    Besides the segment files it keeps (rebuilt from disk on every start):

    * idempotency index  - idempotency_key -> offset      (duplicate producer retries)
    * transaction table  - txn_id -> COMMIT / ABORT / still open
    * high watermark     - records below it are on >= min-ISR brokers (persisted)
    * last stable offset - high watermark, but never past a still-open transaction
    """

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

        self.keys = {}      # offset -> idempotency_key
        self.special = {}   # offset -> meta of transactional / control records
        for segment in self.segments:
            for meta in segment.recovered:
                self._register(meta)
        self._derive()

        self.checkpoint_path = self.directory / "replication.checkpoint"
        self.high_watermark = self._load_high_watermark()

    # ------------------------------------------------------------------
    # in-memory reliability indexes
    # ------------------------------------------------------------------
    def _register(self, meta: dict):
        if meta["idempotency_key"]:
            self.keys[meta["offset"]] = meta["idempotency_key"]
        if meta["txn_id"] or meta["record_type"] != DATA:
            self.special[meta["offset"]] = meta

    def _derive(self):
        self.idem = {key: offset for offset, key in self.keys.items()}
        self.txn_state = {}    # txn_id -> COMMIT | ABORT  (first decision wins)
        self.txn_first = {}    # txn_id -> first data offset
        self.txn_info = {}     # txn_id -> meta of the first data record
        for offset in sorted(self.special):
            self._apply_special(self.special[offset])

    def _apply_special(self, meta: dict):
        txn_id = meta["txn_id"]
        if not txn_id:
            return
        if meta["record_type"] == DATA:
            self.txn_first.setdefault(txn_id, meta["offset"])
            self.txn_info.setdefault(txn_id, meta)
        elif meta["record_type"] in (COMMIT, ABORT):
            self.txn_state.setdefault(txn_id, meta["record_type"])

    def _track(self, record: Record):
        meta = record_meta(record)
        if not meta:
            return
        self._register(meta)
        if meta["idempotency_key"]:
            self.idem[meta["idempotency_key"]] = meta["offset"]
        if meta["offset"] in self.special:
            self._apply_special(meta)

    # ------------------------------------------------------------------
    # high watermark / last stable offset
    # ------------------------------------------------------------------
    def _load_high_watermark(self) -> int:
        data = read_json(self.checkpoint_path, None)
        if not data:
            # First start of an empty partition (hw = 0) or of a log written before this
            # upgrade (treated as committed). Persist it NOW so that an uncommitted tail can
            # never be mistaken for committed data after a crash + restart.
            self.high_watermark = self.next_offset
            self._save_high_watermark()
            return self.high_watermark
        return max(0, min(int(data.get("high_watermark", 0)), self.next_offset))

    def _save_high_watermark(self):
        atomic_write_json(self.checkpoint_path, {"high_watermark": self.high_watermark})

    def set_high_watermark(self, value: int) -> int:
        """Monotonic advance of the committed boundary (persisted)."""
        value = min(int(value), self.next_offset)
        if value > self.high_watermark:
            self.high_watermark = value
            self._save_high_watermark()
        return self.high_watermark

    def pending_txns(self):
        """Open transactions: data written, no COMMIT/ABORT marker yet."""
        return [
            (txn_id, first, self.txn_info[txn_id])
            for txn_id, first in self.txn_first.items()
            if txn_id not in self.txn_state
        ]

    def last_stable_offset(self) -> int:
        lso = self.high_watermark
        for _, first, _ in self.pending_txns():
            lso = min(lso, first)
        return lso

    def is_visible(self, record: Record) -> bool:
        if record.record_type != DATA:
            return False
        return record.txn_id is None or self.txn_state.get(record.txn_id) == COMMIT

    def status_of(self, record: Record) -> str:
        if record.record_type != DATA:
            return "MARKER"
        if record.offset >= self.high_watermark:
            return "UNREPLICATED"
        if record.txn_id is None:
            return "COMMITTED"
        state = self.txn_state.get(record.txn_id)
        return {COMMIT: "COMMITTED", ABORT: "ABORTED"}.get(state, "PENDING")

    # ------------------------------------------------------------------
    # segments
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # writes
    # ------------------------------------------------------------------
    async def append(self, key, value, headers=None, idempotency_key=None, txn_id=None, record_type=DATA):
        async with self.lock:
            record = Record(
                topic=self.topic, partition=self.partition, offset=self.next_offset,
                key=key, value=value, headers=headers or {},
                idempotency_key=idempotency_key, txn_id=txn_id, record_type=record_type,
            )
            self._ensure_segment_capacity(record)
            await self.active_segment.append(record)
            self.next_offset += 1
            self._track(record)
            return record

    async def append_existing(self, record: Record):
        """Append a leader's exact record to a follower log; safe to retry.

        * offset already present and identical  -> duplicate (ignored)
        * offset already present but DIFFERENT  -> LogDivergence (leader tells us to truncate)
        * offset beyond our end                 -> OffsetGap (leader back-fills the missing range)
        """
        async with self.lock:
            if record.topic != self.topic or record.partition != self.partition:
                raise ValueError("Replica record does not match this partition")
            if record.offset < self.next_offset:
                if record.offset >= self.high_watermark:
                    stored = await self._read_local(record.offset)
                    if stored is not None and (
                        stored.idempotency_key != record.idempotency_key
                        or stored.timestamp != record.timestamp
                        or stored.value != record.value
                    ):
                        raise LogDivergence(
                            f"Divergent record at offset {record.offset}", truncate_to=record.offset
                        )
                return {"replicated": True, "duplicate": True, "offset": record.offset}
            if record.offset != self.next_offset:
                raise OffsetGap(
                    f"Replica offset gap: expected {self.next_offset}, received {record.offset}",
                    expected=self.next_offset,
                )
            self._ensure_segment_capacity(record)
            await self.active_segment.append(record)
            self.next_offset += 1
            self._track(record)
            return {"replicated": True, "duplicate": False, "offset": record.offset}

    async def truncate_to(self, offset: int) -> int:
        """Discard every record with offset >= `offset` (uncommitted / divergent tail)."""
        async with self.lock:
            offset = max(0, int(offset))
            if offset >= self.next_offset:
                return 0
            removed = self.next_offset - offset
            kept = []
            for segment in self.segments:
                entries = segment.index.entries
                if kept and entries and entries[0]["offset"] >= offset:
                    segment.delete_files()
                    continue
                segment.truncate_from(offset)
                kept.append(segment)
            self.segments = kept
            self.active_segment = kept[-1]
            self.next_offset = max(
                (e["offset"] for s in kept for e in s.index.entries), default=-1
            ) + 1
            self.keys = {o: k for o, k in self.keys.items() if o < offset}
            self.special = {o: m for o, m in self.special.items() if o < offset}
            self._derive()
            if self.high_watermark > self.next_offset:
                self.high_watermark = self.next_offset
                self._save_high_watermark()
            return removed

    # ------------------------------------------------------------------
    # reads
    # ------------------------------------------------------------------
    async def _read_local(self, offset: int):
        for segment in self.segments:
            entry = segment.index.find(offset)
            if entry is not None:
                return await asyncio.to_thread(segment._read_record, entry)
        return None

    async def read_raw(self, offset=0, limit=20, upto=None):
        """Every physical record (incl. markers and uncommitted ones)."""
        async with self.lock:
            records = []
            for segment in self.segments:
                if len(records) >= limit:
                    break
                records.extend(await segment.read_from(offset, limit - len(records)))
            records.sort(key=lambda record: record.offset)
            if upto is not None:
                records = [record for record in records if record.offset < upto]
            return records[:limit]

    async def read(self, offset=0, limit=20):
        return await self.read_raw(offset, limit)

    async def read_committed(self, offset=0, limit=20):
        """Consumer view: only committed, non-aborted DATA records, never past
        the last stable offset. Returns (records, next_offset_to_read)."""
        lso = self.last_stable_offset()
        visible = []
        position = offset
        while position < lso and len(visible) < limit:
            batch = await self.read_raw(position, 100, upto=lso)
            if not batch:
                break
            for record in batch:
                position = record.offset + 1
                if self.is_visible(record):
                    visible.append(record)
                if len(visible) >= limit:
                    break
        return visible, position

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
            "high_watermark": self.high_watermark,
            "last_stable_offset": self.last_stable_offset(),
            "open_transactions": len(self.pending_txns()),
            "record_count": total_records,
            "segment_count": len(self.segments),
            "active_segment": self.active_segment.file_path.name,
            "segments": segment_data,
        }
