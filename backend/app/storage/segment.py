
import asyncio
import json
import os
import struct
import zlib

from pathlib import Path

from app.models.record import Record
from app.storage.segment_index import SegmentIndex


class Segment:

    def __init__(self, file_path: str):

        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        self.index = SegmentIndex(
            str(self.file_path.with_suffix(".index"))
        )

        self.lock = asyncio.Lock()

        # Rebuild the index from the actual log on startup.
        self._recover()

    def _recover(self):

        entries = []
        valid_position = 0

        if not self.file_path.exists():
            self.file_path.touch()

        with open(self.file_path, "r+b") as file:

            while True:

                position = file.tell()

                header = file.read(4)

                if not header:
                    break

                if len(header) != 4:
                    break

                payload_size = struct.unpack(">I", header)[0]

                # Protect against invalid frame lengths.
                if payload_size > 10_000_000:
                    break

                payload = file.read(payload_size)
                checksum_data = file.read(4)

                if len(payload) != payload_size:
                    break

                if len(checksum_data) != 4:
                    break

                stored_checksum = struct.unpack(
                    ">I", checksum_data
                )[0]

                actual_checksum = zlib.crc32(payload)

                if stored_checksum != actual_checksum:
                    break

                try:
                    record = Record.model_validate_json(payload)
                except Exception:
                    break

                frame_size = 4 + payload_size + 4

                entries.append({
                    "offset": record.offset,
                    "position": position,
                    "size": frame_size
                })

                valid_position = file.tell()

            # Remove an incomplete or corrupted trailing record.
            file.truncate(valid_position)

        self.index.rebuild(entries)

    def _write_record(self, record: Record):

        payload = record.model_dump_json().encode("utf-8")

        checksum = zlib.crc32(payload)

        frame = (
            struct.pack(">I", len(payload))
            + payload
            + struct.pack(">I", checksum)
        )

        with open(self.file_path, "ab") as file:

            position = file.tell()

            file.write(frame)
            file.flush()
            os.fsync(file.fileno())

        return position, len(frame)

    def _read_record(self, entry):

        with open(self.file_path, "rb") as file:

            file.seek(entry["position"])

            header = file.read(4)

            if len(header) != 4:
                raise ValueError("Invalid record header")

            payload_size = struct.unpack(">I", header)[0]

            payload = file.read(payload_size)

            checksum_data = file.read(4)

            if len(checksum_data) != 4:
                raise ValueError("Incomplete record")

            expected_checksum = struct.unpack(
                ">I", checksum_data
            )[0]

            actual_checksum = zlib.crc32(payload)

            if expected_checksum != actual_checksum:
                raise ValueError("Record checksum mismatch")

            return Record.model_validate_json(payload)

    async def append(self, record: Record):

        async with self.lock:

            position, size = await asyncio.to_thread(
                self._write_record,
                record
            )

            await asyncio.to_thread(
                self.index.add_entry,
                record.offset,
                position,
                size
            )

            return {
                "offset": record.offset,
                "position": position,
                "size": size
            }

    async def read(self, offset: int):

        async with self.lock:

            entry = self.index.find(offset)

            if entry is None:
                return None

            return await asyncio.to_thread(
                self._read_record,
                entry
            )

    async def read_from(self, offset: int, limit: int = 20):

        async with self.lock:

            selected = [
                entry for entry in self.index.entries
                if entry["offset"] >= offset
            ][:limit]

            records = await asyncio.gather(*[
                asyncio.to_thread(self._read_record, entry)
                for entry in selected
            ])

            return records

    def metadata(self):

        return {
            "log_file": self.file_path.name,
            "index_file": self.index.file_path.name,
            "record_count": len(self.index.entries),
            "size_bytes": self.file_path.stat().st_size,
            "first_offset": (
                self.index.entries[0]["offset"]
                if self.index.entries else None
            ),
            "last_offset": (
                self.index.entries[-1]["offset"]
                if self.index.entries else None
            )
        }
