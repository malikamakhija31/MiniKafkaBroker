
import struct
from pathlib import Path


class SegmentIndex:

    ENTRY_FORMAT = ">QQI"
    ENTRY_SIZE = struct.calcsize(ENTRY_FORMAT)

    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.entries = []
        self.load()

    def add_entry(self, offset, position, size):
        entry = struct.pack(
            self.ENTRY_FORMAT,
            offset,
            position,
            size
        )

        with open(self.file_path, "ab") as file:
            file.write(entry)
            file.flush()

        self.entries.append({
            "offset": offset,
            "position": position,
            "size": size
        })

    def load(self):
        self.entries = []

        if not self.file_path.exists():
            return

        with open(self.file_path, "rb") as file:
            while True:
                data = file.read(self.ENTRY_SIZE)

                if not data:
                    break

                if len(data) != self.ENTRY_SIZE:
                    break

                offset, position, size = struct.unpack(
                    self.ENTRY_FORMAT, data
                )

                self.entries.append({
                    "offset": offset,
                    "position": position,
                    "size": size
                })

    def rebuild(self, entries):
        self.entries = entries

        with open(self.file_path, "wb") as file:
            for entry in entries:
                file.write(struct.pack(
                    self.ENTRY_FORMAT,
                    entry["offset"],
                    entry["position"],
                    entry["size"]
                ))

    def find(self, offset):
        for entry in self.entries:
            if entry["offset"] == offset:
                return entry

        return None
