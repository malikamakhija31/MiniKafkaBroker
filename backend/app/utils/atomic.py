"""Crash-safe / multi-process-safe file helpers (Windows friendly)."""
import json
import os
import time
from pathlib import Path


def atomic_write_json(path, data, retries: int = 25):
    """Write JSON via temp-file + fsync + atomic replace.

    A crash can never leave a half-written file behind: readers see either the
    old complete file or the new complete file. The temp name is unique per
    process so three broker processes never clobber each other's temp file.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(data, indent=2))
        handle.flush()
        os.fsync(handle.fileno())
    try:
        for attempt in range(retries):
            try:
                os.replace(tmp, path)
                return
            except PermissionError:
                # Windows: another process has the target open for a moment.
                time.sleep(0.01 * (attempt + 1))
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def read_json(path, default, retries: int = 15):
    for _ in range(retries):
        try:
            return json.loads(Path(path).read_text(encoding="utf-8"))
        except FileNotFoundError:
            return default
        except (PermissionError, json.JSONDecodeError, OSError):
            time.sleep(0.02)
    return default


class FileLock:
    """Tiny cross-process lock based on O_EXCL lock-file creation."""

    def __init__(self, path, timeout: float = 5.0, stale_after: float = 10.0):
        self.path = Path(path)
        self.timeout = timeout
        self.stale_after = stale_after

    def __enter__(self):
        deadline = time.time() + self.timeout
        self.path.parent.mkdir(parents=True, exist_ok=True)
        while True:
            try:
                fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, str(os.getpid()).encode())
                os.close(fd)
                return self
            except FileExistsError:
                try:
                    if time.time() - self.path.stat().st_mtime > self.stale_after:
                        self.path.unlink()  # crashed holder
                        continue
                except OSError:
                    pass
                if time.time() > deadline:
                    raise TimeoutError(f"Could not acquire lock {self.path}")
                time.sleep(0.01)

    def __exit__(self, *exc):
        try:
            self.path.unlink()
        except OSError:
            pass
