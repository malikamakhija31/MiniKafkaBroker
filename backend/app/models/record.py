
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


class Record(BaseModel):

    topic: str

    partition: int = Field(ge=0)

    offset: int = Field(ge=0)

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    key: str | None = None

    value: dict[str, Any]

    headers: dict[str, str] = Field(default_factory=dict)

    # --- reliability additions (all optional => old logs stay readable) ---
    # Producer-supplied token. Same token on the same partition == same message.
    idempotency_key: str | None = None

    # Transaction this record belongs to (None = ordinary non-transactional record).
    txn_id: str | None = None

    # DATA = business record, COMMIT / ABORT = transaction control marker.
    record_type: str = "DATA"
