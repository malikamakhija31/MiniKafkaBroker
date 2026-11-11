
from typing import Any

from pydantic import BaseModel, Field


class CreateTopicRequest(BaseModel):

    name: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-zA-Z0-9_-]+$"
    )

    partitions: int = Field(
        ge=1,
        le=32
    )

    segment_max_bytes: int = Field(
        default=1048576,
        ge=1024
    )


class ProduceRequest(BaseModel):

    key: str | None = None

    value: dict[str, Any]

    headers: dict[str, str] = Field(
        default_factory=dict
    )


class CommitOffsetRequest(BaseModel):

    offset: int = Field(ge=0)
