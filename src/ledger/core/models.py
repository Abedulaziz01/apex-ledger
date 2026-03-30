from datetime import datetime, timezone
from typing import Any
import uuid

from pydantic import BaseModel, ConfigDict, Field


class BaseEvent(BaseModel):
    """What you create before storing."""

    model_config = ConfigDict(extra="allow")

    event_type: str
    event_data: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def payload(self) -> dict[str, Any]:
        return self.event_data


class StoredEvent(BaseModel):
    """What comes back out of the database."""

    model_config = ConfigDict(extra="allow")

    id: int
    stream_id: str
    version: int
    stream_position: int
    global_position: int
    event_type: str
    event_data: dict[str, Any]
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    recorded_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def payload(self) -> dict[str, Any]:
        return self.event_data


class StreamMetadata(BaseModel):
    """Current state of a stream."""

    model_config = ConfigDict(extra="allow")

    stream_id: str
    current_version: int
    created_at: datetime
    updated_at: datetime
    archived_at: datetime | None = None
