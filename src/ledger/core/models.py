from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
import uuid


@dataclass
class BaseEvent:
    """What you create before storing."""
    event_type: str
    event_data: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass
class StoredEvent:
    """What comes back out of the database."""
    id: int
    stream_id: str
    version: int
    event_type: str
    event_data: dict[str, Any]
    metadata: dict[str, Any]
    created_at: datetime

    @property
    def payload(self) -> dict[str, Any]:
        return self.event_data

    @property
    def stream_position(self) -> int:
        return self.version


@dataclass
class StreamMetadata:
    """Current state of a stream."""
    stream_id: str
    current_version: int
    created_at: datetime
    updated_at: datetime
    archived_at: datetime | None = None
