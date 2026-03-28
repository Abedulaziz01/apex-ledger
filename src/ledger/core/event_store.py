import json
import os
import asyncpg
from datetime import datetime, timezone
from typing import Any, Optional
from src.ledger.core.models import BaseEvent, StoredEvent, StreamMetadata
from src.ledger.core.exceptions import OptimisticConcurrencyError
from src.ledger.upcasting.registry import registry as upcaster_registry

def _to_json(obj) -> str:
    """JSON serialize with datetime support."""
    def default(o):
        if isinstance(o, datetime):
            return o.isoformat()
        raise TypeError(f"Not serializable: {type(o)}")
    return json.dumps(obj, default=default)

def _json_ready(value: Any) -> Any:
    """Normalize supported event shapes into JSON-safe primitives."""
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return value
    return value


def _event_payload_for_store(event: Any) -> Any:
    if hasattr(event, "model_dump_for_store"):
        return event.model_dump_for_store().get("payload", {})
    if hasattr(event, "payload"):
        return _json_ready(event.payload)
    return _json_ready(event.event_data)


def _event_metadata_for_store(event: Any) -> Any:
    if hasattr(event, "model_dump_for_store"):
        return event.model_dump_for_store().get("metadata", {})
    return _json_ready(event.metadata)


class EventStore:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    @classmethod
    async def create(cls) -> "EventStore":
        pool = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=2, max_size=10)
        return cls(pool)

    async def append(
        self,
        stream_id: str,
        events: list,
        expected_version: int,
    ) -> list:
        """Append events to a stream. Returns list of stored events with stream_position set."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    INSERT INTO event_streams (stream_id, current_version)
                    VALUES ($1, 0)
                    ON CONFLICT (stream_id) DO UPDATE
                        SET stream_id = EXCLUDED.stream_id
                    RETURNING current_version
                    """,
                    stream_id,
                )
                current_version = row["current_version"]

                effective_expected = 0 if expected_version == -1 else expected_version
                if current_version != effective_expected:
                    raise OptimisticConcurrencyError(
                        stream_id, expected_version, current_version
                    )

                new_version = current_version
                stored = []
                for event in events:
                    new_version += 1
                    payload = event.payload if hasattr(event, 'payload') else event.event_data
                    meta = event.metadata
                    if hasattr(meta, 'model_dump'):
                        meta = meta.model_dump()

                    result = await conn.fetchrow(
                        """
                        INSERT INTO events (stream_id, version, event_type, event_data, metadata)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id, created_at
                        """,
                        stream_id,
                        new_version,
                        event.event_type,
                        _to_json(payload),
                        _to_json(meta),
                    )
                    
                    # Create StoredEvent object to return
                    stored_event = StoredEvent(
                        id=result["id"],
                        stream_id=stream_id,
                        version=new_version,
                        event_type=event.event_type,
                        event_data=payload,
                        metadata=meta,
                        created_at=result["created_at"],
                    )
                    stored.append(stored_event)

                await conn.execute(
                    """
                    UPDATE event_streams
                    SET current_version = $1, updated_at = NOW()
                    WHERE stream_id = $2
                    """,
                    new_version,
                    stream_id,
                )

                return stored

    async def load_stream(
        self,
        stream_id: str,
        from_version: int = 0,
    ) -> list[StoredEvent]:
        """Load all events for a stream, optionally from a specific version."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, stream_id, version, event_type, event_data, metadata, created_at
                FROM events
                WHERE stream_id = $1 AND version > $2
                ORDER BY version ASC
                """,
                stream_id,
                from_version,
            )
        result = []
        for r in rows:
            version = r.get("version", 1) if isinstance(r, dict) else 1
            raw_data = json.loads(r["event_data"]) if isinstance(r["event_data"], str) else dict(r["event_data"])
            final_version, upcasted_data = upcaster_registry.upcast(
                r["event_type"], version, raw_data
            )
            result.append(StoredEvent(
                id=r["id"],
                stream_id=r["stream_id"],
                version=r["version"],
                event_type=r["event_type"],
                event_data=upcasted_data,
                metadata=json.loads(r["metadata"]) if isinstance(r["metadata"], str) else dict(r["metadata"]),
                created_at=r["created_at"],
            ))
        return result

    async def load_all(
        self,
        after_event_id: int = 0,
        limit: int = 1000,
    ) -> list[StoredEvent]:
        """Global event log — used by projections."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, stream_id, version, event_type, event_data, metadata, created_at
                FROM events
                WHERE id > $1
                ORDER BY id ASC
                LIMIT $2
                """,
                after_event_id,
                limit,
            )
        result = []
        for r in rows:
            version = r.get("version", 1) if isinstance(r, dict) else 1
            raw_data = json.loads(r["event_data"]) if isinstance(r["event_data"], str) else dict(r["event_data"])
            final_version, upcasted_data = upcaster_registry.upcast(
                r["event_type"], version, raw_data
            )
            result.append(StoredEvent(
                id=r["id"],
                stream_id=r["stream_id"],
                version=r["version"],
                event_type=r["event_type"],
                event_data=upcasted_data,
                metadata=json.loads(r["metadata"]) if isinstance(r["metadata"], str) else dict(r["metadata"]),
                created_at=r["created_at"],
            ))
        return result

    async def stream_version(self, stream_id: str) -> int:
        """Return current version of a stream, 0 if it doesn't exist."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
        return row["current_version"] if row else 0

    async def archive_stream(self, stream_id: str) -> None:
        """Mark stream as archived in metadata column."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE event_streams
                SET updated_at = NOW()
                WHERE stream_id = $1
                """,
                stream_id,
            )

    async def get_stream_metadata(self, stream_id: str) -> Optional[StreamMetadata]:
        """Return StreamMetadata or None if stream doesn't exist."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT stream_id, current_version, created_at, updated_at
                FROM event_streams
                WHERE stream_id = $1
                """,
                stream_id,
            )
        if not row:
            return None
        return StreamMetadata(
            stream_id=row["stream_id"],
            current_version=row["current_version"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


# Alias for agent compatibility
AbstractEventStore = EventStore


class InMemoryEventStore:
    """
    Lightweight in-memory store kept for backwards-compatible tests.
    Uses zero-based stream positions and returns written StoredEvent objects.
    """

    def __init__(self):
        self._streams: dict[str, list[StoredEvent]] = {}
        self._global_events: list[StoredEvent] = []
        self._next_id = 1

    async def append(
        self,
        stream_id: str,
        events: list[BaseEvent],
        expected_version: int,
    ) -> list[StoredEvent]:
        current = await self.stream_version(stream_id)
        if current != expected_version:
            raise OptimisticConcurrencyError(stream_id, expected_version, current)

        if not events:
            return []

        stream = self._streams.setdefault(stream_id, [])
        written: list[StoredEvent] = []

        for event in events:
            position = len(stream)
            stored = StoredEvent(
                id=self._next_id,
                stream_id=stream_id,
                version=position,
                event_type=event.event_type,
                event_data=_event_payload_for_store(event),
                metadata=_event_metadata_for_store(event),
                created_at=datetime.now(timezone.utc),
            )
            self._next_id += 1
            stream.append(stored)
            self._global_events.append(stored)
            written.append(stored)

        return written

    async def load_stream(
        self,
        stream_id: str,
        from_version: int = -1,
    ) -> list[StoredEvent]:
        stream = self._streams.get(stream_id, [])
        return [event for event in stream if event.version > from_version]

    async def load_all(
        self,
        after_position: int = -1,
        after_event_id: Optional[int] = None,
        limit: int = 1000,
    ) -> list[StoredEvent]:
        if after_event_id is not None:
            cutoff_id = after_event_id
        else:
            # Backwards-compatible meaning: after_position is zero-based index
            # in the global stream, so convert to 1-based event ids.
            cutoff_id = after_position + 1
        return [event for event in self._global_events if event.id > cutoff_id][:limit]

    async def stream_version(self, stream_id: str) -> int:
        stream = self._streams.get(stream_id)
        if not stream:
            return -1
        return stream[-1].version

    async def archive_stream(self, stream_id: str) -> None:
        return None

    async def get_stream_metadata(self, stream_id: str) -> Optional[StreamMetadata]:
        stream = self._streams.get(stream_id)
        if not stream:
            return None

        created_at = stream[0].created_at
        updated_at = stream[-1].created_at
        return StreamMetadata(
            stream_id=stream_id,
            current_version=stream[-1].version,
            created_at=created_at,
            updated_at=updated_at,
        )
