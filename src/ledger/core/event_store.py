import json
import asyncpg
from typing import Optional
from src.ledger.core.models import BaseEvent, StoredEvent, StreamMetadata
from src.ledger.core.exceptions import OptimisticConcurrencyError
from src.ledger.upcasting.registry import registry as upcaster_registry


class EventStore:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def append(
        self,
        stream_id: str,
        events: list[BaseEvent],
        expected_version: int,
    ) -> int:
        """Append events to a stream. Raises OptimisticConcurrencyError on conflict."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # Lock the stream row and get current version
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

                if current_version != expected_version:
                    raise OptimisticConcurrencyError(
                        stream_id, expected_version, current_version
                    )

                new_version = current_version
                for event in events:
                    new_version += 1
                    await conn.execute(
                        """
                        INSERT INTO events (stream_id, version, event_type, event_data, metadata)
                        VALUES ($1, $2, $3, $4, $5)
                        """,
                        stream_id,
                        new_version,
                        event.event_type,
                        json.dumps(event.event_data),
                        json.dumps(event.metadata),
                    )

                await conn.execute(
                    """
                    UPDATE event_streams
                    SET current_version = $1, updated_at = NOW()
                    WHERE stream_id = $2
                    """,
                    new_version,
                    stream_id,
                )

                return new_version

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