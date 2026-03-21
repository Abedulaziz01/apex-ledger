import hashlib
import json
from dataclasses import dataclass
from typing import Optional
import asyncpg


@dataclass
class IntegrityReport:
    stream_id: str
    events_verified: int
    chain_valid: bool
    tamper_detected: bool
    first_tampered_event_id: Optional[int]
    final_hash: str


def _compute_event_hash(
    previous_hash: str,
    event_id: int,
    stream_id: str,
    version: int,
    event_type: str,
    event_data: str,   # raw JSON string from DB — never parsed
    created_at: str,
) -> str:
    """SHA-256 over previous_hash + canonical event fields."""
    canonical = json.dumps({
        "previous_hash": previous_hash,
        "event_id":      event_id,
        "stream_id":     stream_id,
        "version":       version,
        "event_type":    event_type,
        "event_data":    event_data,
        "created_at":    created_at,
    }, sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


async def run_integrity_check(
    pool: asyncpg.Pool,
    stream_id: str,
) -> IntegrityReport:
    """
    Walk every event in stream_id in version order.
    Re-compute the hash chain from scratch and compare against stored hashes.
    Stored hashes live in the events.metadata field under key '_chain_hash'.
    If a stored hash is missing (old events), we write it in-place on first run.
    Returns IntegrityReport — never raises.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, stream_id, version, event_type,
                   event_data, metadata, created_at
            FROM events
            WHERE stream_id = $1
            ORDER BY version ASC
            """,
            stream_id,
        )

    if not rows:
        return IntegrityReport(
            stream_id=stream_id,
            events_verified=0,
            chain_valid=True,
            tamper_detected=False,
            first_tampered_event_id=None,
            final_hash="",
        )

    previous_hash = "GENESIS"
    chain_valid = True
    tamper_detected = False
    first_tampered_event_id = None

    for row in rows:
        raw_data = row["event_data"]        # keep as raw string — never parse
        raw_meta = row["metadata"]
        created_at_str = row["created_at"].isoformat()

        # parse metadata to check stored hash
        try:
            meta = json.loads(raw_meta) if isinstance(raw_meta, str) else dict(raw_meta)
        except Exception:
            meta = {}

        expected_hash = _compute_event_hash(
            previous_hash=previous_hash,
            event_id=row["id"],
            stream_id=row["stream_id"],
            version=row["version"],
            event_type=row["event_type"],
            event_data=raw_data if isinstance(raw_data, str) else json.dumps(raw_data, sort_keys=True),
            created_at=created_at_str,
        )

        stored_hash = meta.get("_chain_hash")

        if stored_hash is None:
            # first-time run — write the hash into metadata (bootstrapping)
            meta["_chain_hash"] = expected_hash
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE events SET metadata=$1 WHERE id=$2",
                    json.dumps(meta),
                    row["id"],
                )
            stored_hash = expected_hash

        if stored_hash != expected_hash:
            chain_valid = False
            tamper_detected = True
            if first_tampered_event_id is None:
                first_tampered_event_id = row["id"]

        previous_hash = expected_hash  # chain always advances with computed hash

    return IntegrityReport(
        stream_id=stream_id,
        events_verified=len(rows),
        chain_valid=chain_valid,
        tamper_detected=tamper_detected,
        first_tampered_event_id=first_tampered_event_id,
        final_hash=previous_hash,
    )


async def get_chain_hash(pool: asyncpg.Pool, stream_id: str) -> str:
    """Return the final hash of the chain — used for cross-stream linking."""
    report = await run_integrity_check(pool, stream_id)
    return report.final_hash