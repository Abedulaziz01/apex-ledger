import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
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
    event_data: str,  # raw JSON string from DB - never parsed for canonical hash input
    created_at: str,
) -> str:
    canonical = json.dumps(
        {
            "previous_hash": previous_hash,
            "event_id": event_id,
            "stream_id": stream_id,
            "version": version,
            "event_type": event_type,
            "event_data": event_data,
            "created_at": created_at,
        },
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


async def _append_audit_event(pool: asyncpg.Pool, stream_id: str, report: IntegrityReport) -> None:
    audit_stream_id = f"audit-{stream_id}"
    check_timestamp = datetime.now(timezone.utc).isoformat()
    payload = {
        "entity_id": stream_id,
        "check_timestamp": check_timestamp,
        "events_verified_count": report.events_verified,
        "integrity_hash": report.final_hash,
        "previous_hash": "GENESIS",
        "chain_valid": report.chain_valid,
        "tamper_detected": report.tamper_detected,
        "first_tampered_event_id": report.first_tampered_event_id,
    }
    metadata = {
        "event_id": f"integrity-{int(datetime.now(timezone.utc).timestamp() * 1000)}",
        "occurred_at": check_timestamp,
        "schema_version": 1,
    }

    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                INSERT INTO event_streams (stream_id, current_version)
                VALUES ($1, 0)
                ON CONFLICT (stream_id) DO UPDATE
                    SET stream_id = EXCLUDED.stream_id
                RETURNING current_version
                """,
                audit_stream_id,
            )
            current = row["current_version"]
            next_version = current + 1
            inserted = await conn.fetchrow(
                """
                INSERT INTO events (stream_id, version, event_type, event_data, metadata)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING id
                """,
                audit_stream_id,
                next_version,
                "AuditIntegrityCheckRun",
                json.dumps(payload),
                json.dumps(metadata),
            )
            await conn.execute(
                """
                UPDATE event_streams
                SET current_version = $1, updated_at = NOW()
                WHERE stream_id = $2
                """,
                next_version,
                audit_stream_id,
            )
            await conn.execute(
                """
                INSERT INTO outbox (event_id, destination, payload, status, attempts, created_at)
                VALUES ($1, $2, $3, 'pending', 0, NOW())
                """,
                inserted["id"],
                "audit-sink",
                json.dumps({"audit_stream_id": audit_stream_id, "event_type": "AuditIntegrityCheckRun"}),
            )


async def run_integrity_check(
    pool: asyncpg.Pool,
    stream_id: str,
    *,
    write_audit_event: bool = True,
) -> IntegrityReport:
    """
    Walk every event in stream_id in version order.
    Re-compute the hash chain from scratch and compare against stored hashes.
    Stored hashes live in events.metadata['_chain_hash'].
    Missing hashes are bootstrapped on first run.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, stream_id, version, event_type, event_data, metadata, created_at
            FROM events
            WHERE stream_id = $1
            ORDER BY version ASC
            """,
            stream_id,
        )

    if not rows:
        report = IntegrityReport(
            stream_id=stream_id,
            events_verified=0,
            chain_valid=True,
            tamper_detected=False,
            first_tampered_event_id=None,
            final_hash="",
        )
        if write_audit_event:
            await _append_audit_event(pool, stream_id, report)
        return report

    previous_hash = "GENESIS"
    chain_valid = True
    tamper_detected = False
    first_tampered_event_id = None

    for row in rows:
        raw_data = row["event_data"]
        raw_meta = row["metadata"]
        created_at_str = row["created_at"].isoformat()

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

        previous_hash = expected_hash

    report = IntegrityReport(
        stream_id=stream_id,
        events_verified=len(rows),
        chain_valid=chain_valid,
        tamper_detected=tamper_detected,
        first_tampered_event_id=first_tampered_event_id,
        final_hash=previous_hash,
    )
    if write_audit_event:
        await _append_audit_event(pool, stream_id, report)
    return report


async def get_chain_hash(pool: asyncpg.Pool, stream_id: str) -> str:
    report = await run_integrity_check(pool, stream_id)
    return report.final_hash

