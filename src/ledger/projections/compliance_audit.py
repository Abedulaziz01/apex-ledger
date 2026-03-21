import asyncpg
from datetime import datetime, timezone
from typing import Optional
import asyncio
import asyncpg
from datetime import datetime, timezone
from typing import Optional
import asyncio
import asyncpg
from datetime import datetime, timezone
from typing import Optional


class ComplianceAuditProjection:

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def handle(self, event_type: str, event_data: dict, recorded_at) -> None:
        handler = {
            "ComplianceCheckRequested": self._on_check_requested,
            "ComplianceRulePassed":     self._on_rule_passed,
            "ComplianceRuleFailed":     self._on_rule_failed,
        }.get(event_type)
        if handler:
            await handler(event_data, recorded_at)

    async def get_lag(self) -> float:
        async with self._pool.acquire() as conn:
            checkpoint = await conn.fetchrow(
                "SELECT last_event_id FROM projection_checkpoints WHERE projection_name='main_daemon'"
            )
            last_id = checkpoint["last_event_id"] if checkpoint else 0
            row = await conn.fetchrow(
                "SELECT created_at FROM events WHERE id > $1 ORDER BY id ASC LIMIT 1",
                last_id,
            )
        if not row:
            return 0.0
        now = datetime.now(timezone.utc)
        event_ts = row["created_at"]
        if event_ts.tzinfo is None:
            event_ts = event_ts.replace(tzinfo=timezone.utc)
        return max(0.0, (now - event_ts).total_seconds() * 1000)

    async def get_state_at(self, application_id: str, timestamp: datetime) -> list:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM compliance_audit_view
                WHERE application_id=$1 AND recorded_at <= $2
                ORDER BY recorded_at ASC
                """,
                application_id, timestamp,
            )
        return [dict(r) for r in rows]

    async def rebuild_from_scratch(self, event_store) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute("TRUNCATE compliance_audit_view RESTART IDENTITY")

        after_id = 0
        batch_size = 200
        while True:
            events = await event_store.load_all(
                after_event_id=after_id, limit=batch_size
            )
            if not events:
                break
            for evt in events:
                if evt.event_type in (
                    "ComplianceCheckRequested",
                    "ComplianceRulePassed",
                    "ComplianceRuleFailed",
                ):
                    await self.handle(evt.event_type, evt.event_data, evt.created_at)
                after_id = evt.id
            await asyncio.sleep(0)

        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO projection_checkpoints (projection_name, last_event_id, updated_at)
                VALUES ('compliance_rebuild', 0, NOW())
                ON CONFLICT (projection_name) DO UPDATE SET last_event_id=0, updated_at=NOW()
                """
            )

    async def _on_check_requested(self, d: dict, ts) -> None:
        async with self._pool.acquire() as conn:
            for rule_id in d.get("checks_required", []):
                await conn.execute(
                    """
                    INSERT INTO compliance_audit_view
                        (application_id, rule_id, rule_version, status,
                         regulation_set_version, recorded_at)
                    VALUES ($1,$2,'pending','pending',$3,$4)
                    ON CONFLICT DO NOTHING
                    """,
                    d["application_id"], rule_id,
                    d["regulation_set_version"], ts,
                )

    async def _on_rule_passed(self, d: dict, ts) -> None:
        eval_ts = d["evaluation_timestamp"]
        if isinstance(eval_ts, str):
            eval_ts = datetime.fromisoformat(eval_ts.replace("Z", "+00:00"))
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO compliance_audit_view
                    (application_id, rule_id, rule_version, status,
                     evidence_hash, evaluated_at, recorded_at)
                VALUES ($1,$2,$3,'passed',$4,$5,$6)
                """,
                d["application_id"], d["rule_id"], d["rule_version"],
                d["evidence_hash"], eval_ts, ts,
            )

    async def _on_rule_failed(self, d: dict, ts) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO compliance_audit_view
                    (application_id, rule_id, rule_version, status,
                     failure_reason, remediation_required, recorded_at)
                VALUES ($1,$2,$3,'failed',$4,$5,$6)
                """,
                d["application_id"], d["rule_id"], d["rule_version"],
                d["failure_reason"], d["remediation_required"], ts,
            )

    async def get_for_application(self, application_id: str) -> list:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM compliance_audit_view
                WHERE application_id=$1 ORDER BY recorded_at ASC
                """,
                application_id,
            )
        return [dict(r) for r in rows]