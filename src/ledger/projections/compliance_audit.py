import asyncpg
from typing import Optional


class ComplianceAuditProjection:
    """Regulatory read model with full temporal query support."""

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
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO compliance_audit_view
                    (application_id, rule_id, rule_version, status,
                     evidence_hash, evaluated_at, recorded_at)
                VALUES ($1,$2,$3,'passed',$4,$5,$6)
                """,
                d["application_id"], d["rule_id"], d["rule_version"],
                d["evidence_hash"], d["evaluation_timestamp"], ts,
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
        """All compliance records for an application — full audit trail."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM compliance_audit_view
                WHERE application_id=$1
                ORDER BY recorded_at ASC
                """,
                application_id,
            )
        return [dict(r) for r in rows]

    async def get_at_time(self, application_id: str, as_of) -> list:
        """Temporal query — state of compliance at a specific point in time."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM compliance_audit_view
                WHERE application_id=$1 AND recorded_at <= $2
                ORDER BY recorded_at ASC
                """,
                application_id, as_of,
            )
        return [dict(r) for r in rows]