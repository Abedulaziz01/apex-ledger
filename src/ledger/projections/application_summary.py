import asyncpg
from datetime import datetime, timezone
from typing import Optional


class ApplicationSummaryProjection:

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def handle(self, event_type: str, event_data: dict, recorded_at) -> None:
        handler = {
            "ApplicationSubmitted":    self._on_submitted,
            "CreditAnalysisRequested": self._on_analysis_requested,
            "CreditAnalysisCompleted": self._on_analysis_completed,
            "DecisionGenerated":       self._on_decision_generated,
            "HumanReviewCompleted":    self._on_human_review,
            "ApplicationApproved":     self._on_approved,
            "ApplicationDeclined":     self._on_declined,
        }.get(event_type)
        if handler:
            await handler(event_data, recorded_at)

    async def get_lag(self) -> float:
        """
        Milliseconds between the latest unprocessed event's created_at and now.
        Returns 0.0 if projection is fully caught up.
        """
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
        lag_ms = (now - event_ts).total_seconds() * 1000
        return max(0.0, lag_ms)

    async def _on_submitted(self, d: dict, ts) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO application_summary
                    (application_id, applicant_id, status, requested_amount_usd,
                     submission_channel, submitted_at, last_updated_at)
                VALUES ($1,$2,'Submitted',$3,$4,$5,NOW())
                ON CONFLICT (application_id) DO NOTHING
                """,
                d["application_id"], d["applicant_id"],
                d["requested_amount_usd"], d["submission_channel"], ts,
            )

    async def _on_analysis_requested(self, d: dict, ts) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE application_summary
                SET status='AwaitingAnalysis', assigned_agent_id=$2, last_updated_at=NOW()
                WHERE application_id=$1
                """,
                d["application_id"], d["assigned_agent_id"],
            )

    async def _on_analysis_completed(self, d: dict, ts) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE application_summary
                SET status='AnalysisComplete', risk_tier=$2,
                    confidence_score=$3, last_updated_at=NOW()
                WHERE application_id=$1
                """,
                d["application_id"], d["risk_tier"], d["confidence_score"],
            )

    async def _on_decision_generated(self, d: dict, ts) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE application_summary
                SET recommendation=$2, confidence_score=$3,
                    status='PendingDecision', decided_at=$4, last_updated_at=NOW()
                WHERE application_id=$1
                """,
                d["application_id"], d["recommendation"], d["confidence_score"], ts,
            )

    async def _on_human_review(self, d: dict, ts) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE application_summary
                SET reviewer_id=$2, final_decision=$3, last_updated_at=NOW()
                WHERE application_id=$1
                """,
                d["application_id"], d["reviewer_id"], d["final_decision"],
            )

    async def _on_approved(self, d: dict, ts) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE application_summary
                SET status='FinalApproved', approved_amount_usd=$2,
                    interest_rate=$3, last_updated_at=NOW()
                WHERE application_id=$1
                """,
                d["application_id"], d["approved_amount_usd"], d["interest_rate"],
            )

    async def _on_declined(self, d: dict, ts) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE application_summary
                SET status='FinalDeclined', last_updated_at=NOW()
                WHERE application_id=$1
                """,
                d["application_id"],
            )

    async def get(self, application_id: str) -> Optional[dict]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM application_summary WHERE application_id=$1",
                application_id,
            )
        return dict(row) if row else None