import asyncpg
from typing import Optional


class AgentPerformanceProjection:
    """Tracks per-agent, per-model-version metrics."""

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def handle(self, event_type: str, event_data: dict, recorded_at) -> None:
        handler = {
            "CreditAnalysisCompleted": self._on_credit_analysis,
            "FraudScreeningCompleted": self._on_fraud_screening,
        }.get(event_type)
        if handler:
            await handler(event_data, recorded_at)

    async def _on_credit_analysis(self, d: dict, ts) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO agent_performance_ledger
                    (agent_id, model_version, total_analyses, avg_confidence_score,
                     avg_duration_ms, last_active_at)
                VALUES ($1,$2,1,$3,$4,$5)
                ON CONFLICT (agent_id, model_version) DO UPDATE SET
                    total_analyses        = agent_performance_ledger.total_analyses + 1,
                    avg_confidence_score  = (
                        agent_performance_ledger.avg_confidence_score *
                        agent_performance_ledger.total_analyses + $3
                    ) / (agent_performance_ledger.total_analyses + 1),
                    avg_duration_ms       = (
                        agent_performance_ledger.avg_duration_ms *
                        agent_performance_ledger.total_analyses + $4
                    ) / (agent_performance_ledger.total_analyses + 1),
                    last_active_at        = $5
                """,
                d["agent_id"], d["model_version"],
                d["confidence_score"], d["analysis_duration_ms"], ts,
            )

    async def _on_fraud_screening(self, d: dict, ts) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO agent_performance_ledger
                    (agent_id, model_version, total_fraud_screens, last_active_at)
                VALUES ($1,$2,1,$3)
                ON CONFLICT (agent_id, model_version) DO UPDATE SET
                    total_fraud_screens = agent_performance_ledger.total_fraud_screens + 1,
                    last_active_at      = $3
                """,
                d["agent_id"], d["screening_model_version"], ts,
            )

    async def get(self, agent_id: str) -> list:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM agent_performance_ledger WHERE agent_id=$1",
                agent_id,
            )
        return [dict(r) for r in rows]