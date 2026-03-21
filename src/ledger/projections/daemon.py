import asyncio
import json
import logging
from datetime import datetime

import asyncpg

from src.ledger.projections.application_summary import ApplicationSummaryProjection
from src.ledger.projections.agent_performance import AgentPerformanceProjection
from src.ledger.projections.compliance_audit import ComplianceAuditProjection

logger = logging.getLogger(__name__)

CHECKPOINT_NAME = "main_daemon"
BATCH_SIZE = 100
POLL_INTERVAL_SECONDS = 1.0


class ProjectionDaemon:
    """
    Async daemon that tails the events table and fans out to all projections.
    Fault-tolerant: bad event is logged and skipped, daemon never crashes.
    Checkpoint is persisted in projection_checkpoints so restarts resume safely.
    """

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool
        self._running = False
        self._projections = [
            ApplicationSummaryProjection(pool),
            AgentPerformanceProjection(pool),
            ComplianceAuditProjection(pool),
        ]

    # ── public API ────────────────────────────────────────────────────────────

    async def run_forever(self) -> None:
        """Start the daemon loop. Runs until stop() is called."""
        self._running = True
        logger.info("ProjectionDaemon started")
        while self._running:
            try:
                processed = await self._process_batch()
                if processed == 0:
                    # nothing new — back off before polling again
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
            except Exception as exc:
                # outer loop never crashes — log and wait
                logger.error("Daemon outer loop error: %s", exc, exc_info=True)
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
        logger.info("ProjectionDaemon stopped")

    def stop(self) -> None:
        """Signal the daemon to stop after the current batch."""
        self._running = False

    # ── internals ─────────────────────────────────────────────────────────────

    async def _get_checkpoint(self) -> int:
        """Return last processed event id, 0 if none."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT last_event_id FROM projection_checkpoints WHERE projection_name=$1",
                CHECKPOINT_NAME,
            )
        return row["last_event_id"] if row else 0

    async def _save_checkpoint(self, last_event_id: int) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO projection_checkpoints (projection_name, last_event_id, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (projection_name) DO UPDATE
                    SET last_event_id = $2, updated_at = NOW()
                """,
                CHECKPOINT_NAME, last_event_id,
            )

    async def _fetch_batch(self, after_id: int) -> list:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, event_type, event_data, created_at
                FROM events
                WHERE id > $1
                ORDER BY id ASC
                LIMIT $2
                """,
                after_id, BATCH_SIZE,
            )
        return rows

    async def _process_batch(self) -> int:
        """Fetch next batch, fan out to projections, save checkpoint. Returns count processed."""
        last_id = await self._get_checkpoint()
        rows = await self._fetch_batch(last_id)

        if not rows:
            return 0

        for row in rows:
            await self._dispatch_event(row)
            last_id = row["id"]

        await self._save_checkpoint(last_id)
        logger.debug("Processed batch up to event id=%d", last_id)
        return len(rows)

    async def _dispatch_event(self, row) -> None:
        """Fan out one event to all projections. Bad event = log + skip."""
        event_type = row["event_type"]
        raw = row["event_data"]
        recorded_at = row["created_at"]

        # event_data stored as JSON string — parse it
        try:
            event_data = json.loads(raw) if isinstance(raw, str) else dict(raw)
        except Exception as exc:
            logger.error(
                "Failed to parse event_data for event id=%d type=%s: %s",
                row["id"], event_type, exc,
            )
            return  # skip this event, don't crash

        for projection in self._projections:
            try:
                await projection.handle(event_type, event_data, recorded_at)
            except Exception as exc:
                # one bad projection never kills the others or the daemon
                logger.error(
                    "Projection %s failed on event id=%d type=%s: %s",
                    projection.__class__.__name__, row["id"], event_type, exc,
                    exc_info=True,
                )