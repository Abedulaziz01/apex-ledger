import asyncio
import asyncpg
import os
from dotenv import load_dotenv
from src.ledger.core.event_store import EventStore
from src.ledger.core.models import BaseEvent
from src.ledger.projections.daemon import ProjectionDaemon
from src.ledger.projections.application_summary import ApplicationSummaryProjection

load_dotenv()

async def main():
    pool = await asyncpg.create_pool(os.getenv("DATABASE_URL"), min_size=2, max_size=5)
    store = EventStore(pool)
    daemon = ProjectionDaemon(pool)

    # write one event
    await store.append(
        "loan-daemon-test",
        [BaseEvent(event_type="ApplicationSubmitted", event_data={
            "application_id": "daemon-test-001",
            "applicant_id":   "cust-99",
            "requested_amount_usd": 75000,
            "loan_purpose":   "expansion",
            "submission_channel": "api",
            "submitted_at":   "2026-01-01T00:00:00Z",
        })],
        expected_version=0,
    )
    print("Event written")

    # run one batch
    processed = await daemon._process_batch()
    print(f"Batch processed: {processed} event(s)")

    # check projection
    proj = ApplicationSummaryProjection(pool)
    row = await proj.get("daemon-test-001")
    print(f"Projection row: {row}")

    assert row is not None, "Projection row missing!"
    assert row["status"] == "Submitted"
    print("Daemon end-to-end OK")

    await pool.close()

asyncio.run(main())