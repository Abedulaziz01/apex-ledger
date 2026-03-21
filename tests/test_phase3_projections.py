import asyncio
import asyncpg
import os
import pytest
import pytest_asyncio
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from src.ledger.core.event_store import EventStore
from src.ledger.core.models import BaseEvent
from src.ledger.projections.daemon import ProjectionDaemon
from src.ledger.projections.application_summary import ApplicationSummaryProjection
from src.ledger.projections.compliance_audit import ComplianceAuditProjection

load_dotenv()


@pytest_asyncio.fixture()
async def store_and_pool():
    pool = await asyncpg.create_pool(os.getenv("DATABASE_URL"), min_size=2, max_size=10)
    store = EventStore(pool)

    # clean BEFORE the test too, not just after
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE events, event_streams, outbox, application_summary, "
            "agent_performance_ledger, compliance_audit_view, "
            "projection_checkpoints RESTART IDENTITY CASCADE"
        )

    yield store, pool

    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE events, event_streams, outbox, application_summary, "
            "agent_performance_ledger, compliance_audit_view, "
            "projection_checkpoints RESTART IDENTITY CASCADE"
        )
    await pool.close()


# ── Test 1: ApplicationSummary lag under 500ms at 50 concurrent handlers ──────

@pytest.mark.asyncio
async def test_application_summary_lag_under_500ms(store_and_pool):
    store, pool = store_and_pool
    proj = ApplicationSummaryProjection(pool)
    daemon = ProjectionDaemon(pool)

    async def write_one(i):
        await store.append(
            f"loan-lag-{i}",
            [BaseEvent(event_type="ApplicationSubmitted", event_data={
                "application_id":       f"lag-app-{i}",
                "applicant_id":         f"cust-{i}",
                "requested_amount_usd": 10000 + i,
                "loan_purpose":         "test",
                "submission_channel":   "api",
                "submitted_at":         "2026-01-01T00:00:00Z",
            })],
            expected_version=0,
        )

    await asyncio.gather(*[write_one(i) for i in range(50)])

    total = 0
    while True:
        n = await daemon._process_batch()
        total += n
        if n == 0:
            break

    lag = await proj.get_lag()
    assert lag < 500.0, f"Lag too high: {lag:.1f}ms"
    print(f"Lag after 50 events: {lag:.1f}ms — OK")


# ── Test 2: ComplianceAuditView temporal query ────────────────────────────────

@pytest.mark.asyncio
async def test_compliance_temporal_query(store_and_pool):
    store, pool = store_and_pool
    proj = ComplianceAuditProjection(pool)
    daemon = ProjectionDaemon(pool)

    # write compliance events FIRST, then capture t_before after a small gap
    await store.append(
        "compliance-temporal-001",
        [BaseEvent(event_type="ComplianceCheckRequested", event_data={
            "application_id":         "temporal-001",
            "regulation_set_version": "v2",
            "checks_required":        ["KYC", "AML"],
        })],
        expected_version=0,
    )
    await store.append(
        "compliance-temporal-001",
        [BaseEvent(event_type="ComplianceRulePassed", event_data={
            "application_id":       "temporal-001",
            "rule_id":              "KYC",
            "rule_version":         "v1",
            "evaluation_timestamp": "2026-01-01T00:00:00Z",
            "evidence_hash":        "abc123",
        })],
        expected_version=1,
    )

    # process batch to populate projection
    while await daemon._process_batch():
        pass

    # capture timestamps AFTER processing
    t_after = datetime.now(timezone.utc)
    # t_before = a point in time before these events were recorded
    t_before = datetime(2025, 1, 1, tzinfo=timezone.utc)

    # temporal query — should see KYC passed
    records_after = await proj.get_state_at("temporal-001", t_after)
    assert any(r["rule_id"] == "KYC" and r["status"] == "passed" for r in records_after), \
        "KYC passed record missing after timestamp"

    # temporal query using a date far in the past — should be empty
    records_before = await proj.get_state_at("temporal-001", t_before)
    assert len(records_before) == 0, \
        f"Expected 0 records before events, got {len(records_before)}"

    print(f"Temporal query OK — {len(records_after)} record(s) after, 0 before")


# ── Test 3: rebuild_from_scratch completes without blocking ───────────────────

@pytest.mark.asyncio
async def test_rebuild_from_scratch(store_and_pool):
    store, pool = store_and_pool
    proj = ComplianceAuditProjection(pool)
    daemon = ProjectionDaemon(pool)

    await store.append(
        "compliance-rebuild-001",
        [BaseEvent(event_type="ComplianceCheckRequested", event_data={
            "application_id":         "rebuild-001",
            "regulation_set_version": "v2",
            "checks_required":        ["KYC"],
        })],
        expected_version=0,
    )
    await store.append(
        "compliance-rebuild-001",
        [BaseEvent(event_type="ComplianceRulePassed", event_data={
            "application_id":       "rebuild-001",
            "rule_id":              "KYC",
            "rule_version":         "v1",
            "evaluation_timestamp": "2026-01-01T00:00:00Z",
            "evidence_hash":        "hashxyz",
        })],
        expected_version=1,
    )

    while await daemon._process_batch():
        pass

    records_before = await proj.get_for_application("rebuild-001")
    assert len(records_before) > 0, "Projection empty before rebuild"

    await proj.rebuild_from_scratch(store)

    records_after = await proj.get_for_application("rebuild-001")
    assert len(records_after) > 0, "Projection empty after rebuild"
    assert any(r["rule_id"] == "KYC" for r in records_after), \
        "KYC record missing after rebuild"

    print(f"Rebuild OK — {len(records_after)} record(s) restored")