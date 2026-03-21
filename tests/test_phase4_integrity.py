import asyncio
import asyncpg
import json
import os
import pytest
import pytest_asyncio
from dotenv import load_dotenv

from src.ledger.core.event_store import EventStore
from src.ledger.core.models import BaseEvent
from src.ledger.integrity.audit_chain import run_integrity_check

load_dotenv()


@pytest_asyncio.fixture()
async def store_and_pool():
    pool = await asyncpg.create_pool(os.getenv("DATABASE_URL"), min_size=2, max_size=10)
    store = EventStore(pool)
    yield store, pool
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE events, event_streams, outbox RESTART IDENTITY CASCADE"
        )
    await pool.close()


# ── Test 1: clean chain passes integrity check ────────────────────────────────

@pytest.mark.asyncio
async def test_clean_chain_is_valid(store_and_pool):
    store, pool = store_and_pool

    await store.append(
        "loan-integrity-001",
        [
            BaseEvent(event_type="ApplicationSubmitted", event_data={
                "application_id": "integrity-001",
                "applicant_id":   "cust-1",
                "requested_amount_usd": 50000,
                "loan_purpose":   "expansion",
                "submission_channel": "web",
                "submitted_at":   "2026-01-01T00:00:00Z",
            }),
            BaseEvent(event_type="CreditAnalysisRequested", event_data={
                "application_id":  "integrity-001",
                "assigned_agent_id": "agent-1",
                "requested_at":    "2026-01-01T00:01:00Z",
                "priority":        "high",
            }),
        ],
        expected_version=0,
    )

    report = await run_integrity_check(pool, "loan-integrity-001")

    assert report.events_verified == 2
    assert report.chain_valid is True
    assert report.tamper_detected is False
    assert report.first_tampered_event_id is None
    assert report.final_hash != ""
    print(f"Clean chain OK — {report.events_verified} events, hash={report.final_hash[:16]}...")


# ── Test 2: tampered payload detected ────────────────────────────────────────

@pytest.mark.asyncio
async def test_tampered_payload_detected(store_and_pool):
    store, pool = store_and_pool

    await store.append(
        "loan-integrity-002",
        [
            BaseEvent(event_type="ApplicationSubmitted", event_data={
                "application_id": "integrity-002",
                "applicant_id":   "cust-2",
                "requested_amount_usd": 100000,
                "loan_purpose":   "acquisition",
                "submission_channel": "api",
                "submitted_at":   "2026-01-01T00:00:00Z",
            }),
        ],
        expected_version=0,
    )

    # bootstrap the chain first (writes _chain_hash into metadata)
    report_before = await run_integrity_check(pool, "loan-integrity-002")
    assert report_before.chain_valid is True

    # TAMPER — directly edit event_data in the DB (simulates an attacker)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id FROM events WHERE stream_id='loan-integrity-002' LIMIT 1"
        )
        tampered_data = json.dumps({
            "application_id": "integrity-002",
            "applicant_id":   "cust-2",
            "requested_amount_usd": 999999,   # <-- changed!
            "loan_purpose":   "acquisition",
            "submission_channel": "api",
            "submitted_at":   "2026-01-01T00:00:00Z",
        })
        await conn.execute(
            "UPDATE events SET event_data=$1 WHERE id=$2",
            tampered_data, row["id"],
        )

    # run integrity check again — must detect tamper
    report_after = await run_integrity_check(pool, "loan-integrity-002")

    assert report_after.chain_valid is False, "Expected chain_valid=False after tamper"
    assert report_after.tamper_detected is True, "Expected tamper_detected=True"
    assert report_after.first_tampered_event_id == row["id"]
    print(f"Tamper detected OK — first tampered event id={report_after.first_tampered_event_id}")


# ── Test 3: empty stream returns valid (nothing to check) ─────────────────────

@pytest.mark.asyncio
async def test_empty_stream_is_valid(store_and_pool):
    store, pool = store_and_pool
    report = await run_integrity_check(pool, "stream-that-does-not-exist")
    assert report.chain_valid is True
    assert report.events_verified == 0
    assert report.tamper_detected is False
    print("Empty stream integrity check OK")


# ── Test 4: running check twice on clean data is idempotent ───────────────────

@pytest.mark.asyncio
async def test_integrity_check_is_idempotent(store_and_pool):
    store, pool = store_and_pool

    await store.append(
        "loan-integrity-003",
        [BaseEvent(event_type="ApplicationSubmitted", event_data={
            "application_id": "integrity-003",
            "applicant_id":   "cust-3",
            "requested_amount_usd": 25000,
            "loan_purpose":   "equipment",
            "submission_channel": "branch",
            "submitted_at":   "2026-01-01T00:00:00Z",
        })],
        expected_version=0,
    )

    report1 = await run_integrity_check(pool, "loan-integrity-003")
    report2 = await run_integrity_check(pool, "loan-integrity-003")

    assert report1.final_hash == report2.final_hash
    assert report1.chain_valid is True
    assert report2.chain_valid is True
    print(f"Idempotent OK — same hash both runs: {report1.final_hash[:16]}...")


# ── Test 5: second event tamper detected (chain breaks mid-stream) ────────────

@pytest.mark.asyncio
async def test_mid_stream_tamper_detected(store_and_pool):
    store, pool = store_and_pool

    await store.append(
        "loan-integrity-004",
        [
            BaseEvent(event_type="ApplicationSubmitted", event_data={
                "application_id": "integrity-004",
                "applicant_id":   "cust-4",
                "requested_amount_usd": 60000,
                "loan_purpose":   "refinance",
                "submission_channel": "web",
                "submitted_at":   "2026-01-01T00:00:00Z",
            }),
            BaseEvent(event_type="CreditAnalysisRequested", event_data={
                "application_id":    "integrity-004",
                "assigned_agent_id": "agent-2",
                "requested_at":      "2026-01-01T00:02:00Z",
                "priority":          "normal",
            }),
        ],
        expected_version=0,
    )

    # bootstrap hashes
    await run_integrity_check(pool, "loan-integrity-004")

    # tamper SECOND event only
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id FROM events
            WHERE stream_id='loan-integrity-004'
            ORDER BY version DESC LIMIT 1
            """
        )
        await conn.execute(
            "UPDATE events SET event_data=$1 WHERE id=$2",
            json.dumps({
                "application_id":    "integrity-004",
                "assigned_agent_id": "agent-HACKED",
                "requested_at":      "2026-01-01T00:02:00Z",
                "priority":          "normal",
            }),
            row["id"],
        )

    report = await run_integrity_check(pool, "loan-integrity-004")
    assert report.tamper_detected is True
    assert report.first_tampered_event_id == row["id"]
    print(f"Mid-stream tamper detected OK — tampered event id={row['id']}")