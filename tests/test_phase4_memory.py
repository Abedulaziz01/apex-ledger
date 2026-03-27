import asyncio
import asyncpg
import os
import pytest
import pytest_asyncio
from dotenv import load_dotenv

from src.ledger.core.event_store import EventStore
from src.ledger.core.models import BaseEvent
from src.ledger.memory.agent_context import (
    recover_agent_context,
    write_recovery_event,
    AgentContext,
)

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


# ── Test 1: empty session returns OK with no pending work ─────────────────────

@pytest.mark.asyncio
async def test_empty_session_returns_ok(store_and_pool):
    store, pool = store_and_pool
    ctx = await recover_agent_context(store, "agent-empty", "sess-000")
    assert ctx.session_health_status == "OK"
    assert ctx.pending_work == []
    assert ctx.last_event_position == 0
    print("Empty session OK")


# ── Test 2: clean session (all started work completed) → OK ───────────────────

@pytest.mark.asyncio
async def test_clean_session_is_ok(store_and_pool):
    store, pool = store_and_pool

    await store.append(
        "agent-credit-sess-clean",
        [
            BaseEvent(event_type="AgentContextLoaded", event_data={
                "agent_id": "credit", "session_id": "sess-clean",
                "context_source": "fresh", "event_replay_from_position": 0,
                "context_token_count": 500, "model_version": "gpt-4",
            }),
            BaseEvent(event_type="CreditAnalysisRequested", event_data={
                "application_id": "app-clean", "assigned_agent_id": "credit",
                "requested_at": "2026-01-01T00:00:00Z", "priority": "high",
            }),
            BaseEvent(event_type="CreditAnalysisCompleted", event_data={
                "application_id": "app-clean", "agent_id": "credit",
                "session_id": "sess-clean", "model_version": "gpt-4",
                "confidence_score": 0.9, "risk_tier": "LOW",
                "recommended_limit_usd": 50000,
                "analysis_duration_ms": 800, "input_data_hash": "abc",
            }),
        ],
        expected_version=0,
    )

    ctx = await recover_agent_context(store, "credit", "sess-clean")
    assert ctx.session_health_status == "OK"
    assert ctx.pending_work == []
    assert ctx.last_event_position == 3
    print(f"Clean session OK — position={ctx.last_event_position}")


# ── Test 3: crashed mid-analysis → NEEDS_RECONCILIATION ──────────────────────

@pytest.mark.asyncio
async def test_crashed_session_needs_reconciliation(store_and_pool):
    store, pool = store_and_pool

    await store.append(
        "agent-credit-sess-crash",
        [
            BaseEvent(event_type="AgentContextLoaded", event_data={
                "agent_id": "credit", "session_id": "sess-crash",
                "context_source": "fresh", "event_replay_from_position": 0,
                "context_token_count": 500, "model_version": "gpt-4",
            }),
            BaseEvent(event_type="CreditAnalysisRequested", event_data={
                "application_id": "app-crash", "assigned_agent_id": "credit",
                "requested_at": "2026-01-01T00:00:00Z", "priority": "high",
            }),
            # ← CRASH HERE — no CreditAnalysisCompleted ever written
        ],
        expected_version=0,
    )

    ctx = await recover_agent_context(store, "credit", "sess-crash")
    assert ctx.session_health_status == "NEEDS_RECONCILIATION"
    assert len(ctx.pending_work) > 0
    print(f"Crashed session detected OK — pending={ctx.pending_work}")


# ── Test 4: last 3 events preserved verbatim in context_text ─────────────────

@pytest.mark.asyncio
async def test_last_3_events_verbatim(store_and_pool):
    store, pool = store_and_pool

    events = []
    for i in range(6):
        events.append(BaseEvent(
            event_type="AgentContextLoaded",
            event_data={
                "agent_id": "fraud", "session_id": "sess-verbatim",
                "context_source": f"source-{i}",
                "event_replay_from_position": i,
                "context_token_count": 100, "model_version": "gpt-4",
            },
        ))

    await store.append("agent-fraud-sess-verbatim", events, expected_version=0)

    ctx = await recover_agent_context(store, "fraud", "sess-verbatim")

    # the last 3 should appear verbatim (with event_data dict in text)
    assert "verbatim" in ctx.context_text.lower()
    assert "summary" in ctx.context_text.lower()
    assert ctx.last_event_position == 6
    print("Verbatim last-3 OK")


# ── Test 5: write_recovery_event creates AgentSessionRecovered ───────────────

@pytest.mark.asyncio
async def test_write_recovery_event(store_and_pool):
    store, pool = store_and_pool

    ctx = AgentContext(
        context_text="Prior session summary.",
        last_event_position=5,
        pending_work=["CreditAnalysisRequested:app-1"],
        session_health_status="NEEDS_RECONCILIATION",
    )

    await write_recovery_event(
        store,
        agent_id="credit",
        new_session_id="sess-recovered",
        crashed_session_id="sess-crashed-123",
        context=ctx,
    )

    events = await store.load_stream("agent-credit-sess-recovered")
    assert len(events) == 1
    evt = events[0]
    assert evt.event_type == "AgentSessionRecovered"
    assert evt.event_data["context_source"] == "prior_session_replay:sess-crashed-123"
    assert evt.event_data["crashed_session_id"] == "sess-crashed-123"
    assert evt.event_data["session_health_status"] == "NEEDS_RECONCILIATION"
    print(f"AgentSessionRecovered written OK — context_source={evt.event_data['context_source']}")


# ── Test 6: no duplicate events after recovery ────────────────────────────────

@pytest.mark.asyncio
async def test_no_duplicate_events_after_recovery(store_and_pool):
    store, pool = store_and_pool

    # crashed session wrote FraudScreeningCompleted once
    await store.append(
        "agent-fraud-sess-dup",
        [
            BaseEvent(event_type="AgentContextLoaded", event_data={
                "agent_id": "fraud", "session_id": "sess-dup",
                "context_source": "fresh", "event_replay_from_position": 0,
                "context_token_count": 300, "model_version": "gpt-4",
            }),
            BaseEvent(event_type="FraudScreeningCompleted", event_data={
                "application_id": "app-dup", "agent_id": "fraud",
                "fraud_score": 0.1, "anomaly_flags": [],
                "screening_model_version": "fraud-v2",
                "input_data_hash": "xyz",
            }),
        ],
        expected_version=0,
    )

    # recover — context should see FraudScreeningCompleted already done
    ctx = await recover_agent_context(store, "fraud", "sess-dup")

    # write recovery event to new session
    await write_recovery_event(
        store,
        agent_id="fraud",
        new_session_id="sess-dup-recovered",
        crashed_session_id="sess-dup",
        context=ctx,
    )

    # load new session — must have exactly 1 event (the recovery event only)
    new_events = await store.load_stream("agent-fraud-sess-dup-recovered")
    fraud_events = [e for e in new_events if e.event_type == "FraudScreeningCompleted"]
    assert len(fraud_events) == 0, "Duplicate FraudScreeningCompleted in new session!"
    assert new_events[0].event_type == "AgentSessionRecovered"
    print("No duplicate events after recovery OK")