import asyncio
import asyncpg
import json
import os
import pytest
import pytest_asyncio
from dotenv import load_dotenv

from src.ledger.core.event_store import EventStore
from src.ledger.core.models import BaseEvent
from src.ledger.upcasting.registry import registry

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


# ── Test 1: upcaster chain works in isolation ─────────────────────────────────

def test_credit_analysis_v1_to_v2():
    payload = {
        "application_id": "app-1", "agent_id": "a1", "session_id": "s1",
        "risk_tier": "LOW", "recommended_limit_usd": 50000,
        "analysis_duration_ms": 800, "input_data_hash": "hash1",
    }
    version, result = registry.upcast("CreditAnalysisCompleted", 1, payload)
    assert version == 2
    assert result["model_version"] == "unknown-v1"
    assert result["confidence_score"] == 0.75


def test_decision_generated_v1_to_v2():
    payload = {
        "application_id": "app-1", "orchestrator_agent_id": "orch-1",
        "recommendation": "APPROVE", "confidence_score": 0.9,
        "contributing_agent_sessions": [], "decision_basis_summary": "ok",
    }
    version, result = registry.upcast("DecisionGenerated", 1, payload)
    assert version == 2
    assert result["model_versions"] == {}


def test_v2_payload_passes_through_unchanged():
    payload = {"model_version": "gpt-4", "confidence_score": 0.95, "risk_tier": "LOW"}
    version, result = registry.upcast("CreditAnalysisCompleted", 2, payload)
    assert version == 2
    assert result["model_version"] == "gpt-4"


def test_unknown_event_type_passes_through():
    payload = {"foo": "bar"}
    version, result = registry.upcast("SomeUnknownEvent", 1, payload)
    assert version == 1
    assert result == {"foo": "bar"}


# ── Test 2: immutability — raw DB payload untouched after load_stream() ───────

@pytest.mark.asyncio
async def test_immutability_raw_db_payload_untouched(store_and_pool):
    store, pool = store_and_pool

    # write a v1-style CreditAnalysisCompleted (no model_version, no confidence_score)
    v1_payload = {
        "application_id":      "upcast-app-1",
        "agent_id":            "agent-upcast",
        "session_id":          "sess-upcast",
        "risk_tier":           "MEDIUM",
        "recommended_limit_usd": 30000,
        "analysis_duration_ms":  950,
        "input_data_hash":     "deadbeef",
    }
    await store.append(
        "loan-upcast-app-1",
        [BaseEvent(event_type="CreditAnalysisCompleted", event_data=v1_payload)],
        expected_version=0,
    )

    # read raw from DB — must NOT have model_version
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT event_data FROM events WHERE event_type='CreditAnalysisCompleted' LIMIT 1"
        )
    raw = json.loads(row["event_data"])
    assert "model_version" not in raw, "Raw DB payload was mutated — immutability violated!"
    assert "confidence_score" not in raw, "Raw DB payload was mutated — immutability violated!"

    # load_stream() must return v2 with upcasted fields
    loaded = await store.load_stream("loan-upcast-app-1")
    assert len(loaded) == 1
    evt = loaded[0]
    assert evt.event_data["model_version"] == "unknown-v1"
    assert evt.event_data["confidence_score"] == 0.75

    # read raw again — still untouched
    async with pool.acquire() as conn:
        row2 = await conn.fetchrow(
            "SELECT event_data FROM events WHERE event_type='CreditAnalysisCompleted' LIMIT 1"
        )
    raw2 = json.loads(row2["event_data"])
    assert "model_version" not in raw2, "Raw DB payload mutated after load_stream()!"
    print("Immutability confirmed — DB untouched before and after load_stream()")


# ── Test 3: upcaster does not mutate the input dict ───────────────────────────

def test_upcaster_does_not_mutate_input():
    original = {
        "application_id": "app-1", "agent_id": "a1", "session_id": "s1",
        "risk_tier": "HIGH", "recommended_limit_usd": 10000,
        "analysis_duration_ms": 500, "input_data_hash": "xyz",
    }
    snapshot = dict(original)
    registry.upcast("CreditAnalysisCompleted", 1, original)
    assert original == snapshot, "Upcaster mutated the input dict!"
    print("Upcaster immutability OK")