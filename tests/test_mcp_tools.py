import asyncio
import asyncpg
import os
import pytest
import pytest_asyncio
from dotenv import load_dotenv
from unittest.mock import AsyncMock, MagicMock, patch

from src.ledger.core.event_store import EventStore
from src.ledger.core.exceptions import OptimisticConcurrencyError

load_dotenv()


@pytest_asyncio.fixture()
async def pool():
    p = await asyncpg.create_pool(os.getenv("DATABASE_URL"), min_size=2, max_size=10)
    yield p
    async with p.acquire() as conn:
        await conn.execute(
            "TRUNCATE events, event_streams, outbox RESTART IDENTITY CASCADE"
        )
    await p.close()


@pytest_asyncio.fixture()
async def fresh_store(pool):
    """Patch the module-level pool/store so tools use our test pool."""
    import src.ledger.mcp_server as srv
    srv._pool = pool
    srv._store = EventStore(pool)
    yield srv
    srv._pool = None
    srv._store = None


# ── Tool 1: submit_application ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_submit_application_ok(fresh_store):
    srv = fresh_store
    result = await srv.submit_application(
        application_id="mcp-001",
        applicant_id="cust-1",
        requested_amount_usd=50000,
        loan_purpose="expansion",
        submission_channel="api",
        submitted_at="2026-01-01T00:00:00Z",
    )
    assert result["ok"] is True
    assert result["version"] == 1
    print("submit_application OK")


@pytest.mark.asyncio
async def test_submit_application_duplicate_returns_structured_error(fresh_store):
    srv = fresh_store
    await srv.submit_application("mcp-dup", "c1", 10000, "test", "api", "2026-01-01T00:00:00Z")
    result = await srv.submit_application("mcp-dup", "c1", 10000, "test", "api", "2026-01-01T00:00:00Z")
    assert result.get("error_type") == "OptimisticConcurrencyError"
    assert "suggested_action" in result
    print("submit_application duplicate → structured error OK")


# ── Tool 2: record_credit_analysis ───────────────────────────────────────────

@pytest.mark.asyncio
async def test_record_credit_analysis_ok(fresh_store):
    srv = fresh_store
    await srv.submit_application("mcp-002", "c2", 75000, "wc", "web", "2026-01-01T00:00:00Z")
    result = await srv.record_credit_analysis(
        application_id="mcp-002", agent_id="agent-1", session_id="sess-1",
        model_version="gpt-4", confidence_score=0.88, risk_tier="LOW",
        recommended_limit_usd=75000, analysis_duration_ms=900,
        input_data_hash="hash1", expected_version=1,
    )
    assert result["ok"] is True
    print("record_credit_analysis OK")


# ── Tool 3: record_fraud_screening ───────────────────────────────────────────

@pytest.mark.asyncio
async def test_record_fraud_screening_ok(fresh_store):
    srv = fresh_store
    await srv.submit_application("mcp-003", "c3", 20000, "refi", "branch", "2026-01-01T00:00:00Z")
    result = await srv.record_fraud_screening(
        application_id="mcp-003", agent_id="fraud-1",
        fraud_score=0.05, anomaly_flags=[],
        screening_model_version="fraud-v2",
        input_data_hash="fhash1", expected_version=1,
    )
    assert result["ok"] is True
    print("record_fraud_screening OK")


# ── Tool 4: record_compliance_check ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_record_compliance_check_passed(fresh_store):
    srv = fresh_store
    result = await srv.record_compliance_check(
        application_id="mcp-004", rule_id="KYC", rule_version="v1",
        passed=True, evidence_hash="evhash1",
        evaluation_timestamp="2026-01-01T00:00:00Z",
        failure_reason="", expected_version=0,
    )
    assert result["ok"] is True
    assert result["result"] == "passed"
    print("record_compliance_check passed OK")


@pytest.mark.asyncio
async def test_record_compliance_check_failed(fresh_store):
    srv = fresh_store
    result = await srv.record_compliance_check(
        application_id="mcp-005", rule_id="AML", rule_version="v1",
        passed=False, evidence_hash="",
        evaluation_timestamp="2026-01-01T00:00:00Z",
        failure_reason="sanctions hit", expected_version=0,
    )
    assert result["ok"] is True
    assert result["result"] == "failed"
    print("record_compliance_check failed OK")


# ── Tool 5: generate_decision ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_generate_decision_ok(fresh_store):
    srv = fresh_store
    await srv.submit_application("mcp-006", "c6", 40000, "eq", "api", "2026-01-01T00:00:00Z")
    result = await srv.generate_decision(
        application_id="mcp-006", orchestrator_agent_id="orch-1",
        recommendation="APPROVE", confidence_score=0.91,
        contributing_agent_sessions=["agent-1-sess-1"],
        decision_basis_summary="all checks passed",
        model_versions={"credit": "gpt-4"},
        expected_version=1,
    )
    assert result["ok"] is True
    assert result["recommendation"] == "APPROVE"
    print("generate_decision OK")


@pytest.mark.asyncio
async def test_generate_decision_low_confidence_forced_refer(fresh_store):
    srv = fresh_store
    await srv.submit_application("mcp-007", "c7", 40000, "eq", "api", "2026-01-01T00:00:00Z")
    result = await srv.generate_decision(
        application_id="mcp-007", orchestrator_agent_id="orch-1",
        recommendation="APPROVE", confidence_score=0.45,  # below 0.6
        contributing_agent_sessions=[],
        decision_basis_summary="borderline",
        model_versions={}, expected_version=1,
    )
    assert result["ok"] is True
    assert result["recommendation"] == "REFER"  # forced by confidence floor
    print("generate_decision confidence floor → REFER OK")


# ── Tool 6: record_human_review ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_record_human_review_ok(fresh_store):
    srv = fresh_store
    await srv.submit_application("mcp-008", "c8", 30000, "wc", "web", "2026-01-01T00:00:00Z")
    result = await srv.record_human_review(
        application_id="mcp-008", reviewer_id="officer-1",
        override=False, final_decision="APPROVE",
        override_reason="", expected_version=1,
    )
    assert result["ok"] is True
    print("record_human_review OK")


# ── Tool 7: start_agent_session ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_start_agent_session_ok(fresh_store):
    srv = fresh_store
    result = await srv.start_agent_session(
        agent_id="credit-agent", session_id="sess-mcp-1",
        context_source="fresh_start", event_replay_from_position=0,
        context_token_count=1000, model_version="gpt-4",
    )
    assert result["ok"] is True
    assert result["stream_id"] == "agent-credit-agent-sess-mcp-1"
    print("start_agent_session OK")


# ── Tool 8: run_integrity_check ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_run_integrity_check_ok(fresh_store):
    srv = fresh_store
    await srv.submit_application("mcp-009", "c9", 55000, "acq", "api", "2026-01-01T00:00:00Z")
    result = await srv.run_integrity_check(application_id="mcp-009")
    assert result["ok"] is True
    assert result["chain_valid"] is True
    assert result["tamper_detected"] is False
    assert result["events_verified"] == 1
    print("run_integrity_check OK")