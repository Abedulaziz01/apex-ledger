import asyncio
import asyncpg
import os
import pytest
import pytest_asyncio
from dotenv import load_dotenv

from src.ledger.core.event_store import EventStore
from src.ledger.projections.daemon import ProjectionDaemon

load_dotenv()


@pytest_asyncio.fixture()
async def srv():
    """Patch module-level pool/store so all MCP tools use our test pool."""
    import src.ledger.mcp_server as server
    pool = await asyncpg.create_pool(os.getenv("DATABASE_URL"), min_size=2, max_size=10)
    server._pool = pool
    server._store = EventStore(pool)
    yield server
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE events, event_streams, outbox, application_summary, "
            "agent_performance_ledger, compliance_audit_view, "
            "projection_checkpoints RESTART IDENTITY CASCADE"
        )
    server._pool = None
    server._store = None
    await pool.close()


async def _run_daemon(srv):
    """Process all pending events through projections."""
    daemon = ProjectionDaemon(srv._pool)
    while await daemon._process_batch():
        pass


# ── full lifecycle: submit → analyse → fraud → compliance → decide → review → approve ──

@pytest.mark.asyncio
async def test_full_loan_lifecycle_via_mcp(srv):
    APP_ID = "lifecycle-001"

    # 1. submit application
    r = await srv.submit_application(
        application_id=APP_ID,
        applicant_id="cust-lifecycle",
        requested_amount_usd=80000,
        loan_purpose="expansion",
        submission_channel="api",
        submitted_at="2026-01-01T00:00:00Z",
    )
    assert r["ok"] is True, f"submit failed: {r}"
    assert r["version"] == 1

    # 2. start agent session
    r = await srv.start_agent_session(
        agent_id="credit-agent",
        session_id="sess-lifecycle",
        context_source="fresh_start",
        event_replay_from_position=0,
        context_token_count=1000,
        model_version="gpt-4",
    )
    assert r["ok"] is True

    # 3. record credit analysis
    r = await srv.record_credit_analysis(
        application_id=APP_ID,
        agent_id="credit-agent",
        session_id="sess-lifecycle",
        model_version="gpt-4",
        confidence_score=0.88,
        risk_tier="LOW",
        recommended_limit_usd=80000,
        analysis_duration_ms=950,
        input_data_hash="hash-credit-1",
        expected_version=1,
    )
    assert r["ok"] is True, f"credit analysis failed: {r}"
    assert r["version"] == 2

    # 4. record fraud screening
    r = await srv.record_fraud_screening(
        application_id=APP_ID,
        agent_id="fraud-agent",
        fraud_score=0.03,
        anomaly_flags=[],
        screening_model_version="fraud-v2",
        input_data_hash="hash-fraud-1",
        expected_version=2,
    )
    assert r["ok"] is True, f"fraud screening failed: {r}"
    assert r["version"] == 3

    # 5. record compliance checks
    r = await srv.record_compliance_check(
        application_id=APP_ID,
        rule_id="KYC",
        rule_version="v1",
        passed=True,
        evidence_hash="kyc-hash",
        evaluation_timestamp="2026-01-01T00:05:00Z",
        failure_reason="",
        expected_version=0,
    )
    assert r["ok"] is True
    assert r["result"] == "passed"

    r = await srv.record_compliance_check(
        application_id=APP_ID,
        rule_id="AML",
        rule_version="v1",
        passed=True,
        evidence_hash="aml-hash",
        evaluation_timestamp="2026-01-01T00:06:00Z",
        failure_reason="",
        expected_version=1,
    )
    assert r["ok"] is True

    # 6. generate decision
    r = await srv.generate_decision(
        application_id=APP_ID,
        orchestrator_agent_id="orch-1",
        recommendation="APPROVE",
        confidence_score=0.91,
        contributing_agent_sessions=["agent-credit-agent-sess-lifecycle"],
        decision_basis_summary="low risk, clean fraud screen, full compliance",
        model_versions={"credit": "gpt-4", "fraud": "fraud-v2"},
        expected_version=3,
    )
    assert r["ok"] is True, f"generate_decision failed: {r}"
    assert r["recommendation"] == "APPROVE"

    # 7. record human review
    r = await srv.record_human_review(
        application_id=APP_ID,
        reviewer_id="officer-jones",
        override=False,
        final_decision="APPROVE",
        override_reason="",
        expected_version=4,
    )
    assert r["ok"] is True

    # 8. run integrity check
    r = await srv.run_integrity_check(application_id=APP_ID)
    assert r["ok"] is True
    assert r["chain_valid"] is True
    assert r["tamper_detected"] is False
    assert r["events_verified"] == 5

    print(f"Full lifecycle OK — {r['events_verified']} events, chain valid")


# ── resource: audit trail has all events in order ─────────────────────────────

@pytest.mark.asyncio
async def test_audit_trail_resource(srv):
    APP_ID = "audit-trail-001"

    await srv.submit_application(APP_ID, "cust-2", 30000, "wc", "web", "2026-01-01T00:00:00Z")
    await srv.record_credit_analysis(
        APP_ID, "agent-1", "sess-1", "gpt-4", 0.85, "MEDIUM",
        30000, 700, "hash1", expected_version=1,
    )

    result = await srv.get_audit_trail(APP_ID)  # type: ignore
    # call resource directly
    from src.ledger.mcp_server import get_audit_trail
    result = await get_audit_trail(APP_ID)
    assert result["event_count"] == 2
    assert result["events"][0]["event_type"] == "ApplicationSubmitted"
    assert result["events"][1]["event_type"] == "CreditAnalysisCompleted"
    print("Audit trail resource OK")


# ── resource: compliance view populated after daemon runs ─────────────────────

@pytest.mark.asyncio
async def test_compliance_resource_after_daemon(srv):
    APP_ID = "compliance-res-001"

    await srv.record_compliance_check(
        APP_ID, "KYC", "v1", True, "hash-kyc",
        "2026-01-01T00:00:00Z", "", expected_version=0,
    )

    await _run_daemon(srv)

    from src.ledger.mcp_server import get_application_compliance
    result = await get_application_compliance(APP_ID)
    assert result["application_id"] == APP_ID
    assert len(result["records"]) >= 1
    print(f"Compliance resource OK — {len(result['records'])} record(s)")


# ── resource: ledger health returns lag metrics ───────────────────────────────

@pytest.mark.asyncio
async def test_ledger_health_resource(srv):
    from src.ledger.mcp_server import get_ledger_health
    result = await get_ledger_health()
    assert "application_summary_lag_ms" in result
    assert "agent_performance_lag_ms" in result
    assert "compliance_audit_lag_ms" in result
    assert result["status"] == "healthy"
    print(f"Ledger health OK — lag={result['application_summary_lag_ms']:.1f}ms")


# ── resource: agent session history ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_agent_session_resource(srv):
    await srv.start_agent_session(
        agent_id="credit-res",
        session_id="sess-res-1",
        context_source="fresh",
        event_replay_from_position=0,
        context_token_count=500,
        model_version="gpt-4",
    )

    from src.ledger.mcp_server import get_agent_session
    result = await get_agent_session("credit-res", "sess-res-1")
    assert result["agent_id"] == "credit-res"
    assert result["session_id"] == "sess-res-1"
    assert result["event_count"] == 1
    assert result["events"][0]["event_type"] == "AgentContextLoaded"
    print("Agent session resource OK")


# ── confidence floor enforced via MCP tool ────────────────────────────────────

@pytest.mark.asyncio
async def test_confidence_floor_via_mcp(srv):
    APP_ID = "conf-floor-001"
    await srv.submit_application(APP_ID, "cust-3", 20000, "refi", "api", "2026-01-01T00:00:00Z")

    r = await srv.generate_decision(
        application_id=APP_ID,
        orchestrator_agent_id="orch-2",
        recommendation="APPROVE",
        confidence_score=0.42,          # below 0.6 — must be forced to REFER
        contributing_agent_sessions=[],
        decision_basis_summary="borderline",
        model_versions={},
        expected_version=1,
    )
    assert r["ok"] is True
    assert r["recommendation"] == "REFER", f"Expected REFER, got {r['recommendation']}"
    print("Confidence floor via MCP OK — APPROVE forced to REFER")


# ── structured error on concurrency conflict ──────────────────────────────────

@pytest.mark.asyncio
async def test_concurrency_conflict_returns_structured_error(srv):
    APP_ID = "conflict-001"
    await srv.submit_application(APP_ID, "cust-4", 15000, "eq", "web", "2026-01-01T00:00:00Z")

    # second submit to same stream at version 0 — must conflict
    r = await srv.submit_application(APP_ID, "cust-4", 15000, "eq", "web", "2026-01-01T00:00:00Z")
    assert r.get("error_type") == "OptimisticConcurrencyError"
    assert r.get("suggested_action") == "reload_stream_and_retry"
    print("Structured concurrency error OK")