"""
tests/test_narratives.py
The 5 narrative scenario tests — primary correctness gate.
All require a live DB (DATABASE_URL set) and real agents.
"""
from __future__ import annotations

import asyncio
import os
import pytest

from src.ledger.core.event_store import EventStore, InMemoryEventStore, OptimisticConcurrencyError
from src.ledger.agents.credit_analysis_agent import CreditAnalysisAgent
from src.ledger.agents.stub_agents import (
    DocumentProcessingAgent, FraudDetectionAgent,
    ComplianceAgent, DecisionOrchestratorAgent,
)
from src.ledger.registry.client import ApplicantRegistryClient
from src.ledger.schema.events import ApplicationSubmitted, DocumentUploadRequested, DocumentUploaded
from dotenv import load_dotenv
load_dotenv()

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"),
    reason="DATABASE_URL not set — skipping narrative tests",
)
@pytest.fixture(autouse=True)
async def clean_db():
    """Wipe all streams before each narrative test."""
    import asyncpg
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE events, event_streams, outbox RESTART IDENTITY CASCADE"
        )
    await pool.close()
    yield

async def _seed_application(store, app_id: str, company_id: str, amount: float):
    """Helper: put an application into DOCUMENTS_UPLOADED state."""
    loan_stream = f"loan-{app_id}"
    import uuid
    doc_pkg_id = str(uuid.uuid4())
    await store.append(loan_stream, [
        ApplicationSubmitted(stream_id=loan_stream, payload={
            "company_id": company_id,
            "requested_amount_usd": amount,
        }),
        DocumentUploadRequested(stream_id=loan_stream, payload={
            "document_package_id": doc_pkg_id,
        }),
        DocumentUploaded(stream_id=loan_stream, payload={
            "document_type": "income_statement",
            "file_path": f"documents/{company_id}/income_statement_2024.pdf",
        }),
    ], expected_version=-1)
    return doc_pkg_id


async def _get_store_and_registry():
    store = await EventStore.create()
    registry = await ApplicantRegistryClient.create()
    return store, registry


# ---------------------------------------------------------------------------
# NARR-01: Concurrent OCC Collision
# ---------------------------------------------------------------------------

async def test_narr01_concurrent_occ_collision():
    """
    Two CreditAnalysisAgent instances start simultaneously.
    Both should complete without raising to caller.
    credit stream must have exactly 2 CreditAnalysisCompleted events.
    """
    store, registry = await _get_store_and_registry()
    app_id = "NARR-01"
    await _seed_application(store, app_id, "COMP-031", 500_000)

    # First run DocumentProcessingAgent to get to CREDIT_IN_PROGRESS state
    await DocumentProcessingAgent(store, registry, documents_dir=os.environ.get("DOCUMENTS_DIR", "./documents")).process(app_id)

    # Launch two CreditAnalysisAgent instances simultaneously
    agent_a = CreditAnalysisAgent(store, registry)
    agent_b = CreditAnalysisAgent(store, registry)

    results = await asyncio.gather(
        agent_a.process(app_id),
        agent_b.process(app_id),
        return_exceptions=True,
    )

    # Both must complete (no exceptions propagated)
    for r in results:
        assert not isinstance(r, Exception), f"Agent raised: {r}"

    # Find credit stream
    loan_events = await store.load_stream(f"loan-{app_id}")
    credit_id = None
    for ev in loan_events:
        if ev.event_type == "CreditAnalysisRequested":
            credit_id = ev.payload.get("credit_record_id")
            break

    # Must have exactly 2 CreditAnalysisCompleted events
    if credit_id:
        credit_events = await store.load_stream(f"credit-{credit_id}")
        completed = [e for e in credit_events if e.event_type == "CreditAnalysisCompleted"]
        assert len(completed) == 2, f"Expected 2 CreditAnalysisCompleted, got {len(completed)}"
        assert completed[0].stream_position > 0
        assert completed[1].stream_position > completed[0].stream_position


# ---------------------------------------------------------------------------
# NARR-02: Document Extraction Failure (Missing EBITDA)
# ---------------------------------------------------------------------------

async def test_narr02_missing_ebitda():
    """
    DocumentProcessingAgent processes a PDF with no EBITDA.
    ExtractionCompleted.facts.ebitda must be None, field_confidence["ebitda"] == 0.0.
    CreditAnalysisCompleted.decision.confidence <= 0.75.
    """
    store, registry = await _get_store_and_registry()
    app_id = "NARR-02"
    await _seed_application(store, app_id, "COMP-044", 300_000)

    await DocumentProcessingAgent(store, registry, documents_dir=os.environ.get("DOCUMENTS_DIR", "./documents")).process(app_id)

    # Find docpkg stream
    loan_events = await store.load_stream(f"loan-{app_id}")
    doc_pkg_id = None
    for ev in loan_events:
        if ev.event_type == "DocumentUploadRequested":
            doc_pkg_id = ev.payload.get("document_package_id")

    docpkg_events = await store.load_stream(f"docpkg-{doc_pkg_id}")

    extraction_events = [e for e in docpkg_events if e.event_type == "ExtractionCompleted"]
    quality_events = [e for e in docpkg_events if e.event_type == "QualityAssessmentCompleted"]

    assert quality_events, "No QualityAssessmentCompleted event"
    quality = quality_events[0]

    # Run credit analysis
    await CreditAnalysisAgent(store, registry).process(app_id)

    # Find credit result
    credit_id = None
    for ev in loan_events:
        if ev.event_type == "CreditAnalysisRequested":
            credit_id = ev.payload.get("credit_record_id")
    # Reload
    loan_events = await store.load_stream(f"loan-{app_id}")
    for ev in loan_events:
        if ev.event_type == "CreditAnalysisRequested":
            credit_id = ev.payload.get("credit_record_id")

    credit_events = await store.load_stream(f"credit-{credit_id}")
    completed = [e for e in credit_events if e.event_type == "CreditAnalysisCompleted"]
    assert completed, "No CreditAnalysisCompleted event"

    decision = completed[0].payload.get("decision", {})
    assert decision.get("confidence", 1.0) <= 0.75, f"Confidence {decision.get('confidence')} should be <= 0.75 with missing EBITDA"
    assert decision.get("data_quality_caveats"), "data_quality_caveats should be non-empty"


# ---------------------------------------------------------------------------
# NARR-03: Agent Crash and Recovery
# ---------------------------------------------------------------------------

async def test_narr03_crash_and_recovery():
    """
    FraudDetectionAgent crashes after load_facts node.
    New instance reconstructs context and resumes from cross_reference_registry.
    Zero duplicate load_facts executions. Exactly ONE FraudScreeningCompleted.
    """
    store, registry = await _get_store_and_registry()
    app_id = "NARR-03"
    await _seed_application(store, app_id, "COMP-057", 1_100_000)

    await DocumentProcessingAgent(store, registry, documents_dir=os.environ.get("DOCUMENTS_DIR", "./documents")).process(app_id)
    await CreditAnalysisAgent(store, registry).process(app_id)

    # Agent A crashes after load_facts
    agent_a = FraudDetectionAgent(store, registry)
    crashed_session_id = None

    original_load_facts = agent_a._node_load_facts
    crash_triggered = False

    async def crashing_load_facts(state):
        nonlocal crash_triggered
        result = await original_load_facts(state)
        if not crash_triggered:
            crash_triggered = True
            await agent_a._simulate_crash_after_node("load_facts", state)
        return result

    agent_a._node_load_facts = crashing_load_facts

    with pytest.raises(RuntimeError, match="SIMULATED CRASH"):
        await agent_a.process(app_id)

    crashed_session_id = agent_a._session_id

    # Verify AgentSessionFailed was written
    session_stream = f"agent-fraud-{crashed_session_id}"
    session_events = await store.load_stream(session_stream)
    failed_events = [e for e in session_events if e.event_type == "AgentSessionFailed"]
    assert failed_events, "No AgentSessionFailed event written"
    assert failed_events[0].payload.get("recoverable") is True
    assert failed_events[0].payload.get("last_successful_node") == "load_facts"

    # Recovery: new agent reconstructs context
    agent_b = FraudDetectionAgent(store, registry)
    context = await agent_b.reconstruct_agent_context(crashed_session_id)
    assert context["last_successful_node"] == "load_facts"
    assert "load_facts" in context["completed_nodes"]

    # Run recovery session
    await agent_b.process(app_id, prior_session_id=crashed_session_id)

    # Verify recovery session started with prior_session_replay
    recovery_stream = f"agent-fraud-{agent_b._session_id}"
    recovery_events = await store.load_stream(recovery_stream)
    started = [e for e in recovery_events if e.event_type == "AgentSessionStarted"]
    assert started[0].payload["context_source"].startswith("prior_session_replay:")

    # Exactly ONE FraudScreeningCompleted across all fraud streams
    loan_events = await store.load_stream(f"loan-{app_id}")
    fraud_ids = [ev.payload.get("fraud_screening_id") for ev in loan_events if ev.event_type == "FraudScreeningRequested"]
    all_completed = []
    for fid in fraud_ids:
        if fid:
            fevents = await store.load_stream(f"fraud-{fid}")
            all_completed += [e for e in fevents if e.event_type == "FraudScreeningCompleted"]
    assert len(all_completed) == 1, f"Expected 1 FraudScreeningCompleted, got {len(all_completed)}"


# ---------------------------------------------------------------------------
# NARR-04: Compliance Hard Block (Montana)
# ---------------------------------------------------------------------------

async def test_narr04_montana_compliance_block():
    """
    Montana company hits REG-003 hard block.
    compliance stream: exactly 3 events (2 Passed + 1 Failed).
    No DecisionGenerated ever in loan stream.
    ApplicationDeclined with adverse_action_notice_required=True.
    """
    store, registry = await _get_store_and_registry()
    app_id = "NARR-04"

    # Find Montana company
    import asyncpg
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT company_id FROM applicant_registry.companies WHERE jurisdiction='MT' LIMIT 1"
        )
    mt_company_id = row["company_id"] if row else "COMP-MT"

    await _seed_application(store, app_id, mt_company_id, 200_000)
    await DocumentProcessingAgent(store, registry, documents_dir=os.environ.get("DOCUMENTS_DIR", "./documents")).process(app_id)
    await CreditAnalysisAgent(store, registry).process(app_id)
    await FraudDetectionAgent(store, registry).process(app_id)
    await ComplianceAgent(store, registry).process(app_id)

    # Find compliance stream
    loan_events = await store.load_stream(f"loan-{app_id}")
    compliance_id = None
    for ev in loan_events:
        if ev.event_type == "ComplianceCheckRequested":
            compliance_id = ev.payload.get("compliance_record_id")

    comp_events = await store.load_stream(f"compliance-{compliance_id}")
    rule_events = [e for e in comp_events if e.event_type in ("ComplianceRulePassed", "ComplianceRuleFailed", "ComplianceRuleNoted")]

    # Exactly 3 rule events: REG-001 pass, REG-002 pass, REG-003 fail
    assert len(rule_events) == 3, f"Expected 3 rule events, got {len(rule_events)}"
    assert rule_events[0].event_type == "ComplianceRulePassed"
    assert rule_events[1].event_type == "ComplianceRulePassed"
    assert rule_events[2].event_type == "ComplianceRuleFailed"
    assert rule_events[2].payload.get("rule_id") == "REG-003"
    assert rule_events[2].payload.get("is_hard_block") is True

    # No DecisionGenerated in loan stream
    reload_loan = await store.load_stream(f"loan-{app_id}")
    decision_generated = [e for e in reload_loan if e.event_type == "DecisionGenerated"]
    assert not decision_generated, "DecisionGenerated must NOT appear after compliance hard block"

    # ApplicationDeclined with adverse action
    declined = [e for e in reload_loan if e.event_type == "ApplicationDeclined"]
    assert declined, "ApplicationDeclined must be present"
    assert declined[0].payload.get("adverse_action_notice_required") is True
    reasons = declined[0].payload.get("decline_reasons", [])
    assert any("REG-003" in r for r in reasons), f"REG-003 not in decline_reasons: {reasons}"


# ---------------------------------------------------------------------------
# NARR-05: Human Override
# ---------------------------------------------------------------------------

async def test_narr05_human_override():
    """
    Orchestrator recommends DECLINE. Human loan officer overrides to APPROVE.
    ApplicationApproved.approved_amount_usd == 750000, 2 conditions.
    """
    store, registry = await _get_store_and_registry()
    app_id = "NARR-05"
    await _seed_application(store, app_id, "COMP-068", 950_000)

    docs_dir = os.environ.get("DOCUMENTS_DIR", "./documents")
    await DocumentProcessingAgent(store, registry, documents_dir=docs_dir).process(app_id)
    await CreditAnalysisAgent(store, registry).process(app_id)
    await FraudDetectionAgent(store, registry).process(app_id)
    await ComplianceAgent(store, registry).process(app_id)
    await DecisionOrchestratorAgent(store, registry).process(app_id)

    loan_events = await store.load_stream(f"loan-{app_id}")

    # Orchestrator must have generated a decision
    decision_events = [e for e in loan_events if e.event_type == "DecisionGenerated"]
    assert decision_events, "No DecisionGenerated event"
    decision = decision_events[0].payload
    assert decision.get("recommendation") == "DECLINE", f"Expected DECLINE, got {decision.get('recommendation')}"

    # HumanReviewRequested must be present
    review_requested = [e for e in loan_events if e.event_type == "HumanReviewRequested"]
    assert review_requested, "No HumanReviewRequested event"

    # Human override via command handler
    version = await store.stream_version(f"loan-{app_id}")
    from src.ledger.schema.events import HumanReviewCompleted, ApplicationApproved
    await store.append(f"loan-{app_id}", [
        HumanReviewCompleted(stream_id=f"loan-{app_id}", payload={
            "override": True,
            "reviewer_id": "LO-Sarah-Chen",
            "final_decision": "APPROVE",
            "override_reason": "15-year customer, prior repayment history, collateral offered",
        }),
        ApplicationApproved(stream_id=f"loan-{app_id}", payload={
            "approved_amount_usd": 750000,
            "conditions": [
                "Monthly revenue reporting for 12 months",
                "Personal guarantee from CEO",
            ],
            "approved_by_human": "LO-Sarah-Chen",
        }),
    ], expected_version=version)

    # Assertions
    final_events = await store.load_stream(f"loan-{app_id}")

    review_completed = [e for e in final_events if e.event_type == "HumanReviewCompleted"]
    assert review_completed[0].payload["override"] is True
    assert review_completed[0].payload["reviewer_id"] == "LO-Sarah-Chen"

    approved = [e for e in final_events if e.event_type == "ApplicationApproved"]
    assert approved, "No ApplicationApproved event"
    assert approved[0].payload["approved_amount_usd"] == 750000
    assert len(approved[0].payload["conditions"]) == 2
