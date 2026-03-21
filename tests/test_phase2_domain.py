import pytest
from src.ledger.domain.loan_application import LoanApplicationAggregate, DomainError
from src.ledger.domain.agent_session import AgentSessionAggregate
from src.ledger.domain.agent_session import DomainError as AgentDomainError
from src.ledger.domain.compliance_record import ComplianceRecordAggregate
from src.ledger.domain.compliance_record import DomainError as ComplianceDomainError
from src.ledger.domain.events import (
    ApplicationSubmitted,
    CreditAnalysisRequested,
    CreditAnalysisCompleted,
    DecisionGenerated,
    ApplicationApproved,
    AgentContextLoaded,
    ComplianceCheckRequested,
    ComplianceRulePassed,
    ComplianceRuleFailed,
)


# ── Rule 1: State machine ─────────────────────────────────────────────────────

def test_valid_state_transitions():
    agg = LoanApplicationAggregate()
    agg._apply(ApplicationSubmitted("app-1", "cust-1", 50000, "working capital", "web", "2026-01-01"))
    assert agg.status == "Submitted"
    agg._apply(CreditAnalysisRequested("app-1", "agent-1", "2026-01-01", "high"))
    assert agg.status == "AwaitingAnalysis"


def test_invalid_state_transition_rejected():
    agg = LoanApplicationAggregate()
    agg._apply(ApplicationSubmitted("app-2", "cust-1", 50000, "working capital", "web", "2026-01-01"))
    # Try to skip straight to AnalysisComplete without going through AwaitingAnalysis
    with pytest.raises(DomainError, match="Cannot transition"):
        agg._apply(CreditAnalysisCompleted(
            "app-2", "agent-1", "sess-1", "gpt-4",
            0.9, "LOW", 50000, 1200, "hash-abc"
        ))


def test_no_transition_from_final_state():
    agg = LoanApplicationAggregate()
    agg._apply(ApplicationSubmitted("app-3", "cust-1", 50000, "wc", "web", "2026-01-01"))
    agg._apply(CreditAnalysisRequested("app-3", "agent-1", "2026-01-01", "normal"))
    agg._apply(CreditAnalysisCompleted("app-3", "agent-1", "sess-1", "gpt-4", 0.9, "LOW", 50000, 1200, "hash"))
    agg._apply(DecisionGenerated("app-3", "orch-1", "APPROVE", 0.9, [], "good", {}))
    agg._apply(ApplicationApproved("app-3", 50000, 0.05, [], "human-1", "2026-02-01"))
    assert agg.status == "FinalApproved"
    with pytest.raises(DomainError):
        agg._apply(ApplicationApproved("app-3", 50000, 0.05, [], "human-1", "2026-02-01"))


# ── Rule 2: Gas Town — agent context required ─────────────────────────────────

def test_agent_without_context_raises():
    agg = AgentSessionAggregate()
    with pytest.raises(AgentDomainError, match="Gas Town"):
        agg.assert_context_loaded()


def test_agent_with_context_passes():
    agg = AgentSessionAggregate()
    agg._apply(AgentContextLoaded("agent-1", "sess-1", "event_replay", 0, 1000, "gpt-4"))
    agg.assert_context_loaded()  # no exception


# ── Rule 3: Model version locking ─────────────────────────────────────────────

def test_second_credit_analysis_rejected():
    agg = LoanApplicationAggregate()
    agg._apply(ApplicationSubmitted("app-4", "cust-1", 50000, "wc", "web", "2026-01-01"))
    agg._apply(CreditAnalysisRequested("app-4", "agent-1", "2026-01-01", "normal"))
    agg._apply(CreditAnalysisCompleted("app-4", "agent-1", "sess-1", "gpt-4", 0.9, "LOW", 50000, 1200, "hash"))
    assert agg.credit_analysis_done is True
    with pytest.raises(DomainError, match="already completed"):
        agg.assert_no_credit_analysis()


def test_model_version_mismatch_rejected():
    agg = AgentSessionAggregate()
    agg._apply(AgentContextLoaded("agent-1", "sess-1", "event_replay", 0, 1000, "gpt-4"))
    with pytest.raises(AgentDomainError, match="version mismatch"):
        agg.assert_model_version_current("gpt-3")


# ── Rule 4: Confidence floor ──────────────────────────────────────────────────

def test_low_confidence_forces_refer():
    result = LoanApplicationAggregate.enforce_confidence_floor(0.55, "APPROVE")
    assert result == "REFER"


def test_low_confidence_forces_refer_on_decline():
    result = LoanApplicationAggregate.enforce_confidence_floor(0.59, "DECLINE")
    assert result == "REFER"


def test_high_confidence_passes_through():
    result = LoanApplicationAggregate.enforce_confidence_floor(0.85, "APPROVE")
    assert result == "APPROVE"


def test_exactly_06_passes_through():
    result = LoanApplicationAggregate.enforce_confidence_floor(0.6, "APPROVE")
    assert result == "APPROVE"


# ── Rule 5: Compliance dependency ─────────────────────────────────────────────

def test_cannot_approve_without_compliance():
    agg = LoanApplicationAggregate()
    agg.compliance_cleared = False
    with pytest.raises(DomainError, match="compliance not yet cleared"):
        agg.assert_compliance_cleared()


def test_compliance_cleared_after_all_checks_pass():
    comp = ComplianceRecordAggregate()
    comp._apply(ComplianceCheckRequested("app-5", "v2", ["KYC", "AML"]))
    comp._apply(ComplianceRulePassed("app-5", "KYC", "v1", "2026-01-01", "hash1"))
    comp._apply(ComplianceRulePassed("app-5", "AML", "v1", "2026-01-01", "hash2"))
    comp.assert_all_checks_passed()  # no exception


def test_compliance_fails_with_pending_checks():
    comp = ComplianceRecordAggregate()
    comp._apply(ComplianceCheckRequested("app-6", "v2", ["KYC", "AML", "OFAC"]))
    comp._apply(ComplianceRulePassed("app-6", "KYC", "v1", "2026-01-01", "hash1"))
    with pytest.raises(ComplianceDomainError, match="pending checks"):
        comp.assert_all_checks_passed()


def test_compliance_fails_with_failed_rule():
    comp = ComplianceRecordAggregate()
    comp._apply(ComplianceCheckRequested("app-7", "v2", ["KYC"]))
    comp._apply(ComplianceRulePassed("app-7", "KYC", "v1", "2026-01-01", "hash1"))
    comp._apply(ComplianceRuleFailed("app-7", "AML", "v1", "sanctions hit", True))
    with pytest.raises(ComplianceDomainError, match="failed checks"):
        comp.assert_all_checks_passed()


# ── Rule 6: Causal chain enforcement ──────────────────────────────────────────

def test_causal_chain_rejects_unknown_session():
    agg = LoanApplicationAggregate()
    agg._apply(ApplicationSubmitted("app-8", "cust-1", 50000, "wc", "web", "2026-01-01"))
    known_sessions = {"agent-credit-sess-1", "agent-fraud-sess-1"}
    with pytest.raises(DomainError, match="never processed"):
        agg.assert_causal_chain(
            ["agent-credit-sess-1", "agent-ghost-sess-99"],
            known_sessions,
        )


def test_causal_chain_passes_with_valid_sessions():
    agg = LoanApplicationAggregate()
    known_sessions = {"agent-credit-sess-1", "agent-fraud-sess-1"}
    agg.assert_causal_chain(
        ["agent-credit-sess-1", "agent-fraud-sess-1"],
        known_sessions,
    )  # no exception