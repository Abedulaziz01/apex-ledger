from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ApplicationSubmitted:
    application_id: str
    applicant_id: str
    requested_amount_usd: float
    loan_purpose: str
    submission_channel: str
    submitted_at: str
    event_type: str = "ApplicationSubmitted"
    version: int = 1


@dataclass
class CreditAnalysisRequested:
    application_id: str
    assigned_agent_id: str
    requested_at: str
    priority: str
    event_type: str = "CreditAnalysisRequested"
    version: int = 1


@dataclass
class CreditAnalysisCompleted:
    application_id: str
    agent_id: str
    session_id: str
    model_version: str
    confidence_score: float
    risk_tier: str
    recommended_limit_usd: float
    analysis_duration_ms: int
    input_data_hash: str
    event_type: str = "CreditAnalysisCompleted"
    version: int = 2


@dataclass
class FraudScreeningCompleted:
    application_id: str
    agent_id: str
    fraud_score: float
    anomaly_flags: list = field(default_factory=list)
    screening_model_version: str = ""
    input_data_hash: str = ""
    event_type: str = "FraudScreeningCompleted"
    version: int = 1


@dataclass
class ComplianceCheckRequested:
    application_id: str
    regulation_set_version: str
    checks_required: list = field(default_factory=list)
    event_type: str = "ComplianceCheckRequested"
    version: int = 1


@dataclass
class ComplianceRulePassed:
    application_id: str
    rule_id: str
    rule_version: str
    evaluation_timestamp: str
    evidence_hash: str
    event_type: str = "ComplianceRulePassed"
    version: int = 1


@dataclass
class ComplianceRuleFailed:
    application_id: str
    rule_id: str
    rule_version: str
    failure_reason: str
    remediation_required: bool
    event_type: str = "ComplianceRuleFailed"
    version: int = 1


@dataclass
class DecisionGenerated:
    application_id: str
    orchestrator_agent_id: str
    recommendation: str  # APPROVE / DECLINE / REFER
    confidence_score: float
    contributing_agent_sessions: list = field(default_factory=list)
    decision_basis_summary: str = ""
    model_versions: dict = field(default_factory=dict)
    event_type: str = "DecisionGenerated"
    version: int = 2


@dataclass
class HumanReviewCompleted:
    application_id: str
    reviewer_id: str
    override: bool
    final_decision: str
    override_reason: Optional[str] = None
    event_type: str = "HumanReviewCompleted"
    version: int = 1


@dataclass
class ApplicationApproved:
    application_id: str
    approved_amount_usd: float
    interest_rate: float
    conditions: list = field(default_factory=list)
    approved_by: str = ""
    effective_date: str = ""
    event_type: str = "ApplicationApproved"
    version: int = 1


@dataclass
class ApplicationDeclined:
    application_id: str
    decline_reasons: list = field(default_factory=list)
    declined_by: str = ""
    adverse_action_notice_required: bool = False
    event_type: str = "ApplicationDeclined"
    version: int = 1


@dataclass
class AgentContextLoaded:
    agent_id: str
    session_id: str
    context_source: str
    event_replay_from_position: int
    context_token_count: int
    model_version: str
    event_type: str = "AgentContextLoaded"
    version: int = 1


@dataclass
class AuditIntegrityCheckRun:
    entity_id: str
    check_timestamp: str
    events_verified_count: int
    integrity_hash: str
    previous_hash: str
    event_type: str = "AuditIntegrityCheckRun"
    version: int = 1