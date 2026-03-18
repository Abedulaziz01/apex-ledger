from dataclasses import dataclass, field
from typing import Optional


@dataclass
class SubmitApplicationCommand:
    application_id: str
    applicant_id: str
    requested_amount_usd: float
    loan_purpose: str
    submission_channel: str
    submitted_at: str
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


@dataclass
class RequestCreditAnalysisCommand:
    application_id: str
    assigned_agent_id: str
    requested_at: str
    priority: str = "normal"
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


@dataclass
class CompleteCreditAnalysisCommand:
    application_id: str
    agent_id: str
    session_id: str
    model_version: str
    confidence_score: float
    risk_tier: str
    recommended_limit_usd: float
    duration_ms: int
    input_data: dict = field(default_factory=dict)
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


@dataclass
class CompleteFraudScreeningCommand:
    application_id: str
    agent_id: str
    fraud_score: float
    anomaly_flags: list = field(default_factory=list)
    screening_model_version: str = ""
    input_data: dict = field(default_factory=dict)
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


@dataclass
class RequestComplianceCheckCommand:
    application_id: str
    regulation_set_version: str
    checks_required: list = field(default_factory=list)
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


@dataclass
class PassComplianceRuleCommand:
    application_id: str
    rule_id: str
    rule_version: str
    evaluation_timestamp: str
    evidence_hash: str
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


@dataclass
class FailComplianceRuleCommand:
    application_id: str
    rule_id: str
    rule_version: str
    failure_reason: str
    remediation_required: bool = False
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


@dataclass
class GenerateDecisionCommand:
    application_id: str
    orchestrator_agent_id: str
    recommendation: str
    confidence_score: float
    contributing_agent_sessions: list = field(default_factory=list)
    decision_basis_summary: str = ""
    model_versions: dict = field(default_factory=dict)
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


@dataclass
class CompleteHumanReviewCommand:
    application_id: str
    reviewer_id: str
    override: bool
    final_decision: str
    override_reason: Optional[str] = None
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


@dataclass
class ApproveApplicationCommand:
    application_id: str
    approved_amount_usd: float
    interest_rate: float
    conditions: list = field(default_factory=list)
    approved_by: str = ""
    effective_date: str = ""
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


@dataclass
class DeclineApplicationCommand:
    application_id: str
    decline_reasons: list = field(default_factory=list)
    declined_by: str = ""
    adverse_action_notice_required: bool = False
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


@dataclass
class LoadAgentContextCommand:
    agent_id: str
    session_id: str
    context_source: str
    event_replay_from_position: int
    context_token_count: int
    model_version: str
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None