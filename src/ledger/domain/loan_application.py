import json
from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING
from src.ledger.domain.events import (
    ApplicationSubmitted,
    CreditAnalysisRequested,
    CreditAnalysisCompleted,
    FraudScreeningCompleted,
    DecisionGenerated,
    HumanReviewCompleted,
    ApplicationApproved,
    ApplicationDeclined,
)

if TYPE_CHECKING:
    from src.ledger.core.event_store import EventStore


class DomainError(Exception):
    pass


VALID_TRANSITIONS = {
    None: "Submitted",
    "Submitted": "AwaitingAnalysis",
    "AwaitingAnalysis": "AnalysisComplete",
    "AnalysisComplete": ["ApprovedPendingHuman", "DeclinedPendingHuman"],
    "ApprovedPendingHuman": "FinalApproved",
    "DeclinedPendingHuman": "FinalDeclined",
}


class LoanApplicationAggregate:
    def __init__(self):
        self.application_id: Optional[str] = None
        self.status: Optional[str] = None
        self.version: int = 0
        self.credit_analysis_done: bool = False
        self.compliance_cleared: bool = False
        self.recommended_limit_usd: Optional[float] = None
        self.contributing_sessions: list = []

    # ── rule 1: state machine ─────────────────────────────────────────────

    def _transition(self, new_status: str) -> None:
        allowed = VALID_TRANSITIONS.get(self.status)
        if allowed is None:
            raise DomainError(f"No transitions allowed from '{self.status}'")
        if isinstance(allowed, list):
            if new_status not in allowed:
                raise DomainError(f"Cannot transition from '{self.status}' to '{new_status}'")
        else:
            if new_status != allowed:
                raise DomainError(f"Cannot transition from '{self.status}' to '{new_status}'")
        self.status = new_status

    # ── assertions ────────────────────────────────────────────────────────

    def assert_awaiting_analysis(self) -> None:
        if self.status != "AwaitingAnalysis":
            raise DomainError(f"Expected AwaitingAnalysis, got '{self.status}'")

    def assert_analysis_complete(self) -> None:
        if self.status != "AnalysisComplete":
            raise DomainError(f"Expected AnalysisComplete, got '{self.status}'")

    # rule 3: model version locking
    def assert_no_credit_analysis(self) -> None:
        if self.credit_analysis_done:
            raise DomainError("CreditAnalysis already completed — model version locked")

    # rule 5: compliance dependency
    def assert_compliance_cleared(self) -> None:
        if not self.compliance_cleared:
            raise DomainError("Cannot approve: compliance not yet cleared")

    def assert_pending_decision(self) -> None:
        if self.status != "PendingDecision":
            raise DomainError(f"Expected PendingDecision, got '{self.status}'")

    # rule 4: confidence floor
    @staticmethod
    def enforce_confidence_floor(confidence_score: float, recommendation: str) -> str:
        if confidence_score < 0.6 and recommendation != "REFER":
            return "REFER"
        return recommendation

    # rule 6: causal chain
    def assert_causal_chain(self, contributing_sessions: list, sessions_that_processed: set) -> None:
        invalid = set(contributing_sessions) - sessions_that_processed
        if invalid:
            raise DomainError(
                f"Orchestrator references sessions that never processed this application: {invalid}"
            )

    # ── apply methods ─────────────────────────────────────────────────────

    def _on_application_submitted(self, event: ApplicationSubmitted) -> None:
        self.application_id = event.application_id
        self._transition("Submitted")

    def _on_credit_analysis_requested(self, event: CreditAnalysisRequested) -> None:
        self._transition("AwaitingAnalysis")

    def _on_credit_analysis_completed(self, event: CreditAnalysisCompleted) -> None:
        self.credit_analysis_done = True
        self.recommended_limit_usd = event.recommended_limit_usd
        self._transition("AnalysisComplete")

    def _on_fraud_screening_completed(self, event: FraudScreeningCompleted) -> None:
        pass

    def _on_decision_generated(self, event: DecisionGenerated) -> None:
        self.contributing_sessions = event.contributing_agent_sessions
        if event.recommendation == "APPROVE":
            self._transition("ApprovedPendingHuman")
        elif event.recommendation == "DECLINE":
            self._transition("DeclinedPendingHuman")
        else:
            self._transition("DeclinedPendingHuman")  # REFER goes to declined pending human

    def _on_human_review_completed(self, event: HumanReviewCompleted) -> None:
        pass

    def _on_application_approved(self, event: ApplicationApproved) -> None:
        self._transition("FinalApproved")

    def _on_application_declined(self, event: ApplicationDeclined) -> None:
        self._transition("FinalDeclined")

    # ── dispatcher ────────────────────────────────────────────────────────

    def _apply(self, event) -> None:
        dispatch = {
            "ApplicationSubmitted":    self._on_application_submitted,
            "CreditAnalysisRequested": self._on_credit_analysis_requested,
            "CreditAnalysisCompleted": self._on_credit_analysis_completed,
            "FraudScreeningCompleted": self._on_fraud_screening_completed,
            "DecisionGenerated":       self._on_decision_generated,
            "HumanReviewCompleted":    self._on_human_review_completed,
            "ApplicationApproved":     self._on_application_approved,
            "ApplicationDeclined":     self._on_application_declined,
        }
        handler = dispatch.get(event.event_type)
        if handler:
            handler(event)
        self.version += 1

    # ── loader ────────────────────────────────────────────────────────────

    @classmethod
    async def load(cls, store: "EventStore", application_id: str) -> "LoanApplicationAggregate":
        agg = cls()
        events = await store.load_stream(f"loan-{application_id}")
        for stored in events:
            evt = _reconstruct(stored)
            agg._apply(evt)
        return agg


def _reconstruct(stored) -> object:
    from src.ledger.domain.events import (
        ApplicationSubmitted, CreditAnalysisRequested, CreditAnalysisCompleted,
        FraudScreeningCompleted, DecisionGenerated, HumanReviewCompleted,
        ApplicationApproved, ApplicationDeclined,
    )
    mapping = {
        "ApplicationSubmitted":    ApplicationSubmitted,
        "CreditAnalysisRequested": CreditAnalysisRequested,
        "CreditAnalysisCompleted": CreditAnalysisCompleted,
        "FraudScreeningCompleted": FraudScreeningCompleted,
        "DecisionGenerated":       DecisionGenerated,
        "HumanReviewCompleted":    HumanReviewCompleted,
        "ApplicationApproved":     ApplicationApproved,
        "ApplicationDeclined":     ApplicationDeclined,
    }
    cls = mapping.get(stored.event_type)
    if cls is None:
        class Unknown:
            event_type = stored.event_type
        return Unknown()
    return cls(**{k: v for k, v in stored.event_data.items()
                  if k in cls.__dataclass_fields__})