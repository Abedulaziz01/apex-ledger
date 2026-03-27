"""
ledger/schema/events.py
Single source of truth for all 45 event types.
Every agent, projection, and test imports from here. Never redefine elsewhere.
"""
from __future__ import annotations
import uuid
from datetime import datetime, timezone
from typing import Any, Optional
from pydantic import BaseModel, Field


def _now() -> datetime:
    return datetime.now(timezone.utc)

def _uid() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------

class EventMetadata(BaseModel):
    event_id: str = Field(default_factory=_uid)
    occurred_at: datetime = Field(default_factory=_now)
    causation_id: Optional[str] = None
    correlation_id: Optional[str] = None
    schema_version: int = 1


class BaseEvent(BaseModel):
    event_type: str
    stream_id: str
    stream_position: Optional[int] = None   # set by EventStore on append
    metadata: EventMetadata = Field(default_factory=EventMetadata)
    payload: dict[str, Any] = Field(default_factory=dict)

    def model_dump_for_store(self) -> dict:
        return self.model_dump(mode="json")


# ---------------------------------------------------------------------------
# 1. LoanApplication stream  (loan-{id})
# ---------------------------------------------------------------------------

class ApplicationSubmitted(BaseEvent):
    event_type: str = "ApplicationSubmitted"

class DocumentUploadRequested(BaseEvent):
    event_type: str = "DocumentUploadRequested"

class DocumentUploaded(BaseEvent):
    event_type: str = "DocumentUploaded"

class CreditAnalysisRequested(BaseEvent):
    event_type: str = "CreditAnalysisRequested"

class FraudScreeningRequested(BaseEvent):
    event_type: str = "FraudScreeningRequested"

class ComplianceCheckRequested(BaseEvent):
    event_type: str = "ComplianceCheckRequested"

class DecisionRequested(BaseEvent):
    event_type: str = "DecisionRequested"

class DecisionGenerated(BaseEvent):
    event_type: str = "DecisionGenerated"

class HumanReviewRequested(BaseEvent):
    event_type: str = "HumanReviewRequested"

class HumanReviewCompleted(BaseEvent):
    event_type: str = "HumanReviewCompleted"

class ApplicationApproved(BaseEvent):
    event_type: str = "ApplicationApproved"

class ApplicationDeclined(BaseEvent):
    event_type: str = "ApplicationDeclined"


# ---------------------------------------------------------------------------
# 2. DocumentPackage stream  (docpkg-{id})
# ---------------------------------------------------------------------------

class PackageCreated(BaseEvent):
    event_type: str = "PackageCreated"

class DocumentAdded(BaseEvent):
    event_type: str = "DocumentAdded"

class DocumentFormatValidated(BaseEvent):
    event_type: str = "DocumentFormatValidated"

class ExtractionStarted(BaseEvent):
    event_type: str = "ExtractionStarted"

class ExtractionCompleted(BaseEvent):
    event_type: str = "ExtractionCompleted"

class QualityAssessmentCompleted(BaseEvent):
    event_type: str = "QualityAssessmentCompleted"

class PackageReadyForAnalysis(BaseEvent):
    event_type: str = "PackageReadyForAnalysis"


# ---------------------------------------------------------------------------
# 3. AgentSession stream  (agent-{type}-{session_id})
# ---------------------------------------------------------------------------

class AgentSessionStarted(BaseEvent):
    event_type: str = "AgentSessionStarted"

class AgentInputValidated(BaseEvent):
    event_type: str = "AgentInputValidated"

class AgentInputValidationFailed(BaseEvent):
    event_type: str = "AgentInputValidationFailed"

class AgentNodeExecuted(BaseEvent):
    event_type: str = "AgentNodeExecuted"

class AgentToolCalled(BaseEvent):
    event_type: str = "AgentToolCalled"

class AgentOutputWritten(BaseEvent):
    event_type: str = "AgentOutputWritten"

class AgentSessionCompleted(BaseEvent):
    event_type: str = "AgentSessionCompleted"

class AgentSessionFailed(BaseEvent):
    event_type: str = "AgentSessionFailed"

class AgentSessionRecovered(BaseEvent):
    event_type: str = "AgentSessionRecovered"


# ---------------------------------------------------------------------------
# 4. CreditRecord stream  (credit-{id})
# ---------------------------------------------------------------------------

class CreditRecordOpened(BaseEvent):
    event_type: str = "CreditRecordOpened"

class HistoricalProfileConsumed(BaseEvent):
    event_type: str = "HistoricalProfileConsumed"

class ExtractedFactsConsumed(BaseEvent):
    event_type: str = "ExtractedFactsConsumed"

class CreditAnalysisCompleted(BaseEvent):
    event_type: str = "CreditAnalysisCompleted"

class CreditAnalysisDeferred(BaseEvent):
    event_type: str = "CreditAnalysisDeferred"


# ---------------------------------------------------------------------------
# 5. FraudScreening stream  (fraud-{id})
# ---------------------------------------------------------------------------

class FraudScreeningInitiated(BaseEvent):
    event_type: str = "FraudScreeningInitiated"

class FraudAnomalyDetected(BaseEvent):
    event_type: str = "FraudAnomalyDetected"

class FraudScreeningCompleted(BaseEvent):
    event_type: str = "FraudScreeningCompleted"


# ---------------------------------------------------------------------------
# 6. ComplianceRecord stream  (compliance-{id})
# ---------------------------------------------------------------------------

class ComplianceCheckInitiated(BaseEvent):
    event_type: str = "ComplianceCheckInitiated"

class ComplianceRulePassed(BaseEvent):
    event_type: str = "ComplianceRulePassed"

class ComplianceRuleFailed(BaseEvent):
    event_type: str = "ComplianceRuleFailed"

class ComplianceRuleNoted(BaseEvent):
    event_type: str = "ComplianceRuleNoted"

class ComplianceCheckCompleted(BaseEvent):
    event_type: str = "ComplianceCheckCompleted"


# ---------------------------------------------------------------------------
# 7. AuditLedger stream  (audit-{entity_type}-{id})
# ---------------------------------------------------------------------------

class AuditIntegrityCheckRun(BaseEvent):
    event_type: str = "AuditIntegrityCheckRun"


# ---------------------------------------------------------------------------
# Registry — map event_type string → class
# ---------------------------------------------------------------------------

EVENT_REGISTRY: dict[str, type[BaseEvent]] = {
    cls.model_fields["event_type"].default: cls
    for cls in [
        ApplicationSubmitted, DocumentUploadRequested, DocumentUploaded,
        CreditAnalysisRequested, FraudScreeningRequested, ComplianceCheckRequested,
        DecisionRequested, DecisionGenerated, HumanReviewRequested,
        HumanReviewCompleted, ApplicationApproved, ApplicationDeclined,
        PackageCreated, DocumentAdded, DocumentFormatValidated,
        ExtractionStarted, ExtractionCompleted, QualityAssessmentCompleted,
        PackageReadyForAnalysis,
        AgentSessionStarted, AgentInputValidated, AgentInputValidationFailed,
        AgentNodeExecuted, AgentToolCalled, AgentOutputWritten,
        AgentSessionCompleted, AgentSessionFailed, AgentSessionRecovered,
        CreditRecordOpened, HistoricalProfileConsumed, ExtractedFactsConsumed,
        CreditAnalysisCompleted, CreditAnalysisDeferred,
        FraudScreeningInitiated, FraudAnomalyDetected, FraudScreeningCompleted,
        ComplianceCheckInitiated, ComplianceRulePassed, ComplianceRuleFailed,
        ComplianceRuleNoted, ComplianceCheckCompleted,
        AuditIntegrityCheckRun,
    ]
}


def deserialize_event(raw: dict) -> BaseEvent:
    """Reconstruct a typed event from a DB row dict."""
    event_type = raw["event_type"]
    cls = EVENT_REGISTRY.get(event_type, BaseEvent)
    return cls(**raw)
