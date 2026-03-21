from typing import Optional, TYPE_CHECKING
from src.ledger.domain.events import AgentContextLoaded, CreditAnalysisCompleted, FraudScreeningCompleted

if TYPE_CHECKING:
    from src.ledger.core.event_store import EventStore


class DomainError(Exception):
    pass


class AgentSessionAggregate:
    def __init__(self):
        self.agent_id: Optional[str] = None
        self.session_id: Optional[str] = None
        self.context_loaded: bool = False
        self.model_version: Optional[str] = None
        self.decisions: list = []          # application_ids decided
        self.version: int = 0

    # ── assertions ───────────────────────────────────────────────────────────

    def assert_context_loaded(self) -> None:
        if not self.context_loaded:
            raise DomainError("AgentSession has no AgentContextLoaded event — Gas Town violation")

    def assert_model_version_current(self, model_version: str) -> None:
        if self.model_version and self.model_version != model_version:
            raise DomainError(
                f"Model version mismatch: session has '{self.model_version}', command has '{model_version}'"
            )

    def assert_not_decided_for(self, application_id: str) -> None:
        if application_id in self.decisions:
            raise DomainError(f"Session already produced a decision for application {application_id}")

    # ── apply methods ────────────────────────────────────────────────────────

    def _on_agent_context_loaded(self, event: AgentContextLoaded) -> None:
        self.agent_id = event.agent_id
        self.session_id = event.session_id
        self.model_version = event.model_version
        self.context_loaded = True

    def _on_credit_analysis_completed(self, event: CreditAnalysisCompleted) -> None:
        self.decisions.append(event.application_id)

    def _on_fraud_screening_completed(self, event: FraudScreeningCompleted) -> None:
        self.decisions.append(event.application_id)

    # ── dispatcher ───────────────────────────────────────────────────────────

    def _apply(self, event) -> None:
        dispatch = {
            "AgentContextLoaded":      self._on_agent_context_loaded,
            "CreditAnalysisCompleted": self._on_credit_analysis_completed,
            "FraudScreeningCompleted": self._on_fraud_screening_completed,
        }
        handler = dispatch.get(event.event_type)
        if handler:
            handler(event)
        self.version += 1

    # ── loader ───────────────────────────────────────────────────────────────

    @classmethod
    async def load(cls, store: "EventStore", agent_id: str, session_id: str) -> "AgentSessionAggregate":
        agg = cls()
        events = await store.load_stream(f"agent-{agent_id}-{session_id}")
        for stored in events:
            evt = _reconstruct(stored)
            agg._apply(evt)
        return agg


def _reconstruct(stored) -> object:
    from src.ledger.domain.events import AgentContextLoaded, CreditAnalysisCompleted, FraudScreeningCompleted
    mapping = {
        "AgentContextLoaded":      AgentContextLoaded,
        "CreditAnalysisCompleted": CreditAnalysisCompleted,
        "FraudScreeningCompleted": FraudScreeningCompleted,
    }
    cls = mapping.get(stored.event_type)
    if cls is None:
        class Unknown:
            event_type = stored.event_type
        return Unknown()
    return cls(**{k: v for k, v in stored.event_data.items()
                  if k in cls.__dataclass_fields__})