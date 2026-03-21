from typing import Optional, TYPE_CHECKING
from src.ledger.domain.events import ComplianceCheckRequested, ComplianceRulePassed, ComplianceRuleFailed

if TYPE_CHECKING:
    from src.ledger.core.event_store import EventStore


class DomainError(Exception):
    pass


class ComplianceRecordAggregate:
    def __init__(self):
        self.application_id: Optional[str] = None
        self.checks_required: list = []
        self.checks_passed: list = []       # rule_ids that passed
        self.checks_failed: list = []       # rule_ids that failed
        self.version: int = 0

    # ── assertions ───────────────────────────────────────────────────────────

    def assert_all_checks_passed(self) -> None:
        pending = set(self.checks_required) - set(self.checks_passed)
        if pending:
            raise DomainError(f"Cannot clear compliance: pending checks {pending}")
        if self.checks_failed:
            raise DomainError(f"Cannot clear compliance: failed checks {self.checks_failed}")

    def assert_check_not_already_evaluated(self, rule_id: str) -> None:
        if rule_id in self.checks_passed or rule_id in self.checks_failed:
            raise DomainError(f"Rule {rule_id} already evaluated")

    # ── apply methods ────────────────────────────────────────────────────────

    def _on_compliance_check_requested(self, event: ComplianceCheckRequested) -> None:
        self.application_id = event.application_id
        self.checks_required = list(event.checks_required)

    def _on_compliance_rule_passed(self, event: ComplianceRulePassed) -> None:
        if event.rule_id not in self.checks_passed:
            self.checks_passed.append(event.rule_id)

    def _on_compliance_rule_failed(self, event: ComplianceRuleFailed) -> None:
        if event.rule_id not in self.checks_failed:
            self.checks_failed.append(event.rule_id)

    # ── dispatcher ───────────────────────────────────────────────────────────

    def _apply(self, event) -> None:
        dispatch = {
            "ComplianceCheckRequested": self._on_compliance_check_requested,
            "ComplianceRulePassed":     self._on_compliance_rule_passed,
            "ComplianceRuleFailed":     self._on_compliance_rule_failed,
        }
        handler = dispatch.get(event.event_type)
        if handler:
            handler(event)
        self.version += 1

    # ── loader ───────────────────────────────────────────────────────────────

    @classmethod
    async def load(cls, store: "EventStore", application_id: str) -> "ComplianceRecordAggregate":
        agg = cls()
        events = await store.load_stream(f"compliance-{application_id}")
        for stored in events:
            evt = _reconstruct(stored)
            agg._apply(evt)
        return agg


def _reconstruct(stored) -> object:
    from src.ledger.domain.events import ComplianceCheckRequested, ComplianceRulePassed, ComplianceRuleFailed
    mapping = {
        "ComplianceCheckRequested": ComplianceCheckRequested,
        "ComplianceRulePassed":     ComplianceRulePassed,
        "ComplianceRuleFailed":     ComplianceRuleFailed,
    }
    cls = mapping.get(stored.event_type)
    if cls is None:
        class Unknown:
            event_type = stored.event_type
        return Unknown()
    return cls(**{k: v for k, v in stored.event_data.items()
                  if k in cls.__dataclass_fields__})