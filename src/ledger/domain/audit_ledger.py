from typing import Optional, TYPE_CHECKING

from src.ledger.domain.events import AuditIntegrityCheckRun

if TYPE_CHECKING:
    from src.ledger.core.event_store import EventStore


class DomainError(Exception):
    pass


class AuditLedgerAggregate:
    def __init__(self):
        self.entity_id: Optional[str] = None
        self.last_integrity_hash: Optional[str] = None
        self.last_check_timestamp: Optional[str] = None
        self.events_verified_count: int = 0
        self.version: int = 0

    def _on_integrity_check_run(self, event: AuditIntegrityCheckRun) -> None:
        self.entity_id = event.entity_id
        self.last_integrity_hash = event.integrity_hash
        self.last_check_timestamp = event.check_timestamp
        self.events_verified_count = event.events_verified_count

    def _apply(self, event) -> None:
        if event.event_type == "AuditIntegrityCheckRun":
            self._on_integrity_check_run(event)
        self.version += 1

    @classmethod
    async def load(cls, store: "EventStore", entity_id: str) -> "AuditLedgerAggregate":
        agg = cls()
        events = await store.load_stream(f"audit-{entity_id}")
        for stored in events:
            evt = _reconstruct(stored)
            agg._apply(evt)
        return agg


def _reconstruct(stored) -> object:
    if stored.event_type != "AuditIntegrityCheckRun":
        class Unknown:
            event_type = stored.event_type
        return Unknown()
    return AuditIntegrityCheckRun(
        **{
            k: v
            for k, v in stored.event_data.items()
            if k in AuditIntegrityCheckRun.__dataclass_fields__
        }
    )

