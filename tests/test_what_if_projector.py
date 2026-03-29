from datetime import datetime, timedelta, timezone

import pytest

from src.ledger.core.models import BaseEvent, StoredEvent
from src.what_if.projector import run_what_if


class _ReadOnlyStore:
    def __init__(self, events):
        self._events = events
        self.append_calls = 0

    async def load_stream(self, stream_id: str, from_version: int = 0):
        return [e for e in self._events if e.stream_id == stream_id and e.version > from_version]

    async def append(self, *args, **kwargs):
        self.append_calls += 1
        raise AssertionError("run_what_if must never append to real store")


@pytest.mark.asyncio
async def test_run_what_if_filters_missing_causation_and_never_writes():
    now = datetime.now(timezone.utc)
    base = [
        StoredEvent(
            id=1,
            stream_id="loan-wi-1",
            version=1,
            event_type="ApplicationSubmitted",
            event_data={"application_id": "wi-1"},
            metadata={"event_id": "evt-1"},
            created_at=now,
        )
    ]
    hypothetical = BaseEvent(
        event_type="DecisionGenerated",
        event_data={"recommendation": "APPROVE"},
        metadata={"causation_id": "missing-id"},
    )
    store = _ReadOnlyStore(base)

    result = await run_what_if(
        store,
        "loan-wi-1",
        [hypothetical],
        require_causal_dependencies=True,
    )

    assert result["writes_performed"] is False
    assert result["included_event_count"] == 1
    assert result["filtered_event_count"] == 1
    assert store.append_calls == 0


@pytest.mark.asyncio
async def test_run_what_if_include_types_allowlist():
    now = datetime.now(timezone.utc)
    base = [
        StoredEvent(
            id=1,
            stream_id="loan-wi-2",
            version=1,
            event_type="ApplicationSubmitted",
            event_data={"application_id": "wi-2"},
            metadata={"event_id": "evt-1"},
            created_at=now,
        )
    ]
    hypothetical = BaseEvent(
        event_type="DecisionGenerated",
        event_data={"recommendation": "DECLINE"},
        metadata={"event_id": "evt-cf", "causation_id": "evt-1"},
    )
    store = _ReadOnlyStore(base)

    result = await run_what_if(
        store,
        "loan-wi-2",
        [hypothetical],
        include_event_types={"DecisionGenerated"},
        require_causal_dependencies=False,
    )

    assert result["included_event_count"] == 1
    assert result["simulated_timeline"][0]["event_type"] == "DecisionGenerated"

