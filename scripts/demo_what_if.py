"""
Run a simple what-if demo without touching the real DB.

Usage:
  $env:PYTHONPATH="."
  uv run python scripts/demo_what_if.py
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

from src.ledger.core.models import BaseEvent, StoredEvent
from src.what_if.projector import run_what_if


class DemoStore:
    def __init__(self):
        now = datetime.now(timezone.utc)
        self._events = [
            StoredEvent(
                id=1,
                stream_id="loan-DEMO-WHATIF",
                version=1,
                event_type="ApplicationSubmitted",
                event_data={"application_id": "DEMO-WHATIF", "requested_amount_usd": 250000},
                metadata={"event_id": "evt-1"},
                created_at=now,
            ),
            StoredEvent(
                id=2,
                stream_id="loan-DEMO-WHATIF",
                version=2,
                event_type="CreditAnalysisRequested",
                event_data={"application_id": "DEMO-WHATIF", "credit_record_id": "credit-1"},
                metadata={"event_id": "evt-2", "causation_id": "evt-1"},
                created_at=now,
            ),
        ]

    async def load_stream(self, stream_id: str, from_version: int = 0):
        return [e for e in self._events if e.stream_id == stream_id and e.version > from_version]


async def main() -> None:
    store = DemoStore()

    counterfactual = BaseEvent(
        event_type="DecisionGenerated",
        event_data={"recommendation": "APPROVE", "confidence_score": 0.72},
        metadata={"event_id": "evt-cf-1", "causation_id": "evt-2"},
    )

    result = await run_what_if(
        store=store,
        stream_id="loan-DEMO-WHATIF",
        counterfactual_events=[counterfactual],
        require_causal_dependencies=True,
    )

    print("What-if demo result")
    print("=" * 40)
    print(f"writes_performed: {result['writes_performed']}")
    print(f"base_event_count: {result['base_event_count']}")
    print(f"counterfactual_event_count: {result['counterfactual_event_count']}")
    print(f"included_event_count: {result['included_event_count']}")
    print(f"filtered_event_count: {result['filtered_event_count']}")
    print("")
    print(json.dumps(result["simulated_timeline"], indent=2))


if __name__ == "__main__":
    asyncio.run(main())

