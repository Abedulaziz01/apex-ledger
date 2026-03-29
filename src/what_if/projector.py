"""
Counterfactual "what-if" projector.

This module intentionally never writes to the real event store. It computes an
in-memory simulated timeline by combining persisted events with hypothetical
events, while optionally enforcing causal dependencies.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class _SimEvent:
    event_id: str
    event_type: str
    payload: dict[str, Any]
    metadata: dict[str, Any]
    created_at: datetime
    source: str  # "base" | "counterfactual"


def _ensure_utc(ts: datetime | None) -> datetime:
    if ts is None:
        return datetime.now(timezone.utc)
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


def _event_id_for(raw: Any, fallback: str) -> str:
    metadata = getattr(raw, "metadata", None) or {}
    if isinstance(metadata, dict):
        ev_id = metadata.get("event_id")
        if ev_id:
            return str(ev_id)
    return fallback


def _metadata_for(raw: Any) -> dict[str, Any]:
    metadata = getattr(raw, "metadata", None)
    if metadata is None:
        return {}
    if isinstance(metadata, dict):
        return dict(metadata)
    if hasattr(metadata, "model_dump"):
        return metadata.model_dump(mode="json")
    return {}


def _payload_for(raw: Any) -> dict[str, Any]:
    payload = getattr(raw, "payload", None)
    if payload is not None:
        return dict(payload)
    event_data = getattr(raw, "event_data", None)
    if event_data is not None:
        return dict(event_data)
    return {}


def _event_type_for(raw: Any) -> str:
    return str(getattr(raw, "event_type", "UnknownEvent"))


def _to_sim_event(raw: Any, index: int, source: str) -> _SimEvent:
    return _SimEvent(
        event_id=_event_id_for(raw, f"{source}-{index}"),
        event_type=_event_type_for(raw),
        payload=_payload_for(raw),
        metadata=_metadata_for(raw),
        created_at=_ensure_utc(getattr(raw, "created_at", None)),
        source=source,
    )


def _dependency_satisfied(ev: _SimEvent, known_ids: set[str], require_causation: bool) -> bool:
    if not require_causation:
        return True
    causation_id = ev.metadata.get("causation_id")
    if not causation_id:
        return True
    return str(causation_id) in known_ids


def _count_by_type(events: list[_SimEvent]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for ev in events:
        counts[ev.event_type] = counts.get(ev.event_type, 0) + 1
    return counts


async def run_what_if(
    store: Any,
    stream_id: str,
    counterfactual_events: list[Any],
    *,
    from_version: int = 0,
    include_event_types: set[str] | None = None,
    require_causal_dependencies: bool = True,
) -> dict[str, Any]:
    """
    Build an in-memory counterfactual timeline for one stream.

    Safety contract:
    - This function reads from `store` via `load_stream(...)`.
    - It never calls `append(...)`, never persists, and never mutates DB state.

    Args:
      store:
        Event store with async `load_stream(stream_id, from_version=...)`.
      stream_id:
        Target stream to simulate.
      counterfactual_events:
        Hypothetical event objects (StoredEvent-like or BaseEvent-like).
      from_version:
        Lower bound passed to store.load_stream.
      include_event_types:
        Optional allowlist for event types in final simulated timeline.
      require_causal_dependencies:
        If True, event with metadata.causation_id is included only when that
        causal event id exists in already-included timeline.
    """
    persisted = await store.load_stream(stream_id, from_version=from_version)
    base = [_to_sim_event(ev, i, "base") for i, ev in enumerate(persisted, start=1)]
    hypotheticals = [
        _to_sim_event(ev, i, "counterfactual")
        for i, ev in enumerate(counterfactual_events, start=1)
    ]

    timeline = sorted(base + hypotheticals, key=lambda e: (e.created_at, e.source != "base"))

    included: list[_SimEvent] = []
    filtered_out: list[dict[str, Any]] = []
    known_ids: set[str] = set()

    for ev in timeline:
        if include_event_types and ev.event_type not in include_event_types:
            filtered_out.append(
                {
                    "event_id": ev.event_id,
                    "event_type": ev.event_type,
                    "reason": "event_type_filtered",
                    "source": ev.source,
                }
            )
            continue

        if not _dependency_satisfied(ev, known_ids, require_causal_dependencies):
            filtered_out.append(
                {
                    "event_id": ev.event_id,
                    "event_type": ev.event_type,
                    "reason": "missing_causation_dependency",
                    "source": ev.source,
                    "causation_id": ev.metadata.get("causation_id"),
                }
            )
            continue

        included.append(ev)
        known_ids.add(ev.event_id)

    return {
        "stream_id": stream_id,
        "writes_performed": False,
        "base_event_count": len(base),
        "counterfactual_event_count": len(hypotheticals),
        "included_event_count": len(included),
        "filtered_event_count": len(filtered_out),
        "included_counts_by_type": _count_by_type(included),
        "filtered_out": filtered_out,
        "simulated_timeline": [
            {
                "event_id": ev.event_id,
                "event_type": ev.event_type,
                "created_at": ev.created_at.isoformat(),
                "source": ev.source,
                "payload": ev.payload,
                "metadata": ev.metadata,
            }
            for ev in included
        ],
    }

