import asyncpg
from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from src.ledger.core.event_store import EventStore

# Events that indicate an action was started but may not be finished
UNFINISHED_INDICATORS = {
    "CreditAnalysisRequested",
    "ComplianceCheckRequested",
    "FraudScreeningRequested",
}

# Events that indicate an action completed cleanly
COMPLETED_INDICATORS = {
    "CreditAnalysisCompleted",
    "FraudScreeningCompleted",
    "ComplianceRulePassed",
    "ComplianceRuleFailed",
    "DecisionGenerated",
    "ApplicationApproved",
    "ApplicationDeclined",
}


@dataclass
class AgentContext:
    context_text: str                        # summary of prior session history
    last_event_position: int                 # stream version of last event seen
    pending_work: list = field(default_factory=list)   # unfinished items
    session_health_status: str = "OK"        # "OK" or "NEEDS_RECONCILIATION"


async def recover_agent_context(
    store: "EventStore",
    agent_id: str,
    session_id: str,
) -> AgentContext:
    """
    Replay the agent's session stream from the event store.
    Summarise old events, preserve last 3 verbatim, detect unfinished work.
    """
    events = await store.load_stream(f"agent-{agent_id}-{session_id}")

    if not events:
        return AgentContext(
            context_text="No prior session history found.",
            last_event_position=0,
            pending_work=[],
            session_health_status="OK",
        )

    last_position = events[-1].version

    # ── keep last 3 verbatim, summarise the rest ─────────────────────────────
    verbatim_events = events[-3:]
    summary_events = events[:-3] if len(events) > 3 else []

    summary_lines = []
    if summary_events:
        summary_lines.append(
            f"Prior session summary ({len(summary_events)} earlier event(s)):"
        )
        for evt in summary_events:
            summary_lines.append(f"  - [{evt.version}] {evt.event_type}")

    summary_lines.append(f"\nLast {len(verbatim_events)} event(s) (verbatim):")
    for evt in verbatim_events:
        summary_lines.append(
            f"  - [{evt.version}] {evt.event_type}: {evt.event_data}"
        )

    context_text = "\n".join(summary_lines)

    # ── detect unfinished work ────────────────────────────────────────────────
    started: set = set()
    finished: set = set()

    for evt in events:
        et = evt.event_type
        app_id = evt.event_data.get("application_id", "unknown")
        key = f"{et}:{app_id}"

        if et in UNFINISHED_INDICATORS:
            started.add(key)
        elif et in COMPLETED_INDICATORS:
            # mark the corresponding start as finished
            for start_key in list(started):
                if start_key.endswith(f":{app_id}"):
                    finished.add(start_key)

    pending_work = [k for k in started if k not in finished]

    # ── determine health ──────────────────────────────────────────────────────
    health = "NEEDS_RECONCILIATION" if pending_work else "OK"

    return AgentContext(
        context_text=context_text,
        last_event_position=last_position,
        pending_work=pending_work,
        session_health_status=health,
    )


async def write_recovery_event(
    store: "EventStore",
    agent_id: str,
    new_session_id: str,
    crashed_session_id: str,
    context: AgentContext,
) -> None:
    """
    Write AgentSessionRecovered as the very first event of the new session.
    context_source format: prior_session_replay:{crashed_session_id}
    """
    from src.ledger.core.models import BaseEvent

    recovery_event = BaseEvent(
        event_type="AgentSessionRecovered",
        event_data={
            "agent_id":              agent_id,
            "session_id":            new_session_id,
            "crashed_session_id":    crashed_session_id,
            "context_source":        f"prior_session_replay:{crashed_session_id}",
            "last_event_position":   context.last_event_position,
            "pending_work":          context.pending_work,
            "session_health_status": context.session_health_status,
            "context_summary":       context.context_text,
        },
    )

    await store.append(
        f"agent-{agent_id}-{new_session_id}",
        [recovery_event],
        expected_version=0,
    )