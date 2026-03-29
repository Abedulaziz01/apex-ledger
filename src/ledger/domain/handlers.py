"""
Command handlers using load -> validate -> determine -> append pattern.
"""
from __future__ import annotations

from typing import Any

from src.ledger.core.models import BaseEvent
from src.ledger.domain.loan_application import LoanApplicationAggregate
from src.ledger.domain.agent_session import AgentSessionAggregate
from src.ledger.domain.compliance_record import ComplianceRecordAggregate


async def handle_submit_application(store: Any, command: dict) -> int:
    # load
    agg = await LoanApplicationAggregate.load(store, command["application_id"])
    # validate
    if agg.status is not None:
        raise ValueError("Application already exists")
    # determine
    event = BaseEvent(
        event_type="ApplicationSubmitted",
        event_data=dict(command),
    )
    # append
    written = await store.append(
        f"loan-{command['application_id']}",
        [event],
        expected_version=0,
    )
    return written[-1].stream_position


async def handle_record_credit_analysis(store: Any, command: dict) -> int:
    agg = await LoanApplicationAggregate.load(store, command["application_id"])
    agg.assert_awaiting_analysis()
    agg.assert_no_credit_analysis()
    event = BaseEvent(event_type="CreditAnalysisCompleted", event_data=dict(command))
    written = await store.append(
        f"loan-{command['application_id']}",
        [event],
        expected_version=command["expected_version"],
    )
    return written[-1].stream_position


async def handle_generate_decision(store: Any, command: dict, sessions_that_processed: set[str]) -> int:
    agg = await LoanApplicationAggregate.load(store, command["application_id"])
    agg.assert_analysis_complete()
    recommendation = LoanApplicationAggregate.enforce_confidence_floor(
        command["confidence_score"],
        command["recommendation"],
    )
    agg.assert_causal_chain(command.get("contributing_agent_sessions", []), sessions_that_processed)
    payload = dict(command)
    payload["recommendation"] = recommendation
    event = BaseEvent(event_type="DecisionGenerated", event_data=payload)
    written = await store.append(
        f"loan-{command['application_id']}",
        [event],
        expected_version=command["expected_version"],
    )
    return written[-1].stream_position


async def handle_start_agent_session(store: Any, command: dict) -> int:
    agg = await AgentSessionAggregate.load(store, command["agent_id"], command["session_id"])
    if agg.version > 0:
        raise ValueError("Session already exists")
    event = BaseEvent(event_type="AgentContextLoaded", event_data=dict(command))
    written = await store.append(
        f"agent-{command['agent_id']}-{command['session_id']}",
        [event],
        expected_version=0,
    )
    return written[-1].stream_position


async def handle_record_compliance_rule(store: Any, command: dict) -> int:
    agg = await ComplianceRecordAggregate.load(store, command["application_id"])
    if command["event_type"] in ("ComplianceRulePassed", "ComplianceRuleFailed"):
        agg.assert_check_not_already_evaluated(command["rule_id"])
    event = BaseEvent(event_type=command["event_type"], event_data=dict(command["event_data"]))
    written = await store.append(
        f"compliance-{command['application_id']}",
        [event],
        expected_version=command["expected_version"],
    )
    return written[-1].stream_position

