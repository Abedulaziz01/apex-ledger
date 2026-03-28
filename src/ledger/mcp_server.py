import asyncio
import asyncpg
import os
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from src.ledger.core.event_store import EventStore
from src.ledger.core.models import BaseEvent
from src.ledger.core.exceptions import OptimisticConcurrencyError
from src.ledger.projections.application_summary import ApplicationSummaryProjection
from src.ledger.projections.agent_performance import AgentPerformanceProjection
from src.ledger.projections.compliance_audit import ComplianceAuditProjection
from src.ledger.integrity.audit_chain import run_integrity_check as _run_integrity_check
from src.ledger.memory.agent_context import recover_agent_context, write_recovery_event

load_dotenv()

mcp = FastMCP("apex-ledger")

_pool: asyncpg.Pool = None
_store: EventStore = None


async def _get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            os.getenv("DATABASE_URL"), min_size=2, max_size=10
        )
    return _pool


async def _get_store() -> EventStore:
    global _store
    if _store is None:
        pool = await _get_pool()
        _store = EventStore(pool)
    return _store


def _concurrency_error(detail: str) -> dict:
    return {
        "error_type":       "OptimisticConcurrencyError",
        "detail":           detail,
        "suggested_action": "reload_stream_and_retry",
    }


def _generic_error(error_type: str, detail: str) -> dict:
    return {
        "error_type":       error_type,
        "detail":           detail,
        "suggested_action": "inspect_detail_and_fix",
    }


async def _append_with_deadlock_retry(store: EventStore, stream_id: str, events: list, expected_version: int, retries: int = 3):
    """Retry append on transient DB deadlocks."""
    for attempt in range(retries):
        try:
            return await store.append(stream_id=stream_id, events=events, expected_version=expected_version)
        except asyncpg.exceptions.DeadlockDetectedError:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(0.05 * (2 ** attempt))

def _final_version(append_result) -> int:
    """Normalize EventStore.append return shape (list[StoredEvent] or int)."""
    if isinstance(append_result, list):
        if not append_result:
            return 0
        last = append_result[-1]
        return getattr(last, "stream_position", getattr(last, "version", 0))
    return int(append_result)


@mcp.tool()
async def submit_application(
    application_id: str,
    applicant_id: str,
    requested_amount_usd: float,
    loan_purpose: str,
    submission_channel: str,
    submitted_at: str,
) -> dict:
    """Start a new loan application."""
    store = await _get_store()
    try:
        written = await store.append(
            stream_id=f"loan-{application_id}",
            events=[BaseEvent(
                event_type="ApplicationSubmitted",
                event_data={
                    "application_id":       application_id,
                    "applicant_id":         applicant_id,
                    "requested_amount_usd": requested_amount_usd,
                    "loan_purpose":         loan_purpose,
                    "submission_channel":   submission_channel,
                    "submitted_at":         submitted_at,
                },
            )],
            expected_version=0,
        )
        version = _final_version(written)
        return {"ok": True, "stream_id": f"loan-{application_id}", "version": version}
    except OptimisticConcurrencyError as e:
        return _concurrency_error(str(e))
    except Exception as e:
        return _generic_error("AppendFailed", str(e))


@mcp.tool()
async def record_credit_analysis(
    application_id: str,
    agent_id: str,
    session_id: str,
    model_version: str,
    confidence_score: float,
    risk_tier: str,
    recommended_limit_usd: float,
    analysis_duration_ms: int,
    input_data_hash: str,
    expected_version: int,
) -> dict:
    """Save what the credit agent found."""
    store = await _get_store()
    try:
        written = await store.append(
            stream_id=f"loan-{application_id}",
            events=[BaseEvent(
                event_type="CreditAnalysisCompleted",
                event_data={
                    "application_id":        application_id,
                    "agent_id":              agent_id,
                    "session_id":            session_id,
                    "model_version":         model_version,
                    "confidence_score":      confidence_score,
                    "risk_tier":             risk_tier,
                    "recommended_limit_usd": recommended_limit_usd,
                    "analysis_duration_ms":  analysis_duration_ms,
                    "input_data_hash":       input_data_hash,
                },
            )],
            expected_version=expected_version,
        )
        version = _final_version(written)
        return {"ok": True, "version": version}
    except OptimisticConcurrencyError as e:
        return _concurrency_error(str(e))
    except Exception as e:
        return _generic_error("AppendFailed", str(e))


@mcp.tool()
async def record_fraud_screening(
    application_id: str,
    agent_id: str,
    fraud_score: float,
    anomaly_flags: list,
    screening_model_version: str,
    input_data_hash: str,
    expected_version: int,
) -> dict:
    """Save what the fraud agent found."""
    store = await _get_store()
    try:
        written = await store.append(
            stream_id=f"loan-{application_id}",
            events=[BaseEvent(
                event_type="FraudScreeningCompleted",
                event_data={
                    "application_id":          application_id,
                    "agent_id":                agent_id,
                    "fraud_score":             fraud_score,
                    "anomaly_flags":           anomaly_flags,
                    "screening_model_version": screening_model_version,
                    "input_data_hash":         input_data_hash,
                },
            )],
            expected_version=expected_version,
        )
        version = _final_version(written)
        return {"ok": True, "version": version}
    except OptimisticConcurrencyError as e:
        return _concurrency_error(str(e))
    except Exception as e:
        return _generic_error("AppendFailed", str(e))


@mcp.tool()
async def record_compliance_check(
    application_id: str,
    rule_id: str,
    rule_version: str,
    passed: bool,
    evidence_hash: str,
    evaluation_timestamp: str,
    failure_reason: str,
    expected_version: int,
) -> dict:
    """Save one compliance rule result."""
    store = await _get_store()
    event_type = "ComplianceRulePassed" if passed else "ComplianceRuleFailed"
    event_data: dict = {
        "application_id":       application_id,
        "rule_id":              rule_id,
        "rule_version":         rule_version,
        "evaluation_timestamp": evaluation_timestamp,
    }
    if passed:
        event_data["evidence_hash"] = evidence_hash
    else:
        event_data["failure_reason"]       = failure_reason
        event_data["remediation_required"] = True
    try:
        written = await _append_with_deadlock_retry(
            store=store,
            stream_id=f"compliance-{application_id}",
            events=[BaseEvent(event_type=event_type, event_data=event_data)],
            expected_version=expected_version,
        )
        version = _final_version(written)
        return {"ok": True, "version": version, "result": "passed" if passed else "failed"}
    except OptimisticConcurrencyError as e:
        return _concurrency_error(str(e))
    except Exception as e:
        return _generic_error("AppendFailed", str(e))


@mcp.tool()
async def generate_decision(
    application_id: str,
    orchestrator_agent_id: str,
    recommendation: str,
    confidence_score: float,
    contributing_agent_sessions: list,
    decision_basis_summary: str,
    model_versions: dict,
    expected_version: int,
) -> dict:
    """Save the AI final recommendation."""
    if confidence_score < 0.6 and recommendation != "REFER":
        recommendation = "REFER"
    store = await _get_store()
    try:
        written = await store.append(
            stream_id=f"loan-{application_id}",
            events=[BaseEvent(
                event_type="DecisionGenerated",
                event_data={
                    "application_id":             application_id,
                    "orchestrator_agent_id":       orchestrator_agent_id,
                    "recommendation":              recommendation,
                    "confidence_score":            confidence_score,
                    "contributing_agent_sessions": contributing_agent_sessions,
                    "decision_basis_summary":      decision_basis_summary,
                    "model_versions":              model_versions,
                },
            )],
            expected_version=expected_version,
        )
        version = _final_version(written)
        return {"ok": True, "version": version, "recommendation": recommendation}
    except OptimisticConcurrencyError as e:
        return _concurrency_error(str(e))
    except Exception as e:
        return _generic_error("AppendFailed", str(e))


@mcp.tool()
async def record_human_review(
    application_id: str,
    reviewer_id: str,
    override: bool,
    final_decision: str,
    override_reason: str,
    expected_version: int,
) -> dict:
    """Save what the loan officer decided."""
    store = await _get_store()
    try:
        written = await store.append(
            stream_id=f"loan-{application_id}",
            events=[BaseEvent(
                event_type="HumanReviewCompleted",
                event_data={
                    "application_id":  application_id,
                    "reviewer_id":     reviewer_id,
                    "override":        override,
                    "final_decision":  final_decision,
                    "override_reason": override_reason,
                },
            )],
            expected_version=expected_version,
        )
        version = _final_version(written)
        return {"ok": True, "version": version}
    except OptimisticConcurrencyError as e:
        return _concurrency_error(str(e))
    except Exception as e:
        return _generic_error("AppendFailed", str(e))


@mcp.tool()
async def start_agent_session(
    agent_id: str,
    session_id: str,
    context_source: str,
    event_replay_from_position: int,
    context_token_count: int,
    model_version: str,
) -> dict:
    """Open a new agent work session."""
    store = await _get_store()
    try:
        written = await store.append(
            stream_id=f"agent-{agent_id}-{session_id}",
            events=[BaseEvent(
                event_type="AgentContextLoaded",
                event_data={
                    "agent_id":                   agent_id,
                    "session_id":                 session_id,
                    "context_source":             context_source,
                    "event_replay_from_position": event_replay_from_position,
                    "context_token_count":        context_token_count,
                    "model_version":              model_version,
                },
            )],
            expected_version=0,
        )
        version = _final_version(written)
        return {"ok": True, "stream_id": f"agent-{agent_id}-{session_id}", "version": version}
    except OptimisticConcurrencyError as e:
        return _concurrency_error(str(e))
    except Exception as e:
        return _generic_error("AppendFailed", str(e))


@mcp.tool()
async def run_integrity_check(application_id: str) -> dict:
    """Run tamper-detection hash chain check."""
    pool = await _get_pool()
    try:
        report = await _run_integrity_check(pool, f"loan-{application_id}")
        return {
            "ok":                      True,
            "stream_id":               report.stream_id,
            "events_verified":         report.events_verified,
            "chain_valid":             report.chain_valid,
            "tamper_detected":         report.tamper_detected,
            "first_tampered_event_id": report.first_tampered_event_id,
            "final_hash":              report.final_hash,
        }
    except Exception as e:
        return _generic_error("IntegrityCheckFailed", str(e))


@mcp.tool()
async def recover_agent_session(
    agent_id: str,
    crashed_session_id: str,
    new_session_id: str,
) -> dict:
    """Recover a crashed agent session."""
    store = await _get_store()
    try:
        ctx = await recover_agent_context(store, agent_id, crashed_session_id)
        await write_recovery_event(store, agent_id, new_session_id, crashed_session_id, ctx)
        return {
            "ok":                    True,
            "session_health_status": ctx.session_health_status,
            "last_event_position":   ctx.last_event_position,
            "pending_work":          ctx.pending_work,
        }
    except Exception as e:
        return _generic_error("RecoveryFailed", str(e))


@mcp.resource("ledger://application/{application_id}/summary")
async def get_application_summary(application_id: str) -> dict:
    """Current state of a loan application."""
    pool = await _get_pool()
    row = await ApplicationSummaryProjection(pool).get(application_id)
    return row or {"error_type": "NotFound", "application_id": application_id}


@mcp.resource("ledger://application/{application_id}/history")
async def get_application_history(application_id: str) -> dict:
    """Full raw event history for a loan application."""
    store = await _get_store()
    events = await store.load_stream(f"loan-{application_id}")
    return {
        "application_id": application_id,
        "event_count":    len(events),
        "events": [
            {
                "id":         e.id,
                "version":    e.version,
                "event_type": e.event_type,
                "event_data": e.event_data,
                "created_at": e.created_at.isoformat(),
            }
            for e in events
        ],
    }


@mcp.resource("ledger://agent/{agent_id}/performance")
async def get_agent_performance(agent_id: str) -> dict:
    """Performance metrics for an AI agent."""
    pool = await _get_pool()
    rows = await AgentPerformanceProjection(pool).get(agent_id)
    return {"agent_id": agent_id, "metrics": rows}


@mcp.resource("ledger://compliance/{application_id}/audit")
async def get_compliance_audit(application_id: str) -> dict:
    """Full compliance audit trail."""
    pool = await _get_pool()
    rows = await ComplianceAuditProjection(pool).get_for_application(application_id)
    return {"application_id": application_id, "records": rows}


@mcp.resource("ledger://projection/lag")
async def get_projection_lag() -> dict:
    """Current lag in milliseconds for all projections."""
    pool = await _get_pool()
    return {
        "application_summary_ms": await ApplicationSummaryProjection(pool).get_lag(),
        "agent_performance_ms":   await AgentPerformanceProjection(pool).get_lag(),
        "compliance_audit_ms":    await ComplianceAuditProjection(pool).get_lag(),
    }




@mcp.resource('ledger://applications/{application_id}')
async def get_application_status(application_id: str) -> dict:
    pool = await _get_pool()
    row = await ApplicationSummaryProjection(pool).get(application_id)
    return row or {'error_type': 'NotFound', 'application_id': application_id}


@mcp.resource('ledger://applications/{application_id}/compliance')
async def get_application_compliance(application_id: str) -> dict:
    pool = await _get_pool()
    rows = await ComplianceAuditProjection(pool).get_for_application(application_id)
    return {'application_id': application_id, 'records': rows}


@mcp.resource('ledger://applications/{application_id}/audit-trail')
async def get_audit_trail(application_id: str) -> dict:
    store = await _get_store()
    events = await store.load_stream(f'loan-{application_id}')
    return {
        'application_id': application_id,
        'event_count': len(events),
        'events': [
            {'id': e.id, 'version': e.version, 'event_type': e.event_type,
             'event_data': e.event_data, 'created_at': e.created_at.isoformat()}
            for e in events
        ],
    }


@mcp.resource('ledger://agents/{agent_id}/sessions/{session_id}')
async def get_agent_session(agent_id: str, session_id: str) -> dict:
    store = await _get_store()
    events = await store.load_stream(f'agent-{agent_id}-{session_id}')
    return {
        'agent_id': agent_id,
        'session_id': session_id,
        'event_count': len(events),
        'events': [
            {'id': e.id, 'version': e.version, 'event_type': e.event_type,
             'event_data': e.event_data, 'created_at': e.created_at.isoformat()}
            for e in events
        ],
    }


@mcp.resource('ledger://ledger/health')
async def get_ledger_health() -> dict:
    pool = await _get_pool()
    return {
        'application_summary_lag_ms': await ApplicationSummaryProjection(pool).get_lag(),
        'agent_performance_lag_ms': await AgentPerformanceProjection(pool).get_lag(),
        'compliance_audit_lag_ms': await ComplianceAuditProjection(pool).get_lag(),
        'status': 'healthy',
    }
if __name__ == "__main__":
    print("FastMCP server running on port 8765")
    mcp.run(transport="sse", port=8765)
