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
from src.ledger.projections.daemon import ProjectionDaemon
from src.ledger.integrity.audit_chain import run_integrity_check
from src.ledger.memory.agent_context import recover_agent_context, write_recovery_event

load_dotenv()

mcp = FastMCP("apex-ledger")

# ── shared state ──────────────────────────────────────────────────────────────
_pool: asyncpg.Pool = None
_store: EventStore = None
_daemon_task: asyncio.Task = None


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


# ── COMMANDS (MCP Tools = writes) ─────────────────────────────────────────────

@mcp.tool()
async def append_events(
    stream_id: str,
    event_type: str,
    event_data: dict,
    expected_version: int,
) -> dict:
    """Append a single event to a stream with optimistic concurrency control."""
    store = await _get_store()
    try:
        new_version = await store.append(
            stream_id=stream_id,
            events=[BaseEvent(event_type=event_type, event_data=event_data)],
            expected_version=expected_version,
        )
        return {"ok": True, "new_version": new_version}
    except OptimisticConcurrencyError as e:
        return {"ok": False, "error": "concurrency_conflict", "detail": str(e)}
    except Exception as e:
        return {"ok": False, "error": "append_failed", "detail": str(e)}


@mcp.tool()
async def submit_application(
    application_id: str,
    applicant_id: str,
    requested_amount_usd: float,
    loan_purpose: str,
    submission_channel: str,
    submitted_at: str,
) -> dict:
    """Submit a new loan application — writes ApplicationSubmitted event."""
    store = await _get_store()
    try:
        version = await store.append(
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
        return {"ok": True, "stream_id": f"loan-{application_id}", "version": version}
    except OptimisticConcurrencyError:
        return {"ok": False, "error": "application_already_exists"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


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
    """Record a completed credit analysis — writes CreditAnalysisCompleted event."""
    store = await _get_store()
    try:
        version = await store.append(
            stream_id=f"loan-{application_id}",
            events=[BaseEvent(
                event_type="CreditAnalysisCompleted",
                event_data={
                    "application_id":       application_id,
                    "agent_id":             agent_id,
                    "session_id":           session_id,
                    "model_version":        model_version,
                    "confidence_score":     confidence_score,
                    "risk_tier":            risk_tier,
                    "recommended_limit_usd": recommended_limit_usd,
                    "analysis_duration_ms": analysis_duration_ms,
                    "input_data_hash":      input_data_hash,
                },
            )],
            expected_version=expected_version,
        )
        return {"ok": True, "version": version}
    except OptimisticConcurrencyError as e:
        return {"ok": False, "error": "concurrency_conflict", "detail": str(e)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@mcp.tool()
async def recover_agent_session(
    agent_id: str,
    crashed_session_id: str,
    new_session_id: str,
) -> dict:
    """Recover a crashed agent session — replays history and writes AgentSessionRecovered."""
    store = await _get_store()
    try:
        ctx = await recover_agent_context(store, agent_id, crashed_session_id)
        await write_recovery_event(store, agent_id, new_session_id, crashed_session_id, ctx)
        return {
            "ok": True,
            "session_health_status": ctx.session_health_status,
            "last_event_position":   ctx.last_event_position,
            "pending_work":          ctx.pending_work,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── QUERIES (MCP Resources = reads) ──────────────────────────────────────────

@mcp.resource("ledger://application/{application_id}/summary")
async def get_application_summary(application_id: str) -> dict:
    """Current state of a loan application from the projection."""
    pool = await _get_pool()
    proj = ApplicationSummaryProjection(pool)
    row = await proj.get(application_id)
    if not row:
        return {"error": "not_found", "application_id": application_id}
    return row


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


@mcp.resource("ledger://application/{application_id}/integrity")
async def get_integrity_report(application_id: str) -> dict:
    """SHA-256 hash chain integrity check for a loan application stream."""
    pool = await _get_pool()
    report = await run_integrity_check(pool, f"loan-{application_id}")
    return {
        "stream_id":                report.stream_id,
        "events_verified":          report.events_verified,
        "chain_valid":              report.chain_valid,
        "tamper_detected":          report.tamper_detected,
        "first_tampered_event_id":  report.first_tampered_event_id,
        "final_hash":               report.final_hash,
    }


@mcp.resource("ledger://agent/{agent_id}/performance")
async def get_agent_performance(agent_id: str) -> dict:
    """Performance metrics for an AI agent across all model versions."""
    pool = await _get_pool()
    proj = AgentPerformanceProjection(pool)
    rows = await proj.get(agent_id)
    return {"agent_id": agent_id, "metrics": rows}


@mcp.resource("ledger://compliance/{application_id}/audit")
async def get_compliance_audit(application_id: str) -> dict:
    """Full compliance audit trail for an application."""
    pool = await _get_pool()
    proj = ComplianceAuditProjection(pool)
    rows = await proj.get_for_application(application_id)
    return {"application_id": application_id, "records": rows}


@mcp.resource("ledger://projection/lag")
async def get_projection_lag() -> dict:
    """Current lag in milliseconds for all three projections."""
    pool = await _get_pool()
    return {
        "application_summary_ms": await ApplicationSummaryProjection(pool).get_lag(),
        "agent_performance_ms":   await AgentPerformanceProjection(pool).get_lag(),
        "compliance_audit_ms":    await ComplianceAuditProjection(pool).get_lag(),
    }


# ── startup / entry point ─────────────────────────────────────────────────────
if __name__ == "__main__":
    print("FastMCP server running on port 8765")
    mcp.run(transport="sse")