"""
Regulatory examination package generator.

Creates a self-contained JSON-serializable structure with:
- full event timeline for a loan application and related streams
- projection-like states as-of examination date
- integrity verification summary for each stream
- human-readable narrative
- agent/model metadata extracted from events
"""
from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import asyncpg


def _to_utc(dt: datetime | None) -> datetime:
    if dt is None:
        return datetime.now(timezone.utc)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _to_dict_payload(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {"_raw": raw}
    return dict(raw)


def _to_dict_metadata(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {"_raw": raw}
    return dict(raw)


def _stream_from_app_event(event_type: str, payload: dict[str, Any]) -> list[str]:
    stream_ids: list[str] = []
    if event_type == "DocumentUploadRequested":
        doc_pkg = payload.get("document_package_id")
        if doc_pkg:
            stream_ids.append(f"docpkg-{doc_pkg}")
    if event_type == "CreditAnalysisRequested":
        credit_id = payload.get("credit_record_id")
        if credit_id:
            stream_ids.append(f"credit-{credit_id}")
    if event_type == "FraudScreeningRequested":
        fraud_id = payload.get("fraud_screening_id")
        if fraud_id:
            stream_ids.append(f"fraud-{fraud_id}")
    if event_type == "ComplianceCheckRequested":
        compliance_id = payload.get("compliance_record_id")
        if compliance_id:
            stream_ids.append(f"compliance-{compliance_id}")
    return stream_ids


async def _load_stream_rows(
    pool: asyncpg.Pool, stream_id: str, examination_date: datetime
) -> list[dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, stream_id, version, event_type, event_data, metadata, created_at
            FROM events
            WHERE stream_id = $1 AND created_at <= $2
            ORDER BY version ASC
            """,
            stream_id,
            examination_date,
        )
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "id": row["id"],
                "stream_id": row["stream_id"],
                "version": row["version"],
                "event_type": row["event_type"],
                "payload": _to_dict_payload(row["event_data"]),
                "metadata": _to_dict_metadata(row["metadata"]),
                "created_at": _to_utc(row["created_at"]),
                "_raw_event_data": row["event_data"],
            }
        )
    return out


def _compute_chain_hash(
    previous_hash: str,
    event_id: int,
    stream_id: str,
    version: int,
    event_type: str,
    event_data: Any,
    created_at_iso: str,
) -> str:
    canonical = json.dumps(
        {
            "previous_hash": previous_hash,
            "event_id": event_id,
            "stream_id": stream_id,
            "version": version,
            "event_type": event_type,
            "event_data": event_data if isinstance(event_data, str) else json.dumps(event_data, sort_keys=True),
            "created_at": created_at_iso,
        },
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def _verify_stream_integrity(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "events_verified": 0,
            "chain_valid": True,
            "tamper_detected": False,
            "first_tampered_event_id": None,
            "final_hash": "",
            "missing_chain_hash_count": 0,
        }

    previous_hash = "GENESIS"
    chain_valid = True
    first_tampered_event_id = None
    missing_hashes = 0

    for row in rows:
        expected = _compute_chain_hash(
            previous_hash=previous_hash,
            event_id=row["id"],
            stream_id=row["stream_id"],
            version=row["version"],
            event_type=row["event_type"],
            event_data=row["_raw_event_data"],
            created_at_iso=row["created_at"].isoformat(),
        )
        stored = row.get("metadata", {}).get("_chain_hash")
        if stored is None:
            missing_hashes += 1
        elif stored != expected:
            chain_valid = False
            if first_tampered_event_id is None:
                first_tampered_event_id = row["id"]
        previous_hash = expected

    return {
        "events_verified": len(rows),
        "chain_valid": chain_valid,
        "tamper_detected": not chain_valid,
        "first_tampered_event_id": first_tampered_event_id,
        "final_hash": previous_hash,
        "missing_chain_hash_count": missing_hashes,
    }


def _project_application_summary(loan_rows: list[dict[str, Any]], application_id: str) -> dict[str, Any]:
    state: dict[str, Any] = {
        "application_id": application_id,
        "status": "Unknown",
    }
    for row in loan_rows:
        event_type = row["event_type"]
        payload = row["payload"]
        if event_type == "ApplicationSubmitted":
            state.update(
                {
                    "status": "Submitted",
                    "requested_amount_usd": payload.get("requested_amount_usd"),
                    "submission_channel": payload.get("submission_channel"),
                }
            )
        elif event_type == "DecisionGenerated":
            state.update(
                {
                    "status": "PendingDecision",
                    "recommendation": payload.get("recommendation"),
                    "decision_confidence": payload.get("confidence_score"),
                }
            )
        elif event_type == "HumanReviewCompleted":
            state.update(
                {
                    "reviewer_id": payload.get("reviewer_id"),
                    "final_decision": payload.get("final_decision"),
                }
            )
        elif event_type == "ApplicationApproved":
            state.update(
                {
                    "status": "FinalApproved",
                    "approved_amount_usd": payload.get("approved_amount_usd"),
                    "interest_rate": payload.get("interest_rate"),
                }
            )
        elif event_type == "ApplicationDeclined":
            state.update({"status": "FinalDeclined"})
    return state


def _project_compliance_state(compliance_rows: list[dict[str, Any]]) -> dict[str, Any]:
    rules = {"passed": [], "failed": [], "noted": []}
    for row in compliance_rows:
        payload = row["payload"]
        if row["event_type"] == "ComplianceRulePassed":
            rules["passed"].append(payload.get("rule_id"))
        elif row["event_type"] == "ComplianceRuleFailed":
            rules["failed"].append(payload.get("rule_id"))
        elif row["event_type"] == "ComplianceRuleNoted":
            rules["noted"].append(payload.get("rule_id"))
    return {
        "rules_passed": [r for r in rules["passed"] if r],
        "rules_failed": [r for r in rules["failed"] if r],
        "rules_noted": [r for r in rules["noted"] if r],
        "hard_block_present": len(rules["failed"]) > 0,
    }


def _extract_agent_model_metadata(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_agent: dict[str, dict[str, Any]] = defaultdict(lambda: {"models": set(), "sessions": 0})
    for row in rows:
        payload = row["payload"]
        stream_id = row["stream_id"]
        if row["event_type"] == "AgentSessionCompleted":
            if stream_id.startswith("agent-"):
                parts = stream_id.split("-")
                if len(parts) >= 3:
                    agent_type = parts[1]
                    by_agent[agent_type]["sessions"] += 1
            model_version = payload.get("model_version")
            if model_version and stream_id.startswith("agent-"):
                agent_type = stream_id.split("-")[1]
                by_agent[agent_type]["models"].add(str(model_version))

        for key in ("model_version", "screening_model_version", "llm_model"):
            value = payload.get(key)
            if value and stream_id.startswith("agent-"):
                agent_type = stream_id.split("-")[1]
                by_agent[agent_type]["models"].add(str(value))

    return {
        agent: {
            "sessions": info["sessions"],
            "models": sorted(info["models"]),
        }
        for agent, info in by_agent.items()
    }


def _build_narrative(
    application_id: str,
    examination_date: datetime,
    streams: dict[str, list[dict[str, Any]]],
    summary_state: dict[str, Any],
    compliance_state: dict[str, Any],
) -> str:
    total_events = sum(len(v) for v in streams.values())
    stream_count = len(streams)
    status = summary_state.get("status", "Unknown")
    rec = summary_state.get("recommendation")
    final = summary_state.get("final_decision")
    failed_rules = compliance_state.get("rules_failed", [])
    parts = [
        f"Examination package for application {application_id} as of {examination_date.isoformat()}.",
        f"Captured {total_events} events across {stream_count} related streams.",
        f"Application state at exam time: {status}.",
    ]
    if rec:
        parts.append(f"Orchestrator recommendation: {rec}.")
    if final:
        parts.append(f"Human-reviewed final decision: {final}.")
    if failed_rules:
        parts.append(f"Compliance failed rules: {', '.join(failed_rules)}.")
    return " ".join(parts)


async def generate_regulatory_package(
    pool: asyncpg.Pool,
    application_id: str,
    *,
    examination_date: datetime | None = None,
) -> dict[str, Any]:
    """
    Generate a self-contained regulatory examination package.
    """
    exam_ts = _to_utc(examination_date)
    loan_stream_id = f"loan-{application_id}"

    loan_rows = await _load_stream_rows(pool, loan_stream_id, exam_ts)
    discovered = {loan_stream_id}
    for row in loan_rows:
        for sid in _stream_from_app_event(row["event_type"], row["payload"]):
            discovered.add(sid)

    all_stream_rows: dict[str, list[dict[str, Any]]] = {}
    for sid in sorted(discovered):
        all_stream_rows[sid] = await _load_stream_rows(pool, sid, exam_ts)

    all_rows_flat = [row for rows in all_stream_rows.values() for row in rows]
    all_rows_flat.sort(key=lambda r: (r["created_at"], r["id"]))

    integrity = {
        sid: _verify_stream_integrity(rows)
        for sid, rows in all_stream_rows.items()
    }
    projections_at_exam = {
        "application_summary": _project_application_summary(loan_rows, application_id),
        "compliance_audit": _project_compliance_state(
            [row for sid, rows in all_stream_rows.items() if sid.startswith("compliance-") for row in rows]
        ),
    }
    model_metadata = _extract_agent_model_metadata(all_rows_flat)
    narrative = _build_narrative(
        application_id=application_id,
        examination_date=exam_ts,
        streams=all_stream_rows,
        summary_state=projections_at_exam["application_summary"],
        compliance_state=projections_at_exam["compliance_audit"],
    )

    package = {
        "package_type": "regulatory_examination",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "examination_date": exam_ts.isoformat(),
        "application_id": application_id,
        "streams": {
            sid: [
                {
                    "id": row["id"],
                    "version": row["version"],
                    "event_type": row["event_type"],
                    "payload": row["payload"],
                    "metadata": row["metadata"],
                    "created_at": row["created_at"].isoformat(),
                }
                for row in rows
            ]
            for sid, rows in all_stream_rows.items()
        },
        "projection_states_at_examination": projections_at_exam,
        "integrity_verification": integrity,
        "agent_model_metadata": model_metadata,
        "narrative": narrative,
    }
    return package

