"""
NARR-05 — Human Override Demo
A loan officer overrides an AI DECLINE recommendation to APPROVE.
Must complete in under 90 seconds.
Writes: artifacts/regulatory_package_NARR05.json
"""
import asyncio
import asyncpg
import json
import os
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
from src.ledger.core.event_store import EventStore
from src.ledger.core.models import BaseEvent
from src.ledger.projections.daemon import ProjectionDaemon
from src.ledger.integrity.audit_chain import run_integrity_check

load_dotenv()

APP_ID = f"NARR05-{int(time.time())}"


async def main():
    start = time.time()
    pool  = await asyncpg.create_pool(os.getenv("DATABASE_URL"), min_size=2, max_size=10)
    store = EventStore(pool)
    daemon = ProjectionDaemon(pool)

    print(f"NARR-05: Human Override Demo — app={APP_ID}")

    # 1. submit
    await store.append(f"loan-{APP_ID}", [BaseEvent(
        event_type="ApplicationSubmitted",
        event_data={
            "application_id": APP_ID, "applicant_id": "cust-narr05",
            "requested_amount_usd": 95000, "loan_purpose": "acquisition",
            "submission_channel": "branch", "submitted_at": _now(),
        },
    )], expected_version=0)

    # 2. credit analysis — borderline
    await store.append(f"loan-{APP_ID}", [BaseEvent(
        event_type="CreditAnalysisCompleted",
        event_data={
            "application_id": APP_ID, "agent_id": "credit-narr05",
            "session_id": "sess-narr05", "model_version": "gpt-4",
            "confidence_score": 0.62, "risk_tier": "HIGH",
            "recommended_limit_usd": 50000, "analysis_duration_ms": 1100,
            "input_data_hash": "hash-narr05-credit",
        },
    )], expected_version=1)

    # 3. AI generates DECLINE
    await store.append(f"loan-{APP_ID}", [BaseEvent(
        event_type="DecisionGenerated",
        event_data={
            "application_id": APP_ID,
            "orchestrator_agent_id": "orch-narr05",
            "recommendation": "DECLINE", "confidence_score": 0.62,
            "contributing_agent_sessions": ["agent-credit-narr05-sess-narr05"],
            "decision_basis_summary": "high risk tier, limit below requested",
            "model_versions": {"credit": "gpt-4"},
        },
    )], expected_version=2)

    # 4. loan officer overrides to APPROVE
    await store.append(f"loan-{APP_ID}", [BaseEvent(
        event_type="HumanReviewCompleted",
        event_data={
            "application_id": APP_ID, "reviewer_id": "officer-smith",
            "override": True, "final_decision": "APPROVE",
            "override_reason": "Long-standing client, collateral verified offline",
        },
    )], expected_version=3)

    # 5. approved
    await store.append(f"loan-{APP_ID}", [BaseEvent(
        event_type="ApplicationApproved",
        event_data={
            "application_id": APP_ID,
            "approved_amount_usd": 95000,
            "interest_rate": 0.067,
            "conditions": ["collateral_deed_required"],
            "approved_by": "officer-smith",
            "effective_date": "2026-02-01",
        },
    )], expected_version=4)

    # 6. run projections
    while await daemon._process_batch():
        pass

    # 7. integrity check
    report = await run_integrity_check(pool, f"loan-{APP_ID}")

    # 8. load full history
    events = await store.load_stream(f"loan-{APP_ID}")

    elapsed = time.time() - start

    # assertions
    assert report.chain_valid,        "FAIL: chain integrity violated"
    assert not report.tamper_detected, "FAIL: tamper detected"
    assert len(events) == 5,           f"FAIL: expected 5 events, got {len(events)}"
    override_evt = next(e for e in events if e.event_type == "HumanReviewCompleted")
    assert override_evt.event_data["override"] is True, "FAIL: override not recorded"
    assert elapsed < 90,               f"FAIL: took {elapsed:.1f}s — over 90s limit"

    print(f"All assertions passed in {elapsed:.2f}s")

    # 9. write regulatory package
    os.makedirs("artifacts", exist_ok=True)
    package = {
        "narrative":      "NARR-05",
        "description":    "Human override: officer approves AI-declined application",
        "application_id": APP_ID,
        "run_at":         datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(elapsed, 2),
        "events": [
            {
                "version":    e.version,
                "event_type": e.event_type,
                "event_data": e.event_data,
                "created_at": e.created_at.isoformat(),
            }
            for e in events
        ],
        "integrity": {
            "chain_valid":     report.chain_valid,
            "tamper_detected": report.tamper_detected,
            "events_verified": report.events_verified,
            "final_hash":      report.final_hash,
        },
        "human_override": {
            "reviewer_id":    override_evt.event_data["reviewer_id"],
            "override_reason": override_evt.event_data["override_reason"],
            "final_decision": override_evt.event_data["final_decision"],
        },
        "compliance_met": True,
        "slo_met":        elapsed < 90,
    }

    out = "artifacts/regulatory_package_NARR05.json"
    with open(out, "w") as f:
        json.dump(package, f, indent=2)

    print(f"Regulatory package written: {out}")

    # write narrative test results
    with open("artifacts/narrative_test_results.txt", "w") as f:
        f.write("Narrative Test Results\n")
        f.write("="*50 + "\n")
        f.write(f"NARR-05: Human Override\n")
        f.write(f"  Status:  PASSED\n")
        f.write(f"  Elapsed: {elapsed:.2f}s (limit: 90s)\n")
        f.write(f"  Events:  {len(events)}\n")
        f.write(f"  Override verified: True\n")
        f.write(f"  Chain valid: {report.chain_valid}\n")

    print("artifact: narrative_test_results.txt")
    await pool.close()


def _now():
    return datetime.now(timezone.utc).isoformat()


if __name__ == "__main__":
    asyncio.run(main())