"""
NARR-05 Live Demo: Orchestrator recommends DECLINE, human officer overrides to APPROVE.
Run: uv run python scripts/demo_narr05.py
Must complete in under 90 seconds.
"""
import asyncio
import os
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

import asyncpg
from src.ledger.core.event_store import EventStore
from src.ledger.registry.client import ApplicantRegistryClient
from src.ledger.schema.events import (
    ApplicationSubmitted, DocumentUploadRequested,
    HumanReviewCompleted, ApplicationApproved,
)
from src.ledger.agents.stub_agents import DocumentProcessingAgent, FraudDetectionAgent, ComplianceAgent
from src.ledger.agents.credit_analysis_agent import CreditAnalysisAgent
from src.ledger.agents.stub_agents import DecisionOrchestratorAgent


async def main():
    t_start = time.time()
    print(f"\n{'='*60}")
    print("NARR-05: Human Override Demo")
    print(f"{'='*60}\n")

    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])

    # Clean previous NARR-05 run
    async with pool.acquire() as conn:
        await conn.execute("""
            DELETE FROM events WHERE stream_id LIKE '%NARR-05%'
              OR stream_id LIKE '%narr-05%'
              OR stream_id LIKE '%narr05%'
        """)
        await conn.execute("""
            DELETE FROM event_streams WHERE stream_id LIKE '%NARR-05%'
              OR stream_id LIKE '%narr-05%'
              OR stream_id LIKE '%narr05%'
        """)

    store = EventStore(pool)
    registry = ApplicantRegistryClient(pool)

    app_id = "NARR-05"
    loan_stream = f"loan-{app_id}"

    # Seed application
    print("📋 Seeding NARR-05 application (COMP-068, $950,000 loan request)...")
    await store.append(loan_stream, [
        ApplicationSubmitted(
            stream_id=loan_stream,
            payload={"company_id": "COMP-068", "requested_amount_usd": 950_000},
        ),
        DocumentUploadRequested(
            stream_id=loan_stream,
            payload={
                "document_package_id": "docpkg-NARR-05",
                "document_type": "income_statement",
                "file_path": "documents/COMP-068/income_statement_2024.pdf",
            },
        ),
    ], expected_version=0)

    # Run pipeline
    print("\n🤖 Running agent pipeline...")

    print("  1/4 DocumentProcessingAgent...", end=" ", flush=True)
    t = time.time()
    await DocumentProcessingAgent(store, registry, documents_dir="./documents").process(app_id)
    print(f"✅ ({time.time()-t:.1f}s)")

    print("  2/4 CreditAnalysisAgent...", end=" ", flush=True)
    t = time.time()
    await CreditAnalysisAgent(store, registry).process(app_id)
    print(f"✅ ({time.time()-t:.1f}s)")

    print("  3/4 FraudDetectionAgent...", end=" ", flush=True)
    t = time.time()
    await FraudDetectionAgent(store, registry).process(app_id)
    print(f"✅ ({time.time()-t:.1f}s)")

    print("  4/4 ComplianceAgent...", end=" ", flush=True)
    t = time.time()
    await ComplianceAgent(store, registry).process(app_id)
    print(f"✅ ({time.time()-t:.1f}s)")

    # Orchestrator recommends DECLINE
    print("\n⚖️  DecisionOrchestratorAgent recommending...", end=" ", flush=True)
    t = time.time()
    await DecisionOrchestratorAgent(store, registry).process(app_id)
    print(f"✅ ({time.time()-t:.1f}s)")

    # Check orchestrator decision
    loan_events = await store.load_stream(loan_stream)
    decision_event = next((e for e in loan_events if e.event_type == "DecisionGenerated"), None)
    if decision_event:
        rec = decision_event.payload.get("recommendation", "UNKNOWN")
        print(f"\n📊 Orchestrator recommendation: {rec}")

    # Human override: APPROVE
    print("\n👤 Human loan officer overrides to APPROVE...")
    loan_version = await store.stream_version(loan_stream)
    await store.append(loan_stream, [
        HumanReviewCompleted(
            stream_id=loan_stream,
            payload={
                "reviewer_id": "officer-jane-smith",
                "decision": "APPROVE",
                "override_reason": "Strong 3-year growth trajectory justifies exception approval",
                "conditions": ["Personal guarantee required", "Quarterly financial reporting"],
            },
        ),
        ApplicationApproved(
            stream_id=loan_stream,
            payload={
                "approved_amount_usd": 750_000,
                "conditions": ["Personal guarantee required", "Quarterly financial reporting"],
                "approved_by": "officer-jane-smith",
                "adverse_action_notice_required": False,
            },
        ),
    ], expected_version=loan_version)
    print("✅ Human override written to event store")

    # Final state
    loan_events = await store.load_stream(loan_stream)
    approved = next((e for e in loan_events if e.event_type == "ApplicationApproved"), None)

    elapsed = time.time() - t_start
    print(f"\n{'='*60}")
    print("NARR-05 COMPLETE")
    print(f"{'='*60}")
    print(f"  Total events in loan stream: {len(loan_events)}")
    if approved:
        print(f"  Approved amount: ${approved.payload.get('approved_amount_usd'):,.0f}")
        print(f"  Conditions: {approved.payload.get('conditions')}")
        print(f"  Approved by: {approved.payload.get('approved_by')}")
    print(f"  Total time: {elapsed:.1f}s")
    print(f"  Status: {'✅ UNDER 90s' if elapsed < 90 else '⚠️ OVER 90s'}")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())