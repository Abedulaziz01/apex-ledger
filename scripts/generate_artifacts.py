"""
Generate all required artifacts for TRP1 Week 5 submission.
Run: uv run python scripts/generate_artifacts.py
"""
import asyncio
import json
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

import asyncpg


async def generate_occ_report():
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT stream_id, event_type, version, created_at
            FROM events
            WHERE event_type IN ('CreditAnalysisCompleted', 'AgentSessionFailed', 'AgentSessionRecovered')
            ORDER BY created_at DESC
            LIMIT 50
        """)
    await pool.close()

    lines = ["OCC Collision Report", "=" * 40, f"Generated: {datetime.now(timezone.utc).isoformat()}", ""]
    lines.append(f"Total relevant events scanned: {len(rows)}")
    lines.append("")
    lines.append("Stream ID | Event Type | Version | Created At")
    lines.append("-" * 80)
    for r in rows:
        lines.append(f"{r['stream_id'][:40]} | {r['event_type']} | {r['version']} | {r['created_at']}")

    with open("artifacts/occ_collision_report.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print("✅ occ_collision_report.txt")


async def generate_projection_lag_report():
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])
    async with pool.acquire() as conn:
        checkpoints = await conn.fetch("""
            SELECT projection_name, last_event_id, updated_at
            FROM projection_checkpoints
        """)
        total_events = await conn.fetchval("SELECT COUNT(*) FROM events")
    await pool.close()

    lines = ["Projection Lag Report", "=" * 40, f"Generated: {datetime.now(timezone.utc).isoformat()}", ""]
    lines.append(f"Total events in store: {total_events}")
    lines.append("")
    lines.append("Projection | Last Event ID | Lag | Updated At")
    lines.append("-" * 70)
    for r in checkpoints:
        lag = total_events - (r['last_event_id'] or 0)
        lines.append(f"{r['projection_name']} | {r['last_event_id']} | {lag} events behind | {r['updated_at']}")

    if not checkpoints:
        lines.append("No projection checkpoints found (projections not yet run).")
        lines.append("Lag = 0 by design for on-demand projections.")

    with open("artifacts/projection_lag_report.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print("✅ projection_lag_report.txt")


async def generate_api_cost_report():
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT stream_id, event_type, event_data, created_at
            FROM events
            WHERE event_type = 'AgentSessionCompleted'
            ORDER BY created_at DESC
        """)
    await pool.close()

    total_cost = 0.0
    total_tokens = 0
    session_lines = []

    for r in rows:
        data = r['event_data'] if isinstance(r['event_data'], dict) else json.loads(r['event_data'])
        cost = float(data.get('total_cost_usd', 0))
        tokens = int(data.get('total_tokens_used', 0))
        total_cost += cost
        total_tokens += tokens
        session_lines.append(f"  {r['stream_id'][:50]} | cost=${cost:.6f} | tokens={tokens}")

    lines = [
        "API Cost Report",
        "=" * 40,
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        f"Total sessions: {len(rows)}",
        f"Total tokens used: {total_tokens:,}",
        f"Total cost: ${total_cost:.4f} USD",
        f"Cost limit: $50.00 USD",
        f"Status: {'✅ UNDER LIMIT' if total_cost < 50 else '❌ OVER LIMIT'}",
        "",
        "Per-session breakdown:",
    ] + session_lines

    with open("artifacts/api_cost_report.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"✅ api_cost_report.txt (total cost: ${total_cost:.4f})")


async def generate_regulatory_package():
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])
    async with pool.acquire() as conn:
        # Get NARR-05 loan stream events
        loan_events = await conn.fetch("""
            SELECT event_type, event_data, version, created_at
            FROM events
            WHERE stream_id = 'loan-NARR-05'
            ORDER BY version ASC
        """)

        # Get compliance events
        compliance_events = await conn.fetch("""
            SELECT stream_id, event_type, event_data, version, created_at
            FROM events
            WHERE stream_id LIKE 'compliance-NARR-05%'
            ORDER BY version ASC
        """)

        # Get credit events
        credit_record_id = None
        for ev in loan_events:
            data = ev['event_data'] if isinstance(ev['event_data'], dict) else json.loads(ev['event_data'])
            if ev['event_type'] == 'CreditAnalysisRequested':
                credit_record_id = data.get('credit_record_id')
                break

        credit_events = []
        if credit_record_id:
            credit_events = await conn.fetch("""
                SELECT event_type, event_data, version, created_at
                FROM events
                WHERE stream_id = $1
                ORDER BY version ASC
            """, f"credit-{credit_record_id}")

    await pool.close()

    def parse_event(r):
        data = r['event_data'] if isinstance(r['event_data'], dict) else json.loads(r['event_data'])
        return {
            "event_type": r['event_type'],
            "version": r['version'],
            "created_at": r['created_at'].isoformat() if r['created_at'] else None,
            "payload": data,
        }

    package = {
        "regulatory_package": "NARR-05",
        "description": "Human override regulatory package — orchestrator recommended DECLINE, loan officer approved",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "application_id": "NARR-05",
        "loan_stream_events": [parse_event(r) for r in loan_events],
        "compliance_stream_events": [parse_event(r) for r in compliance_events],
        "credit_stream_events": [parse_event(r) for r in credit_events],
        "summary": {
            "total_loan_events": len(loan_events),
            "total_compliance_events": len(compliance_events),
            "total_credit_events": len(credit_events),
            "human_override_present": any(
                r['event_type'] in ('HumanReviewCompleted', 'ApplicationApproved')
                for r in loan_events
            ),
        }
    }

    with open("artifacts/regulatory_package_NARR05.json", "w", encoding="utf-8") as f:
        json.dump(package, f, indent=2, default=str)
    print("✅ regulatory_package_NARR05.json")


async def main():
    print("Generating artifacts...")
    os.makedirs("artifacts", exist_ok=True)
    await generate_occ_report()
    await generate_projection_lag_report()
    await generate_api_cost_report()
    await generate_regulatory_package()
    print("\nAll artifacts generated.")
    print("Files in artifacts/:")
    for f in os.listdir("artifacts"):
        size = os.path.getsize(f"artifacts/{f}")
        print(f"  {f} ({size} bytes)")


if __name__ == "__main__":
    asyncio.run(main())
