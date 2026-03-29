"""
Interactive Loan Application Tester
Usage: uv run python scripts/test_loan.py
"""
import asyncio
import os
import time
from dotenv import load_dotenv
load_dotenv()

import asyncpg
from src.ledger.core.event_store import EventStore
from src.ledger.registry.client import ApplicantRegistryClient
from src.ledger.schema.events import ApplicationSubmitted, DocumentUploadRequested
from src.ledger.agents.stub_agents import (
    DocumentProcessingAgent, FraudDetectionAgent,
    ComplianceAgent, DecisionOrchestratorAgent
)
from src.ledger.agents.credit_analysis_agent import CreditAnalysisAgent

COMPANIES = {
    "1": {"id": "COMP-031", "name": "Acme Manufacturing",     "industry": "Manufacturing", "revenue": "$2.1M"},
    "2": {"id": "COMP-044", "name": "Retail Plus",            "industry": "Retail",        "revenue": "$1.5M"},
    "3": {"id": "COMP-057", "name": "TechNova Solutions",     "industry": "Technology",    "revenue": "$3.0M"},
    "4": {"id": "COMP-068", "name": "HealthCare Partners",    "industry": "Healthcare",    "revenue": "$4.0M"},
    "5": {"id": "COMP-MT",  "name": "Montana Agriculture Co", "industry": "Agriculture",   "revenue": "$800K  ⚠️  COMPLIANCE RISK"},
}

def clear():
    os.system("cls" if os.name == "nt" else "clear")

def print_header():
    print("=" * 60)
    print("   APEX LEDGER — LOAN APPLICATION SYSTEM")
    print("=" * 60)

def print_companies():
    print("\nAvailable Companies:")
    print("-" * 60)
    for key, c in COMPANIES.items():
        print(f"  [{key}] {c['name']}")
        print(f"       Industry: {c['industry']}  |  Revenue: {c['revenue']}")
    print("-" * 60)

def get_company_choice():
    while True:
        print_companies()
        choice = input("\nSelect company [1-5]: ").strip()
        if choice in COMPANIES:
            return COMPANIES[choice]
        print("❌ Invalid choice. Please enter 1-5.")

def get_loan_amount():
    while True:
        print("\nLoan Amount Examples:")
        print("  Small:  50000    ($50K)")
        print("  Medium: 500000   ($500K)")
        print("  Large:  2000000  ($2M)")
        raw = input("\nEnter loan amount in USD (numbers only): ").strip()
        try:
            amount = int(raw.replace(",", "").replace("$", ""))
            if amount <= 0:
                print("❌ Amount must be positive.")
                continue
            if amount > 50_000_000:
                print("❌ Maximum loan amount is $50,000,000.")
                continue
            return amount
        except ValueError:
            print("❌ Please enter a valid number (e.g. 500000)")

def print_progress(step, total, name):
    bar = "█" * step + "░" * (total - step)
    print(f"\r  [{bar}] {step}/{total} {name}    ", end="", flush=True)

async def run_application(company, loan_amount):
    app_id = f"APP-{int(time.time())}"
    loan_stream = f"loan-{app_id}"

    print(f"\n{'='*60}")
    print(f"  PROCESSING APPLICATION")
    print(f"{'='*60}")
    print(f"  Company:     {company['name']}")
    print(f"  Industry:    {company['industry']}")
    print(f"  Annual Rev:  {company['revenue']}")
    print(f"  Loan Req:    ${loan_amount:,.0f}")
    print(f"  App ID:      {app_id}")
    print(f"{'='*60}\n")

    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])
    store = EventStore(pool)
    registry = ApplicantRegistryClient(pool)

    # Submit
    await store.append(loan_stream, [
        ApplicationSubmitted(
            stream_id=loan_stream,
            payload={"company_id": company["id"], "requested_amount_usd": loan_amount},
        ),
        DocumentUploadRequested(
            stream_id=loan_stream,
            payload={
                "document_package_id": f"docpkg-{app_id}",
                "document_type": "income_statement",
                "file_path": f"documents/{company['id']}/income_statement_2024.pdf",
            },
        ),
    ], expected_version=0)

    steps = [
        ("📄 Reading financial documents ", DocumentProcessingAgent(store, registry, documents_dir="./documents")),
        ("🔍 Analyzing credit risk       ", CreditAnalysisAgent(store, registry)),
        ("🕵️  Screening for fraud         ", FraudDetectionAgent(store, registry)),
        ("⚖️  Checking compliance rules   ", ComplianceAgent(store, registry)),
        ("🧠 Making final decision        ", DecisionOrchestratorAgent(store, registry)),
    ]

    t_start = time.time()
    errors = []

    for i, (name, agent) in enumerate(steps, 1):
        print(f"  {name}", end=" ", flush=True)
        t = time.time()
        try:
            await agent.process(app_id)
            print(f"✅  ({time.time()-t:.1f}s)")
        except Exception as e:
            print(f"⚠️   ({time.time()-t:.1f}s)")
            errors.append(str(e))

    elapsed = time.time() - t_start

    # Read results
    loan_events = await store.load_stream(loan_stream)

    print(f"\n{'='*60}")
    print(f"  DECISION SUMMARY")
    print(f"{'='*60}")

    ai_rec = None
    final_status = "PENDING"
    compliance_blocks = []
    credit_decision = None

    for ev in loan_events:
        if ev.event_type == "CreditAnalysisCompleted":
            d = ev.event_data if isinstance(ev.event_data, dict) else {}
            decision = d.get("decision", {})
            if decision:
                credit_decision = decision
                tier = decision.get("risk_tier", "?")
                conf = decision.get("confidence", 0)
                limit = decision.get("recommended_limit_usd", 0)
                print(f"\n  📊 Credit Analysis:")
                print(f"     Risk Tier:        {tier}")
                print(f"     Confidence:       {conf:.0%}")
                print(f"     Recommended Max:  ${limit:,.0f}")

        if ev.event_type == "FraudScreeningCompleted":
            d = ev.event_data if isinstance(ev.event_data, dict) else {}
            score = d.get("fraud_score", 0)
            print(f"\n  🕵️  Fraud Screening:")
            print(f"     Fraud Score:  {score:.2f} ({'LOW RISK' if score < 0.3 else 'MEDIUM RISK' if score < 0.7 else 'HIGH RISK'})")

        if ev.event_type == "ComplianceCheckCompleted":
            d = ev.event_data if isinstance(ev.event_data, dict) else {}
            passed = d.get("passed", True)
            rule = d.get("rule_id", "")
            if not passed:
                compliance_blocks.append(rule)

        if ev.event_type == "DecisionGenerated":
            d = ev.event_data if isinstance(ev.event_data, dict) else {}
            ai_rec = d.get("recommendation", "UNKNOWN")
            conf = d.get("confidence_score", 0)
            print(f"\n  🤖 AI Recommendation:")
            print(f"     Decision:    {ai_rec}")
            print(f"     Confidence:  {conf:.0%}")

        if ev.event_type == "ApplicationApproved":
            d = ev.event_data if isinstance(ev.event_data, dict) else {}
            amount = d.get("approved_amount_usd", 0)
            final_status = "APPROVED"
            print(f"\n  ✅ FINAL: APPROVED")
            print(f"     Amount: ${amount:,.0f}")

        if ev.event_type == "ApplicationDeclined":
            final_status = "DECLINED"
            d = ev.event_data if isinstance(ev.event_data, dict) else {}
            reason = d.get("reason", "Risk assessment")
            print(f"\n  ❌ FINAL: DECLINED")
            print(f"     Reason: {reason}")

    if compliance_blocks:
        print(f"\n  🚫 COMPLIANCE BLOCKS:")
        for block in compliance_blocks:
            print(f"     Rule: {block}")

    if final_status == "PENDING" and ai_rec:
        print(f"\n  📋 Status: Awaiting human review")
        print(f"     AI recommended: {ai_rec}")

    print(f"\n{'='*60}")
    print(f"  Total events recorded: {len(loan_events)}")
    print(f"  Processing time:       {elapsed:.1f}s")
    print(f"{'='*60}")

    await pool.close()
    return final_status

async def main():
    clear()
    print_header()

    while True:
        print("\n\nWhat would you like to do?")
        print("  [1] Submit a new loan application")
        print("  [2] Exit")

        choice = input("\nYour choice: ").strip()

        if choice == "2":
            print("\nGoodbye!\n")
            break

        elif choice == "1":
            clear()
            print_header()
            company = get_company_choice()
            loan_amount = get_loan_amount()

            print(f"\n⏳ Submitting application for {company['name']}...")
            status = await run_application(company, loan_amount)

            input("\n\nPress ENTER to submit another application...")
            clear()
            print_header()

        else:
            print("❌ Invalid choice. Please enter 1 or 2.")

if __name__ == "__main__":
    asyncio.run(main())