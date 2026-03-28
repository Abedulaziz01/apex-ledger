"""
ledger/agents/credit_analysis_agent.py
CreditAnalysisAgent — REFERENCE IMPLEMENTATION.
Read this before implementing any other agent.
"""
from __future__ import annotations

import json
import time
import uuid
from typing import Any

from langgraph.graph import StateGraph, END

from src.ledger.agents.base_agent import BaseApexAgent
from src.ledger.schema.events import (
    CreditRecordOpened, HistoricalProfileConsumed, ExtractedFactsConsumed,
    CreditAnalysisCompleted, CreditAnalysisDeferred,
    AgentInputValidated, AgentInputValidationFailed, AgentOutputWritten,
    FraudScreeningRequested,
)
from src.ledger.core.exceptions import OptimisticConcurrencyError


CREDIT_SYSTEM_PROMPT = """You are a commercial credit analyst at Apex Financial Services.
You receive structured financial data for a loan applicant and produce a credit risk assessment.

Your output must be valid JSON with exactly these fields:
{
  "risk_tier": "LOW" | "MEDIUM" | "HIGH",
  "recommended_limit_usd": <float>,
  "confidence": <float between 0.0 and 1.0>,
  "rationale": "<3-5 sentence explanation>",
  "data_quality_caveats": ["<caveat 1>", ...]
}

Rules you must follow:
- Base your assessment on the financial facts provided, not assumptions.
- If a critical field (total_revenue, net_income, total_assets) is missing, include it in data_quality_caveats and reduce confidence accordingly.
- Do NOT mention specific applicant names in your rationale.
- Do NOT make decisions based on jurisdiction, race, gender, or any protected characteristic.
- Confidence reflects data completeness, not just risk level.
"""

CREDIT_USER_TEMPLATE = """
## Company Financial Summary

**Revenue (current year):** ${total_revenue:,.0f}
**Net Income:** ${net_income:,.0f}
**Total Assets:** ${total_assets:,.0f}
**Total Liabilities:** ${total_liabilities:,.0f}
**EBITDA:** {ebitda}
**Gross Margin:** {gross_margin}%
**Debt-to-Equity Ratio:** {debt_to_equity}

## 3-Year Revenue Trend
{revenue_trend}

## Requested Loan Amount
${requested_amount_usd:,.0f}

## Prior Loan History
{loan_history}

## Data Quality Notes
{quality_caveats}

Provide your credit risk assessment as JSON only. No preamble, no explanation outside the JSON.
"""


class CreditAnalysisAgent(BaseApexAgent):
    agent_type = "credit"

    def _build_graph(self):
        graph = StateGraph(dict)
        graph.add_node("validate_inputs", self._node_validate_inputs)
        graph.add_node("open_credit_record", self._node_open_credit_record)
        graph.add_node("load_external_data", self._node_load_external_data)
        graph.add_node("analyze_credit_risk", self._node_analyze_credit_risk)
        graph.add_node("apply_policy_constraints", self._node_apply_policy_constraints)
        graph.add_node("write_output", self._node_write_output)

        graph.set_entry_point("validate_inputs")
        graph.add_edge("validate_inputs", "open_credit_record")
        graph.add_edge("open_credit_record", "load_external_data")
        graph.add_edge("load_external_data", "analyze_credit_risk")
        graph.add_edge("analyze_credit_risk", "apply_policy_constraints")
        graph.add_edge("apply_policy_constraints", "write_output")
        graph.add_edge("write_output", END)

        return graph.compile()

    async def process(self, application_id: str, prior_session_id: str = None) -> dict:
        await self._start_session(application_id, prior_session_id)
        graph = self._build_graph()
        state = {"application_id": application_id}
        try:
            result = await graph.ainvoke(state)
            await self._end_session(next_agent="fraud")
            return result
        except Exception as e:
            await self._fail_session(
                error_type=type(e).__name__,
                error_message=str(e),
                last_node=state.get("_last_node", "unknown"),
                recoverable=True,
            )
            raise

    # ------------------------------------------------------------------
    # Nodes
    # ------------------------------------------------------------------

    async def _node_validate_inputs(self, state: dict) -> dict:
        t0 = time.time()
        application_id = state["application_id"]
        loan_stream_id = f"loan-{application_id}"

        loan_events = await self._store.load_stream(loan_stream_id)
        await self._record_tool_call(
            tool="load_stream",
            inp=f"stream_id={loan_stream_id}",
            out=f"{len(loan_events)} events loaded",
            ms=0,
        )

        # Find requested document package + credit record ids from loan stream.
        doc_pkg_id = None
        requested_credit_record_id = None
        for ev in loan_events:
            if ev.event_type == "DocumentUploadRequested":
                doc_pkg_id = ev.payload.get("document_package_id")
            if ev.event_type == "CreditAnalysisRequested":
                doc_pkg_id = ev.payload.get("document_package_id", doc_pkg_id)
                requested_credit_record_id = ev.payload.get(
                    "credit_record_id", requested_credit_record_id
                )

        missing = []
        if not loan_events:
            missing.append("loan stream")
        if not doc_pkg_id:
            missing.append("document_package_id")

        if missing:
            await self._store.append(
                self._session_stream_id,
                [AgentInputValidationFailed(
                    stream_id=self._session_stream_id,
                    payload={"missing_inputs": missing, "validation_errors": missing},
                )],
                expected_version=self._session_version,
            )
            self._session_version += 1
            raise ValueError(f"Missing inputs: {missing}")

        await self._append_session_event(
            AgentInputValidated(
                stream_id=self._session_stream_id,
                payload={
                    "inputs_validated": ["loan_stream", "document_package_id"],
                    "validation_duration_ms": int((time.time() - t0) * 1000),
                },
            )
        )

        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution(
            "validate_inputs",
            ["application_id"],
            ["loan_events", "doc_pkg_id", "requested_credit_record_id"],
            ms,
        )
        return {
            **state,
            "loan_events": loan_events,
            "doc_pkg_id": doc_pkg_id,
            "requested_credit_record_id": requested_credit_record_id,
            "_last_node": "validate_inputs",
        }

    async def _node_open_credit_record(self, state: dict) -> dict:
        t0 = time.time()
        application_id = state["application_id"]
        credit_record_id = state.get("requested_credit_record_id") or str(uuid.uuid4())
        credit_stream_id = f"credit-{credit_record_id}"

        # Idempotent open for concurrent runs: only one session should write
        # CreditRecordOpened at stream creation.
        async def _build_open_event(version):
            if version > 0:
                return []
            return [
                CreditRecordOpened(
                    stream_id=credit_stream_id,
                    payload={
                        "credit_record_id": credit_record_id,
                        "application_id": application_id,
                        "opened_by_session": self._session_id,
                    },
                )
            ]

        await self._append_with_occ_retry(credit_stream_id, _build_open_event)

        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution(
            "open_credit_record", ["application_id"], ["credit_record_id", "credit_stream_id"], ms
        )
        return {**state, "credit_record_id": credit_record_id, "credit_stream_id": credit_stream_id, "_last_node": "open_credit_record"}

    async def _node_load_external_data(self, state: dict) -> dict:
        t0 = time.time()
        application_id = state["application_id"]

        # Load company profile from registry
        company_id = None
        for ev in state["loan_events"]:
            if ev.event_type == "ApplicationSubmitted":
                company_id = ev.payload.get("company_id")
                break

        t_reg = time.time()
        company_profile = await self._registry.get_company_profile(company_id)
        financial_history = await self._registry.get_financial_history(company_id)
        compliance_flags = await self._registry.get_compliance_flags(company_id)
        loan_relationships = await self._registry.get_loan_relationships(company_id)
        await self._record_tool_call(
            tool="query_applicant_registry",
            inp=f"company_id={company_id}",
            out=f"Loaded 3yr financials ({len(financial_history)} rows), {len(compliance_flags)} flags, {len(loan_relationships['loan_history'])} loans",
            ms=int((time.time() - t_reg) * 1000),
        )

        # Load extracted facts from docpkg stream
        t_docpkg = time.time()
        doc_events = await self._store.load_stream(f"docpkg-{state['doc_pkg_id']}")
        await self._record_tool_call(
            tool="load_docpkg_stream",
            inp=f"doc_pkg_id={state['doc_pkg_id']}",
            out=f"{len(doc_events)} events",
            ms=int((time.time() - t_docpkg) * 1000),
        )

        extracted_facts = {}
        quality_flags = {}
        for ev in doc_events:
            if ev.event_type == "ExtractionCompleted":
                extracted_facts.update(ev.payload.get("facts", {}))
                quality_flags.update(ev.payload.get("field_confidence", {}))
            if ev.event_type == "QualityAssessmentCompleted":
                quality_flags["_quality"] = ev.payload

        # Append consumed events to credit stream with OCC retry for concurrent runs.
        async def _build_consumed_events(version):
            return [
                HistoricalProfileConsumed(
                    stream_id=state["credit_stream_id"],
                    payload={"company_id": company_id, "years_loaded": len(financial_history)},
                ),
                ExtractedFactsConsumed(
                    stream_id=state["credit_stream_id"],
                    payload={"doc_pkg_id": state["doc_pkg_id"], "fields_loaded": list(extracted_facts.keys())},
                ),
            ]

        await self._append_with_occ_retry(state["credit_stream_id"], _build_consumed_events)

        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution(
            "load_external_data",
            ["loan_events", "doc_pkg_id"],
            ["company_profile", "financial_history", "extracted_facts", "compliance_flags"],
            ms,
        )
        return {
            **state,
            "company_profile": company_profile,
            "financial_history": financial_history,
            "extracted_facts": extracted_facts,
            "quality_flags": quality_flags,
            "compliance_flags": compliance_flags,
            "loan_relationships": loan_relationships,
            "_last_node": "load_external_data",
        }

    async def _node_analyze_credit_risk(self, state: dict) -> dict:
        t0 = time.time()
        facts = state["extracted_facts"]
        history = state["financial_history"]
        loan_rels = state["loan_relationships"]
        quality = state.get("quality_flags", {})

        # Build revenue trend string
        revenue_trend = "\n".join(
            f"  {row.get('fiscal_year')}: ${row.get('total_revenue', 0):,.0f}"
            for row in history
        )

        # Build loan history string
        loan_history_str = "\n".join(
            f"  {r.get('loan_year', 'N/A')}: ${r.get('amount_usd', 0):,.0f} — "
            f"{'DEFAULT' if r.get('default_occurred') else 'Repaid'}"
            for r in loan_rels.get("loan_history", [])
        ) or "No prior loan history."

        # Collect quality caveats
        quality_assessment = quality.get("_quality", {})
        caveats = quality_assessment.get("anomalies", []) + [
            f"Missing field: {f}" for f in quality_assessment.get("critical_missing_fields", [])
        ]

        user_msg = CREDIT_USER_TEMPLATE.format(
            total_revenue=facts.get("total_revenue") or 0,
            net_income=facts.get("net_income") or 0,
            total_assets=facts.get("total_assets") or 0,
            total_liabilities=facts.get("total_liabilities") or 0,
            ebitda=f"${facts['ebitda']:,.0f}" if facts.get("ebitda") else "Not available",
            gross_margin=round(facts.get("gross_margin", 0) * 100, 1) if facts.get("gross_margin") else "N/A",
            debt_to_equity=round(facts.get("debt_to_equity", 0), 2) if facts.get("debt_to_equity") else "N/A",
            revenue_trend=revenue_trend or "  No historical data.",
            requested_amount_usd=state["company_profile"].get("requested_amount_usd", 0),
            loan_history=loan_history_str,
            quality_caveats="\n".join(f"  - {c}" for c in caveats) or "  None",
        )

        raw, tok_in, tok_out, cost = await self._call_llm(
            system=CREDIT_SYSTEM_PROMPT,
            user=user_msg,
            max_tokens=1024,
        )

        # Parse JSON — strip markdown fences if present
        clean = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        try:
            decision = json.loads(clean)
        except json.JSONDecodeError:
            decision = {
                "risk_tier": "HIGH",
                "recommended_limit_usd": 0,
                "confidence": 0.3,
                "rationale": "LLM returned unparseable response — defaulting to HIGH risk.",
                "data_quality_caveats": ["LLM parse error"],
            }

        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution(
            "analyze_credit_risk",
            ["company_profile", "financial_history", "extracted_facts"],
            ["decision"],
            ms,
            llm_tokens_input=tok_in,
            llm_tokens_output=tok_out,
            llm_cost_usd=cost,
        )
        return {**state, "decision": decision, "_last_node": "analyze_credit_risk"}

    async def _node_apply_policy_constraints(self, state: dict) -> dict:
        t0 = time.time()
        decision = state["decision"]
        facts = state["extracted_facts"]
        history = state["financial_history"]
        compliance_flags = state["compliance_flags"]
        loan_rels = state["loan_relationships"]

        annual_revenue_raw = facts.get("total_revenue") or (history[0].get("total_revenue") if history else 0) or 1
        annual_revenue = float(annual_revenue_raw)

        # Policy 1: Max loan-to-revenue ratio 35%
        max_limit = annual_revenue * 0.35
        if float(decision.get("recommended_limit_usd", 0) or 0) > max_limit:
            decision["recommended_limit_usd"] = max_limit
            decision.setdefault("data_quality_caveats", []).append(
                f"Limit capped at 35% of revenue (${max_limit:,.0f})"
            )

        # Policy 2: Prior default → force HIGH risk
        has_default = any(r.get("default_occurred") for r in loan_rels.get("loan_history", []))
        if has_default:
            decision["risk_tier"] = "HIGH"
            decision.setdefault("data_quality_caveats", []).append("Prior default on record — risk_tier forced to HIGH")

        # Policy 3: Active HIGH compliance flag → cap confidence at 0.50
        active_high_flags = [
            f for f in compliance_flags
            if f.get("is_active") and f.get("severity") == "HIGH"
        ]
        if active_high_flags:
            decision["confidence"] = min(decision.get("confidence", 1.0), 0.50)
            decision.setdefault("data_quality_caveats", []).append(
                f"Active HIGH compliance flag — confidence capped at 0.50"
            )

        # Cap EBITDA-missing confidence
        quality = state.get("quality_flags", {}).get("_quality", {})
        if "ebitda" in quality.get("critical_missing_fields", []):
            decision["confidence"] = min(decision.get("confidence", 1.0), 0.75)
            decision.setdefault("data_quality_caveats", []).append("EBITDA missing — confidence capped at 0.75")

        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution(
            "apply_policy_constraints", ["decision"], ["decision"], ms
        )
        return {**state, "decision": decision, "_last_node": "apply_policy_constraints"}

    async def _node_write_output(self, state: dict) -> dict:
        t0 = time.time()
        application_id = state["application_id"]
        decision = state["decision"]

        # Append CreditAnalysisCompleted to credit stream (with OCC retry)
        credit_stream_id = state["credit_stream_id"]

        async def _build_events(version):
            return [
                CreditAnalysisCompleted(
                    stream_id=credit_stream_id,
                    payload={
                        "credit_record_id": state["credit_record_id"],
                        "application_id": application_id,
                        "decision": decision,
                        "session_id": self._session_id,
                    },
                )
            ]
        
        try:
            written = await self._append_with_occ_retry(credit_stream_id, _build_events)
        except OptimisticConcurrencyError:
            # Other agent already wrote to this credit stream — load what was written
            written = await self._store.load_stream(credit_stream_id)

        # Trigger next agent: append FraudScreeningRequested to loan stream
        loan_stream_id = f"loan-{application_id}"
        fraud_screening_id = str(uuid.uuid4())
        
        async def _build_fraud_event(version):
            return [
                FraudScreeningRequested(
                    stream_id=loan_stream_id,
                    payload={
                        "fraud_screening_id": fraud_screening_id,
                        "credit_record_id": state["credit_record_id"],
                        "triggered_by_session": self._session_id,
                    },
                )
            ]
        
        try:
            await self._append_with_occ_retry(loan_stream_id, _build_fraud_event)
        except OptimisticConcurrencyError:
            pass  # Other agent already wrote FraudScreeningRequested — that's fine

        await self._append_session_event(
            AgentOutputWritten(
                stream_id=self._session_stream_id,
                payload={
                    "events_written": [
                        {"stream_id": credit_stream_id, "event_type": "CreditAnalysisCompleted",
                         "stream_position": written[0].stream_position if written else None}
                    ],
                    "output_summary": f"risk_tier={decision.get('risk_tier')} confidence={decision.get('confidence'):.2f}",
                },
            )
        )

        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution("write_output", ["decision"], [], ms)
        return {**state, "fraud_screening_id": fraud_screening_id, "_last_node": "write_output"}
