"""
ledger/agents/stub_agents.py
Four agent implementations following the CreditAnalysisAgent reference pattern.
"""
from __future__ import annotations

import json
import time
import uuid
from typing import Any, Optional

from langgraph.graph import StateGraph, END

from src.ledger.agents.base_agent import BaseApexAgent
from src.ledger.schema.events import (
    PackageCreated, DocumentAdded, DocumentFormatValidated,
    ExtractionStarted, ExtractionCompleted, QualityAssessmentCompleted,
    PackageReadyForAnalysis, CreditAnalysisRequested,
    AgentInputValidated, AgentInputValidationFailed, AgentOutputWritten,
    FraudScreeningInitiated, FraudAnomalyDetected, FraudScreeningCompleted,
    ComplianceCheckRequested, ComplianceCheckInitiated,
    ComplianceRulePassed, ComplianceRuleFailed, ComplianceRuleNoted,
    ComplianceCheckCompleted, DecisionRequested, DecisionGenerated,
    HumanReviewRequested, ApplicationApproved, ApplicationDeclined,
)


# =============================================================================
# Agent 1 — DocumentProcessingAgent
# =============================================================================

QUALITY_SYSTEM_PROMPT = """
You are a financial document quality analyst. You receive structured data
extracted from a company's financial statements.

Check ONLY:
1. Internal consistency (Gross Profit = Revenue - COGS, Assets = Liabilities + Equity)
2. Implausible values (margins > 80%, negative equity without note)
3. Critical missing fields (total_revenue, net_income, total_assets, total_liabilities)

Return JSON with exactly these fields:
{
  "overall_confidence": <float 0.0-1.0>,
  "is_coherent": <bool>,
  "anomalies": [<string>, ...],
  "critical_missing_fields": [<string>, ...],
  "reextraction_recommended": <bool>,
  "auditor_notes": "<string>"
}

DO NOT make credit or lending decisions. DO NOT suggest loan outcomes.
"""


class DocumentProcessingAgent(BaseApexAgent):
    agent_type = "docproc"

    def __init__(self, *args, documents_dir: str = "./documents", **kwargs):
        super().__init__(*args, **kwargs)
        self._documents_dir = documents_dir

    def _build_graph(self):
        graph = StateGraph(dict)
        graph.add_node("validate_inputs", self._node_validate_inputs)
        graph.add_node("open_package", self._node_open_package)
        graph.add_node("validate_document_formats", self._node_validate_formats)
        graph.add_node("extract_income_statement", self._node_extract_income)
        graph.add_node("extract_balance_sheet", self._node_extract_balance)
        graph.add_node("assess_quality", self._node_assess_quality)
        graph.add_node("write_output", self._node_write_output)

        graph.set_entry_point("validate_inputs")
        graph.add_edge("validate_inputs", "open_package")
        graph.add_edge("open_package", "validate_document_formats")
        graph.add_edge("validate_document_formats", "extract_income_statement")
        graph.add_edge("extract_income_statement", "extract_balance_sheet")
        graph.add_edge("extract_balance_sheet", "assess_quality")
        graph.add_edge("assess_quality", "write_output")
        graph.add_edge("write_output", END)
        return graph.compile()

    async def process(self, application_id: str, prior_session_id: str = None) -> dict:
        await self._start_session(application_id, prior_session_id)
        graph = self._build_graph()
        state = {"application_id": application_id}
        try:
            result = await graph.ainvoke(state)
            await self._end_session(next_agent="credit")
            return result
        except Exception as e:
            await self._fail_session(type(e).__name__, str(e), state.get("_last_node", "unknown"))
            raise

    async def _node_validate_inputs(self, state: dict) -> dict:
        t0 = time.time()
        app_id = state["application_id"]
        loan_events = await self._store.load_stream(f"loan-{app_id}")

        company_id = None
        for ev in loan_events:
            if ev.event_type == "ApplicationSubmitted":
                company_id = ev.payload.get("company_id")

        if not company_id:
            raise ValueError(f"No ApplicationSubmitted event for {app_id}")

        await self._append_session_event(
            AgentInputValidated(
                stream_id=self._session_stream_id,
                payload={"inputs_validated": ["loan_stream", "company_id"],
                         "validation_duration_ms": int((time.time() - t0) * 1000)},
            )
        )
        await self._record_node_execution("validate_inputs", ["application_id"], ["company_id"], int((time.time()-t0)*1000))
        return {**state, "company_id": company_id, "loan_events": loan_events, "_last_node": "validate_inputs"}

    async def _node_open_package(self, state: dict) -> dict:
        t0 = time.time()
        app_id = state["application_id"]
        pkg_id = None
        for ev in state.get("loan_events", []):
            if ev.event_type == "DocumentUploadRequested":
                pkg_id = ev.payload.get("document_package_id")
                break
        pkg_id = pkg_id or str(uuid.uuid4())
        pkg_stream = f"docpkg-{pkg_id}"

        await self._store.append(pkg_stream, [
            PackageCreated(stream_id=pkg_stream, payload={"package_id": pkg_id, "application_id": app_id}),
        ], expected_version=-1)

        await self._record_node_execution("open_package", ["company_id"], ["pkg_id", "pkg_stream"], int((time.time()-t0)*1000))
        return {**state, "pkg_id": pkg_id, "pkg_stream": pkg_stream, "_last_node": "open_package"}

    async def _node_validate_formats(self, state: dict) -> dict:
        t0 = time.time()
        import os
        company_id = state["company_id"]
        docs_dir = self._documents_dir
        pkg_stream = state["pkg_stream"]
        pkg_version = await self._store.stream_version(pkg_stream)

        doc_files = {
            "income_statement": f"{docs_dir}/{company_id}/income_statement_2024.pdf",
            "balance_sheet": f"{docs_dir}/{company_id}/balance_sheet_2024.pdf",
        }

        events = []
        for doc_type, path in doc_files.items():
            exists = os.path.exists(path)
            events.append(DocumentFormatValidated(
                stream_id=pkg_stream,
                payload={"doc_type": doc_type, "path": path, "format_valid": exists,
                         "format": "pdf" if path.endswith(".pdf") else "unknown"},
            ))

        await self._store.append(pkg_stream, events, expected_version=pkg_version)
        await self._record_node_execution("validate_document_formats", ["pkg_stream"], ["format_events"], int((time.time()-t0)*1000))
        return {**state, "doc_files": doc_files, "_last_node": "validate_document_formats"}

    async def _node_extract_income(self, state: dict) -> dict:
        t0 = time.time()
        pkg_stream = state["pkg_stream"]
        path = state["doc_files"].get("income_statement", "")
        pkg_version = await self._store.stream_version(pkg_stream)

        await self._store.append(pkg_stream, [
            ExtractionStarted(stream_id=pkg_stream, payload={"doc_type": "income_statement", "path": path}),
        ], expected_version=pkg_version)

        # ---- Week 3 integration point ----
        facts = {}
        field_confidence = {}
        extraction_notes = []
        try:
            from document_refinery.pipeline import extract_financial_facts
            result = extract_financial_facts(path, doc_type="income_statement")
            facts = result.facts if result else {}
            field_confidence = result.field_confidence if result else {}
        except ImportError:
            # Week 3 pipeline not installed — use stub extraction
            facts, field_confidence, extraction_notes = _stub_extract_income(path)

        # Null critical fields get confidence 0.0, not defaulted to 0
        for critical in ["total_revenue", "net_income", "gross_profit"]:
            if facts.get(critical) is None:
                field_confidence[critical] = 0.0
                extraction_notes.append(f"{critical} not found in document")

        pkg_version = await self._store.stream_version(pkg_stream)
        await self._store.append(pkg_stream, [
            ExtractionCompleted(stream_id=pkg_stream, payload={
                "doc_type": "income_statement", "path": path,
                "facts": facts, "field_confidence": field_confidence,
                "extraction_notes": extraction_notes,
            }),
        ], expected_version=pkg_version)

        await self._record_node_execution("extract_income_statement", ["doc_files"], ["income_facts"], int((time.time()-t0)*1000))
        return {**state, "income_facts": facts, "income_confidence": field_confidence, "_last_node": "extract_income_statement"}

    async def _node_extract_balance(self, state: dict) -> dict:
        t0 = time.time()
        pkg_stream = state["pkg_stream"]
        path = state["doc_files"].get("balance_sheet", "")
        pkg_version = await self._store.stream_version(pkg_stream)

        await self._store.append(pkg_stream, [
            ExtractionStarted(stream_id=pkg_stream, payload={"doc_type": "balance_sheet", "path": path}),
        ], expected_version=pkg_version)

        facts = {}
        field_confidence = {}
        extraction_notes = []
        try:
            from document_refinery.pipeline import extract_financial_facts
            result = extract_financial_facts(path, doc_type="balance_sheet")
            facts = result.facts if result else {}
            field_confidence = result.field_confidence if result else {}
        except ImportError:
            facts, field_confidence, extraction_notes = _stub_extract_balance(path)

        for critical in ["total_assets", "total_liabilities", "total_equity"]:
            if facts.get(critical) is None:
                field_confidence[critical] = 0.0
                extraction_notes.append(f"{critical} not found in document")

        pkg_version = await self._store.stream_version(pkg_stream)
        await self._store.append(pkg_stream, [
            ExtractionCompleted(stream_id=pkg_stream, payload={
                "doc_type": "balance_sheet", "path": path,
                "facts": facts, "field_confidence": field_confidence,
                "extraction_notes": extraction_notes,
            }),
        ], expected_version=pkg_version)

        await self._record_node_execution("extract_balance_sheet", ["doc_files"], ["balance_facts"], int((time.time()-t0)*1000))
        return {**state, "balance_facts": facts, "balance_confidence": field_confidence, "_last_node": "extract_balance_sheet"}

    async def _node_assess_quality(self, state: dict) -> dict:
        t0 = time.time()
        income = state.get("income_facts", {})
        balance = state.get("balance_facts", {})
        income_conf = state.get("income_confidence", {})
        balance_conf = state.get("balance_confidence", {})

        all_facts = {**income, **balance}
        all_confidence = {**income_conf, **balance_conf}

        user_msg = f"""
Income Statement Facts:
{json.dumps(income, indent=2)}

Balance Sheet Facts:
{json.dumps(balance, indent=2)}

Field Confidence Scores:
{json.dumps(all_confidence, indent=2)}

Assess the quality of these extracted financial facts.
"""
        raw, tok_in, tok_out, cost = await self._call_llm(
            system=QUALITY_SYSTEM_PROMPT,
            user=user_msg,
            max_tokens=512,
        )
        clean = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        try:
            quality = json.loads(clean)
        except json.JSONDecodeError:
            quality = {"overall_confidence": 0.5, "is_coherent": False,
                       "anomalies": ["LLM parse error"], "critical_missing_fields": [],
                       "reextraction_recommended": False, "auditor_notes": "Parse error"}

        pkg_stream = state["pkg_stream"]
        pkg_version = await self._store.stream_version(pkg_stream)
        await self._store.append(pkg_stream, [
            QualityAssessmentCompleted(stream_id=pkg_stream, payload={
                "overall_confidence": quality.get("overall_confidence"),
                "is_coherent": quality.get("is_coherent"),
                "anomalies": quality.get("anomalies", []),
                "critical_missing_fields": quality.get("critical_missing_fields", []),
                "reextraction_recommended": quality.get("reextraction_recommended"),
                "auditor_notes": quality.get("auditor_notes"),
            }),
        ], expected_version=pkg_version)

        await self._record_node_execution("assess_quality", ["income_facts", "balance_facts"], ["quality"], int((time.time()-t0)*1000),
                                          llm_tokens_input=tok_in, llm_tokens_output=tok_out, llm_cost_usd=cost)
        return {**state, "quality": quality, "_last_node": "assess_quality"}

    async def _node_write_output(self, state: dict) -> dict:
        t0 = time.time()
        pkg_stream = state["pkg_stream"]
        app_id = state["application_id"]
        pkg_version = await self._store.stream_version(pkg_stream)

        await self._store.append(pkg_stream, [
            PackageReadyForAnalysis(stream_id=pkg_stream, payload={
                "package_id": state["pkg_id"],
                "application_id": app_id,
                "is_coherent": state["quality"].get("is_coherent"),
            }),
        ], expected_version=pkg_version)

        # Trigger CreditAnalysisAgent
        loan_stream = f"loan-{app_id}"
        loan_version = await self._store.stream_version(loan_stream)
        credit_record_id = str(uuid.uuid4())
        await self._store.append(loan_stream, [
            CreditAnalysisRequested(stream_id=loan_stream, payload={
                "credit_record_id": credit_record_id,
                "document_package_id": state["pkg_id"],
            }),
        ], expected_version=loan_version)

        await self._record_node_execution("write_output", ["quality"], [], int((time.time()-t0)*1000))
        return {**state, "_last_node": "write_output"}


# =============================================================================
# Agent 3 — FraudDetectionAgent
# =============================================================================

FRAUD_SYSTEM_PROMPT = """You are a financial fraud detection analyst.
Given extracted current-year financial figures and 3-year historical data, identify anomalous patterns.

Return JSON:
{
  "anomalies": [
    {
      "anomaly_type": "<string>",
      "description": "<string>",
      "severity": <float 0.0-1.0>,
      "evidence": "<string>"
    }
  ],
  "fraud_score": <float 0.0-1.0>,
  "summary": "<1-2 sentence summary>"
}

Severity weights: critical=0.4, high=0.25, medium=0.15, low=0.05
fraud_score = sum of severity weights, capped at 1.0
"""


class FraudDetectionAgent(BaseApexAgent):
    agent_type = "fraud"

    def _build_graph(self):
        graph = StateGraph(dict)
        graph.add_node("validate_inputs", self._node_validate_inputs)
        graph.add_node("open_fraud_record", self._node_open_fraud_record)
        graph.add_node("load_facts", self._node_load_facts)
        graph.add_node("cross_reference_registry", self._node_cross_reference)
        graph.add_node("analyze_fraud_patterns", self._node_analyze_patterns)
        graph.add_node("write_output", self._node_write_output)

        graph.set_entry_point("validate_inputs")
        graph.add_edge("validate_inputs", "open_fraud_record")
        graph.add_edge("open_fraud_record", "load_facts")
        graph.add_edge("load_facts", "cross_reference_registry")
        graph.add_edge("cross_reference_registry", "analyze_fraud_patterns")
        graph.add_edge("analyze_fraud_patterns", "write_output")
        graph.add_edge("write_output", END)
        return graph.compile()

    async def process(self, application_id: str, prior_session_id: str = None) -> dict:
        await self._start_session(application_id, prior_session_id)
        graph = self._build_graph()
        state = {"application_id": application_id}
        try:
            result = await graph.ainvoke(state)
            await self._end_session(next_agent="compliance")
            return result
        except Exception as e:
            await self._fail_session(type(e).__name__, str(e), state.get("_last_node", "unknown"))
            raise

    async def _node_validate_inputs(self, state: dict) -> dict:
        t0 = time.time()
        app_id = state["application_id"]
        loan_events = await self._store.load_stream(f"loan-{app_id}")

        company_id = None
        doc_pkg_id = None
        fraud_id = None
        for ev in loan_events:
            if ev.event_type == "ApplicationSubmitted":
                company_id = ev.payload.get("company_id")
            if ev.event_type == "DocumentUploadRequested":
                doc_pkg_id = ev.payload.get("document_package_id")
            if ev.event_type == "CreditAnalysisRequested":
                doc_pkg_id = ev.payload.get("document_package_id", doc_pkg_id)
            if ev.event_type == "FraudScreeningRequested":
                fraud_id = ev.payload.get("fraud_screening_id", fraud_id)

        await self._append_session_event(AgentInputValidated(
            stream_id=self._session_stream_id,
            payload={"inputs_validated": ["loan_stream", "company_id", "doc_pkg_id"],
                     "validation_duration_ms": int((time.time()-t0)*1000)},
        ))
        await self._record_node_execution(
            "validate_inputs",
            ["application_id"],
            ["company_id", "doc_pkg_id", "fraud_id"],
            int((time.time()-t0)*1000),
        )
        return {
            **state,
            "company_id": company_id,
            "doc_pkg_id": doc_pkg_id,
            "fraud_id": fraud_id,
            "loan_events": loan_events,
            "_last_node": "validate_inputs",
        }

    async def _node_open_fraud_record(self, state: dict) -> dict:
        t0 = time.time()
        fraud_id = state.get("fraud_id") or str(uuid.uuid4())
        fraud_stream = f"fraud-{fraud_id}"

        async def _build_open_event(version):
            if version > 0:
                return []
            return [FraudScreeningInitiated(stream_id=fraud_stream, payload={
                "fraud_screening_id": fraud_id,
                "application_id": state["application_id"],
            })]

        await self._append_with_occ_retry(fraud_stream, _build_open_event)
        await self._record_node_execution("open_fraud_record", ["application_id"], ["fraud_stream"], int((time.time()-t0)*1000))
        return {**state, "fraud_id": fraud_id, "fraud_stream": fraud_stream, "_last_node": "open_fraud_record"}

    async def _node_load_facts(self, state: dict) -> dict:
        t0 = time.time()
        doc_events = await self._store.load_stream(f"docpkg-{state['doc_pkg_id']}")
        await self._record_tool_call("load_docpkg_stream", f"doc_pkg_id={state['doc_pkg_id']}", f"{len(doc_events)} events", int((time.time()-t0)*1000))

        current_facts = {}
        for ev in doc_events:
            if ev.event_type == "ExtractionCompleted":
                current_facts.update(ev.payload.get("facts", {}))

        await self._record_node_execution("load_facts", ["doc_pkg_id"], ["current_facts"], int((time.time()-t0)*1000))
        return {**state, "current_facts": current_facts, "_last_node": "load_facts"}

    async def _node_cross_reference(self, state: dict) -> dict:
        t0 = time.time()
        company_id = state["company_id"]

        t_reg = time.time()
        history = await self._registry.get_financial_history(company_id)
        await self._record_tool_call("query_applicant_registry", f"company_id={company_id} financials", f"{len(history)} years", int((time.time()-t_reg)*1000))

        # Compute year-over-year deltas
        deltas = {}
        if history and len(history) >= 2:
            curr = history[0]
            prev = history[1]
            for field in ["total_revenue", "ebitda", "gross_margin"]:
                c = curr.get(field)
                p = prev.get(field)
                if c and p and p != 0:
                    deltas[f"{field}_yoy_pct"] = round((c - p) / abs(p) * 100, 2)

        await self._record_node_execution("cross_reference_registry", ["company_id"], ["history", "deltas"], int((time.time()-t0)*1000))
        return {**state, "financial_history": history, "yoy_deltas": deltas, "_last_node": "cross_reference_registry"}

    async def _node_analyze_patterns(self, state: dict) -> dict:
        t0 = time.time()
        user_msg = f"""
Current Year Extracted Facts:
{json.dumps(state['current_facts'], indent=2, default=lambda o: float(o) if hasattr(o, '__float__') else str(o))}

3-Year Historical Data:
{json.dumps(state['financial_history'], indent=2, default=lambda o: float(o) if hasattr(o, '__float__') else str(o))}

Year-over-Year Deltas:
{json.dumps(state['yoy_deltas'], indent=2, default=lambda o: float(o) if hasattr(o, '__float__') else str(o))}

Identify any anomalous patterns that could indicate fraud or misrepresentation.
"""
        raw, tok_in, tok_out, cost = await self._call_llm(FRAUD_SYSTEM_PROMPT, user_msg, max_tokens=1024)
        clean = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        try:
            result = json.loads(clean)
        except json.JSONDecodeError:
            result = {"anomalies": [], "fraud_score": 0.0, "summary": "Parse error — defaulting to no anomalies"}

        await self._record_node_execution("analyze_fraud_patterns", ["current_facts", "financial_history"], ["fraud_result"], int((time.time()-t0)*1000),
                                          llm_tokens_input=tok_in, llm_tokens_output=tok_out, llm_cost_usd=cost)
        return {**state, "fraud_result": result, "_last_node": "analyze_fraud_patterns"}

    async def _node_write_output(self, state: dict) -> dict:
        t0 = time.time()
        fraud_stream = state["fraud_stream"]
        result = state["fraud_result"]
        fraud_score = result.get("fraud_score", 0.0)

        fraud_version = await self._store.stream_version(fraud_stream)
        anomaly_events = []
        for anomaly in result.get("anomalies", []):
            anomaly_events.append(FraudAnomalyDetected(
                stream_id=fraud_stream,
                payload={"anomaly_type": anomaly.get("anomaly_type"), "description": anomaly.get("description"),
                         "severity": anomaly.get("severity"), "evidence": anomaly.get("evidence")},
            ))

        if anomaly_events:
            await self._store.append(fraud_stream, anomaly_events, expected_version=fraud_version)
            fraud_version += len(anomaly_events)

        if fraud_score > 0.60:
            risk_level = "HIGH"
            recommendation = "DECLINE"
        elif fraud_score >= 0.30:
            risk_level = "MEDIUM"
            recommendation = "FLAG_FOR_REVIEW"
        else:
            risk_level = "LOW"
            recommendation = "CLEAR"

        await self._store.append(fraud_stream, [
            FraudScreeningCompleted(stream_id=fraud_stream, payload={
                "fraud_screening_id": state["fraud_id"],
                "fraud_score": fraud_score,
                "risk_level": risk_level,
                "anomalies_found": len(result.get("anomalies", [])),
                "recommendation": recommendation,
                "summary": result.get("summary"),
            }),
        ], expected_version=fraud_version)

        # Trigger compliance
        loan_stream = f"loan-{state['application_id']}"
        loan_version = await self._store.stream_version(loan_stream)
        compliance_id = str(uuid.uuid4())
        await self._store.append(loan_stream, [
            ComplianceCheckRequested(stream_id=loan_stream, payload={
                "compliance_record_id": compliance_id,
                "fraud_screening_id": state["fraud_id"],
            }),
        ], expected_version=loan_version)

        await self._record_node_execution("write_output", ["fraud_result"], [], int((time.time()-t0)*1000))
        return {**state, "compliance_id": compliance_id, "_last_node": "write_output"}


# =============================================================================
# Agent 4 — ComplianceAgent
# =============================================================================

class ComplianceAgent(BaseApexAgent):
    agent_type = "compliance"

    def _build_graph(self):
        graph = StateGraph(dict)
        graph.add_node("validate_inputs", self._node_validate_inputs)
        graph.add_node("open_compliance_record", self._node_open_record)
        graph.add_node("load_external_data", self._node_load_data)
        graph.add_node("evaluate_reg001", self._node_eval_reg001)
        graph.add_node("evaluate_reg002", self._node_eval_reg002)
        graph.add_node("evaluate_reg003", self._node_eval_reg003)
        graph.add_node("evaluate_reg004", self._node_eval_reg004)
        graph.add_node("evaluate_reg005", self._node_eval_reg005)
        graph.add_node("evaluate_reg006", self._node_eval_reg006)
        graph.add_node("write_output", self._node_write_output)

        graph.set_entry_point("validate_inputs")
        graph.add_edge("validate_inputs", "open_compliance_record")
        graph.add_edge("open_compliance_record", "load_external_data")
        graph.add_edge("load_external_data", "evaluate_reg001")

        def _next_or_block(next_node):
            def _fn(s):
                return "write_output" if s.get("hard_block") else next_node
            return _fn

        graph.add_conditional_edges("evaluate_reg001", _next_or_block("evaluate_reg002"), {"evaluate_reg002": "evaluate_reg002", "write_output": "write_output"})
        graph.add_conditional_edges("evaluate_reg002", _next_or_block("evaluate_reg003"), {"evaluate_reg003": "evaluate_reg003", "write_output": "write_output"})
        graph.add_conditional_edges("evaluate_reg003", _next_or_block("evaluate_reg004"), {"evaluate_reg004": "evaluate_reg004", "write_output": "write_output"})
        graph.add_conditional_edges("evaluate_reg004", _next_or_block("evaluate_reg005"), {"evaluate_reg005": "evaluate_reg005", "write_output": "write_output"})
        graph.add_conditional_edges("evaluate_reg005", _next_or_block("evaluate_reg006"), {"evaluate_reg006": "evaluate_reg006", "write_output": "write_output"})
        graph.add_edge("evaluate_reg006", "write_output")
        graph.add_edge("write_output", END)
        return graph.compile()

    async def process(self, application_id: str, prior_session_id: str = None) -> dict:
        await self._start_session(application_id, prior_session_id)
        graph = self._build_graph()
        state = {"application_id": application_id, "hard_block": False, "rules_evaluated": 0, "rule_results": []}
        try:
            result = await graph.ainvoke(state)
            await self._end_session(next_agent="orchestrator")
            return result
        except Exception as e:
            await self._fail_session(type(e).__name__, str(e), state.get("_last_node", "unknown"))
            raise

    async def _node_validate_inputs(self, state: dict) -> dict:
        t0 = time.time()
        loan_events = await self._store.load_stream(f"loan-{state['application_id']}")
        compliance_id = None
        for ev in loan_events:
            if ev.event_type == "ComplianceCheckRequested":
                compliance_id = ev.payload.get("compliance_record_id", compliance_id)

        await self._append_session_event(AgentInputValidated(
            stream_id=self._session_stream_id,
            payload={"inputs_validated": ["application_id"], "validation_duration_ms": 0},
        ))
        await self._record_node_execution("validate_inputs", ["application_id"], ["compliance_id"], int((time.time()-t0)*1000))
        return {**state, "compliance_id": compliance_id, "_last_node": "validate_inputs"}

    async def _node_open_record(self, state: dict) -> dict:
        t0 = time.time()
        compliance_id = state.get("compliance_id") or str(uuid.uuid4())
        comp_stream = f"compliance-{compliance_id}"

        async def _build_open_event(version):
            if version > 0:
                return []
            return [ComplianceCheckInitiated(stream_id=comp_stream, payload={
                "compliance_record_id": compliance_id,
                "application_id": state["application_id"],
            })]

        await self._append_with_occ_retry(comp_stream, _build_open_event)
        await self._record_node_execution("open_compliance_record", [], ["comp_stream"], int((time.time()-t0)*1000))
        return {**state, "compliance_id": compliance_id, "comp_stream": comp_stream, "_last_node": "open_compliance_record"}

    async def _node_load_data(self, state: dict) -> dict:
        t0 = time.time()
        loan_events = await self._store.load_stream(f"loan-{state['application_id']}")
        company_id = next((ev.payload.get("company_id") for ev in loan_events if ev.event_type == "ApplicationSubmitted"), None)

        t_reg = time.time()
        company = await self._registry.get_company_profile(company_id)
        flags = await self._registry.get_compliance_flags(company_id)
        await self._record_tool_call("query_applicant_registry", f"company_id={company_id}", f"profile + {len(flags)} flags", int((time.time()-t_reg)*1000))

        requested_amount = None
        for ev in loan_events:
            if ev.event_type == "ApplicationSubmitted":
                requested_amount = ev.payload.get("requested_amount_usd")

        await self._record_node_execution("load_external_data", ["application_id"], ["company_profile", "flags"], int((time.time()-t0)*1000))
        return {**state, "company": company, "flags": flags, "requested_amount": requested_amount, "_last_node": "load_external_data"}

    async def _append_rule_result(self, state: dict, rule_id: str, rule_name: str, passed: bool, is_hard: bool = False, note: str = None):
        comp_stream = state["comp_stream"]
        version = await self._store.stream_version(comp_stream)
        if note:
            ev = ComplianceRuleNoted(stream_id=comp_stream, payload={
                "rule_id": rule_id, "rule_name": rule_name, "note": note, "note_type": "CRA_CONSIDERATION",
            })
        elif passed:
            ev = ComplianceRulePassed(stream_id=comp_stream, payload={
                "rule_id": rule_id, "rule_name": rule_name, "is_hard_block": is_hard,
            })
        else:
            ev = ComplianceRuleFailed(stream_id=comp_stream, payload={
                "rule_id": rule_id, "rule_name": rule_name, "is_hard_block": is_hard,
            })
        await self._store.append(comp_stream, [ev], expected_version=version)

    async def _node_eval_reg001(self, state: dict) -> dict:
        t0 = time.time()
        flags = state["flags"]
        passes = not any(f.get("flag_type") == "AML_WATCH" and f.get("is_active") for f in flags)
        await self._append_rule_result(state, "REG-001", "Bank Secrecy Act Check", passes, is_hard=False)
        await self._record_node_execution("evaluate_reg001", ["flags"], ["hard_block"], int((time.time()-t0)*1000))
        rule_results = state["rule_results"] + [{"rule_id": "REG-001", "passed": passes}]
        return {**state, "rules_evaluated": state["rules_evaluated"] + 1, "rule_results": rule_results, "_last_node": "evaluate_reg001"}

    async def _node_eval_reg002(self, state: dict) -> dict:
        t0 = time.time()
        flags = state["flags"]
        passes = not any(f.get("flag_type") == "SANCTIONS_REVIEW" and f.get("is_active") for f in flags)
        await self._append_rule_result(state, "REG-002", "OFAC Sanctions Screening", passes, is_hard=True)
        await self._record_node_execution("evaluate_reg002", ["flags"], ["hard_block"], int((time.time()-t0)*1000))
        rule_results = state["rule_results"] + [{"rule_id": "REG-002", "passed": passes}]
        return {**state, "hard_block": not passes, "rules_evaluated": state["rules_evaluated"] + 1, "rule_results": rule_results, "_last_node": "evaluate_reg002"}

    async def _node_eval_reg003(self, state: dict) -> dict:
        t0 = time.time()
        company = state["company"]
        passes = company.get("jurisdiction") != "MT"
        await self._append_rule_result(state, "REG-003", "Jurisdiction Lending Eligibility", passes, is_hard=True)
        await self._record_node_execution("evaluate_reg003", ["company"], ["hard_block"], int((time.time()-t0)*1000))
        rule_results = state["rule_results"] + [{"rule_id": "REG-003", "passed": passes}]
        return {**state, "hard_block": not passes, "rules_evaluated": state["rules_evaluated"] + 1, "rule_results": rule_results, "_last_node": "evaluate_reg003"}

    async def _node_eval_reg004(self, state: dict) -> dict:
        t0 = time.time()
        company = state["company"]
        amount = state.get("requested_amount") or 0
        is_sole_prop = company.get("legal_type") == "Sole Proprietor"
        passes = not (is_sole_prop and amount > 250_000)
        await self._append_rule_result(state, "REG-004", "Legal Entity Type Eligibility", passes, is_hard=False)
        await self._record_node_execution("evaluate_reg004", ["company"], [], int((time.time()-t0)*1000))
        rule_results = state["rule_results"] + [{"rule_id": "REG-004", "passed": passes}]
        return {**state, "rules_evaluated": state["rules_evaluated"] + 1, "rule_results": rule_results, "_last_node": "evaluate_reg004"}

    async def _node_eval_reg005(self, state: dict) -> dict:
        t0 = time.time()
        company = state["company"]
        import datetime
        current_year = datetime.datetime.now().year
        founded = company.get("founded_year", current_year)
        passes = (current_year - founded) >= 2
        await self._append_rule_result(state, "REG-005", "Minimum Operating History", passes, is_hard=True)
        await self._record_node_execution("evaluate_reg005", ["company"], ["hard_block"], int((time.time()-t0)*1000))
        rule_results = state["rule_results"] + [{"rule_id": "REG-005", "passed": passes}]
        return {**state, "hard_block": not passes, "rules_evaluated": state["rules_evaluated"] + 1, "rule_results": rule_results, "_last_node": "evaluate_reg005"}

    async def _node_eval_reg006(self, state: dict) -> dict:
        t0 = time.time()
        await self._append_rule_result(state, "REG-006", "CRA Community Reinvestment Act", True, is_hard=False, note="CRA_CONSIDERATION")
        await self._record_node_execution("evaluate_reg006", [], [], int((time.time()-t0)*1000))
        rule_results = state["rule_results"] + [{"rule_id": "REG-006", "noted": True}]
        return {**state, "rules_evaluated": state["rules_evaluated"] + 1, "rule_results": rule_results, "_last_node": "evaluate_reg006"}

    async def _node_write_output(self, state: dict) -> dict:
        t0 = time.time()
        comp_stream = state["comp_stream"]
        hard_block = state.get("hard_block", False)
        rule_results = state.get("rule_results", [])

        # Determine overall verdict
        if hard_block:
            verdict = "BLOCKED"
        elif any(not r.get("passed", True) for r in rule_results if "passed" in r):
            verdict = "CONDITIONAL"
        else:
            verdict = "CLEAR"

        version = await self._store.stream_version(comp_stream)
        await self._store.append(comp_stream, [
            ComplianceCheckCompleted(stream_id=comp_stream, payload={
                "compliance_record_id": state["compliance_id"],
                "overall_verdict": verdict,
                "has_hard_block": hard_block,
                "rules_evaluated": state["rules_evaluated"],
                "rule_results": rule_results,
            }),
        ], expected_version=version)

        # If hard block, immediately decline application
        loan_stream = f"loan-{state['application_id']}"
        loan_version = await self._store.stream_version(loan_stream)
        if hard_block:
            failed_rules = [r["rule_id"] for r in rule_results if not r.get("passed", True)]
            await self._store.append(loan_stream, [
                ApplicationDeclined(stream_id=loan_stream, payload={
                    "decline_reasons": failed_rules,
                    "adverse_action_notice_required": True,
                    "declined_by": "compliance_hard_block",
                    "compliance_record_id": state["compliance_id"],
                }),
            ], expected_version=loan_version)
        else:
            # Trigger DecisionOrchestrator
            await self._store.append(loan_stream, [
                DecisionRequested(stream_id=loan_stream, payload={
                    "compliance_record_id": state["compliance_id"],
                }),
            ], expected_version=loan_version)

        await self._record_node_execution("write_output", ["rule_results", "hard_block"], [], int((time.time()-t0)*1000))
        return {**state, "_last_node": "write_output"}


# =============================================================================
# Agent 5 — DecisionOrchestratorAgent
# =============================================================================

ORCHESTRATOR_SYSTEM_PROMPT = """You are the final decision orchestrator for Apex Financial Services.
You receive the results of credit analysis, fraud screening, and compliance checks.
Synthesise these into a final recommendation.

Return JSON:
{
  "executive_summary": "<3-5 sentence summary for the loan file>",
  "key_risks": ["<risk 1>", ...],
  "recommendation": "APPROVE" | "DECLINE" | "REFER",
  "confidence": <float 0.0-1.0>,
  "rationale": "<brief rationale>"
}

IMPORTANT:
- You cannot override hard compliance blocks — those are handled separately.
- If confidence < 0.60, set recommendation to REFER.
- Base your recommendation on the provided data only.
"""


class DecisionOrchestratorAgent(BaseApexAgent):
    agent_type = "orchestrator"

    def _build_graph(self):
        graph = StateGraph(dict)
        graph.add_node("validate_inputs", self._node_validate_inputs)
        graph.add_node("load_credit", self._node_load_credit)
        graph.add_node("load_fraud", self._node_load_fraud)
        graph.add_node("load_compliance", self._node_load_compliance)
        graph.add_node("synthesize_decision", self._node_synthesize)
        graph.add_node("apply_hard_constraints", self._node_hard_constraints)
        graph.add_node("write_output", self._node_write_output)

        graph.set_entry_point("validate_inputs")
        graph.add_edge("validate_inputs", "load_credit")
        graph.add_edge("load_credit", "load_fraud")
        graph.add_edge("load_fraud", "load_compliance")
        graph.add_edge("load_compliance", "synthesize_decision")
        graph.add_edge("synthesize_decision", "apply_hard_constraints")
        graph.add_edge("apply_hard_constraints", "write_output")
        graph.add_edge("write_output", END)
        return graph.compile()

    async def process(self, application_id: str, prior_session_id: str = None) -> dict:
        await self._start_session(application_id, prior_session_id)
        graph = self._build_graph()
        state = {"application_id": application_id}
        try:
            result = await graph.ainvoke(state)
            await self._end_session(next_agent=None)
            return result
        except Exception as e:
            await self._fail_session(type(e).__name__, str(e), state.get("_last_node", "unknown"))
            raise

    async def _node_validate_inputs(self, state: dict) -> dict:
        t0 = time.time()
        loan_events = await self._store.load_stream(f"loan-{state['application_id']}")
        credit_id = fraud_id = compliance_id = None
        for ev in loan_events:
            if ev.event_type == "FraudScreeningRequested":
                fraud_id = ev.payload.get("fraud_screening_id")
            if ev.event_type == "ComplianceCheckRequested":
                compliance_id = ev.payload.get("compliance_record_id")
            if ev.event_type == "CreditAnalysisRequested":
                credit_id = ev.payload.get("credit_record_id")

        await self._append_session_event(AgentInputValidated(
            stream_id=self._session_stream_id,
            payload={"inputs_validated": ["loan_stream", "credit_id", "fraud_id", "compliance_id"],
                     "validation_duration_ms": int((time.time()-t0)*1000)},
        ))
        await self._record_node_execution("validate_inputs", ["application_id"], ["credit_id", "fraud_id", "compliance_id"], int((time.time()-t0)*1000))
        return {**state, "loan_events": loan_events, "credit_id": credit_id, "fraud_id": fraud_id, "compliance_id": compliance_id, "_last_node": "validate_inputs"}

    async def _node_load_credit(self, state: dict) -> dict:
        t0 = time.time()
        credit_events = await self._store.load_stream(f"credit-{state['credit_id']}")
        credit_result = {}
        for ev in credit_events:
            if ev.event_type == "CreditAnalysisCompleted":
                credit_result = ev.payload.get("decision", {})
        await self._record_node_execution("load_credit", ["credit_id"], ["credit_result"], int((time.time()-t0)*1000))
        return {**state, "credit_result": credit_result, "_last_node": "load_credit"}

    async def _node_load_fraud(self, state: dict) -> dict:
        t0 = time.time()
        fraud_events = await self._store.load_stream(f"fraud-{state['fraud_id']}")
        fraud_result = {}
        for ev in fraud_events:
            if ev.event_type == "FraudScreeningCompleted":
                fraud_result = ev.payload
        await self._record_node_execution("load_fraud", ["fraud_id"], ["fraud_result"], int((time.time()-t0)*1000))
        return {**state, "fraud_result": fraud_result, "_last_node": "load_fraud"}

    async def _node_load_compliance(self, state: dict) -> dict:
        t0 = time.time()
        comp_events = await self._store.load_stream(f"compliance-{state['compliance_id']}")
        comp_result = {}
        for ev in comp_events:
            if ev.event_type == "ComplianceCheckCompleted":
                comp_result = ev.payload
        await self._record_node_execution("load_compliance", ["compliance_id"], ["comp_result"], int((time.time()-t0)*1000))
        return {**state, "comp_result": comp_result, "_last_node": "load_compliance"}

    async def _node_synthesize(self, state: dict) -> dict:
        t0 = time.time()
        user_msg = f"""
## Credit Analysis Result
{json.dumps(state['credit_result'], indent=2)}

## Fraud Screening Result
{json.dumps(state['fraud_result'], indent=2)}

## Compliance Check Result
{json.dumps(state['comp_result'], indent=2)}

Synthesise these results into a final loan decision recommendation.
"""
        raw, tok_in, tok_out, cost = await self._call_llm(ORCHESTRATOR_SYSTEM_PROMPT, user_msg, max_tokens=1024)
        clean = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        try:
            orch_decision = json.loads(clean)
        except json.JSONDecodeError:
            orch_decision = {"executive_summary": "Parse error", "key_risks": ["Parse error"],
                             "recommendation": "REFER", "confidence": 0.3, "rationale": "Parse error"}

        await self._record_node_execution("synthesize_decision", ["credit_result", "fraud_result", "comp_result"], ["orch_decision"], int((time.time()-t0)*1000),
                                          llm_tokens_input=tok_in, llm_tokens_output=tok_out, llm_cost_usd=cost)
        return {**state, "orch_decision": orch_decision, "_last_node": "synthesize_decision"}

    async def _node_hard_constraints(self, state: dict) -> dict:
        t0 = time.time()
        decision = dict(state["orch_decision"])
        credit = state["credit_result"]
        fraud = state["fraud_result"]
        comp = state["comp_result"]

        # Hard constraint: compliance BLOCKED → DECLINE
        if comp.get("overall_verdict") == "BLOCKED":
            decision["recommendation"] = "DECLINE"
            decision.setdefault("key_risks", []).append("Compliance hard block")

        # Hard constraint: HIGH credit risk should default to DECLINE.
        if credit.get("risk_tier") == "HIGH":
            decision["recommendation"] = "DECLINE"
            decision.setdefault("key_risks", []).append("Credit risk tier HIGH")

        # Hard constraint: confidence < 0.60 should prevent auto-approval.
        if decision.get("confidence", 1.0) < 0.60 and decision.get("recommendation") == "APPROVE":
            decision["recommendation"] = "REFER"

        # Hard constraint: high fraud score should block auto-approval, but
        # should not override an explicit DECLINE recommendation.
        if fraud.get("fraud_score", 0.0) > 0.60 and decision.get("recommendation") == "APPROVE":
            decision["recommendation"] = "REFER"
            decision.setdefault("key_risks", []).append(f"Fraud score {fraud.get('fraud_score'):.2f} exceeds threshold")

        # For adverse fraud outcomes, force a conservative decline decision.
        if fraud.get("risk_level") in {"MEDIUM", "HIGH"}:
            decision["recommendation"] = "DECLINE"
            decision.setdefault("key_risks", []).append(
                f"Fraud risk level {fraud.get('risk_level')} requires decline"
            )

        await self._record_node_execution("apply_hard_constraints", ["orch_decision"], ["orch_decision"], int((time.time()-t0)*1000))
        return {**state, "orch_decision": decision, "_last_node": "apply_hard_constraints"}

    async def _node_write_output(self, state: dict) -> dict:
        t0 = time.time()
        decision = state["orch_decision"]
        app_id = state["application_id"]
        loan_stream = f"loan-{app_id}"

        # Aggregate rule: confidence floor prevents low-confidence auto-approval.
        from src.ledger.domain.loan_application import LoanApplicationAggregate
        recommendation = decision.get("recommendation", "REFER")
        confidence = decision.get("confidence", 0.5)
        if recommendation == "APPROVE":
            recommendation = LoanApplicationAggregate.enforce_confidence_floor(confidence, recommendation)

        loan_version = await self._store.stream_version(loan_stream)

        # Append DecisionGenerated
        await self._store.append(loan_stream, [
            DecisionGenerated(stream_id=loan_stream, payload={
                "recommendation": recommendation,
                "confidence": confidence,
                "executive_summary": decision.get("executive_summary"),
                "key_risks": decision.get("key_risks", []),
                "rationale": decision.get("rationale"),
            }),
        ], expected_version=loan_version)
        loan_version += 1

        if recommendation == "APPROVE":
            requested = None
            for ev in state["loan_events"]:
                if ev.event_type == "ApplicationSubmitted":
                    requested = ev.payload.get("requested_amount_usd")
            await self._store.append(loan_stream, [
                ApplicationApproved(stream_id=loan_stream, payload={
                    "approved_amount_usd": requested,
                    "conditions": [],
                    "decision_session": self._session_id,
                }),
            ], expected_version=loan_version)

        elif recommendation == "DECLINE":
            await self._store.append(loan_stream, [
                HumanReviewRequested(stream_id=loan_stream, payload={
                    "reason": "Orchestrator recommended DECLINE — human override permitted",
                    "decision_session": self._session_id,
                }),
            ], expected_version=loan_version)

        elif recommendation == "REFER":
            await self._store.append(loan_stream, [
                HumanReviewRequested(stream_id=loan_stream, payload={
                    "reason": "Low confidence or fraud/compliance flags — human review required",
                    "decision_session": self._session_id,
                }),
            ], expected_version=loan_version)

        await self._record_node_execution("write_output", ["orch_decision"], [], int((time.time()-t0)*1000))
        return {**state, "_last_node": "write_output"}


# =============================================================================
# Stub extraction helpers (used when Week 3 pipeline not installed)
# =============================================================================

def _stub_extract_income(path: str) -> tuple[dict, dict, list]:
    """Minimal stub — returns placeholder facts so pipeline doesn't crash without Week 3."""
    facts = {
        "total_revenue": None,
        "cost_of_goods_sold": None,
        "gross_profit": None,
        "operating_expenses": None,
        "ebitda": None,
        "net_income": None,
        "gross_margin": None,
    }
    confidence = {k: 0.0 for k in facts}
    notes = ["Week 3 pipeline not installed — using stub extraction"]
    return facts, confidence, notes


def _stub_extract_balance(path: str) -> tuple[dict, dict, list]:
    facts = {
        "total_assets": None,
        "total_liabilities": None,
        "total_equity": None,
        "current_assets": None,
        "current_liabilities": None,
        "debt_to_equity": None,
    }
    confidence = {k: 0.0 for k in facts}
    notes = ["Week 3 pipeline not installed — using stub extraction"]
    return facts, confidence, notes
