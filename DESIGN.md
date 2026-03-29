# Apex Ledger Design

This document explains the architectural decisions behind Apex Ledger, including event modeling, concurrency controls, agent orchestration, and operational constraints.

## 1. Scope and Goals

Apex Ledger is designed as an event-sourced lending core with deterministic auditability and agent-driven workflow execution.

Primary goals:

- preserve complete decision history through append-only streams
- support concurrent processing with explicit conflict detection
- provide deterministic policy guardrails around probabilistic LLM outputs
- keep regulatory evidence reproducible through artifacts and replay

Non-goals:

- replacing a full BI/reporting warehouse
- acting as a generic message broker
- storing mutable "current state" as a source of truth

## 2. Design Principles

1. Events are source of truth.
2. State is derived, never overwritten.
3. Conflicts are explicit (OCC), not hidden.
4. Agent outputs are constrained by deterministic policy logic.
5. Test narratives define acceptance behavior for end-to-end correctness.

## 3. High-Level Architecture

Core layers:

- `core`: event store primitives, model contracts, OCC
- `schema`: canonical event types used across agents/tests
- `agents`: workflow orchestration and policy application
- `domain`: aggregate invariants and transition rules
- `projections`: read-side materializations and lag tracking
- `mcp_server`: external tool/resource API for orchestration clients

Event flow (simplified):

```text
loan submit -> doc processing -> credit -> fraud -> compliance -> orchestrator -> human override/finalize
```

## 4. Stream and Data Model

### 4.1 Stream Taxonomy

- `loan-{application_id}`: lifecycle decisions and final outcomes
- `docpkg-{document_package_id}`: extraction and quality assessments
- `credit-{credit_record_id}`: credit analysis context and outcome
- `fraud-{fraud_screening_id}`: fraud anomalies and screening verdict
- `compliance-{compliance_record_id}`: rule-level compliance evaluation
- `agent-{agent_type}-{session_id}`: telemetry, tool calls, recovery context

### 4.2 Event Shape

Canonical schema events include:

- stable `event_type`
- `stream_id`
- `payload` (business data)
- `metadata` (event id, timestamp, causation/correlation)

The design separates business payload from operational metadata to preserve replay compatibility.

### 4.3 Versioning Semantics

Two event store semantics are supported in tests/runtime:

- DB-backed store with stream versions tracked in `event_streams.current_version`
- in-memory store for phase-level tests with zero-based stream positions

Compatibility notes:

- tests may assert on `stream_position` directly
- append return values are normalized where APIs expect version integers

### 4.4 Projection Model

Projections are read-side views that consume append-only events.

Benefits:

- query speed for operational reads
- isolation from write model
- measurable lag and recoverable rebuild behavior

## 5. Concurrency and Reliability Strategy

### 5.1 OCC as First-Class Control

All critical appends use expected-version checks to prevent silent overwrite during parallel agent execution.

Design intent:

- one writer wins on stale expectation
- losing writer gets explicit `OptimisticConcurrencyError`
- caller decides retry/reload semantics

### 5.2 Idempotent Open-Record Behavior

Agent record-open steps are designed to be safe under duplicate invocations:

- if stream already exists, no duplicate "open" event is required
- concurrent sessions can converge on same stream id in narrative scenarios

### 5.3 Deadlock Handling

DB deadlocks are treated as transient infrastructure contention, not business failure.

Mitigation pattern:

- bounded retry with short exponential backoff
- preserve original error when max retries reached

### 5.4 Deterministic Policy Guards

Because LLM output can vary, deterministic post-processing enforces critical risk policies.

Examples:

- confidence floors and caps
- missing EBITDA confidence cap
- hard-block compliance behavior
- conservative recommendation normalization for override workflows

This keeps acceptance tests stable while allowing model-backed reasoning.

## 6. Agent Workflow Design

### 6.1 Document Processing Agent

Responsibilities:

- validate document package context
- emit extraction events for income/balance inputs
- emit quality assessment output for downstream risk controls

Key invariant:

- downstream agents must consume the same package lineage used in loan request flow.

### 6.2 Credit Analysis Agent

Responsibilities:

- load extracted facts + registry context
- produce credit decision proposal
- apply deterministic constraints before output event emission

Key invariant:

- credit decisions written for the requested credit record stream (not ad-hoc stream ids).

### 6.3 Fraud Detection Agent

Responsibilities:

- evaluate extracted facts against historical signals
- output anomaly indicators and risk-level recommendation

Key invariant:

- completed fraud outcome must exist on requested fraud stream id for orchestration and recovery tests.

### 6.4 Compliance Agent

Responsibilities:

- evaluate ordered rules (`REG-001 ... REG-006`)
- enforce hard-stop semantics where required
- emit compliance completion + route to decline or decision

Key invariant:

- rule events append to the requested compliance stream for deterministic audit lookup.

### 6.5 Decision Orchestrator Agent

Responsibilities:

- synthesize credit/fraud/compliance outcomes
- apply hard constraints and confidence gates
- emit final recommendation and route to human review when needed

Key invariant:

- recommendation policy must support deterministic narrative expectations for decline/override scenarios.

### 6.6 Narrative Mapping

- `NARR-01`: concurrent OCC collision behavior and dual completion expectations
- `NARR-02`: missing EBITDA quality impacts confidence cap
- `NARR-03`: crash + context reconstruction + no duplicate terminal fraud event
- `NARR-04`: jurisdictional hard block and adverse action decline
- `NARR-05`: decline recommendation followed by human override approval
