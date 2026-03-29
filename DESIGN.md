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
