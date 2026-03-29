# Apex Ledger

Enterprise-grade, event-sourced lending infrastructure built for traceability, reliability, and agent-driven decisioning.

![Python](https://img.shields.io/badge/python-3.12%2B-blue)
![PostgreSQL](https://img.shields.io/badge/postgresql-14%2B-blue)
![Tests](https://img.shields.io/badge/tests-narratives%20%2B%20phases-green)
![Status](https://img.shields.io/badge/status-active-success)

## Quick Start

```powershell
uv venv
.\.venv\Scripts\Activate.ps1
uv pip install -e .
uv run pytest tests/test_narratives.py -v
```

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Why Apex Ledger](#why-apex-ledger)
3. [System Architecture](#system-architecture)
4. [Domain Streams](#domain-streams)
5. [Repository Layout](#repository-layout)
6. [Tech Stack](#tech-stack)
7. [Local Setup](#local-setup)
8. [Environment Configuration](#environment-configuration)
9. [Database and Runtime](#database-and-runtime)
10. [Running Tests](#running-tests)
11. [Narrative Validation](#narrative-validation)
12. [MCP Tools and Resources](#mcp-tools-and-resources)
13. [Scripts and Artifact Generation](#scripts-and-artifact-generation)
14. [Reliability and Safety Patterns](#reliability-and-safety-patterns)
15. [Troubleshooting](#troubleshooting)
16. [Contribution Workflow](#contribution-workflow)
17. [License](#license)

---

## Executive Summary

Apex Ledger models loan decisioning as immutable event streams.
Every lifecycle transition is captured as an auditable event, enabling:

- deterministic replay
- conflict-safe writes with optimistic concurrency control (OCC)
- projection-based read models
- policy-aware agent orchestration
- regulatory package generation

This repository includes full scenario narratives (`NARR-01` to `NARR-05`) and phase-level tests for event store behavior, domain rules, projections, integrity checks, and MCP integration.

---

## Why Apex Ledger

Traditional lending systems often sacrifice either auditability or speed.
Apex Ledger is designed to deliver both:

- **Auditability by default**: every change is append-only and replayable.
- **Operational resilience**: OCC, deadlock retries, and deterministic guards.
- **AI-ready workflows**: domain-safe agent decisions with explicit event outputs.
- **Regulatory friendliness**: compliance checkpoints and reproducible evidence artifacts.

---

## System Architecture

High-level flow:

1. Application submitted to `loan-*` stream.
2. Document package extracted and quality-assessed in `docpkg-*`.
3. Credit analysis writes to `credit-*`.
4. Fraud screening writes to `fraud-*`.
5. Compliance checks write to `compliance-*`.
6. Orchestrator emits final recommendation and human-review route.
7. Human override path can finalize approval/decline with explicit events.

The system is event-first: state is always derived from events, never mutated in-place.

### Sequence View

```text
ApplicationSubmitted -> loan-{id}
DocumentProcessingAgent -> docpkg-{id}
CreditAnalysisAgent -> credit-{id}
FraudDetectionAgent -> fraud-{id}
ComplianceAgent -> compliance-{id}
DecisionOrchestratorAgent -> loan-{id} DecisionGenerated
HumanReviewCompleted (optional) -> loan-{id}
```

### Testing Matrix

| Layer | Purpose | Example |
|---|---|---|
| Narrative | End-to-end acceptance flow | `tests/test_narratives.py` |
| Phase 1 | Event store/OCC contracts | `tests/phase1/test_event_store.py` |
| Phase 2 | Domain invariants/transitions | `tests/test_phase2_domain.py` |
| Phase 3 | Projection correctness | `tests/test_phase3_projections.py` |
| Phase 4 | Integrity/upcasting/memory | `tests/test_phase4_*` |
| MCP | Tool/resource contracts | `tests/test_mcp_tools.py` |

---

## Domain Streams

- `loan-{application_id}`
  - submission, decision, review, approval/decline events
- `docpkg-{document_package_id}`
  - extraction lifecycle, quality outputs
- `credit-{credit_record_id}`
  - credit rationale and policy-adjusted decisions
- `fraud-{fraud_screening_id}`
  - anomaly detections and risk outcomes
- `compliance-{compliance_record_id}`
  - rule-by-rule pass/fail/noted results and final verdict
- `agent-{agent_type}-{session_id}`
  - session lifecycle, tool calls, crash/recovery context

---

## Repository Layout

```text
src/ledger/
  agents/               # Agent logic (document, credit, fraud, compliance, orchestration)
  core/                 # Event store, models, exceptions
  domain/               # Aggregate rules and transitions
  projections/          # Read-side models and daemon
  schema/               # Canonical event schema
  mcp_server.py         # MCP tool/resource entrypoint

tests/
  phase1/               # Event store contract tests
  test_narratives.py    # End-to-end acceptance narratives
  test_mcp_tools.py     # MCP tool contract tests
  test_full_lifecycle_via_mcp.py
  ...

scripts/                # Utilities and demos
artifacts/              # Generated reports and regulatory bundles
```

---

## Tech Stack

- Python 3.12+
- PostgreSQL 14+
- asyncpg
- pydantic
- pytest + pytest-asyncio
- MCP server framework
- OpenAI-compatible client integrations (provider configurable)

---

## Local Setup

### 1) Clone

```powershell
git clone <your-repository-url>
cd apex-ledger
```

### 2) Create virtual environment

```powershell
uv venv
.\.venv\Scripts\Activate.ps1
```

### 3) Install dependencies

```powershell
uv pip install -e .
```

---

## Environment Configuration

Create `.env` in the project root.

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=apex_ledger
POSTGRES_USER=apex_user
POSTGRES_PASSWORD=apex_secret_password
DATABASE_URL=postgresql://apex_user:apex_secret_password@localhost:5433/apex_ledger

# Provider configuration (example shown for OpenAI-compatible Groq endpoint)
OPENAI_API_KEY=<your_key>
OPENAI_BASE_URL=https://api.groq.com/openai/v1
```

Notes:

- Keep secrets out of git.
- Rotate immediately if a key is exposed.
- Ensure the active shell has the expected environment loaded.

---

## Database and Runtime

Use local Postgres or docker-compose (if provided in your workflow).

Minimum checks before running tests:

```powershell
Test-NetConnection localhost -Port 5433
```

If connection fails, update `.env` or bring up the DB service.

---

## Running Tests

### Full suite

```powershell
uv run pytest --tb=no -q
```

### Narrative suite only

```powershell
uv run pytest tests/test_narratives.py -v
```

### Single narrative

```powershell
uv run pytest tests/test_narratives.py::test_narr05_human_override -v -s
```

### Save output to artifact file

```powershell
uv run pytest tests/test_narratives.py -v 2>&1 | tee artifacts\narrative_test_results.txt
```

### Pre-Release Checklist

```powershell
uv run pytest tests/test_narratives.py -v
uv run pytest --tb=no -q
uv run python scripts\generate_artifacts.py
```

Release gate checklist:

- narratives pass (`5/5`)
- full suite passes
- artifacts generated without encoding errors
- no secret values committed
- README and changelog updated

---

## Narrative Validation

`tests/test_narratives.py` is the primary acceptance gate:

- `NARR-01` concurrent OCC behavior
- `NARR-02` missing EBITDA handling and confidence cap
- `NARR-03` crash + recovery continuation
- `NARR-04` compliance hard block behavior
- `NARR-05` human override flow

These scenarios are intentionally cross-agent and event-sequence strict.

---

## MCP Tools and Resources

MCP entrypoint:

- `src/ledger/mcp_server.py`

Tool capabilities include:

- submit application
- record credit/fraud/compliance outcomes
- generate final decision
- record human review
- run integrity checks

Resource capabilities include:

- application summary and history
- compliance audit view
- projection lag/health
- agent session details

---

## Scripts and Artifact Generation

### Run utility scripts

For scripts importing `src...`, run in module mode:

```powershell
uv run python -m scripts.test_loan
```

Alternative:

```powershell
$env:PYTHONPATH="."
uv run python scripts\test_loan.py
```

### Generate artifacts

```powershell
uv run python scripts\generate_artifacts.py
```

Expected outputs:

- `artifacts/occ_collision_report.txt`
- `artifacts/projection_lag_report.txt`
- `artifacts/api_cost_report.txt`
- `artifacts/regulatory_package_NARR05.json`

---

## Reliability and Safety Patterns

- OCC on event appends
- idempotent stream-open behavior for concurrent flows
- deterministic policy constraints where LLM variance exists
- deadlock retry for contested DB append paths
- event integrity verification support

## Operational Notes

- OCC conflicts are expected in concurrent paths and should be handled, not ignored.
- DB deadlocks can occur under parallel writes; retry policies are built for this.
- Generated artifacts are excluded from pytest collection by configuration.
- Prefer module execution for scripts that import `src...`.

---

## Troubleshooting

### `ModuleNotFoundError: No module named 'src'`

Cause: running scripts directly from `scripts/` path context.

Fix:

```powershell
uv run python -m scripts.test_loan
```

### `openai.APIConnectionError: Connection error`

Cause: outbound network/provider access issue in current environment.

Check:

- API key loaded in shell
- base URL correct for provider
- firewall/VPN/proxy constraints

### `UnicodeEncodeError` on Windows when writing artifacts

Use UTF-8 file encoding and optionally set terminal code page:

```powershell
chcp 65001
```

### Pytest collects non-test files

Pytest config limits collection to `tests/` using:

- `testpaths = ["tests"]`
- `norecursedirs` excludes `artifacts`, `.pytest_cache`, `scripts`, etc.

---

## Contribution Workflow

1. Branch from main.
2. Keep each change scoped and test-backed.
3. Validate narratives and full suite:
   - `uv run pytest tests/test_narratives.py -v`
   - `uv run pytest --tb=no -q`
4. Generate required artifacts if needed.
5. Open PR with:
   - problem statement
   - change summary
   - test evidence
   - risk notes

---

## License

Add your preferred license file (for example `MIT`) in `LICENSE`.
