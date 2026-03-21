# TRP1 — The Ledger

Agentic Event Store & Enterprise Audit Infrastructure  
Phase Coverage: **Phase 1 (Event Store Core) + Phase 2 (Domain Logic)**

---

## 1. Prerequisites

Install all required tools before running the project.

### System Requirements

- Python **3.11+**
- PostgreSQL **14+**
- Git
- (Recommended) `uv` for dependency management

### Verify Installation

```bash
python --version
psql --version
git --version
```

2. Project Setup
   2.1 Clone Repository
   git clone <https://github.com/Abedulaziz01/apex-ledger.git>
   cd <apex-ledger>
   2.2 Create Virtual Environment

Using uv (recommended):

```bash
uv venv
source .venv/bin/activate # Linux / macOS
.venv\Scripts\activate # Windows
```

2.3 Install Dependencies

```bash
uv pip install -r requirements.txt
```

OR (if using pyproject):

```bash
uv pip install -e .
```

3. Database Setup
   3.1 Create PostgreSQL Database

```bash
CREATE DATABASE ledger_db;
```

3.2 Configure Connection

Set environment variable:

```bash
export DATABASE_URL=postgresql://<user>:<password>@localhost:5432/ledger_db
```

4. Run Migrations (Schema Setup)

The schema is defined in:

```bash
src/schema.sql
```

Apply it using psql:

```bash
psql -U <user> -d ledger_db -f src/schema.sql
```

4.1 Verify Tables

```bash
\dt
```

Expected tables:

- events
- event_streams
- projection_checkpoints
- outbox

5. Running the Application (Core Validation)

At this stage, the system supports:

- Event append with optimistic concurrency
- Stream loading and replay
- Aggregate reconstruction
- Command handler execution

You can test manually via Python REPL:

```bash
python
```

```python
from src.event_store import EventStore

store = EventStore()
```

6. Running Tests
   6.1 Run Full Test Suite

```bash
pytest -v
```

### Project Structure

```bash
src/
├── schema.sql
├── event_store.py
├── models/
│ └── events.py
├── aggregates/
│ ├── loan_application.py
│ └── agent_session.py
├── commands/
│ └── handlers.py

tests/
└── test_concurrency.py
```

## Key Features Implemented (Interim)

# Event Store Core

- Append-only event storage
- Stream-based partitioning
- Global ordering via global_position
- Optimistic concurrency (expected_version)
- Transactional outbox writes

### Domain Logic

- Aggregate reconstruction via event replay
- LoanApplication state machine
- AgentSession (Gas Town pattern enforcement)
- Command handler pattern:

```bash
load → validate → determine → append
```
