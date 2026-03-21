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
uv venv
source .venv/bin/activate # Linux / macOS
.venv\Scripts\activate # Windows
