from datetime import datetime, timedelta, timezone

import pytest

from src.regulatory.package import generate_regulatory_package


class _FakeConn:
    def __init__(self, data):
        self._data = data

    async def fetch(self, query, *args):
        stream_id = args[0]
        exam_ts = args[1]
        rows = self._data.get(stream_id, [])
        return [r for r in rows if r["created_at"] <= exam_ts]


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakePool:
    def __init__(self, data):
        self._conn = _FakeConn(data)

    def acquire(self):
        return _AcquireCtx(self._conn)


def _row(
    event_id: int,
    stream_id: str,
    version: int,
    event_type: str,
    payload: dict,
    created_at: datetime,
    metadata: dict | None = None,
):
    return {
        "id": event_id,
        "stream_id": stream_id,
        "version": version,
        "event_type": event_type,
        "event_data": payload,
        "metadata": metadata or {},
        "created_at": created_at,
    }


@pytest.mark.asyncio
async def test_generate_regulatory_package_contains_required_sections():
    now = datetime.now(timezone.utc)
    app_id = "PKG-001"
    loan_stream = f"loan-{app_id}"
    credit_id = "credit-123"
    compliance_id = "comp-123"

    data = {
        loan_stream: [
            _row(1, loan_stream, 1, "ApplicationSubmitted", {"application_id": app_id}, now),
            _row(
                2,
                loan_stream,
                2,
                "CreditAnalysisRequested",
                {"credit_record_id": credit_id},
                now + timedelta(seconds=1),
            ),
            _row(
                3,
                loan_stream,
                3,
                "ComplianceCheckRequested",
                {"compliance_record_id": compliance_id},
                now + timedelta(seconds=2),
            ),
            _row(
                4,
                loan_stream,
                4,
                "DecisionGenerated",
                {"recommendation": "DECLINE", "confidence_score": 0.67},
                now + timedelta(seconds=3),
            ),
        ],
        f"credit-{credit_id}": [
            _row(
                5,
                f"credit-{credit_id}",
                1,
                "CreditAnalysisCompleted",
                {"risk_tier": "HIGH"},
                now + timedelta(seconds=4),
            )
        ],
        f"compliance-{compliance_id}": [
            _row(
                6,
                f"compliance-{compliance_id}",
                1,
                "ComplianceRuleFailed",
                {"rule_id": "REG-003"},
                now + timedelta(seconds=5),
            )
        ],
    }
    pool = _FakePool(data)

    package = await generate_regulatory_package(pool, app_id, examination_date=now + timedelta(days=1))

    assert package["application_id"] == app_id
    assert "streams" in package
    assert "projection_states_at_examination" in package
    assert "integrity_verification" in package
    assert "narrative" in package
    assert package["projection_states_at_examination"]["application_summary"]["recommendation"] == "DECLINE"

