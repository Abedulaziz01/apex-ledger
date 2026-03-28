import asyncio
import pytest
from src.ledger.core.models import BaseEvent
from src.ledger.core.exceptions import OptimisticConcurrencyError


@pytest.mark.asyncio
async def test_append_new_stream(store):
    events = [BaseEvent(event_type="LoanApplicationCreated", event_data={"amount": 5000})]
    written = await store.append("loan-001", events, expected_version=0)
    assert written[-1].stream_position == 1


@pytest.mark.asyncio
async def test_load_stream_returns_events(store):
    events = [
        BaseEvent(event_type="LoanApplicationCreated", event_data={"amount": 5000}),
        BaseEvent(event_type="CreditCheckPassed", event_data={"score": 720}),
    ]
    await store.append("loan-002", events, expected_version=0)
    loaded = await store.load_stream("loan-002")
    assert len(loaded) == 2
    assert loaded[0].event_type == "LoanApplicationCreated"
    assert loaded[1].event_type == "CreditCheckPassed"


@pytest.mark.asyncio
async def test_stream_version(store):
    events = [BaseEvent(event_type="LoanApplicationCreated")]
    await store.append("loan-003", events, expected_version=0)
    v = await store.stream_version("loan-003")
    assert v == 1


@pytest.mark.asyncio
async def test_wrong_expected_version_raises(store):
    events = [BaseEvent(event_type="LoanApplicationCreated")]
    await store.append("loan-004", events, expected_version=0)

    with pytest.raises(OptimisticConcurrencyError) as exc_info:
        await store.append("loan-004", [BaseEvent(event_type="Foo")], expected_version=0)

    assert exc_info.value.expected == 0
    assert exc_info.value.actual == 1


@pytest.mark.asyncio
async def test_load_all_global_log(store):
    await store.append("loan-005", [BaseEvent(event_type="A")], expected_version=0)
    await store.append("loan-006", [BaseEvent(event_type="B")], expected_version=0)
    all_events = await store.load_all()
    assert len(all_events) == 2


@pytest.mark.asyncio
async def test_get_stream_metadata(store):
    await store.append("loan-007", [BaseEvent(event_type="X")], expected_version=0)
    meta = await store.get_stream_metadata("loan-007")
    assert meta is not None
    assert meta.stream_id == "loan-007"
    assert meta.current_version == 1


@pytest.mark.asyncio
async def test_get_stream_metadata_missing_stream(store):
    meta = await store.get_stream_metadata("does-not-exist")
    assert meta is None


@pytest.mark.asyncio
async def test_double_decision_concurrency(store):
    """
    The critical test: two tasks append to the same stream at version=3.
    Exactly one must win. One must get OptimisticConcurrencyError.
    Final stream must have exactly 4 events, not 5.
    """
    # Set up stream at version 3
    setup_events = [
        BaseEvent(event_type="LoanApplicationCreated"),
        BaseEvent(event_type="CreditCheckPassed"),
        BaseEvent(event_type="UnderwritingStarted"),
    ]
    await store.append("loan-concurrent", setup_events, expected_version=0)

    # Two agents both try to append at expected_version=3
    agent_a = BaseEvent(event_type="CreditDecisionMade", event_data={"decision": "approved"})
    agent_b = BaseEvent(event_type="CreditDecisionMade", event_data={"decision": "declined"})

    results = {"wins": 0, "conflicts": 0}

    async def try_append(event):
        try:
            await store.append("loan-concurrent", [event], expected_version=3)
            results["wins"] += 1
        except OptimisticConcurrencyError:
            results["conflicts"] += 1

    await asyncio.gather(
        try_append(agent_a),
        try_append(agent_b),
    )

    assert results["wins"] == 1, "Exactly one agent should win"
    assert results["conflicts"] == 1, "Exactly one agent should get a conflict error"

    final_events = await store.load_stream("loan-concurrent")
    assert len(final_events) == 4, f"Expected 4 events, got {len(final_events)}"
