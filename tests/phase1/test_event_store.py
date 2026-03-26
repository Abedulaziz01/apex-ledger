"""
tests/phase1/test_event_store.py
10 event store tests using InMemoryEventStore (no DB needed).
"""
import asyncio
import pytest

from src.ledger.core.event_store import InMemoryEventStore, OptimisticConcurrencyError
from src.ledger.domain.events import ApplicationSubmitted, DocumentUploaded, BaseEvent
@pytest.fixture
def store():
    return InMemoryEventStore()


async def test_append_new_stream(store):
    event = ApplicationSubmitted(stream_id="loan-001", payload={"company_id": "COMP-01"})
    written = await store.append("loan-001", [event], expected_version=-1)
    assert len(written) == 1
    assert written[0].stream_position == 0


async def test_stream_version_empty(store):
    v = await store.stream_version("nonexistent")
    assert v == -1


async def test_stream_version_after_append(store):
    e = ApplicationSubmitted(stream_id="loan-002", payload={})
    await store.append("loan-002", [e], expected_version=-1)
    v = await store.stream_version("loan-002")
    assert v == 0


async def test_load_stream_ordered(store):
    events = [
        ApplicationSubmitted(stream_id="loan-003", payload={"company_id": "A"}),
        DocumentUploaded(stream_id="loan-003", payload={"doc_type": "income"}),
    ]
    await store.append("loan-003", [events[0]], expected_version=-1)
    await store.append("loan-003", [events[1]], expected_version=0)
    loaded = await store.load_stream("loan-003")
    assert len(loaded) == 2
    assert loaded[0].event_type == "ApplicationSubmitted"
    assert loaded[1].event_type == "DocumentUploaded"
    assert loaded[0].stream_position == 0
    assert loaded[1].stream_position == 1


async def test_occ_wrong_expected_version(store):
    e = ApplicationSubmitted(stream_id="loan-004", payload={})
    await store.append("loan-004", [e], expected_version=-1)
    with pytest.raises(OptimisticConcurrencyError):
        # Expected -1 but actual is 0
        await store.append("loan-004", [e], expected_version=-1)


async def test_load_all_returns_all_events(store):
    await store.append("loan-005", [ApplicationSubmitted(stream_id="loan-005", payload={})], expected_version=-1)
    await store.append("loan-006", [ApplicationSubmitted(stream_id="loan-006", payload={})], expected_version=-1)
    all_events = await store.load_all()
    assert len(all_events) >= 2


async def test_load_all_after_position(store):
    await store.append("loan-007", [ApplicationSubmitted(stream_id="loan-007", payload={})], expected_version=-1)
    await store.append("loan-008", [ApplicationSubmitted(stream_id="loan-008", payload={})], expected_version=-1)
    all_events = await store.load_all()
    later = await store.load_all(after_position=0)
    assert len(later) < len(all_events)


async def test_multiple_events_in_single_append(store):
    events = [
        ApplicationSubmitted(stream_id="loan-009", payload={}),
        DocumentUploaded(stream_id="loan-009", payload={}),
    ]
    written = await store.append("loan-009", events, expected_version=-1)
    assert len(written) == 2
    assert written[0].stream_position == 0
    assert written[1].stream_position == 1


async def test_concurrent_double_append_exactly_one_succeeds():
    """
    Two concurrent appends to the same stream at version -1.
    Exactly one must succeed, the other must raise OptimisticConcurrencyError.
    """
    store = InMemoryEventStore()
    results = []

    async def try_append():
        try:
            e = ApplicationSubmitted(stream_id="loan-occ", payload={})
            await store.append("loan-occ", [e], expected_version=-1)
            results.append("success")
        except OptimisticConcurrencyError:
            results.append("occ_error")

    await asyncio.gather(try_append(), try_append())
    assert results.count("success") == 1
    assert results.count("occ_error") == 1


async def test_empty_events_list_noop(store):
    written = await store.append("loan-010", [], expected_version=-1)
    assert written == []
    v = await store.stream_version("loan-010")
    assert v == -1
