"""
ledger/agents/base_agent.py
BaseApexAgent — provides Gas Town session management, per-node event recording,
tool call recording, OCC retry scaffolding, and LLM cost tracking.
All agents inherit from this. You implement the nodes; base handles everything else.
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Any, Optional
from openai import AsyncOpenAI
import anthropic

from src.ledger.core.event_store import AbstractEventStore, OptimisticConcurrencyError
from src.ledger.schema.events import (
    AgentSessionStarted, AgentNodeExecuted, AgentToolCalled,
    AgentOutputWritten, AgentSessionCompleted, AgentSessionFailed,
    AgentSessionRecovered, AgentInputValidated, AgentInputValidationFailed,
)

MAX_OCC_RETRIES = 5
OCC_BACKOFF_BASE = 0.1  # seconds


class BaseApexAgent:
    agent_type: str = "base"

    def __init__(
        self,
        event_store: AbstractEventStore,
        registry_client=None,
        model: str = "llama-3.1-8b-instant",
    ):
        self._store = event_store
        self._registry = registry_client
        self._model = model
        
        api_key = os.environ.get("GROQ_API_KEY")
        self._client = AsyncOpenAI(
            api_key=api_key,
            base_url="https://api.groq.com/openai/v1",
        )

        # Session state (populated in _start_session)
        self._session_id: Optional[str] = None
        self._session_stream_id: Optional[str] = None
        self._session_version: int = -1
        self._total_llm_calls: int = 0
        self._total_tokens_in: int = 0
        self._total_tokens_out: int = 0
        self._total_cost_usd: float = 0.0
        self._nodes_executed: int = 0

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    async def _start_session(
        self,
        application_id: str,
        prior_session_id: Optional[str] = None,
    ) -> str:
        """Gas Town: append AgentSessionStarted as very first event."""
        self._session_id = str(uuid.uuid4())
        self._session_stream_id = f"agent-{self.agent_type}-{self._session_id}"
        context_source = (
            f"prior_session_replay:{prior_session_id}"
            if prior_session_id
            else "fresh"
        )

        event = AgentSessionStarted(
            stream_id=self._session_stream_id,
            payload={
                "session_id": self._session_id,
                "agent_type": self.agent_type,
                "application_id": application_id,
                "model_version": self._model,
                "context_source": context_source,
                "context_token_count": 0,
            },
        )
        events = await self._store.append(self._session_stream_id, [event], expected_version=-1)
        self._session_version = events[0].stream_position
        return self._session_id

    async def _end_session(self, next_agent: Optional[str] = None):
        event = AgentSessionCompleted(
            stream_id=self._session_stream_id,
            payload={
                "session_id": self._session_id,
                "total_nodes_executed": self._nodes_executed,
                "total_llm_calls": self._total_llm_calls,
                "total_tokens_used": self._total_tokens_in + self._total_tokens_out,
                "total_cost_usd": round(self._total_cost_usd, 6),
                "next_agent_triggered": next_agent,
            },
        )
        await self._append_session_event(event)

    async def _fail_session(
        self,
        error_type: str,
        error_message: str,
        last_node: str,
        recoverable: bool = True,
    ):
        event = AgentSessionFailed(
            stream_id=self._session_stream_id,
            payload={
                "session_id": self._session_id,
                "error_type": error_type,
                "error_message": error_message,
                "last_successful_node": last_node,
                "recoverable": recoverable,
            },
        )
        await self._append_session_event(event)

    async def _append_session_event(self, event):
        events = await self._store.append(
            self._session_stream_id,
            [event],
            expected_version=self._session_version,
        )
        self._session_version = events[0].stream_position

    # ------------------------------------------------------------------
    # Per-node recording (call at END of every node)
    # ------------------------------------------------------------------

    async def _record_node_execution(
        self,
        node_name: str,
        input_keys: list[str],
        output_keys: list[str],
        duration_ms: int,
        llm_tokens_input: Optional[int] = None,
        llm_tokens_output: Optional[int] = None,
        llm_cost_usd: Optional[float] = None,
        llm_called: bool = False,
    ):
        self._nodes_executed += 1
        event = AgentNodeExecuted(
            stream_id=self._session_stream_id,
            payload={
                "session_id": self._session_id,
                "node_name": node_name,
                "node_sequence": self._nodes_executed,
                "input_keys": input_keys,
                "output_keys": output_keys,
                "llm_called": llm_called or (llm_tokens_input is not None),
                "llm_tokens_input": llm_tokens_input,
                "llm_tokens_output": llm_tokens_output,
                "llm_cost_usd": llm_cost_usd,
                "duration_ms": duration_ms,
            },
        )
        await self._append_session_event(event)

    async def _record_tool_call(
        self,
        tool: str,
        inp: str,
        out: str,
        ms: int,
    ):
        event = AgentToolCalled(
            stream_id=self._session_stream_id,
            payload={
                "session_id": self._session_id,
                "tool_name": tool,
                "tool_input_summary": inp,
                "tool_output_summary": out,
                "tool_duration_ms": ms,
            },
        )
        await self._append_session_event(event)

    # ------------------------------------------------------------------
    # LLM call helper
    # ------------------------------------------------------------------

    async def _call_llm(
        self,
        system: str,
        user: str,
        max_tokens: int = 1024,
    ) -> tuple[str, int, int, float]:
        """
        Returns (text, tokens_in, tokens_out, cost_usd).
        Cost estimate for Groq: $0.10/M input + $0.10/M output.
        """
        response = await self._client.chat.completions.create(
            model=self._model,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        tok_in = response.usage.prompt_tokens
        tok_out = response.usage.completion_tokens
        cost = (tok_in / 1_000_000 * 0.1) + (tok_out / 1_000_000 * 0.1)

        self._total_llm_calls += 1
        self._total_tokens_in += tok_in
        self._total_tokens_out += tok_out
        self._total_cost_usd += cost

        text = response.choices[0].message.content if response.choices else ""
        return text, tok_in, tok_out, cost

    # ------------------------------------------------------------------
    # OCC retry helper
    # ------------------------------------------------------------------

    async def _append_with_occ_retry(
        self,
        stream_id: str,
        events_fn,          # callable(current_version) -> list[BaseEvent]
        max_retries: int = MAX_OCC_RETRIES,
    ) -> list[BaseEvent]:
        """
        Retry loop for OCC conflicts. events_fn is called with the current
        stream version each attempt, so callers can re-read and re-decide.
        """
        for attempt in range(max_retries):
            version = await self._store.stream_version(stream_id)
            events = await events_fn(version)
            try:
                return await self._store.append(stream_id, events, expected_version=version)
            except OptimisticConcurrencyError:
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(OCC_BACKOFF_BASE * (2 ** attempt))
        raise OptimisticConcurrencyError("Max retries exceeded")

    # ------------------------------------------------------------------
    # Crash recovery
    # ------------------------------------------------------------------

    async def reconstruct_agent_context(self, crashed_session_id: str) -> dict:
        """
        Read the crashed session stream and return the last successful node
        so the new session can skip completed work.
        """
        stream_id = f"agent-{self.agent_type}-{crashed_session_id}"
        events = await self._store.load_stream(stream_id)

        last_node = None
        completed_nodes = []
        for ev in events:
            if ev.event_type == "AgentNodeExecuted":
                last_node = ev.payload.get("node_name")
                completed_nodes.append(last_node)

        return {
            "crashed_session_id": crashed_session_id,
            "last_successful_node": last_node,
            "completed_nodes": completed_nodes,
        }

    # ------------------------------------------------------------------
    # Simulate crash (for NARR-03 test)
    # ------------------------------------------------------------------

    async def _simulate_crash_after_node(self, node_name: str, state: dict):
        """Used in tests to simulate a mid-execution crash."""
        await self._fail_session(
            error_type="SimulatedCrash",
            error_message=f"Crash injected after node: {node_name}",
            last_node=node_name,
            recoverable=True,
        )
        raise RuntimeError(f"[SIMULATED CRASH] after {node_name}")