from typing import Callable, Dict, Tuple


class UpcasterRegistry:
    """
    Holds upcasters keyed by (event_type, from_version).
    upcast() walks the chain v1→v2→v3 automatically.
    Stored data is never touched — transformation happens at read time only.
    """

    def __init__(self):
        # key: (event_type, from_version)  value: callable(payload) -> payload
        self._upcasters: Dict[Tuple[str, int], Callable] = {}

    def register(self, event_type: str, from_version: int):
        """Decorator — register an upcaster for event_type from_version → from_version+1."""
        def decorator(fn: Callable) -> Callable:
            self._upcasters[(event_type, from_version)] = fn
            return fn
        return decorator

    def upcast(self, event_type: str, version: int, payload: dict) -> Tuple[int, dict]:
        """
        Walk the upcaster chain starting at version.
        Returns (final_version, upcasted_payload).
        Payload dict is never mutated — always copies.
        """
        current_version = version
        current_payload = dict(payload)  # copy — never mutate original

        while True:
            key = (event_type, current_version)
            upcaster = self._upcasters.get(key)
            if upcaster is None:
                break
            current_payload = upcaster(dict(current_payload))  # copy before passing
            current_version += 1

        return current_version, current_payload


# ── Singleton registry used by the event store ───────────────────────────────

registry = UpcasterRegistry()


# ── Upcaster 1: CreditAnalysisCompleted v1 → v2 ──────────────────────────────
# v1 fields: application_id, agent_id, session_id, risk_tier,
#             recommended_limit_usd, analysis_duration_ms, input_data_hash
# v2 adds:   model_version, confidence_score
# Inference strategy: model_version defaults to "unknown-v1" (pre-dates tracking);
# confidence_score defaults to 0.75 (conservative mid-tier assumption for
# historical LOW/MEDIUM risk decisions — documented in DOMAIN_NOTES).

@registry.register("CreditAnalysisCompleted", from_version=1)
def upcast_credit_analysis_v1_to_v2(payload: dict) -> dict:
    payload.setdefault("model_version", "unknown-v1")
    payload.setdefault("confidence_score", 0.75)
    return payload


# ── Upcaster 2: DecisionGenerated v1 → v2 ────────────────────────────────────
# v1 fields: application_id, orchestrator_agent_id, recommendation,
#             confidence_score, contributing_agent_sessions,
#             decision_basis_summary
# v2 adds:   model_versions{} dict
# Inference strategy: empty dict — v1 decisions predate per-model tracking;
# downstream consumers must treat empty model_versions as "legacy decision".

@registry.register("DecisionGenerated", from_version=1)
def upcast_decision_generated_v1_to_v2(payload: dict) -> dict:
    payload.setdefault("model_versions", {})
    return payload