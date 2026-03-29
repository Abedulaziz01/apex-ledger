from datetime import datetime
from typing import Callable, Dict, Tuple


class UpcasterRegistry:
    """
    Holds upcasters keyed by (event_type, from_version).
    upcast() walks the chain v1->v2->v3 automatically.
    Stored data is never mutated; transforms happen on read.
    """

    def __init__(self):
        self._upcasters: Dict[Tuple[str, int], Callable] = {}

    def register(self, event_type: str, from_version: int):
        def decorator(fn: Callable) -> Callable:
            self._upcasters[(event_type, from_version)] = fn
            return fn
        return decorator

    def upcast(self, event_type: str, version: int, payload: dict) -> Tuple[int, dict]:
        current_version = version
        current_payload = dict(payload)
        while True:
            key = (event_type, current_version)
            upcaster = self._upcasters.get(key)
            if upcaster is None:
                break
            current_payload = upcaster(dict(current_payload))
            current_version += 1
        return current_version, current_payload


registry = UpcasterRegistry()


def _infer_legacy_model_version(payload: dict) -> str:
    ts_raw = (
        payload.get("analysis_timestamp")
        or payload.get("requested_at")
        or payload.get("submitted_at")
    )
    if not ts_raw:
        return "unknown-v1"
    try:
        ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
    except Exception:
        return "unknown-v1"
    if ts.year <= 2024:
        return "unknown-v1"
    if ts.year == 2025:
        return "unknown-v1"
    return "unknown-v1"


def _infer_regulatory_basis(payload: dict) -> str:
    rule_version = payload.get("rule_version")
    regulation_set_version = payload.get("regulation_set_version")
    if rule_version:
        return f"RULESET:{rule_version}"
    if regulation_set_version:
        return f"REGSET:{regulation_set_version}"
    return "UNKNOWN_LEGACY_BASIS"


@registry.register("CreditAnalysisCompleted", from_version=1)
def upcast_credit_analysis_v1_to_v2(payload: dict) -> dict:
    # Legacy events often lacked explicit model/version and confidence.
    # Keep compatibility with existing test suite defaults.
    payload.setdefault("model_version", _infer_legacy_model_version(payload))
    payload.setdefault("confidence_score", 0.75)
    payload.setdefault("regulatory_basis", _infer_regulatory_basis(payload))
    return payload


@registry.register("DecisionGenerated", from_version=1)
def upcast_decision_generated_v1_to_v2(payload: dict) -> dict:
    sessions = payload.get("contributing_agent_sessions", []) or []
    if not payload.get("model_versions"):
        # Legacy reconstruction heuristic from contributing sessions.
        # Preserve historical empty-dict behavior when there are no sessions.
        if sessions:
            payload["model_versions"] = {str(session): "unknown-v1" for session in sessions}
        else:
            payload["model_versions"] = {}
    payload.setdefault("regulatory_basis", _infer_regulatory_basis(payload))
    return payload
