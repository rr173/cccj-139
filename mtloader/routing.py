"""Pure routing semantics: predicate evaluation and per-row decisions.

This module is the single source of truth for routing.  The SQL loader and
the independent in-memory test interpreter both call into it; it performs no
I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional

from .config import And, Cmp, Or, Pred, RouteConfig, _value_matches_type
from .digest import canonical_json
from .errors import LoaderError

# Quarantine reasons (stable tokens; may carry a ":<field>" suffix).
REASON_MULTI_MATCH = "route_multi_match"
REASON_NO_MATCH = "no_matching_rule"
REASON_MISSING_KEY = "missing_key"
REASON_TYPE_MISMATCH = "field_type_mismatch"


@dataclass(frozen=True)
class Decision:
    target: str            # business target name or the quarantine name
    quarantined: bool
    reason: Optional[str]  # set iff quarantined


def _compare(op: str, left: Any, right: Any) -> bool:
    if op == "eq":
        return left == right
    if op == "ne":
        return left != right
    if op == "lt":
        return left < right
    if op == "le":
        return left <= right
    if op == "gt":
        return left > right
    if op == "ge":
        return left >= right
    raise LoaderError(f"unknown predicate op {op!r}")


def eval_pred(pred: Pred, row: Mapping[str, Any]) -> bool:
    """Evaluate a predicate against a logical row.

    A comparison against a missing or NULL field is false (never raises);
    row values were type-checked against the declared field types before
    routing, so same-type comparisons cannot raise TypeError here.
    """
    if isinstance(pred, Cmp):
        if pred.field not in row:
            return False
        value = row[pred.field]
        if value is None:
            return False
        return _compare(pred.op, value, pred.value)
    if isinstance(pred, And):
        return all(eval_pred(c, row) for c in pred.children)
    if isinstance(pred, Or):
        return any(eval_pred(c, row) for c in pred.children)
    raise LoaderError(f"unknown predicate {pred!r}")


def biz_key_of(cfg: RouteConfig, row: Mapping[str, Any]) -> Optional[str]:
    """Canonical business-key string, or None if any key field is missing/NULL."""
    values = []
    for f in cfg.key_fields:
        if f not in row or row[f] is None:
            return None
        values.append(row[f])
    return canonical_json(values)


def route_row(cfg: RouteConfig, row: Mapping[str, Any]) -> Decision:
    """Decide where one logical row belongs.

    A row is legal iff it matches exactly one routing rule (or, matching
    none, the default is a business target) and carries a complete business
    key.  A row matching *multiple* rules is a configuration conflict that
    must not be masked by rule order: it is isolated with reason
    ``route_multi_match``.
    """
    for fname, ftype in cfg.field_types.items():
        if fname in row and not _value_matches_type(ftype, row[fname]):
            return Decision(cfg.quarantine.name, True, f"{REASON_TYPE_MISMATCH}:{fname}")

    matches = [r for r in cfg.rules if eval_pred(r.pred, row)]
    if len(matches) > 1:
        return Decision(cfg.quarantine.name, True, REASON_MULTI_MATCH)
    target = matches[0].target if matches else cfg.default
    if target == cfg.quarantine.name:
        return Decision(target, True, REASON_NO_MATCH)
    if biz_key_of(cfg, row) is None:
        return Decision(cfg.quarantine.name, True, REASON_MISSING_KEY)
    return Decision(target, False, None)
