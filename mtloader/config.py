"""Route configuration: pure declarative data, fully validated.

A configuration never contains SQL fragments or executable code:

* identifiers (table / column / field names) must match ``[A-Za-z_][A-Za-z0-9_]{0,62}``
  and are *additionally* passed through ``psycopg2.sql.Identifier`` quoting;
* predicates are data ASTs built from typed comparisons combined with AND/OR;
* field types come from a fixed enum.

Everything is JSON-round-trippable so the active configuration can be stored
inside the database (``mt_meta`` / ``mt_route_history``) next to the data it
governs.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence, Tuple

from .digest import canonical_json, sha256_hex
from .errors import ConfigError

FIELD_TYPES = ("text", "int", "numeric", "bool")
OPS = ("eq", "ne", "lt", "le", "gt", "ge")
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


def _ident(kind: str, name: Any) -> str:
    if not isinstance(name, str) or not _IDENT_RE.match(name):
        raise ConfigError(f"{kind} must match {_IDENT_RE.pattern!r}, got {name!r}")
    return name


# ---------------------------------------------------------------------------
# Predicate AST (data only)
# ---------------------------------------------------------------------------


class Pred:
    """Base class of the predicate AST."""
    __slots__ = ()


@dataclass(frozen=True)
class Cmp(Pred):
    field: str
    op: str
    value: Any


@dataclass(frozen=True)
class And(Pred):
    children: Tuple[Pred, ...]


@dataclass(frozen=True)
class Or(Pred):
    children: Tuple[Pred, ...]


def pred_from_dict(d: Any) -> Pred:
    if not isinstance(d, Mapping) or len(d) != 1:
        raise ConfigError(f"predicate node must be a single-key object, got {d!r}")
    if "cmp" in d:
        c = d["cmp"]
        if not isinstance(c, Mapping) or set(c) != {"field", "op", "value"}:
            raise ConfigError(f'"cmp" must have field/op/value keys, got {c!r}')
        return Cmp(c["field"], c["op"], c["value"])
    if "and" in d:
        return And(tuple(pred_from_dict(x) for x in _seq("and", d["and"])))
    if "or" in d:
        return Or(tuple(pred_from_dict(x) for x in _seq("or", d["or"])))
    raise ConfigError(f"unknown predicate node {d!r}")


def _seq(kind: str, xs: Any) -> Sequence[Any]:
    if not isinstance(xs, (list, tuple)) or not xs:
        raise ConfigError(f'"{kind}" needs a non-empty list of predicates')
    return xs


def pred_to_dict(p: Pred) -> Any:
    if isinstance(p, Cmp):
        return {"cmp": {"field": p.field, "op": p.op, "value": p.value}}
    if isinstance(p, And):
        return {"and": [pred_to_dict(c) for c in p.children]}
    if isinstance(p, Or):
        return {"or": [pred_to_dict(c) for c in p.children]}
    raise ConfigError(f"unknown predicate {p!r}")


def _value_matches_type(ftype: str, value: Any) -> bool:
    if value is None:
        return True
    if ftype == "text":
        return isinstance(value, str)
    if ftype == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if ftype == "numeric":
        return (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(value)
        )
    if ftype == "bool":
        return isinstance(value, bool)
    raise ConfigError(f"unknown field type {ftype!r}")


# ---------------------------------------------------------------------------
# Config objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TargetDef:
    """A business target: physical table + logical->physical column map."""

    name: str
    table: str
    columns: Mapping[str, str]


@dataclass(frozen=True)
class QuarantineDef:
    """The isolation target.  Physical schema is fixed:
    (row_id text PK, reason text, data jsonb)."""

    name: str
    table: str


@dataclass(frozen=True)
class Rule:
    pred: Pred
    target: str


@dataclass(frozen=True)
class RouteConfig:
    field_types: Mapping[str, str]
    key_fields: Tuple[str, ...]
    targets: Tuple[TargetDef, ...]
    quarantine: QuarantineDef
    rules: Tuple[Rule, ...]
    default: str  # business target name or the quarantine name

    def __post_init__(self) -> None:
        ft = dict(self.field_types or {})
        if not ft:
            raise ConfigError("field_types must not be empty")
        for fname, ftype in ft.items():
            _ident("field name", fname)
            if ftype not in FIELD_TYPES:
                raise ConfigError(f"field {fname!r}: type must be one of {FIELD_TYPES}")
        object.__setattr__(self, "field_types", ft)

        key_fields = tuple(self.key_fields or ())
        if not key_fields:
            raise ConfigError("key_fields must not be empty")
        for k in key_fields:
            if k not in ft:
                raise ConfigError(f"key field {k!r} is not declared in field_types")
        if len(set(key_fields)) != len(key_fields):
            raise ConfigError("key_fields contain duplicates")
        object.__setattr__(self, "key_fields", key_fields)

        targets = tuple(self.targets or ())
        if not targets:
            raise ConfigError("at least one business target is required")
        names = set()
        tables = set()
        for t in targets:
            _ident("target name", t.name)
            _ident("table name", t.table)
            if t.name in names:
                raise ConfigError(f"duplicate target name {t.name!r}")
            names.add(t.name)
            if t.table in tables:
                raise ConfigError(f"table {t.table!r} is used by more than one target")
            tables.add(t.table)
            cols = dict(t.columns or {})
            if not cols:
                raise ConfigError(f"target {t.name!r} maps no columns")
            if len(set(cols.values())) != len(cols):
                raise ConfigError(f"target {t.name!r} maps two fields to one column")
            for logical, physical in cols.items():
                if logical not in ft:
                    raise ConfigError(
                        f"target {t.name!r} maps undeclared field {logical!r}"
                    )
                _ident("column name", physical)
            object.__setattr__(t, "columns", cols)
        object.__setattr__(self, "targets", targets)

        _ident("quarantine name", self.quarantine.name)
        _ident("quarantine table", self.quarantine.table)
        if self.quarantine.name in names:
            raise ConfigError("quarantine name collides with a business target")
        if self.quarantine.table in tables:
            raise ConfigError("quarantine table collides with a business table")

        rules = tuple(self.rules or ())
        for r in rules:
            if r.target not in names:
                raise ConfigError(
                    f"rule routes to unknown business target {r.target!r} "
                    "(route to quarantine via the default instead)"
                )
            self._validate_pred(r.pred, ft)
        object.__setattr__(self, "rules", rules)

        if self.default != self.quarantine.name and self.default not in names:
            raise ConfigError(f"default target {self.default!r} is not declared")

        object.__setattr__(
            self, "target_map", {t.name: t for t in targets}
        )

    # -- validation helpers -------------------------------------------------

    @classmethod
    def _validate_pred(cls, pred: Pred, ft: Mapping[str, str]) -> None:
        if isinstance(pred, Cmp):
            if pred.field not in ft:
                raise ConfigError(f"predicate uses undeclared field {pred.field!r}")
            if pred.op not in OPS:
                raise ConfigError(f"predicate op must be one of {OPS}")
            if pred.value is None:
                raise ConfigError("predicate value must not be null")
            if not _value_matches_type(ft[pred.field], pred.value):
                raise ConfigError(
                    f"predicate value {pred.value!r} does not match type "
                    f"{ft[pred.field]!r} of field {pred.field!r}"
                )
        elif isinstance(pred, (And, Or)):
            if not pred.children:
                raise ConfigError("and/or need at least one child")
            for c in pred.children:
                cls._validate_pred(c, ft)
        else:
            raise ConfigError(f"unknown predicate {pred!r}")

    # -- identity ------------------------------------------------------------

    def fingerprint(self) -> str:
        """Hash of the parts that reroute may NOT change."""
        return sha256_hex(
            canonical_json(
                {
                    "field_types": sorted(self.field_types.items()),
                    "key_fields": list(self.key_fields),
                    "quarantine": [self.quarantine.name, self.quarantine.table],
                    "targets": [
                        [t.name, t.table, sorted(t.columns.items())] for t in self.targets
                    ],
                }
            )
        )

    # -- serialization -------------------------------------------------------

    def to_dict(self) -> Any:
        return {
            "field_types": dict(self.field_types),
            "key_fields": list(self.key_fields),
            "targets": [
                {"name": t.name, "table": t.table, "columns": dict(t.columns)}
                for t in self.targets
            ],
            "quarantine": {"name": self.quarantine.name, "table": self.quarantine.table},
            "rules": [
                {"pred": pred_to_dict(r.pred), "target": r.target} for r in self.rules
            ],
            "default": self.default,
        }

    @staticmethod
    def from_dict(d: Mapping[str, Any]) -> "RouteConfig":
        try:
            return RouteConfig(
                field_types=d["field_types"],
                key_fields=tuple(d["key_fields"]),
                targets=tuple(
                    TargetDef(t["name"], t["table"], t["columns"]) for t in d["targets"]
                ),
                quarantine=QuarantineDef(
                    d["quarantine"]["name"], d["quarantine"]["table"]
                ),
                rules=tuple(
                    Rule(pred_from_dict(r["pred"]), r["target"]) for r in d["rules"]
                ),
                default=d["default"],
            )
        except (KeyError, TypeError) as exc:
            raise ConfigError(f"malformed config document: {exc!r}") from exc
