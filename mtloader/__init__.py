"""Multi-target loading operator: public API."""

from .config import And, Cmp, Or, Pred, QuarantineDef, RouteConfig, Rule, TargetDef
from .errors import (
    BatchError,
    BatchFailed,
    ConfigError,
    KeyConflict,
    LoaderError,
    PositionConflict,
    RetryableError,
    StaleExpectedPrevious,
    UnknownPartition,
)
from .loader import (
    ApplyResult,
    Batch,
    Delete,
    Insert,
    LogicalRow,
    MultiTargetLoader,
    RerouteResult,
    Snapshot,
    Update,
)
from .routing import Decision, biz_key_of, eval_pred, route_row

__all__ = [
    "And", "Cmp", "Or", "Pred", "QuarantineDef", "RouteConfig", "Rule", "TargetDef",
    "BatchError", "BatchFailed", "ConfigError", "KeyConflict", "LoaderError",
    "PositionConflict", "RetryableError", "StaleExpectedPrevious", "UnknownPartition",
    "ApplyResult", "Batch", "Delete", "Insert", "LogicalRow", "MultiTargetLoader",
    "RerouteResult", "Snapshot", "Update",
    "Decision", "biz_key_of", "eval_pred", "route_row",
]
