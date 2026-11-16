"""Multi-target loading operator.

One logical change stream is routed to several physical target tables plus a
quarantine table.  All target writes, the ownership ledger, the route
version, the batch digest and the partition position commit in **one**
PostgreSQL transaction; readers on other connections only ever see complete
batches (MVCC).  No local progress files are involved - the database is the
only source of truth.

See README.md for the full contract (concurrency, retries, privileges).
"""

from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

import psycopg2
import psycopg2.errors
import psycopg2.extras
from psycopg2 import sql
from psycopg2.extras import Json

from .config import RouteConfig
from .digest import canonical_json, sha256_hex
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
from .routing import Decision, biz_key_of, route_row

_LOG = logging.getLogger("mtloader")

#: Single global advisory-lock key serializing all writer operations
#: (apply / reroute / declare_partition).  Readers are never blocked.
GLOBAL_LOCK_KEY = 7885950780400726642  # 0x6D744C6F61646572, "mtLoader"

_PG_TYPE = {"text": "text", "int": "bigint", "numeric": "double precision", "bool": "boolean"}

META = "mt_meta"
PARTITIONS = "mt_partition_state"
BATCHES = "mt_applied_batches"
OWNERSHIP = "mt_ownership"
ROUTE_HISTORY = "mt_route_history"
OWNERSHIP_KEY_CONSTRAINT = "mt_ownership_biz_key_uniq"


# ---------------------------------------------------------------------------
# Batch model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Insert:
    row_id: str
    fields: Mapping[str, Any]  # full post-image


@dataclass(frozen=True)
class Update:
    row_id: str
    fields: Mapping[str, Any]  # full post-image; may change key/route fields


@dataclass(frozen=True)
class Delete:
    row_id: str


@dataclass(frozen=True)
class Batch:
    """One ordered logical change batch for one partition.

    ``position`` is strictly increasing per partition; the first batch must
    sit exactly at the declared start position.  ``expected_previous`` is the
    position the caller believes was applied last (None before the first
    batch) and is compared atomically at commit time.
    """

    partition: str
    position: int
    expected_previous: Optional[int]
    changes: Tuple[Any, ...]

    def digest(self) -> str:
        return batch_digest(self)


def _change_to_dict(change: Any) -> Any:
    if isinstance(change, Insert):
        return {"op": "insert", "row_id": change.row_id, "fields": change.fields}
    if isinstance(change, Update):
        return {"op": "update", "row_id": change.row_id, "fields": change.fields}
    if isinstance(change, Delete):
        return {"op": "delete", "row_id": change.row_id}
    raise BatchError(f"unknown change type {type(change).__name__}")


def batch_digest(batch: Batch) -> str:
    """Content digest over everything that defines the batch's effect."""
    return sha256_hex(
        canonical_json(
            {
                "partition": batch.partition,
                "position": batch.position,
                "expected_previous": batch.expected_previous,
                "changes": [_change_to_dict(c) for c in batch.changes],
            }
        )
    )


# ---------------------------------------------------------------------------
# Results / read models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ApplyResult:
    partition: str
    position: int
    digest: str
    commit_seq: int          # global, totally orders committed writer ops
    route_version: int
    already_applied: bool    # True => idempotent replay, zero side effects
    affected: int


@dataclass(frozen=True)
class RerouteResult:
    route_version: int
    commit_seq: int
    moved: int


@dataclass(frozen=True)
class LogicalRow:
    row_id: str
    target: str
    biz_key: Optional[str]
    fields: Mapping[str, Any]


@dataclass(frozen=True)
class Snapshot:
    """Consistent full read (single REPEATABLE READ transaction)."""

    logical: Tuple[LogicalRow, ...]
    targets: Mapping[str, Tuple[Mapping[str, Any], ...]]
    partitions: Mapping[str, Mapping[str, Any]]
    route_version: int


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


class MultiTargetLoader:
    """The loading operator.  Thread-safe; every call uses a fresh connection
    from ``conn_factory`` so concurrent callers serialize on the database.

    ``fault_hook(point)`` is a test-only probe invoked at "after_lock" and
    "before_commit" inside the writer transaction.  It exists solely to
    observe transaction boundaries (block, or raise to force a rollback);
    production code should leave it None.  There is no recovery ledger -
    a failed operation simply never commits.
    """

    def __init__(
        self,
        conn_factory: Callable[[], "psycopg2.extensions.connection"],
        *,
        fault_hook: Optional[Callable[[str], None]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._factory = conn_factory
        self._fault_hook = fault_hook
        self._log = logger or _LOG

    # -- connection helpers --------------------------------------------------

    def _connect(self):
        conn = self._factory()
        psycopg2.extras.register_default_jsonb(conn)
        return conn

    def _fault(self, point: str) -> None:
        if self._fault_hook is not None:
            self._fault_hook(point)

    @staticmethod
    def _lock(cur) -> None:
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (GLOBAL_LOCK_KEY,))

    # -- schema management ---------------------------------------------------

    def init_schema(
        self, config: RouteConfig, partitions: Mapping[str, int]
    ) -> None:
        """Create (idempotently) all loader and target tables and declare the
        initial route config plus per-partition start positions.

        Re-running with the identical config is a no-op; running with a
        *different* config on an initialized database raises ConfigError -
        evolve routing through :meth:`reroute` instead.
        """
        if not isinstance(config, RouteConfig):
            raise ConfigError("init_schema needs a RouteConfig")
        if not partitions:
            raise ConfigError("declare at least one partition")
        for name, start in partitions.items():
            if not isinstance(name, str) or not name:
                raise ConfigError(f"bad partition name {name!r}")
            if not isinstance(start, int) or isinstance(start, bool):
                raise ConfigError(f"bad start position for partition {name!r}")

        conn = self._connect()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {META} (
                        key text PRIMARY KEY,
                        value text NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS {PARTITIONS} (
                        partition text PRIMARY KEY,
                        start_position bigint NOT NULL,
                        last_position bigint,
                        last_digest text
                    );
                    CREATE TABLE IF NOT EXISTS {BATCHES} (
                        partition text NOT NULL,
                        position bigint NOT NULL,
                        digest text NOT NULL,
                        commit_seq bigint NOT NULL,
                        route_version bigint NOT NULL,
                        PRIMARY KEY (partition, position)
                    );
                    CREATE TABLE IF NOT EXISTS {OWNERSHIP} (
                        row_id text PRIMARY KEY,
                        target text NOT NULL,
                        biz_key text,
                        data jsonb NOT NULL,
                        CONSTRAINT {OWNERSHIP_KEY_CONSTRAINT}
                            UNIQUE (biz_key) DEFERRABLE INITIALLY DEFERRED
                    );
                    CREATE TABLE IF NOT EXISTS {ROUTE_HISTORY} (
                        route_version bigint PRIMARY KEY,
                        commit_seq bigint NOT NULL,
                        config jsonb NOT NULL
                    );
                    """
                )
                for key, value in (
                    ("route_version", "1"),
                    ("commit_seq", "0"),
                    ("schema_fingerprint", config.fingerprint()),
                    ("route_config", canonical_json(config.to_dict())),
                ):
                    cur.execute(
                        f"INSERT INTO {META} (key, value) VALUES (%s, %s)"
                        " ON CONFLICT (key) DO NOTHING",
                        (key, value),
                    )
                self._check_config_compatible(cur, config, initializing=True)
                self._create_or_verify_targets(cur, config)
                for name, start in partitions.items():
                    self._declare_partition(cur, name, start)
            self._log.info(
                "schema initialized: targets=%s quarantine=%s partitions=%s",
                [t.name for t in config.targets],
                config.quarantine.name,
                sorted(partitions),
            )
        finally:
            conn.close()

    def drop_schema(self, config: RouteConfig) -> None:
        """Drop every table this operator manages.  Development/test helper."""
        conn = self._connect()
        try:
            with conn:
                cur = conn.cursor()
                names = [META, PARTITIONS, BATCHES, OWNERSHIP, ROUTE_HISTORY]
                names += [t.table for t in config.targets]
                names.append(config.quarantine.table)
                for n in names:
                    cur.execute(
                        sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                            sql.Identifier(n)
                        )
                    )
        finally:
            conn.close()

    def _create_or_verify_targets(self, cur, config: RouteConfig) -> None:
        for t in config.targets:
            cols = [sql.SQL("row_id text PRIMARY KEY")]
            for logical, physical in sorted(t.columns.items()):
                cols.append(
                    sql.SQL("{} {}").format(
                        sql.Identifier(physical),
                        sql.SQL(_PG_TYPE[config.field_types[logical]]),
                    )
                )
            cur.execute(
                sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
                    sql.Identifier(t.table), sql.SQL(", ").join(cols)
                )
            )
            self._verify_columns(cur, t.table, {"row_id", *t.columns.values()})
        q = config.quarantine
        cur.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} ("
                "row_id text PRIMARY KEY, reason text NOT NULL, data jsonb NOT NULL)"
            ).format(sql.Identifier(q.table))
        )
        self._verify_columns(cur, q.table, {"row_id", "reason", "data"})

    @staticmethod
    def _verify_columns(cur, table: str, expected: set) -> None:
        cur.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_schema = current_schema() AND table_name = %s",
            (table,),
        )
        actual = {r[0] for r in cur.fetchall()}
        if actual != expected:
            raise ConfigError(
                f"table {table!r} exists with columns {sorted(actual)},"
                f" expected {sorted(expected)}"
            )

    def _check_config_compatible(
        self, cur, config: RouteConfig, initializing: bool = False
    ) -> None:
        cur.execute(f"SELECT value FROM {META} WHERE key = 'schema_fingerprint'")
        row = cur.fetchone()
        if row is None:
            raise ConfigError("schema is not initialized (run init_schema first)")
        if row[0] != config.fingerprint():
            raise ConfigError(
                "config targets/keys/field-types differ from the initialized"
                " schema; reroute() may only change rules/default"
            )
        if initializing:
            cur.execute(f"SELECT value FROM {META} WHERE key = 'route_config'")
            stored = RouteConfig.from_dict(json.loads(cur.fetchone()[0]))
            if stored.to_dict() != config.to_dict():
                raise ConfigError(
                    "database already initialized with different routing rules;"
                    " use reroute() to evolve them"
                )

    def _declare_partition(self, cur, partition: str, start: int) -> None:
        cur.execute(
            f"INSERT INTO {PARTITIONS} (partition, start_position)"
            " VALUES (%s, %s) ON CONFLICT (partition) DO NOTHING",
            (partition, start),
        )
        cur.execute(
            f"SELECT start_position FROM {PARTITIONS} WHERE partition = %s",
            (partition,),
        )
        existing = cur.fetchone()[0]
        if existing != start:
            raise ConfigError(
                f"partition {partition!r} already declared with start"
                f" {existing}, not {start}"
            )

    def declare_partition(self, partition: str, start_position: int) -> None:
        """Declare an additional partition after init."""
        conn = self._connect()
        try:
            with conn:
                cur = conn.cursor()
                self._lock(cur)
                if not isinstance(start_position, int) or isinstance(start_position, bool):
                    raise ConfigError("start_position must be an integer")
                self._declare_partition(cur, partition, start_position)
        finally:
            conn.close()

    # -- config access -------------------------------------------------------

    def _load_config(self, cur) -> RouteConfig:
        cur.execute(f"SELECT value FROM {META} WHERE key = 'route_config'")
        row = cur.fetchone()
        if row is None:
            raise ConfigError("schema is not initialized (run init_schema first)")
        return RouteConfig.from_dict(json.loads(row[0]))

    def current_config(self) -> RouteConfig:
        conn = self._connect()
        try:
            with conn:
                return self._load_config(conn.cursor())
        finally:
            conn.close()

    # -- apply ---------------------------------------------------------------

    def apply(self, batch: Batch) -> ApplyResult:
        """Apply one ordered batch atomically.  See module docstring/README."""
        self._validate_batch(batch)
        digest = batch.digest()
        conn = self._connect()
        try:
            with conn:
                cur = conn.cursor()
                self._lock(cur)
                self._fault("after_lock")

                cur.execute(
                    f"SELECT start_position, last_position FROM {PARTITIONS}"
                    " WHERE partition = %s",
                    (batch.partition,),
                )
                row = cur.fetchone()
                if row is None:
                    raise UnknownPartition(
                        f"partition {batch.partition!r} is not declared"
                    )
                start_position, last_position = row

                # Idempotent replay / position conflict.
                cur.execute(
                    f"SELECT digest, commit_seq, route_version FROM {BATCHES}"
                    " WHERE partition = %s AND position = %s",
                    (batch.partition, batch.position),
                )
                prev = cur.fetchone()
                if prev is not None:
                    if prev[0] == digest:
                        self._log.info(
                            "partition %s position %s already applied (replay,"
                            " no side effects)", batch.partition, batch.position,
                        )
                        return ApplyResult(
                            batch.partition, batch.position, digest,
                            prev[1], prev[2], True, 0,
                        )
                    raise PositionConflict(
                        f"partition {batch.partition!r} position {batch.position}"
                        " already holds a batch with a different content digest"
                    )
                if last_position is not None and batch.position <= last_position:
                    raise PositionConflict(
                        f"partition {batch.partition!r} position {batch.position}"
                        f" is not ahead of last applied position {last_position}"
                    )
                if batch.expected_previous != last_position:
                    raise StaleExpectedPrevious(
                        f"partition {batch.partition!r}: expected_previous="
                        f"{batch.expected_previous!r} but last applied position"
                        f" is {last_position!r}"
                    )
                if last_position is None and batch.position != start_position:
                    raise PositionConflict(
                        f"first batch for partition {batch.partition!r} must be"
                        f" at declared start position {start_position}, got"
                        f" {batch.position}"
                    )

                config = self._load_config(cur)
                route_version = self._route_version(cur)

                for change in batch.changes:
                    self._apply_change(cur, config, change)
                self._check_key_collisions(cur)

                # Atomic compare-and-set of the partition progress.
                cur.execute(
                    f"UPDATE {PARTITIONS} SET last_position = %s, last_digest = %s"
                    " WHERE partition = %s"
                    " AND last_position IS NOT DISTINCT FROM %s",
                    (
                        batch.position,
                        digest,
                        batch.partition,
                        batch.expected_previous,
                    ),
                )
                if cur.rowcount != 1:
                    raise StaleExpectedPrevious(
                        f"partition {batch.partition!r}: progress compare-and-set"
                        " failed (concurrent writer moved the position)"
                    )
                commit_seq = self._bump_seq(cur)
                cur.execute(
                    f"INSERT INTO {BATCHES}"
                    " (partition, position, digest, commit_seq, route_version)"
                    " VALUES (%s, %s, %s, %s, %s)",
                    (batch.partition, batch.position, digest, commit_seq, route_version),
                )
                self._fault("before_commit")
            self._log.info(
                "applied partition=%s position=%s changes=%d commit_seq=%d"
                " route_version=%d",
                batch.partition, batch.position, len(batch.changes),
                commit_seq, route_version,
            )
            return ApplyResult(
                batch.partition, batch.position, digest,
                commit_seq, route_version, False, len(batch.changes),
            )
        except (
            psycopg2.errors.SerializationFailure,
            psycopg2.errors.DeadlockDetected,
        ) as exc:
            raise RetryableError(
                f"{type(exc).__name__}: {self._pg_message(exc)}"
            ) from exc
        except psycopg2.errors.UniqueViolation as exc:
            if getattr(exc.diag, "constraint_name", None) == OWNERSHIP_KEY_CONSTRAINT:
                raise KeyConflict(
                    "business key uniqueness violated at commit (deferred"
                    " constraint backstop)"
                ) from exc
            raise BatchFailed(f"unique violation: {self._pg_message(exc)}") from exc
        except psycopg2.Error as exc:
            raise BatchFailed(
                f"database error ({type(exc).__name__}): {self._pg_message(exc)}"
            ) from exc
        finally:
            conn.close()

    def apply_with_retry(
        self,
        batch: Batch,
        *,
        max_attempts: int = 8,
        base_delay: float = 0.05,
        retry_on: Tuple[type, ...] = (
            RetryableError,
            psycopg2.OperationalError,
            psycopg2.InterfaceError,
        ),
    ) -> ApplyResult:
        """Retry wrapper implementing the documented retry convention.

        Only transient errors are retried, always with the *same* batch
        object: digest idempotency makes an ambiguous previous attempt
        (e.g. connection lost after commit) safe.  Deterministic
        rejections (KeyConflict, PositionConflict, ...) propagate at once.
        """
        attempt = 0
        while True:
            attempt += 1
            try:
                return self.apply(batch)
            except retry_on as exc:
                if attempt >= max_attempts:
                    raise
                delay = base_delay * (2 ** (attempt - 1))
                self._log.warning(
                    "apply attempt %d failed transiently (%s); retrying in %.2fs",
                    attempt, type(exc).__name__, delay,
                )
                time.sleep(delay)

    # -- apply internals -----------------------------------------------------

    @staticmethod
    def _validate_batch(batch: Batch) -> None:
        if not isinstance(batch.partition, str) or not batch.partition:
            raise BatchError("partition must be a non-empty string")
        for name, value in (("position", batch.position),
                            ("expected_previous", batch.expected_previous)):
            if value is not None and (not isinstance(value, int) or isinstance(value, bool)):
                raise BatchError(f"{name} must be an integer or None")
        if not isinstance(batch.changes, (tuple, list)):
            raise BatchError("changes must be a tuple/list")
        for change in batch.changes:
            if not isinstance(change, (Insert, Update, Delete)):
                raise BatchError(f"unknown change {change!r}")
            if not isinstance(change.row_id, str) or not change.row_id:
                raise BatchError("row_id must be a non-empty string")
            if isinstance(change, (Insert, Update)):
                MultiTargetLoader._validate_fields(change.fields)

    @staticmethod
    def _validate_fields(fields: Any) -> None:
        if not isinstance(fields, Mapping):
            raise BatchError("fields must be a mapping")
        for key, value in fields.items():
            if not isinstance(key, str) or not key:
                raise BatchError("field names must be non-empty strings")
            if value is None or isinstance(value, (str, bool, int)):
                continue
            if isinstance(value, float):
                if not math.isfinite(value):
                    raise BatchError("non-finite floats are not supported")
                continue
            raise BatchError(
                f"field {key!r}: value of type {type(value).__name__} is not"
                " a JSON scalar"
            )

    def _apply_change(self, cur, config: RouteConfig, change: Any) -> None:
        if isinstance(change, Insert):
            cur.execute(
                f"SELECT 1 FROM {OWNERSHIP} WHERE row_id = %s", (change.row_id,)
            )
            if cur.fetchone() is not None:
                raise BatchError(f"insert of existing row_id {change.row_id!r}")
            decision = route_row(config, change.fields)
            self._write_physical(cur, config, change.row_id, change.fields, decision)
            self._upsert_ownership(cur, config, change.row_id, change.fields, decision)
        elif isinstance(change, Update):
            old_target = self._ownership_target(cur, change.row_id)
            if old_target is None:
                raise BatchError(f"update of unknown row_id {change.row_id!r}")
            decision = route_row(config, change.fields)
            self._delete_physical(cur, config, old_target, change.row_id)
            self._write_physical(cur, config, change.row_id, change.fields, decision)
            self._upsert_ownership(cur, config, change.row_id, change.fields, decision)
        elif isinstance(change, Delete):
            old_target = self._ownership_target(cur, change.row_id)
            if old_target is None:
                raise BatchError(f"delete of unknown row_id {change.row_id!r}")
            self._delete_physical(cur, config, old_target, change.row_id)
            cur.execute(f"DELETE FROM {OWNERSHIP} WHERE row_id = %s", (change.row_id,))

    def _ownership_target(self, cur, row_id: str) -> Optional[str]:
        cur.execute(f"SELECT target FROM {OWNERSHIP} WHERE row_id = %s", (row_id,))
        row = cur.fetchone()
        return row[0] if row else None

    def _table_for(self, config: RouteConfig, target_name: str) -> str:
        if target_name == config.quarantine.name:
            return config.quarantine.table
        target = config.target_map.get(target_name)
        if target is None:
            raise LoaderError(f"ownership points at unknown target {target_name!r}")
        return target.table

    def _write_physical(
        self,
        cur,
        config: RouteConfig,
        row_id: str,
        fields: Mapping[str, Any],
        decision: Decision,
    ) -> None:
        if decision.quarantined:
            cur.execute(
                sql.SQL("INSERT INTO {} (row_id, reason, data) VALUES (%s, %s, %s)").format(
                    sql.Identifier(config.quarantine.table)
                ),
                (row_id, decision.reason, Json(fields)),
            )
            return
        target = config.target_map[decision.target]
        columns = ["row_id"]
        values: List[Any] = [row_id]
        for logical, physical in sorted(target.columns.items()):
            if logical in fields:
                columns.append(physical)
                values.append(fields[logical])
        cur.execute(
            sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(target.table),
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(", ").join(sql.Placeholder() * len(columns)),
            ),
            values,
        )

    def _delete_physical(
        self, cur, config: RouteConfig, target_name: str, row_id: str
    ) -> None:
        table = self._table_for(config, target_name)
        cur.execute(
            sql.SQL("DELETE FROM {} WHERE row_id = %s").format(sql.Identifier(table)),
            (row_id,),
        )
        if cur.rowcount != 1:
            raise LoaderError(
                f"ownership/physical divergence: row_id {row_id!r} not found in"
                f" target table {table!r}"
            )

    def _upsert_ownership(
        self,
        cur,
        config: RouteConfig,
        row_id: str,
        fields: Mapping[str, Any],
        decision: Decision,
    ) -> None:
        biz_key = None if decision.quarantined else biz_key_of(config, fields)
        cur.execute(
            f"INSERT INTO {OWNERSHIP} (row_id, target, biz_key, data)"
            " VALUES (%s, %s, %s, %s)"
            " ON CONFLICT (row_id) DO UPDATE SET"
            " target = EXCLUDED.target, biz_key = EXCLUDED.biz_key,"
            " data = EXCLUDED.data",
            (row_id, decision.target, biz_key, Json(fields)),
        )

    def _check_key_collisions(self, cur) -> None:
        """Whole-batch final-state uniqueness across the union of targets.

        The deferred constraint is the hard backstop; this pre-check runs
        inside the serialized writer transaction and produces actionable,
        non-sensitive diagnostics (row_ids + key digests only).
        """
        cur.execute(
            f"SELECT biz_key FROM {OWNERSHIP} WHERE biz_key IS NOT NULL"
            " GROUP BY biz_key HAVING count(*) > 1 LIMIT 16"
        )
        dup_keys = [r[0] for r in cur.fetchall()]
        if not dup_keys:
            return
        cur.execute(
            f"SELECT row_id, biz_key FROM {OWNERSHIP} WHERE biz_key = ANY(%s)"
            " ORDER BY biz_key, row_id",
            (dup_keys,),
        )
        holders: Dict[str, List[str]] = {}
        for row_id, key in cur.fetchall():
            holders.setdefault(key, []).append(row_id)
        detail = "; ".join(
            f"key#{sha256_hex(k)[:12]} held by row_ids {sorted(ids)}"
            for k, ids in sorted(holders.items())
        )
        raise KeyConflict(f"business key collision across targets: {detail}")

    # -- reroute -------------------------------------------------------------

    def reroute(self, new_config: RouteConfig) -> RerouteResult:
        """Re-route every existing canonical row under a new rules version.

        Shares the consistency boundary with normal batches (same global
        serialization, same transaction discipline): no reader can observe a
        mix of old-rule and new-rule placements.
        """
        if not isinstance(new_config, RouteConfig):
            raise ConfigError("reroute needs a RouteConfig")
        conn = self._connect()
        try:
            with conn:
                cur = conn.cursor()
                self._lock(cur)
                self._fault("after_lock")
                config = self._load_config(cur)
                if new_config.fingerprint() != config.fingerprint():
                    raise ConfigError(
                        "reroute may only change rules/default; targets,"
                        " key fields and field types are fixed for the schema"
                    )
                cur.execute(f"SELECT row_id, target, data FROM {OWNERSHIP}")
                rows = cur.fetchall()
                moved = 0
                for row_id, target, data in rows:
                    decision = route_row(new_config, data)
                    if decision.target != target:
                        self._delete_physical(cur, config, target, row_id)
                        self._write_physical(cur, config, row_id, data, decision)
                        self._upsert_ownership(cur, config, row_id, data, decision)
                        moved += 1
                    elif decision.quarantined:
                        # Reason may change even when the target does not.
                        cur.execute(
                            sql.SQL(
                                "UPDATE {} SET reason = %s WHERE row_id = %s"
                                " AND reason IS DISTINCT FROM %s"
                            ).format(sql.Identifier(config.quarantine.table)),
                            (decision.reason, row_id, decision.reason),
                        )
                self._check_key_collisions(cur)
                cur.execute(
                    f"UPDATE {META} SET value = (value::bigint + 1)::text"
                    " WHERE key = 'route_version' RETURNING value"
                )
                route_version = int(cur.fetchone()[0])
                commit_seq = self._bump_seq(cur)
                cur.execute(
                    f"UPDATE {META} SET value = %s WHERE key = 'route_config'",
                    (canonical_json(new_config.to_dict()),),
                )
                cur.execute(
                    f"INSERT INTO {ROUTE_HISTORY} (route_version, commit_seq, config)"
                    " VALUES (%s, %s, %s)",
                    (route_version, commit_seq, Json(new_config.to_dict())),
                )
                self._fault("before_commit")
            self._log.info(
                "rerouted to route_version=%d: moved=%d commit_seq=%d",
                route_version, moved, commit_seq,
            )
            return RerouteResult(route_version, commit_seq, moved)
        except (
            psycopg2.errors.SerializationFailure,
            psycopg2.errors.DeadlockDetected,
        ) as exc:
            raise RetryableError(
                f"{type(exc).__name__}: {self._pg_message(exc)}"
            ) from exc
        except psycopg2.errors.UniqueViolation as exc:
            if getattr(exc.diag, "constraint_name", None) == OWNERSHIP_KEY_CONSTRAINT:
                raise KeyConflict(
                    "business key uniqueness violated at commit (deferred"
                    " constraint backstop)"
                ) from exc
            raise BatchFailed(f"unique violation: {self._pg_message(exc)}") from exc
        except psycopg2.Error as exc:
            raise BatchFailed(
                f"database error ({type(exc).__name__}): {self._pg_message(exc)}"
            ) from exc
        finally:
            conn.close()

    # -- reads ---------------------------------------------------------------

    def read_logical(self) -> List[LogicalRow]:
        """The current logical table: every row_id, its target, its key and
        its full canonical business fields."""
        conn = self._connect()
        try:
            with conn:
                return self._read_logical(conn.cursor())
        finally:
            conn.close()

    def _read_logical(self, cur) -> List[LogicalRow]:
        cur.execute(
            f"SELECT row_id, target, biz_key, data FROM {OWNERSHIP} ORDER BY row_id"
        )
        return [
            LogicalRow(row_id, target, biz_key, data)
            for row_id, target, biz_key, data in cur.fetchall()
        ]

    def read_target(self, name: str) -> List[Mapping[str, Any]]:
        """Physical rows of one target (business or quarantine)."""
        conn = self._connect()
        try:
            with conn:
                cur = conn.cursor()
                config = self._load_config(cur)
                return self._read_target(cur, config, name)
        finally:
            conn.close()

    def _read_target(self, cur, config: RouteConfig, name: str):
        if name == config.quarantine.name:
            cur.execute(
                sql.SQL(
                    "SELECT row_id, reason, data FROM {} ORDER BY row_id"
                ).format(sql.Identifier(config.quarantine.table))
            )
            return [
                {"row_id": r, "reason": reason, "data": data}
                for r, reason, data in cur.fetchall()
            ]
        target = config.target_map.get(name)
        if target is None:
            raise LoaderError(f"unknown target {name!r}")
        physical = sorted(target.columns.values())
        cur.execute(
            sql.SQL("SELECT row_id, {} FROM {} ORDER BY row_id").format(
                sql.SQL(", ").join(map(sql.Identifier, physical)),
                sql.Identifier(target.table),
            )
        )
        return [
            {"row_id": row[0], **dict(zip(physical, row[1:]))}
            for row in cur.fetchall()
        ]

    def partition_status(
        self, partition: Optional[str] = None
    ) -> Mapping[str, Mapping[str, Any]]:
        conn = self._connect()
        try:
            with conn:
                cur = conn.cursor()
                return self._read_partitions(cur, partition)
        finally:
            conn.close()

    @staticmethod
    def _read_partitions(cur, partition: Optional[str]):
        if partition is None:
            cur.execute(
                f"SELECT partition, start_position, last_position, last_digest"
                f" FROM {PARTITIONS} ORDER BY partition"
            )
        else:
            cur.execute(
                f"SELECT partition, start_position, last_position, last_digest"
                f" FROM {PARTITIONS} WHERE partition = %s",
                (partition,),
            )
        return {
            p: {
                "start_position": start,
                "last_position": last,
                "last_digest": digest,
            }
            for p, start, last, digest in cur.fetchall()
        }

    def route_version(self) -> int:
        conn = self._connect()
        try:
            with conn:
                return self._route_version(conn.cursor())
        finally:
            conn.close()

    @staticmethod
    def _route_version(cur) -> int:
        cur.execute(f"SELECT value FROM {META} WHERE key = 'route_version'")
        row = cur.fetchone()
        if row is None:
            raise ConfigError("schema is not initialized (run init_schema first)")
        return int(row[0])

    @staticmethod
    def _bump_seq(cur) -> int:
        cur.execute(
            f"UPDATE {META} SET value = (value::bigint + 1)::text"
            " WHERE key = 'commit_seq' RETURNING value"
        )
        return int(cur.fetchone()[0])

    def snapshot(self) -> Snapshot:
        """One consistent read of everything (REPEATABLE READ, read-only)."""
        conn = self._connect()
        try:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
            with conn:
                cur = conn.cursor()
                config = self._load_config(cur)
                logical = tuple(self._read_logical(cur))
                names = [t.name for t in config.targets] + [config.quarantine.name]
                targets = {
                    name: tuple(self._read_target(cur, config, name)) for name in names
                }
                partitions = self._read_partitions(cur, None)
                version = self._route_version(cur)
            return Snapshot(logical, targets, partitions, version)
        finally:
            conn.close()

    # -- misc ----------------------------------------------------------------

    @staticmethod
    def _pg_message(exc: psycopg2.Error) -> str:
        try:
            msg = exc.diag.message_primary
        except Exception:
            msg = None
        return msg or str(exc).strip().splitlines()[0]
