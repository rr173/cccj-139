"""End-to-end tests of the multi-target loader against real PostgreSQL.

Every test runs on a temporary local PostgreSQL 15 instance (see conftest.py)
with multiple real connections where concurrency is exercised.  An
independent in-memory interpreter (tests/interpreter.py) computes the
expected state from the committed serial operations; the real query results
are saved under /workspace/artifacts/.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import os
import threading
import time
from unittest import mock

import psycopg2
import psycopg2.errors
import pytest

from mtloader import (
    And,
    Batch,
    BatchError,
    BatchFailed,
    Cmp,
    ConfigError,
    Delete,
    Insert,
    KeyConflict,
    MultiTargetLoader,
    Or,
    PositionConflict,
    QuarantineDef,
    RetryableError,
    RouteConfig,
    Rule,
    StaleExpectedPrevious,
    TargetDef,
    UnknownPartition,
    Update,
)
from interpreter import Model

ARTIFACTS = os.environ.get("MTLOADER_ARTIFACTS", "/workspace/artifacts")


# ---------------------------------------------------------------------------
# Config builders
# ---------------------------------------------------------------------------


def base_cfg() -> RouteConfig:
    """east/west business targets with different column names AND column
    sets, plus a quarantine target; default route is quarantine."""
    return RouteConfig(
        field_types={"bk": "text", "region": "text", "amount": "int"},
        key_fields=("bk",),
        targets=(
            TargetDef("east", "t_east", {"bk": "bk_e", "region": "region", "amount": "amt"}),
            TargetDef("west", "t_west", {"bk": "bk_w", "amount": "amount"}),
        ),
        quarantine=QuarantineDef("quarantine", "t_quarantine"),
        rules=(
            Rule(Cmp("region", "eq", "east"), "east"),
            Rule(Cmp("region", "eq", "west"), "west"),
        ),
        default="quarantine",
    )


def v2_cfg() -> RouteConfig:
    """Same schema, new routing version: directions swapped, north legalized."""
    cfg = base_cfg()
    return dataclasses.replace(
        cfg,
        rules=(
            Rule(Cmp("region", "eq", "east"), "west"),
            Rule(Cmp("region", "eq", "west"), "east"),
            Rule(Cmp("region", "eq", "north"), "east"),
        ),
    )


def overlap_cfg() -> RouteConfig:
    """Rules whose predicates overlap: a row can hit more than one branch."""
    cfg = base_cfg()
    return dataclasses.replace(
        cfg,
        rules=(
            Rule(Cmp("region", "eq", "east"), "east"),
            Rule(Cmp("amount", "gt", 0), "west"),
        ),
    )


# ---------------------------------------------------------------------------
# Consistency harness
# ---------------------------------------------------------------------------


def collect_actual(loader: MultiTargetLoader) -> dict:
    """Real query results, taken in one consistent (REPEATABLE READ) snapshot."""
    snap = loader.snapshot()
    return {
        "logical": [
            {"row_id": r.row_id, "target": r.target, "biz_key": r.biz_key, "fields": r.fields}
            for r in snap.logical
        ],
        "targets": {name: [dict(row) for row in rows] for name, rows in snap.targets.items()},
        "partitions": {p: dict(s) for p, s in snap.partitions.items()},
        "route_version": snap.route_version,
    }


def check_consistency(loader: MultiTargetLoader, model: Model, name: str) -> dict:
    """Save the real query results, then assert DB == independent model."""
    actual = collect_actual(loader)
    os.makedirs(ARTIFACTS, exist_ok=True)
    with open(os.path.join(ARTIFACTS, f"{name}.json"), "w") as fh:
        json.dump(actual, fh, indent=2, sort_keys=True, default=str)
    expected = model.expected_state()
    assert actual == expected, f"state mismatch at {name}"
    return actual


# ---------------------------------------------------------------------------
# 1. The walkthrough: A/B/C, key swap + migration + legalization, then
#    illegal-ization + delete.
# ---------------------------------------------------------------------------


def test_walkthrough_swap_migrate_legalize(loader):
    cfg = base_cfg()
    loader.init_schema(cfg, {"P": 100})
    model = Model(cfg, {"P": 100})

    # -- step 1: A -> east, B -> west, C -> quarantine (default route)
    b1 = Batch("P", 100, None, (
        Insert("A", {"bk": "K1", "region": "east", "amount": 10}),
        Insert("B", {"bk": "K2", "region": "west", "amount": 20}),
        Insert("C", {"bk": "K3", "region": "north", "amount": 30}),
    ))
    r1 = loader.apply(b1)
    assert not r1.already_applied and r1.route_version == 1
    model.apply(b1)

    assert loader.read_target("east") == [
        {"row_id": "A", "amt": 10, "bk_e": "K1", "region": "east"}]
    assert loader.read_target("west") == [
        {"row_id": "B", "amount": 20, "bk_w": "K2"}]
    assert loader.read_target("quarantine") == [
        {"row_id": "C", "reason": "no_matching_rule",
         "data": {"bk": "K3", "region": "north", "amount": 30}}]
    logical = {r.row_id: r for r in loader.read_logical()}
    assert logical["A"].target == "east"
    assert logical["B"].target == "west"
    assert logical["C"].target == "quarantine"
    check_consistency(loader, model, "walkthrough_step1")

    # -- step 2: ONE batch swaps A/B business keys, migrates A east->west,
    #    and makes C legal.  Intermediate states would violate key
    #    uniqueness; the final state does not, so the batch must commit.
    b2 = Batch("P", 101, 100, (
        Update("A", {"bk": "K2", "region": "west", "amount": 11}),
        Update("B", {"bk": "K1", "region": "west", "amount": 21}),
        Update("C", {"bk": "K3", "region": "east", "amount": 31}),
    ))
    loader.apply(b2)
    model.apply(b2)

    assert loader.read_target("east") == [
        {"row_id": "C", "amt": 31, "bk_e": "K3", "region": "east"}]
    assert loader.read_target("west") == [
        {"row_id": "A", "amount": 11, "bk_w": "K2"},
        {"row_id": "B", "amount": 21, "bk_w": "K1"}]
    assert loader.read_target("quarantine") == []
    logical = {r.row_id: r for r in loader.read_logical()}
    assert logical["A"].target == "west" and logical["A"].fields["bk"] == "K2"
    assert logical["B"].target == "west" and logical["B"].fields["bk"] == "K1"
    assert logical["C"].target == "east"
    check_consistency(loader, model, "walkthrough_step2")

    # -- step 3: B becomes illegal (quarantined, releasing its key), A is
    #    deleted, and D reuses the key B just released - all in one batch.
    b3 = Batch("P", 102, 101, (
        Update("B", {"bk": "K1", "region": "nowhere", "amount": 22}),
        Delete("A"),
        Insert("D", {"bk": "K1", "region": "east", "amount": 40}),
    ))
    loader.apply(b3)
    model.apply(b3)

    assert loader.read_target("west") == []          # old ownership fully revoked
    assert loader.read_target("east") == [
        {"row_id": "C", "amt": 31, "bk_e": "K3", "region": "east"},
        {"row_id": "D", "amt": 40, "bk_e": "K1", "region": "east"}]
    q = loader.read_target("quarantine")
    assert [r["row_id"] for r in q] == ["B"]
    assert q[0]["reason"] == "no_matching_rule"
    assert q[0]["data"] == {"bk": "K1", "region": "nowhere", "amount": 22}
    # A is gone from every physical table, the logical table and ownership
    assert "A" not in {r.row_id for r in loader.read_logical()}
    for target in ("east", "west", "quarantine"):
        assert all(r["row_id"] != "A" for r in loader.read_target(target))
    check_consistency(loader, model, "walkthrough_step3")


# ---------------------------------------------------------------------------
# 2. Cross-target final-state key collision rejects the whole batch.
# ---------------------------------------------------------------------------


def test_cross_target_key_collision_rejected(loader):
    cfg = base_cfg()
    loader.init_schema(cfg, {"P": 1})
    model = Model(cfg, {"P": 1})

    # two row_ids, same business key, two different targets: no local UNIQUE
    # on either physical table could catch this - the loader must.
    bad = Batch("P", 1, None, (
        Insert("X", {"bk": "K9", "region": "east", "amount": 1}),
        Insert("Y", {"bk": "K9", "region": "west", "amount": 2}),
    ))
    with pytest.raises(KeyConflict):
        loader.apply(bad)
    with pytest.raises(KeyConflict):
        model.apply(bad)
    assert loader.read_logical() == []
    assert loader.partition_status()["P"]["last_position"] is None
    check_consistency(loader, model, "collision_cross_target")

    # same-target collision is rejected identically
    bad2 = Batch("P", 1, None, (
        Insert("X", {"bk": "K9", "region": "east", "amount": 1}),
        Insert("Z", {"bk": "K9", "region": "east", "amount": 3}),
    ))
    with pytest.raises(KeyConflict):
        loader.apply(bad2)
    with pytest.raises(KeyConflict):
        model.apply(bad2)
    check_consistency(loader, model, "collision_same_target")

    # collision against already-committed state (another target)
    ok = Batch("P", 1, None, (Insert("X", {"bk": "K9", "region": "east", "amount": 1}),))
    loader.apply(ok)
    model.apply(ok)
    clash = Batch("P", 2, 1, (Insert("Y", {"bk": "K9", "region": "west", "amount": 2}),))
    with pytest.raises(KeyConflict):
        loader.apply(clash)
    with pytest.raises(KeyConflict):
        model.apply(clash)
    assert loader.partition_status()["P"]["last_position"] == 1
    check_consistency(loader, model, "collision_committed")


# ---------------------------------------------------------------------------
# 3. Same row_id modified several times within one batch.
# ---------------------------------------------------------------------------


def test_same_row_multiple_changes_in_batch(loader):
    cfg = base_cfg()
    loader.init_schema(cfg, {"P": 1})
    model = Model(cfg, {"P": 1})

    # insert -> update -> update, hopping targets; only the final image stays
    b1 = Batch("P", 1, None, (
        Insert("X", {"bk": "K1", "region": "east", "amount": 1, "note": "transient"}),
        Update("X", {"bk": "K2", "region": "west", "amount": 2}),
        Update("X", {"bk": "K3", "region": "east", "amount": 3}),
    ))
    loader.apply(b1)
    model.apply(b1)
    assert loader.read_target("west") == []
    assert loader.read_target("east") == [
        {"row_id": "X", "amt": 3, "bk_e": "K3", "region": "east"}]
    logical = {r.row_id: r for r in loader.read_logical()}
    # update supplies a full post-image: the old "note" field is gone
    assert logical["X"].fields == {"bk": "K3", "region": "east", "amount": 3}
    check_consistency(loader, model, "same_row_hop")

    # insert+delete of one row_id frees its key for another row_id, same batch
    b2 = Batch("P", 2, 1, (
        Insert("Y", {"bk": "K4", "region": "west", "amount": 4}),
        Delete("Y"),
        Insert("Z", {"bk": "K4", "region": "east", "amount": 5}),
    ))
    loader.apply(b2)
    model.apply(b2)
    assert "Y" not in {r.row_id for r in loader.read_logical()}
    assert loader.read_target("east")[-1]["row_id"] == "Z"
    check_consistency(loader, model, "same_row_insert_delete")

    # delete then re-insert the same row_id, same batch
    b3 = Batch("P", 3, 2, (
        Delete("X"),
        Insert("X", {"bk": "K5", "region": "west", "amount": 6}),
    ))
    loader.apply(b3)
    model.apply(b3)
    assert loader.read_target("west") == [
        {"row_id": "X", "amount": 6, "bk_w": "K5"}]
    check_consistency(loader, model, "same_row_delete_insert")


# ---------------------------------------------------------------------------
# 4. Positions: declared start, idempotent redelivery, digest conflicts,
#    stale expected_previous, unknown/late partitions.
# ---------------------------------------------------------------------------


def test_positions_duplicates_expected_previous(loader):
    cfg = base_cfg()
    loader.init_schema(cfg, {"P": 5})
    model = Model(cfg, {"P": 5})

    # first batch must start exactly at the declared start position
    with pytest.raises(PositionConflict):
        loader.apply(Batch("P", 6, None, (
            Insert("A", {"bk": "K1", "region": "east", "amount": 1}),)))
    # unknown partition
    with pytest.raises(UnknownPartition):
        loader.apply(Batch("NOPE", 1, None, (
            Insert("A", {"bk": "K1", "region": "east", "amount": 1}),)))

    b1 = Batch("P", 5, None, (Insert("A", {"bk": "K1", "region": "east", "amount": 1}),))
    r1 = loader.apply(b1)
    model.apply(b1)

    # identical redelivery: no-op success, original commit_seq, no side effects
    r1b = loader.apply(b1)
    assert r1b.already_applied and r1b.commit_seq == r1.commit_seq and r1b.affected == 0
    assert loader.apply(b1).already_applied
    check_consistency(loader, model, "positions_redelivery")

    # same position, different content -> conflict
    with pytest.raises(PositionConflict):
        loader.apply(Batch("P", 5, None, (
            Insert("B", {"bk": "K2", "region": "east", "amount": 2}),)))

    # position gaps are allowed when expected_previous matches
    b2 = Batch("P", 8, 5, (Insert("B", {"bk": "K2", "region": "west", "amount": 2}),))
    loader.apply(b2)
    model.apply(b2)

    # stale expected_previous -> rejected, nothing applied
    with pytest.raises(StaleExpectedPrevious):
        loader.apply(Batch("P", 9, 5, (
            Insert("C", {"bk": "K3", "region": "east", "amount": 3}),)))
    # position buried in history
    with pytest.raises(PositionConflict):
        loader.apply(Batch("P", 7, 8, (
            Insert("C", {"bk": "K3", "region": "east", "amount": 3}),)))
    # stream-semantics violations
    with pytest.raises(BatchError):
        loader.apply(Batch("P", 9, 8, (Update("NOPE", {"bk": "K9", "region": "east", "amount": 1}),)))
    with pytest.raises(BatchError):
        loader.apply(Batch("P", 9, 8, (Delete("NOPE"),)))
    with pytest.raises(BatchError):
        loader.apply(Batch("P", 9, 8, (Insert("A", {"bk": "K1", "region": "east", "amount": 1}),)))
    assert loader.partition_status()["P"]["last_position"] == 8

    # partitions can be declared later; redeclaring with another start fails
    loader.declare_partition("R", 3)
    with pytest.raises(ConfigError):
        loader.declare_partition("R", 4)
    model.start["R"] = 3
    model.last["R"] = None
    model.last_digest["R"] = None
    b3 = Batch("R", 3, None, (Insert("C", {"bk": "K3", "region": "east", "amount": 3}),))
    loader.apply(b3)
    model.apply(b3)
    check_consistency(loader, model, "positions_final")


# ---------------------------------------------------------------------------
# 5. Route multi-hit and other illegal rows land in quarantine with reasons.
# ---------------------------------------------------------------------------


def test_route_multi_match_and_illegals(loader):
    cfg = overlap_cfg()
    # overlapping predicates are accepted: the conflict is detected per row,
    # not masked by rule order
    loader.init_schema(cfg, {"P": 1})
    model = Model(cfg, {"P": 1})

    b = Batch("P", 1, None, (
        Insert("M", {"bk": "K1", "region": "east", "amount": 5}),   # hits BOTH rules
        Insert("E", {"bk": "K2", "region": "east", "amount": -5}),  # east only
        Insert("N", {"bk": "K3", "region": "north", "amount": -5}), # default
        Insert("X", {"region": "east", "amount": -1}),              # missing key
        Insert("T", {"bk": "K4", "region": 123, "amount": 1}),      # type mismatch
    ))
    loader.apply(b)
    model.apply(b)

    assert loader.read_target("east") == [
        {"row_id": "E", "amt": -5, "bk_e": "K2", "region": "east"}]
    assert loader.read_target("west") == []
    q = {r["row_id"]: r for r in loader.read_target("quarantine")}
    assert q["M"]["reason"] == "route_multi_match"
    assert q["M"]["data"] == {"bk": "K1", "region": "east", "amount": 5}
    assert q["N"]["reason"] == "no_matching_rule"
    assert q["X"]["reason"] == "missing_key"
    assert q["T"]["reason"] == "field_type_mismatch:region"
    check_consistency(loader, model, "route_multi_match")


# ---------------------------------------------------------------------------
# 6. Reroute to a new rules version.
# ---------------------------------------------------------------------------


def test_reroute_new_version(loader):
    cfg = base_cfg()
    loader.init_schema(cfg, {"P": 1})
    model = Model(cfg, {"P": 1})
    b1 = Batch("P", 1, None, (
        Insert("A", {"bk": "K1", "region": "east", "amount": 1}),
        Insert("B", {"bk": "K2", "region": "west", "amount": 2}),
        Insert("C", {"bk": "K3", "region": "north", "amount": 3}),
    ))
    loader.apply(b1)
    model.apply(b1)

    cfg2 = v2_cfg()
    r = loader.reroute(cfg2)
    assert r.route_version == 2 and r.moved == 3
    model.reroute(cfg2)

    # row_ids preserved, business fields untouched, placements migrated
    logical = {row.row_id: row for row in loader.read_logical()}
    assert logical["A"].target == "west" and logical["A"].fields["amount"] == 1
    assert logical["B"].target == "east" and logical["B"].fields["bk"] == "K2"
    assert logical["C"].target == "east"   # north is legal under v2
    assert loader.read_target("quarantine") == []
    assert loader.route_version() == 2
    assert loader.current_config().to_dict() == cfg2.to_dict()
    check_consistency(loader, model, "reroute_v2")

    # new batches are routed by the new version
    b2 = Batch("P", 2, 1, (Insert("E", {"bk": "K5", "region": "west", "amount": 5}),))
    loader.apply(b2)
    model.apply(b2)
    assert loader.read_target("east")[-1]["row_id"] == "E"
    check_consistency(loader, model, "reroute_v2_apply")

    # reroute may only change rules/default
    with pytest.raises(ConfigError):
        loader.reroute(dataclasses.replace(cfg2, key_fields=("bk", "region")))

    # a reroute whose final state collides on the business key is rejected
    b3 = Batch("P", 3, 2, (Insert("F", {"bk": "K1", "region": "south", "amount": 6}),))
    loader.apply(b3)   # F quarantined by default; quarantine holds no key
    model.apply(b3)
    cfg3 = dataclasses.replace(
        cfg2, rules=cfg2.rules + (Rule(Cmp("region", "eq", "south"), "west"),))
    with pytest.raises(KeyConflict):
        loader.reroute(cfg3)   # F would become legal with K1, colliding with A
    with pytest.raises(KeyConflict):
        model.reroute(cfg3)
    assert loader.route_version() == 2
    assert [r["row_id"] for r in loader.read_target("quarantine")] == ["F"]
    check_consistency(loader, model, "reroute_collision_rejected")

    # a clean reroute afterwards still works
    r3 = loader.reroute(cfg2)
    assert r3.moved == 0 and r3.route_version == 3
    model.reroute(cfg2)
    check_consistency(loader, model, "reroute_v3")


# ---------------------------------------------------------------------------
# 7. Mid-batch SQL error and injected fault: full rollback, nothing commits.
# ---------------------------------------------------------------------------


def test_sql_error_and_fault_rollback(conn_factory):
    loader = MultiTargetLoader(conn_factory)
    cfg = base_cfg()
    loader.init_schema(cfg, {"P": 1})
    model = Model(cfg, {"P": 1})
    b1 = Batch("P", 1, None, (Insert("A", {"bk": "K1", "region": "east", "amount": 1}),))
    loader.apply(b1)
    model.apply(b1)

    # 2**70 overflows bigint mid-batch -> the whole batch must roll back
    bad = Batch("P", 2, 1, (
        Insert("ok1", {"bk": "K2", "region": "east", "amount": 2}),
        Insert("bad", {"bk": "K3", "region": "east", "amount": 2 ** 70}),
        Insert("ok2", {"bk": "K4", "region": "west", "amount": 4}),
    ))
    with pytest.raises(BatchFailed):
        loader.apply(bad)
    assert loader.partition_status()["P"]["last_position"] == 1
    assert {r.row_id for r in loader.read_logical()} == {"A"}
    check_consistency(loader, model, "sql_error_rollback")

    # injected fault at the commit boundary: nothing commits either
    state = {"armed": True}

    def hook(point):
        if point == "before_commit" and state["armed"]:
            state["armed"] = False
            raise RuntimeError("injected fault before commit")

    flaky = MultiTargetLoader(conn_factory, fault_hook=hook)
    b2 = Batch("P", 2, 1, (Insert("B", {"bk": "K5", "region": "west", "amount": 5}),))
    with pytest.raises(RuntimeError):
        flaky.apply(b2)
    assert loader.partition_status()["P"]["last_position"] == 1
    assert {r.row_id for r in loader.read_logical()} == {"A"}

    # once the fault is gone the very same batch applies cleanly
    loader.apply(b2)
    model.apply(b2)
    check_consistency(loader, model, "fault_rollback_then_apply")


# ---------------------------------------------------------------------------
# 8. Readers only ever see complete batches (observed via the fault hook
#    parked at the commit boundary - no recovery ledger involved).
# ---------------------------------------------------------------------------


def test_reader_consistency_during_commit(conn_factory):
    cfg = base_cfg()
    plain = MultiTargetLoader(conn_factory)
    plain.init_schema(cfg, {"P": 1})
    model = Model(cfg, {"P": 1})
    b1 = Batch("P", 1, None, tuple(
        Insert(f"r{i:02d}", {"bk": f"K{i}", "region": "east", "amount": i})
        for i in range(10)
    ))
    plain.apply(b1)
    model.apply(b1)
    expected_mid = model.expected_state()

    entered = threading.Event()
    proceed = threading.Event()

    def hook(point):
        if point == "before_commit":
            entered.set()
            assert proceed.wait(10)

    writer = MultiTargetLoader(conn_factory, fault_hook=hook)
    b2 = Batch("P", 2, 1, tuple(
        Insert(f"n{i:02d}", {"bk": f"N{i}", "region": "west", "amount": i})
        for i in range(50)
    ))
    outcome = {}

    def do_apply():
        outcome["r"] = writer.apply(b2)

    t = threading.Thread(target=do_apply)
    t.start()
    assert entered.wait(5)
    # the writer is parked inside its transaction with 50 rows already
    # written; concurrent readers must see exactly the pre-batch state
    snap = collect_actual(plain)
    assert snap == expected_mid
    assert snap["partitions"]["P"]["last_position"] == 1
    assert all(not r["row_id"].startswith("n") for r in snap["logical"])
    proceed.set()
    t.join(10)
    assert not t.is_alive()
    assert outcome["r"].position == 2

    model.apply(b2)
    check_consistency(plain, model, "reader_consistency_after_commit")


# ---------------------------------------------------------------------------
# 9. Two connections contend for the same business key.
# ---------------------------------------------------------------------------


def test_concurrent_same_key_contention(conn_factory):
    cfg = base_cfg()
    loader = MultiTargetLoader(conn_factory)
    loader.init_schema(cfg, {"P1": 1, "P2": 1})
    model = Model(cfg, {"P1": 1, "P2": 1})

    barrier = threading.Barrier(2)
    results = {}
    batches = {
        "w1": Batch("P1", 1, None, (Insert("X", {"bk": "SHARED", "region": "east", "amount": 1}),)),
        "w2": Batch("P2", 1, None, (Insert("Y", {"bk": "SHARED", "region": "west", "amount": 2}),)),
    }

    def worker(tag):
        barrier.wait(10)
        try:
            results[tag] = ("ok", loader.apply(batches[tag]))
        except KeyConflict as exc:
            results[tag] = ("conflict", exc)

    threads = [threading.Thread(target=worker, args=(t,)) for t in ("w1", "w2")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(15)
    assert all(not t.is_alive() for t in threads)

    # exactly one winner; the loser must NOT report success
    assert sorted(v[0] for v in results.values()) == ["conflict", "ok"]
    winner = next(t for t, v in results.items() if v[0] == "ok")
    model.apply(batches[winner])

    actual = check_consistency(loader, model, "concurrent_same_key")
    # no duplicate across the union of all physical targets
    holders = [r["row_id"] for rows in actual["targets"].values() for r in rows]
    assert holders == (["X"] if winner == "w1" else ["Y"])
    # positions moved only where the batch committed
    winner_p, loser_p = ("P1", "P2") if winner == "w1" else ("P2", "P1")
    assert actual["partitions"][winner_p]["last_position"] == 1
    assert actual["partitions"][loser_p]["last_position"] is None


# ---------------------------------------------------------------------------
# 10. reroute meets apply: deterministic orders (via fault-hook gating) and
#     a barrier-synchronized race.  The result must match the serial order
#     given by commit_seq.
# ---------------------------------------------------------------------------


def _setup_race(conn_factory):
    cfg = base_cfg()
    loader = MultiTargetLoader(conn_factory)
    loader.init_schema(cfg, {"P": 1})
    model = Model(cfg, {"P": 1})
    b1 = Batch("P", 1, None, (
        Insert("A", {"bk": "K1", "region": "east", "amount": 1}),
        Insert("B", {"bk": "K2", "region": "west", "amount": 2}),
    ))
    loader.apply(b1)
    model.apply(b1)
    b2 = Batch("P", 2, 1, (Insert("C", {"bk": "K3", "region": "east", "amount": 3}),))
    return loader, model, b2


def _replay(model, ops, b2, cfg2):
    for kind, _seq in sorted(ops, key=lambda x: x[1]):
        if kind == "apply":
            model.apply(b2)
        else:
            model.reroute(cfg2)


def _ordered_race(conn_factory, first, artifact):
    loader, model, b2 = _setup_race(conn_factory)
    cfg2 = v2_cfg()
    entered, proceed = threading.Event(), threading.Event()

    def hook(point):
        if point == "after_lock":
            entered.set()
            assert proceed.wait(10)

    gated = MultiTargetLoader(conn_factory, fault_hook=hook)
    out = {}

    def run_gated():
        if first == "apply":
            out["apply"] = gated.apply(b2)
        else:
            out["reroute"] = gated.reroute(cfg2)

    def run_plain():
        if first == "apply":
            out["reroute"] = loader.reroute(cfg2)
        else:
            out["apply"] = loader.apply(b2)

    t1 = threading.Thread(target=run_gated)
    t1.start()
    assert entered.wait(5)          # gated op now holds the global lock
    t2 = threading.Thread(target=run_plain)
    t2.start()
    time.sleep(0.3)
    assert t2.is_alive()            # the other op is serialized behind the lock
    proceed.set()
    t1.join(10)
    t2.join(10)
    assert not t1.is_alive() and not t2.is_alive()

    ops = [("apply", out["apply"].commit_seq), ("reroute", out["reroute"].commit_seq)]
    assert [k for k, _ in sorted(ops, key=lambda x: x[1])] == [
        first, "reroute" if first == "apply" else "apply"]
    _replay(model, ops, b2, cfg2)
    check_consistency(loader, model, artifact)


def test_apply_then_reroute_order(conn_factory):
    _ordered_race(conn_factory, "apply", "race_apply_first")


def test_reroute_then_apply_order(conn_factory):
    _ordered_race(conn_factory, "reroute", "race_reroute_first")


def test_reroute_apply_barrier_race(conn_factory):
    loader, model, b2 = _setup_race(conn_factory)
    cfg2 = v2_cfg()
    barrier = threading.Barrier(2)
    out = {}

    def t_apply():
        barrier.wait(10)
        out["apply"] = loader.apply(b2)

    def t_reroute():
        barrier.wait(10)
        out["reroute"] = loader.reroute(cfg2)

    threads = [threading.Thread(target=t_apply), threading.Thread(target=t_reroute)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(15)
    assert not any(t.is_alive() for t in threads)

    # both succeed; commit_seq reveals the committed serial order
    ops = [("apply", out["apply"].commit_seq), ("reroute", out["reroute"].commit_seq)]
    _replay(model, ops, b2, cfg2)
    actual = check_consistency(loader, model, "race_barrier")
    assert actual["route_version"] == 2


# ---------------------------------------------------------------------------
# 11. Retry convention.
# ---------------------------------------------------------------------------


def test_retry_wrapper(conn_factory):
    cfg = base_cfg()
    loader = MultiTargetLoader(conn_factory)
    loader.init_schema(cfg, {"P": 1})
    model = Model(cfg, {"P": 1})

    # a transient serialization failure surfaces as RetryableError; the
    # retried identical batch then succeeds exactly once
    state = {"failures": 1}

    def hook(point):
        if point == "before_commit" and state["failures"] > 0:
            state["failures"] -= 1
            raise psycopg2.errors.SerializationFailure("simulated")

    flaky = MultiTargetLoader(conn_factory, fault_hook=hook)
    b1 = Batch("P", 1, None, (Insert("A", {"bk": "K1", "region": "east", "amount": 1}),))
    with pytest.raises(RetryableError):
        flaky.apply(b1)
    assert loader.partition_status()["P"]["last_position"] is None
    r = flaky.apply_with_retry(b1, max_attempts=3)
    assert not r.already_applied
    model.apply(b1)
    check_consistency(loader, model, "retry_transient")

    # deterministic rejections are not retried
    clash = Batch("P", 2, 1, (
        Insert("X", {"bk": "KK", "region": "east", "amount": 1}),
        Insert("Y", {"bk": "KK", "region": "west", "amount": 2}),
    ))
    with mock.patch.object(loader, "apply", wraps=loader.apply) as spy:
        with pytest.raises(KeyConflict):
            loader.apply_with_retry(clash, max_attempts=5)
    assert spy.call_count == 1
    check_consistency(loader, model, "retry_no_side_effects")


# ---------------------------------------------------------------------------
# 12. Config validation (pure, no database needed).
# ---------------------------------------------------------------------------


def test_config_validation():
    def build(**kw):
        args = dict(
            field_types={"bk": "text", "region": "text"},
            key_fields=("bk",),
            targets=(TargetDef("east", "t_east", {"bk": "bk_e"}),),
            quarantine=QuarantineDef("quarantine", "t_q"),
            rules=(Rule(Cmp("region", "eq", "east"), "east"),),
            default="quarantine",
        )
        args.update(kw)
        return RouteConfig(**args)

    with pytest.raises(ConfigError):  # identifier carrying a SQL payload
        build(targets=(TargetDef("east", 't_east"; DROP TABLE x; --', {"bk": "bk_e"}),))
    with pytest.raises(ConfigError):  # unknown operator
        build(rules=(Rule(Cmp("region", "contains", "east"), "east"),))
    with pytest.raises(ConfigError):  # predicate value type mismatch
        build(rules=(Rule(Cmp("region", "eq", 5), "east"),))
    with pytest.raises(ConfigError):  # undeclared predicate field
        build(rules=(Rule(Cmp("nope", "eq", "x"), "east"),))
    with pytest.raises(ConfigError):  # rule to unknown target
        build(rules=(Rule(Cmp("region", "eq", "east"), "nowhere"),))
    with pytest.raises(ConfigError):  # unknown default
        build(default="nowhere")
    with pytest.raises(ConfigError):  # duplicate physical table
        build(targets=(TargetDef("east", "t", {"bk": "a"}),
                       TargetDef("west", "t", {"bk": "b"})))
    with pytest.raises(ConfigError):  # undeclared key field
        build(key_fields=("nope",))
    with pytest.raises(ConfigError):  # unknown field type
        build(field_types={"bk": "uuid"})
    with pytest.raises(ConfigError):  # null predicate value
        build(rules=(Rule(Cmp("region", "eq", None), "east"),))

    cfg = build(rules=(Rule(And((Cmp("region", "eq", "east"),
                                 Or((Cmp("bk", "ne", "x"),)))), "east"),))
    # configs are pure data: dict/JSON round-trips preserve them
    assert RouteConfig.from_dict(cfg.to_dict()).to_dict() == cfg.to_dict()
    assert RouteConfig.from_dict(json.loads(json.dumps(cfg.to_dict()))).to_dict() == cfg.to_dict()


def test_init_idempotent_and_guarded(loader):
    cfg = base_cfg()
    loader.init_schema(cfg, {"P": 1})
    loader.init_schema(cfg, {"P": 1})          # identical re-init is a no-op
    with pytest.raises(ConfigError):
        loader.init_schema(v2_cfg(), {"P": 1})  # rules differ -> use reroute()
    with pytest.raises(ConfigError):
        loader.init_schema(cfg, {"P": 7})       # start position differs


# ---------------------------------------------------------------------------
# 13. Errors/logs point at the reason without dumping sensitive business rows.
# ---------------------------------------------------------------------------


def test_error_message_hygiene(loader, caplog):
    cfg = base_cfg()
    loader.init_schema(cfg, {"P": 1})
    secret = "TOPSECRET-BUSINESS-KEY"
    b = Batch("P", 1, None, (
        Insert("X", {"bk": secret, "region": "east", "amount": 1}),
        Insert("Y", {"bk": secret, "region": "west", "amount": 2}),
    ))
    with caplog.at_level(logging.INFO, logger="mtloader"):
        with pytest.raises(KeyConflict) as excinfo:
            loader.apply(b)
    # the raw business key never appears; row_ids and a key digest do
    assert secret not in str(excinfo.value)
    assert secret not in caplog.text
    assert "X" in str(excinfo.value) and "Y" in str(excinfo.value)
