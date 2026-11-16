"""Independent in-memory interpreter of the loader contract.

Used by the tests to derive the expected database state purely from the
sequence of *committed* serial operations, without touching the database.
It reuses only the pure routing spec (mtloader.routing) and the digest
format; all state transitions are re-implemented here independently.
"""

from __future__ import annotations

import copy
from typing import Any, Dict, Mapping, Optional, Tuple

from mtloader.digest import canonical_json
from mtloader.errors import (
    BatchError,
    KeyConflict,
    PositionConflict,
    StaleExpectedPrevious,
    UnknownPartition,
)
from mtloader.loader import Batch, Delete, Insert, Update
from mtloader.routing import biz_key_of, route_row


class Model:
    def __init__(self, cfg, partitions: Mapping[str, int]):
        self.cfg = cfg
        # row_id -> [target_name, fields_dict, quarantine_reason_or_None]
        self.owner: Dict[str, list] = {}
        self.start = dict(partitions)
        self.last: Dict[str, Optional[int]] = {p: None for p in partitions}
        self.last_digest: Dict[str, Optional[str]] = {p: None for p in partitions}
        self.batches: Dict[Tuple[str, int], Tuple[str, int]] = {}  # (p,pos)->(digest,seq)
        self.version = 1
        self.commit_seq = 0

    # -- writers -------------------------------------------------------------

    def apply(self, batch: Batch):
        p, pos, exp = batch.partition, batch.position, batch.expected_previous
        if p not in self.last:
            raise UnknownPartition(p)
        digest = batch.digest()
        seen = self.batches.get((p, pos))
        if seen is not None:
            if seen[0] == digest:
                return ("already", seen[1])
            raise PositionConflict(f"{p}@{pos}: different digest")
        last = self.last[p]
        if last is not None and pos <= last:
            raise PositionConflict(f"{p}@{pos} <= {last}")
        if exp != last:
            raise StaleExpectedPrevious(f"{p}: expected {exp!r}, last {last!r}")
        if last is None and pos != self.start[p]:
            raise PositionConflict(f"{p}: first batch must be at {self.start[p]}")

        owner = copy.deepcopy(self.owner)
        for ch in batch.changes:
            if isinstance(ch, Insert):
                if ch.row_id in owner:
                    raise BatchError(f"insert existing {ch.row_id!r}")
                owner[ch.row_id] = self._place(ch.fields)
            elif isinstance(ch, Update):
                if ch.row_id not in owner:
                    raise BatchError(f"update missing {ch.row_id!r}")
                owner[ch.row_id] = self._place(ch.fields)
            elif isinstance(ch, Delete):
                if ch.row_id not in owner:
                    raise BatchError(f"delete missing {ch.row_id!r}")
                del owner[ch.row_id]
            else:
                raise BatchError(f"unknown change {ch!r}")
        self._check_keys(owner)

        self.owner = owner
        self.last[p] = pos
        self.last_digest[p] = digest
        self.commit_seq += 1
        self.batches[(p, pos)] = (digest, self.commit_seq)
        return ("ok", self.commit_seq)

    def reroute(self, cfg) -> Tuple[int, int]:
        if cfg.fingerprint() != self.cfg.fingerprint():
            raise ValueError("reroute may only change rules/default")
        owner = copy.deepcopy(self.owner)
        moved = 0
        for row_id, (target, fields, _reason) in owner.items():
            placed = self._place(fields, cfg)
            if placed[0] != target or placed[2] != _reason:
                moved += 1
            owner[row_id] = placed
        self._check_keys(owner, cfg)
        self.owner = owner
        self.cfg = cfg
        self.version += 1
        self.commit_seq += 1
        return (self.version, self.commit_seq, moved)

    # -- semantics helpers ----------------------------------------------------

    def _place(self, fields, cfg=None):
        cfg = cfg or self.cfg
        d = route_row(cfg, fields)
        return [d.target, dict(fields), d.reason if d.quarantined else None]

    def _check_keys(self, owner, cfg=None):
        cfg = cfg or self.cfg
        seen: Dict[str, str] = {}
        for row_id, (target, fields, _r) in owner.items():
            if target == cfg.quarantine.name:
                continue
            key = biz_key_of(cfg, fields)
            if key in seen:
                raise KeyConflict(
                    f"key collision between {seen[key]!r} and {row_id!r}"
                )
            seen[key] = row_id

    # -- expected state --------------------------------------------------------

    def expected_logical(self):
        rows = []
        for row_id in sorted(self.owner):
            target, fields, _reason = self.owner[row_id]
            key = (
                None
                if target == self.cfg.quarantine.name
                else biz_key_of(self.cfg, fields)
            )
            rows.append(
                {"row_id": row_id, "target": target, "biz_key": key, "fields": fields}
            )
        return rows

    def expected_target(self, name):
        out = []
        if name == self.cfg.quarantine.name:
            for row_id in sorted(self.owner):
                target, fields, reason = self.owner[row_id]
                if target == name:
                    out.append({"row_id": row_id, "reason": reason, "data": fields})
            return out
        tdef = self.cfg.target_map[name]
        for row_id in sorted(self.owner):
            target, fields, _reason = self.owner[row_id]
            if target != name:
                continue
            row = {"row_id": row_id}
            for logical, physical in sorted(tdef.columns.items()):
                row[physical] = fields.get(logical)
            out.append(row)
        return out

    def expected_partitions(self):
        return {
            p: {
                "start_position": self.start[p],
                "last_position": self.last[p],
                "last_digest": self.last_digest[p],
            }
            for p in sorted(self.start)
        }

    def expected_state(self):
        names = [t.name for t in self.cfg.targets] + [self.cfg.quarantine.name]
        return {
            "logical": self.expected_logical(),
            "targets": {n: self.expected_target(n) for n in names},
            "partitions": self.expected_partitions(),
            "route_version": self.version,
        }
