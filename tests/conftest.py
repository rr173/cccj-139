"""pytest fixtures: a real, temporary PostgreSQL instance on this machine.

No SQLite stand-ins: every test runs against PostgreSQL 15 started locally
on a unix socket.  The server is reused across test sessions; each test gets
a fresh database.
"""

from __future__ import annotations

import os
import subprocess
import uuid

import psycopg2
import pytest

PGBIN = os.environ.get("MTLOADER_PGBIN", "/workspace/pgroot/usr/lib/postgresql/15/bin")
PGDATA = os.environ.get("MTLOADER_PGDATA", "/workspace/pgdata")
PGSOCK = os.environ.get("MTLOADER_PGSOCK", "/workspace/pgsock")
PGPORT = int(os.environ.get("MTLOADER_PGPORT", "55432"))
PGUSER = os.environ.get("MTLOADER_PGUSER", "postgres")

ARTIFACTS = "/workspace/artifacts"


def _pg_isready() -> bool:
    return (
        subprocess.run(
            [os.path.join(PGBIN, "pg_isready"), "-h", PGSOCK, "-p", str(PGPORT)],
            capture_output=True,
        ).returncode
        == 0
    )


@pytest.fixture(scope="session")
def pg_params():
    """Ensure the temporary PostgreSQL instance is up; yield conn params."""
    if not _pg_isready():
        os.makedirs(PGSOCK, exist_ok=True)
        if not os.path.exists(os.path.join(PGDATA, "PG_VERSION")):
            os.makedirs(PGDATA, exist_ok=True)
            subprocess.run(
                [
                    os.path.join(PGBIN, "initdb"), "-D", PGDATA, "-U", PGUSER,
                    "--auth=trust", "--no-locale", "-E", "UTF8",
                ],
                check=True, capture_output=True, text=True,
            )
        subprocess.run(
            [
                os.path.join(PGBIN, "pg_ctl"), "-D", PGDATA,
                "-l", os.path.join(PGDATA, "server.log"),
                "-o", f"-k {PGSOCK} -c listen_addresses='' -p {PGPORT}",
                "-w", "start",
            ],
            check=True, capture_output=True, text=True,
        )
    if not _pg_isready():
        raise RuntimeError("could not start temporary PostgreSQL")
    return {"host": PGSOCK, "port": PGPORT, "user": PGUSER}


@pytest.fixture()
def db(pg_params):
    """A fresh database per test, dropped afterwards."""
    dbname = "t_" + uuid.uuid4().hex[:16]
    admin = psycopg2.connect(dbname="postgres", **pg_params)
    admin.autocommit = True
    admin.cursor().execute(f'CREATE DATABASE "{dbname}"')
    admin.close()
    params = dict(pg_params, dbname=dbname)
    yield params
    admin = psycopg2.connect(dbname="postgres", **pg_params)
    admin.autocommit = True
    admin.cursor().execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
    admin.close()


@pytest.fixture()
def conn_factory(db):
    def factory():
        return psycopg2.connect(**db)

    return factory


@pytest.fixture()
def loader(conn_factory):
    from mtloader import MultiTargetLoader

    return MultiTargetLoader(conn_factory)
