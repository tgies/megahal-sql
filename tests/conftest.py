"""Shared fixtures for megahal-sql tests."""

import time
from pathlib import Path

import psycopg
import pytest
import subprocess

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_DIR = PROJECT_ROOT / "schema"
DATA_DIR = PROJECT_ROOT / "data"

DSN = "postgresql://megahal:megahal@localhost:5434/megahal"


@pytest.fixture(scope="session")
def pg_container():
    """Start the PostgreSQL container for the test session, tear down after."""
    subprocess.run(
        ["docker", "compose", "up", "-d", "--wait"],
        cwd=PROJECT_ROOT,
        check=True,
    )
    # Poll until PostgreSQL actually accepts connections
    for _ in range(30):
        try:
            with psycopg.connect(DSN) as conn:
                conn.execute("SELECT 1")
            break
        except psycopg.OperationalError:
            time.sleep(0.5)
    else:
        raise RuntimeError("PostgreSQL not ready after 15 seconds")
    yield
    subprocess.run(
        ["docker", "compose", "down", "-v"],
        cwd=PROJECT_ROOT,
        check=True,
    )


@pytest.fixture()
def db(pg_container):
    """Per-test database connection with transactional isolation.

    Opens a connection, BEGINs a transaction, drops/recreates the schema,
    runs the schema DDL and seed data, yields the connection for the test,
    then ROLLBACKs everything.
    """
    conn = psycopg.connect(DSN, autocommit=False)
    try:
        # Clean slate (safe inside transaction -- rolled back on teardown)
        conn.execute("DROP SCHEMA public CASCADE")
        conn.execute("CREATE SCHEMA public")

        # Execute schema files inside the transaction
        for sql_file in sorted(SCHEMA_DIR.glob("*.sql")):
            sql = sql_file.read_text().strip()
            if sql and not all(
                line.lstrip().startswith("--") or line.strip() == ""
                for line in sql.splitlines()
            ):
                conn.execute(sql)

        # Load support data (banned words, aux words, etc.) via COPY FROM STDIN
        from driver import load_support_data
        load_support_data(conn, DATA_DIR)

        yield conn
    finally:
        conn.rollback()
        conn.close()

