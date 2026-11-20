"""Data-access layer.

Both databases use the same schema:

* ``ledger_entries``   - the business write protected by the prepared
  transaction (a debit row in the debit database, a credit row in the credit
  database). A unique constraint on settlement_no makes duplicate posting
  impossible even if recovery is replayed.
* ``xact_log``         - the coordinator log. Rows are written in short
  autocommit transactions, so they survive even when the prepared business
  transaction is later rolled back.
* ``reversals``        - registry of accepted reversal numbers. Written in
  both databases before any business transaction is prepared, so a number
  once accepted as a reversal can never be taken by an ordinary settlement.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row

from .config import GID_RE

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS ledger_entries (
    id            BIGSERIAL PRIMARY KEY,
    settlement_no TEXT NOT NULL,
    direction     TEXT NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount        NUMERIC(18, 2) NOT NULL CHECK (amount >= 0),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_ledger_settlement
    ON ledger_entries(settlement_no);

CREATE TABLE IF NOT EXISTS xact_log (
    id            BIGSERIAL PRIMARY KEY,
    settlement_no TEXT NOT NULL,
    event         TEXT NOT NULL CHECK (event IN (
                      'prepared', 'decision_commit', 'decision_abort',
                      'committed', 'aborted')),
    note          TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_xact_log_settlement ON xact_log(settlement_no);
"""

# Kept separate from SCHEMA_SQL so an already-initialised database can add the
# reversal registry without taking any DDL lock on ledger_entries (a residual
# prepared transaction may hold locks on it).
REVERSALS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS reversals (
    id            BIGSERIAL PRIMARY KEY,
    original_no   TEXT NOT NULL,
    reversal_no   TEXT NOT NULL UNIQUE,
    status        TEXT NOT NULL CHECK (status IN ('pending', 'committed', 'aborted')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_reversals_original ON reversals(original_no);
CREATE UNIQUE INDEX IF NOT EXISTS uq_reversals_committed_original
    ON reversals(original_no) WHERE status = 'committed';
"""

EVENT_PREPARED = "prepared"
EVENT_DECISION_COMMIT = "decision_commit"
EVENT_DECISION_ABORT = "decision_abort"
EVENT_COMMITTED = "committed"
EVENT_ABORTED = "aborted"


def _sql_literal(value: str) -> str:
    """Quote a value as a PostgreSQL string literal.

    Only used for GIDs in PREPARE/COMMIT/ROLLBACK TRANSACTION, which cannot
    take bind parameters. Callers additionally validate against GID_RE, so
    the generic doubling here is defense in depth.
    """
    return "'" + value.replace("'", "''") + "'"


def _check_gid(gid: str) -> str:
    if not GID_RE.fullmatch(gid):
        raise ValueError(f"unsafe prepared-transaction GID: {gid!r}")
    return gid


@contextmanager
def connect(dsn: str) -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(dsn, connect_timeout=10)
    try:
        yield conn
    finally:
        conn.close()


def _table_exists(conn: psycopg.Connection, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = %s",
            (table,),
        )
        return cur.fetchone() is not None


def init_schema(dsn: str) -> None:
    """Create the schema when absent.

    Tables are checked through pg_tables first: if the schema already exists
    we run no DDL at all, so startup never blocks on a strong relation lock
    held by a residual prepared transaction from a crashed coordinator.
    """
    tables = ("ledger_entries", "xact_log", "reversals")
    with connect(dsn) as conn, conn.cursor() as cur:
        if all(_table_exists(conn, table) for table in tables):
            conn.commit()
            return
        if not _table_exists(conn, "reversals"):
            cur.execute(REVERSALS_SCHEMA_SQL)
        if not (_table_exists(conn, "ledger_entries")
                and _table_exists(conn, "xact_log")):
            cur.execute(SCHEMA_SQL)
        conn.commit()


def list_prepared(dsn: str) -> list[dict[str, Any]]:
    """Residual prepared transactions visible in pg_prepared_xacts."""
    with connect(dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT gid, owner, database AS dbname, prepared
            FROM pg_prepared_xacts
            ORDER BY prepared
            """
        )
        rows = cur.fetchall()
    for row in rows:
        if isinstance(row.get("prepared"), datetime):
            row["prepared"] = row["prepared"].isoformat(timespec="seconds")
    return rows


def prepared_exists(dsn: str, gid: str) -> bool:
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_prepared_xacts WHERE gid = %s", (gid,))
        return cur.fetchone() is not None


def log_event(dsn: str, settlement_no: str, event: str, note: str | None = None) -> None:
    """Append a coordinator log row in its own committed transaction."""
    with connect(dsn) as conn, conn.cursor() as cur:
        conn.autocommit = True
        cur.execute(
            "INSERT INTO xact_log (settlement_no, event, note) VALUES (%s, %s, %s)",
            (settlement_no, event, note),
        )


def get_logs(dsn: str, settlement_no: str) -> list[dict[str, Any]]:
    with connect(dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT event, note, created_at
            FROM xact_log
            WHERE settlement_no = %s
            ORDER BY id
            """,
            (settlement_no,),
        )
        rows = cur.fetchall()
    for row in rows:
        if isinstance(row.get("created_at"), datetime):
            row["created_at"] = row["created_at"].isoformat(timespec="seconds")
    return rows


def has_ledger_entry(dsn: str, settlement_no: str) -> bool:
    return get_ledger_entry(dsn, settlement_no) is not None


def get_ledger_entry(dsn: str, settlement_no: str) -> dict[str, Any] | None:
    """The single business row for a settlement number, if committed."""
    with connect(dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT settlement_no, direction, amount FROM ledger_entries "
            "WHERE settlement_no = %s",
            (settlement_no,),
        )
        row = cur.fetchone()
    if row is not None:
        row["amount"] = str(row["amount"])   # NUMERIC -> Decimal; JSON-safe str
    return row


# ---------------------------------------------------------------------------
# Reversal registry
# ---------------------------------------------------------------------------

REVERSAL_PENDING = "pending"
REVERSAL_COMMITTED = "committed"
REVERSAL_ABORTED = "aborted"


def get_reversal(dsn: str, reversal_no: str) -> dict[str, Any] | None:
    """Registry row for a reversal number, on one database."""
    with connect(dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT original_no, reversal_no, status,
                   created_at, updated_at
            FROM reversals
            WHERE reversal_no = %s
            """,
            (reversal_no,),
        )
        row = cur.fetchone()
    return _stringify_timestamps(row)


def get_reversals_for_original(dsn: str, original_no: str) -> list[dict[str, Any]]:
    with connect(dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT original_no, reversal_no, status,
                   created_at, updated_at
            FROM reversals
            WHERE original_no = %s
            ORDER BY id
            """,
            (original_no,),
        )
        rows = cur.fetchall()
    return [_stringify_timestamps(row) for row in rows]


def _stringify_timestamps(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    for key, value in row.items():
        if isinstance(value, datetime):
            row[key] = value.isoformat(timespec="seconds")
    return row


def reserve_reversal(dsn: str, original_no: str, reversal_no: str) -> None:
    """Record an accepted reversal before any business write is prepared.

    Idempotent for the same (original, reversal) pair: a crashed attempt that
    only got as far as reserving the number can be resumed with the same
    number. A different original can never take the number (the caller checks
    that first, and the WHERE clause makes the update a no-op otherwise).
    """
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO reversals (original_no, reversal_no, status)
            VALUES (%s, %s, %s)
            ON CONFLICT (reversal_no) DO UPDATE
               SET status = EXCLUDED.status, updated_at = now()
             WHERE reversals.original_no = EXCLUDED.original_no
            """,
            (original_no, reversal_no, REVERSAL_PENDING),
        )
        conn.commit()


def delete_reversal(dsn: str, reversal_no: str) -> None:
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM reversals WHERE reversal_no = %s", (reversal_no,))
        conn.commit()


def set_reversal_status(dsn: str, reversal_no: str, status: str) -> None:
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE reversals SET status = %s, updated_at = now() "
            "WHERE reversal_no = %s",
            (status, reversal_no),
        )
        conn.commit()


def reversal_number_exists(dsn: str, reversal_no: str) -> bool:
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM reversals WHERE reversal_no = %s", (reversal_no,))
        return cur.fetchone() is not None



def prepare_business_tx(dsn: str, settlement_no: str, direction: str,
                        amount: float, gid: str) -> None:
    """Prepare the business transaction.

    The unique index on settlement_no turns any replay attempt into a hard
    database error instead of a double posting. GID is validated, then
    embedded as a literal because PREPARE TRANSACTION rejects bind params.
    """
    _check_gid(gid)
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("BEGIN")
        cur.execute(
            "INSERT INTO ledger_entries (settlement_no, direction, amount) "
            "VALUES (%s, %s, %s)",
            (settlement_no, direction, amount),
        )
        cur.execute(f"PREPARE TRANSACTION {_sql_literal(gid)}")
        # PREPARE TRANSACTION already committed the work into a 2PC state;
        # psycopg's commit() is now a no-op for this connection.


def commit_prepared(dsn: str, gid: str) -> str:
    """COMMIT PREPARED. Returns 'committed' or 'already_gone' (idempotent)."""
    _check_gid(gid)
    with connect(dsn) as conn, conn.cursor() as cur:
        conn.autocommit = True
        try:
            cur.execute(f"COMMIT PREPARED {_sql_literal(gid)}")
            return "committed"
        except psycopg.errors.UndefinedObject:
            return "already_gone"


def rollback_prepared(dsn: str, gid: str) -> str:
    """ROLLBACK PREPARED. Returns 'rolled_back' or 'already_gone'."""
    _check_gid(gid)
    with connect(dsn) as conn, conn.cursor() as cur:
        conn.autocommit = True
        try:
            cur.execute(f"ROLLBACK PREPARED {_sql_literal(gid)}")
            return "rolled_back"
        except psycopg.errors.UndefinedObject:
            return "already_gone"


@contextmanager
def recovery_lock(dsn: str, settlement_no: str) -> Iterator[bool]:
    """Non-blocking, process-wide interlock for one settlement number.

    A session-level advisory lock held on an open connection. Concurrent or
    duplicate recovery of the same settlement gets ``False`` and must stop;
    a coordinator crash releases the lock automatically when Postgres drops
    the dead connection, so restart can always proceed.
    """
    with _advisory_lock(dsn, "settle_recovery", settlement_no) as acquired:
        yield acquired


@contextmanager
def reversal_lock(dsn: str, original_no: str) -> Iterator[bool]:
    """Interlock for reversal of one original settlement number.

    Held only for the accept/decide portion of a reversal. Recovery later
    re-acquires it by reversal number, so a crashed reversal can be resumed
    from the reversal number alone (the original number is in the registry).
    """
    with _advisory_lock(dsn, "settle_reversal", original_no) as acquired:
        yield acquired


@contextmanager
def number_lock(dsn: str, number: str) -> Iterator[bool]:
    """Interlock for whichever transaction is about to write ``number``.

    Both an ordinary settlement (keyed by its settlement number) and a
    reversal (keyed by its reversal number) take this lock before writing, so
    the two cannot race on the same number: either the settlement sees the
    reversal registry entry, or the reversal sees the settlement's state.
    """
    with _advisory_lock(dsn, "settle_number", number) as acquired:
        yield acquired


@contextmanager
def _advisory_lock(dsn: str, namespace: str, key: str) -> Iterator[bool]:
    conn = psycopg.connect(dsn, connect_timeout=10)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_lock(hashtext(%s), hashtext(%s))",
                (namespace, key),
            )
            acquired = cur.fetchone()[0]
        try:
            yield bool(acquired)
        finally:
            if acquired:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_advisory_unlock(hashtext(%s), hashtext(%s))",
                        (namespace, key),
                    )
    finally:
        conn.close()
