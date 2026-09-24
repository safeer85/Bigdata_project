"""PostgreSQL helpers shared by the streaming sinks, Airflow tasks and the API.

The one idea worth understanding here is `upsert`. Every sink in this project
writes with `INSERT ... ON CONFLICT (natural key) DO UPDATE`. That is what makes
the pipeline safe to replay:

  * Structured Streaming gives at-least-once delivery to a `foreachBatch` sink.
    After a driver restart Spark re-runs the last micro-batch, so the same rows
    arrive twice.
  * With plain INSERTs that would double-count every figure on the dashboard.
    With an upsert on the natural key the second write simply overwrites the first
    with the same value, so the result is unchanged. The sink is idempotent, which
    is the practical stand-in for exactly-once.

The natural key per table is documented at each call site and in db/init/01_schema.sql.
"""
from __future__ import annotations

import contextlib
from typing import Any, Dict, Iterable, List, Optional, Sequence

import psycopg
from psycopg.rows import dict_row

from common import config, metrics


def connect(autocommit: bool = True) -> psycopg.Connection:
    """Open a connection using the shared DSN."""
    return psycopg.connect(config.dsn(), autocommit=autocommit, row_factory=dict_row)


@contextlib.contextmanager
def cursor(autocommit: bool = True):
    """Context manager yielding a dict cursor and closing the connection after."""
    conn = connect(autocommit=autocommit)
    try:
        with conn.cursor() as cur:
            yield cur
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


def query(sql: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
    """Run a SELECT and return all rows as dicts."""
    with cursor() as cur:
        cur.execute(sql, params or ())
        return list(cur.fetchall())


def query_one(sql: str, params: Optional[Sequence[Any]] = None) -> Optional[Dict[str, Any]]:
    """Run a SELECT and return the first row, or None."""
    rows = query(sql, params)
    return rows[0] if rows else None


def execute(sql: str, params: Optional[Sequence[Any]] = None) -> int:
    """Run a statement and return the affected row count."""
    with cursor() as cur:
        cur.execute(sql, params or ())
        return cur.rowcount


def upsert(
    table: str,
    rows: Iterable[Dict[str, Any]],
    conflict_keys: Sequence[str],
    update_columns: Optional[Sequence[str]] = None,
    conn: Optional[psycopg.Connection] = None,
) -> int:
    """INSERT ... ON CONFLICT DO UPDATE for a batch of dict rows.

    `conflict_keys` is the table's NATURAL key, not a surrogate id. For example
    `rt_zone_hourly` conflicts on (zone_id, window_start): replaying the micro-batch
    that produced hour 09:00 for Z03 overwrites that one row instead of adding a
    second one.

    `update_columns` defaults to every column that is not part of the key.
    """
    rows = list(rows)
    if not rows:
        return 0

    columns = list(rows[0].keys())
    if update_columns is None:
        update_columns = [c for c in columns if c not in conflict_keys]

    placeholders = ", ".join(["%s"] * len(columns))
    col_list = ", ".join(columns)
    conflict_list = ", ".join(conflict_keys)

    if update_columns:
        assignments = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)
        action = f"DO UPDATE SET {assignments}"
    else:
        # A key-only table (nothing to update) still must not raise on replay.
        action = "DO NOTHING"

    sql = (
        f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict_list}) {action}"
    )
    values = [tuple(row.get(c) for c in columns) for row in rows]

    own_connection = conn is None
    connection = conn or connect(autocommit=True)
    try:
        with connection.cursor() as cur:
            cur.executemany(sql, values)
        metrics.DB_UPSERT_ROWS.labels(table=table).inc(len(values))
        return len(values)
    except Exception:
        metrics.DB_WRITE_ERRORS.labels(table=table).inc()
        raise
    finally:
        if own_connection:
            connection.close()


def replace_date_partition(
    table: str,
    sim_date: str,
    rows: Iterable[Dict[str, Any]],
    date_column: str = "sim_date",
) -> int:
    """Delete-then-insert one simulated date inside a single transaction.

    This is how `load_batch_views` stays idempotent (SPEC 8.2 task 6). A rerun for
    a resubmitted v2 expense file must leave the same number of rows with updated
    values, never a mixture of old and new. Wrapping both statements in one
    transaction means a reader never sees the date half-deleted.
    """
    rows = list(rows)
    conn = connect(autocommit=False)
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {table} WHERE {date_column} = %s", (sim_date,))
            if rows:
                columns = list(rows[0].keys())
                placeholders = ", ".join(["%s"] * len(columns))
                cur.executemany(
                    f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
                    [tuple(r.get(c) for c in columns) for r in rows],
                )
        conn.commit()
        metrics.DB_UPSERT_ROWS.labels(table=table).inc(len(rows))
        return len(rows)
    except Exception:
        conn.rollback()
        metrics.DB_WRITE_ERRORS.labels(table=table).inc()
        raise
    finally:
        conn.close()


def wait_for_db(timeout_s: float = 120.0) -> None:
    """Block until PostgreSQL accepts a connection, or raise after `timeout_s`."""
    import time

    deadline = time.time() + timeout_s
    last_error: Optional[Exception] = None
    while time.time() < deadline:
        try:
            with cursor() as cur:
                cur.execute("SELECT 1")
            return
        except Exception as exc:  # noqa: BLE001 - retried deliberately
            last_error = exc
            time.sleep(2)
    raise RuntimeError(f"PostgreSQL not reachable after {timeout_s}s: {last_error}")
