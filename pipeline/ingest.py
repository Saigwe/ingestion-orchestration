"""Incremental ingestion into DuckDB: CDC merge with a stored watermark, and API extraction with retries."""
from __future__ import annotations

import time
from typing import Callable

import duckdb

from .sources import BillingAPI, OperationalDB, TransientAPIError

PRIMARY_KEYS = {"customers": "customer_id", "orders": "order_id"}
COLUMNS = {"customers": ["customer_id", "name", "plan", "updated_at"],
           "orders": ["order_id", "customer_id", "amount", "status", "updated_at"]}


def init_warehouse(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("create schema if not exists raw")
    con.execute("create schema if not exists meta")
    con.execute("create table if not exists meta.state (key varchar primary key, value varchar)")
    con.execute("create table if not exists raw.customers (customer_id varchar primary key, name varchar, "
                "plan varchar, updated_at varchar)")
    con.execute("create table if not exists raw.orders (order_id varchar primary key, customer_id varchar, "
                "amount double, status varchar, updated_at varchar)")
    con.execute("create table if not exists raw.invoices (invoice_id varchar primary key, customer_id varchar, "
                "amount double, paid boolean, updated_at varchar)")


def get_state(con, key: str, default: str) -> str:
    row = con.execute("select value from meta.state where key = ?", [key]).fetchone()
    return row[0] if row else default


def set_state(con, key: str, value: str) -> None:
    con.execute("insert or replace into meta.state values (?, ?)", [key, value])


def apply_cdc(con: duckdb.DuckDBPyConnection, db: OperationalDB) -> dict:
    """Apply change events after the stored LSN. Idempotent: rerunning applies nothing new."""
    watermark = int(get_state(con, "cdc_lsn", "0"))
    changes = db.changes_since(watermark)
    counts = {"c": 0, "u": 0, "d": 0}
    con.execute("begin")
    for ch in changes:
        table, pk = ch["table"], PRIMARY_KEYS[ch["table"]]
        if ch["op"] == "d":
            con.execute(f"delete from raw.{table} where {pk} = ?", [ch["pk"]])
        else:
            cols = COLUMNS[table]
            con.execute(f"insert or replace into raw.{table} ({', '.join(cols)}) values ({', '.join('?' * len(cols))})",
                        [ch["after"][c] for c in cols])
        counts[ch["op"]] += 1
    if changes:
        set_state(con, "cdc_lsn", str(changes[-1]["lsn"]))
    con.execute("commit")
    return {"events": len(changes), "inserts": counts["c"], "updates": counts["u"], "deletes": counts["d"],
            "lsn": int(get_state(con, "cdc_lsn", "0"))}


def with_retry(fn: Callable, attempts: int = 6, base_delay: float = 0.01) -> tuple[object, int]:
    """Call fn with exponential backoff on transient errors. Returns (result, retries used)."""
    for attempt in range(attempts):
        try:
            return fn(), attempt
        except TransientAPIError:
            if attempt == attempts - 1:
                raise
            time.sleep(base_delay * 2 ** attempt)
    raise RuntimeError("unreachable")


def extract_invoices(con: duckdb.DuckDBPyConnection, api: BillingAPI) -> dict:
    cursor_ts = get_state(con, "invoices_updated_since", "1970-01-01")
    cursor, pages, retries, rows, newest = 0, 0, 0, 0, cursor_ts
    while cursor is not None:
        page, used = with_retry(lambda c=cursor: api.list_invoices(cursor_ts, c))
        retries += used
        pages += 1
        for inv in page["data"]:
            con.execute("insert or replace into raw.invoices values (?, ?, ?, ?, ?)",
                        [inv["invoice_id"], inv["customer_id"], inv["amount"], inv["paid"], inv["updated_at"]])
            newest = max(newest, inv["updated_at"])
            rows += 1
        cursor = page["next_cursor"]
    set_state(con, "invoices_updated_since", newest)
    return {"pages": pages, "rows": rows, "retries": retries}
