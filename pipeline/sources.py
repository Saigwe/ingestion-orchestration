"""Simulated sources.

1. An operational database (SQLite standing in for Postgres) whose changes are captured in a Debezium-style log
   (`cdc_log`: lsn, op c/u/d, table, primary key, row after the change).
2. A paginated billing API that sometimes fails with transient errors (HTTP 503 / rate limits).
"""
from __future__ import annotations

import json
import random
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

T0 = datetime(2026, 10, 1, 6, 0)


class TransientAPIError(Exception):
    """Retryable failure, like an HTTP 503 or 429."""


class OperationalDB:
    def __init__(self, path: str | Path):
        self.con = sqlite3.connect(str(path))
        self.con.executescript("""
            create table if not exists customers (customer_id text primary key, name text, plan text, updated_at text);
            create table if not exists orders (order_id text primary key, customer_id text, amount real,
                                               status text, updated_at text);
            create table if not exists cdc_log (lsn integer primary key autoincrement, op text, tbl text,
                                                pk text, after text, ts text);
        """)

    def _write(self, op: str, table: str, pk_col: str, row: dict | None, pk: str, ts: datetime):
        if op == "d":
            self.con.execute(f"delete from {table} where {pk_col} = ?", [pk])
        else:
            cols = ", ".join(row)
            self.con.execute(f"insert or replace into {table} ({cols}) values ({', '.join('?' * len(row))})",
                             list(row.values()))
        self.con.execute("insert into cdc_log (op, tbl, pk, after, ts) values (?, ?, ?, ?, ?)",
                         [op, table, pk, json.dumps(row) if row else None, ts.isoformat()])

    def simulate_hour(self, hour: int, seed: int = 0) -> None:
        """One hour of application activity: signups, plan changes, new orders, status updates, deletes."""
        rng = random.Random(seed * 1000 + hour)
        ts = T0 + timedelta(hours=hour)
        existing = [r[0] for r in self.con.execute("select customer_id from customers")]
        for i in range(rng.randint(8, 15)):
            cid = f"C{hour:03d}{i:02d}"
            self._write("c", "customers", "customer_id",
                        {"customer_id": cid, "name": f"Company {cid}", "plan": "starter", "updated_at": ts.isoformat()},
                        cid, ts)
            existing.append(cid)
        for cid in rng.sample(existing, min(4, len(existing))):          # upgrades
            self._write("u", "customers", "customer_id",
                        {"customer_id": cid, "name": f"Company {cid}", "plan": rng.choice(["growth", "enterprise"]),
                         "updated_at": ts.isoformat()}, cid, ts)
        orders = [r[0] for r in self.con.execute("select order_id from orders where status = 'placed'")]
        for i in range(rng.randint(15, 30)):
            oid = f"O{hour:03d}{i:03d}"
            self._write("c", "orders", "order_id",
                        {"order_id": oid, "customer_id": rng.choice(existing), "amount": round(rng.uniform(20, 900), 2),
                         "status": "placed", "updated_at": ts.isoformat()}, oid, ts)
        for oid in rng.sample(orders, min(10, len(orders))):             # fulfilment updates
            row = dict(zip(["order_id", "customer_id", "amount", "status", "updated_at"],
                           self.con.execute("select * from orders where order_id = ?", [oid]).fetchone()))
            row.update(status=rng.choice(["shipped", "cancelled"]), updated_at=ts.isoformat())
            self._write("u", "orders", "order_id", row, oid, ts)
        if hour and rng.random() < 0.7 and len(existing) > 5:            # GDPR-style account deletion
            cid = rng.choice(existing[:-5])
            self._write("d", "customers", "customer_id", None, cid, ts)
        self.con.commit()

    def changes_since(self, lsn: int, limit: int = 10_000) -> list[dict]:
        rows = self.con.execute("select lsn, op, tbl, pk, after, ts from cdc_log where lsn > ? order by lsn limit ?",
                                [lsn, limit]).fetchall()
        return [{"lsn": r[0], "op": r[1], "table": r[2], "pk": r[3],
                 "after": json.loads(r[4]) if r[4] else None, "ts": r[5]} for r in rows]


class BillingAPI:
    """Cursor-paginated invoices API with injected transient failures."""

    def __init__(self, db: OperationalDB, failure_rate: float = 0.25, seed: int = 1):
        self.db, self.rng, self.failure_rate = db, random.Random(seed), failure_rate
        self.calls = 0

    def list_invoices(self, updated_since: str, cursor: int = 0, page_size: int = 20) -> dict:
        self.calls += 1
        if self.rng.random() < self.failure_rate:
            raise TransientAPIError("503 Service Unavailable")
        rows = self.db.con.execute(
            "select order_id, customer_id, amount, updated_at from orders where status != 'cancelled' "
            "and updated_at > ? order by updated_at, order_id limit ? offset ?",
            [updated_since, page_size, cursor]).fetchall()
        data = [{"invoice_id": f"INV-{r[0]}", "customer_id": r[1], "amount": r[2],
                 "paid": hash(r[0]) % 3 != 0, "updated_at": r[3]} for r in rows]
        return {"data": data, "next_cursor": cursor + page_size if len(rows) == page_size else None}
