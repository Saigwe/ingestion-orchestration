"""Model the customer health table, and reverse-ETL changed rows to the CRM."""
from __future__ import annotations

import json
from pathlib import Path

import duckdb

HEALTH_SQL = """
create or replace table marts.customer_health as
with orders as (
    select customer_id,
           count(*) filter (where status != 'cancelled')                 as orders,
           coalesce(sum(amount) filter (where status != 'cancelled'), 0) as revenue,
           count(*) filter (where status = 'cancelled')                  as cancellations
    from raw.orders group by 1
),
unpaid as (
    select customer_id, coalesce(sum(amount) filter (where not paid), 0) as unpaid_amount
    from raw.invoices group by 1
)
select
    c.customer_id,
    c.plan,
    coalesce(o.orders, 0)                       as orders,
    round(coalesce(o.revenue, 0), 2)            as revenue,
    round(coalesce(u.unpaid_amount, 0), 2)      as unpaid_amount,
    case
        when coalesce(u.unpaid_amount, 0) > 1000 or coalesce(o.cancellations, 0) >= 3 then 'at_risk'
        when coalesce(o.orders, 0) >= 3 then 'healthy'
        else 'new'
    end                                         as health
from raw.customers c
left join orders o using (customer_id)
left join unpaid u using (customer_id)
"""


def build_health(con: duckdb.DuckDBPyConnection) -> int:
    con.execute("create schema if not exists marts")
    con.execute(HEALTH_SQL)
    return con.execute("select count(*) from marts.customer_health").fetchone()[0]


def reverse_etl(con: duckdb.DuckDBPyConnection, outbox: Path) -> dict:
    """Send only rows whose CRM-facing fields changed since the last sync (and deletions)."""
    con.execute("create table if not exists meta.crm_synced as "
                "select customer_id, plan, health, revenue from marts.customer_health where false")
    changed = con.execute("""
        select h.customer_id, h.plan, h.health, h.revenue from marts.customer_health h
        left join meta.crm_synced s using (customer_id)
        where s.customer_id is null or s.plan != h.plan or s.health != h.health or s.revenue != h.revenue
    """).fetchall()
    removed = [r[0] for r in con.execute("""select customer_id from meta.crm_synced
                                            where customer_id not in (select customer_id from marts.customer_health)""").fetchall()]
    outbox.parent.mkdir(parents=True, exist_ok=True)
    with outbox.open("a", encoding="utf-8") as f:
        for cid, plan, health, revenue in changed:
            f.write(json.dumps({"op": "upsert", "object": "Account", "external_id": cid,
                                "fields": {"Plan__c": plan, "Health__c": health, "Revenue__c": revenue}}) + "\n")
        for cid in removed:
            f.write(json.dumps({"op": "delete", "object": "Account", "external_id": cid}) + "\n")
    con.execute("create or replace table meta.crm_synced as "
                "select customer_id, plan, health, revenue from marts.customer_health")
    return {"upserts": len(changed), "deletes": len(removed),
            "unchanged": con.execute("select count(*) from marts.customer_health").fetchone()[0] - len(changed)}
