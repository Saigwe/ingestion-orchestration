"""Dagster assets, checks and an hourly schedule. Run the UI with: dagster dev -m pipeline.definitions"""
from pathlib import Path

import duckdb
from dagster import (AssetCheckResult, AssetExecutionContext, ConfigurableResource, Definitions, ScheduleDefinition,
                     asset, asset_check, define_asset_job)

from .ingest import apply_cdc, extract_invoices, get_state, init_warehouse
from .sources import BillingAPI, OperationalDB
from .transform import build_health, reverse_etl

ROOT = Path(__file__).resolve().parent.parent


class Platform(ConfigurableResource):
    warehouse_path: str = str(ROOT / "data" / "warehouse.duckdb")
    source_path: str = str(ROOT / "data" / "app.sqlite")
    outbox_path: str = str(ROOT / "data" / "crm_outbox.jsonl")
    api_failure_rate: float = 0.25

    def warehouse(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(self.warehouse_path)
        init_warehouse(con)
        return con


@asset(group_name="ingest", description="Customers and orders from the app database via change data capture")
def cdc_app_tables(context: AssetExecutionContext, platform: Platform) -> dict:
    with platform.warehouse() as con:
        stats = apply_cdc(con, OperationalDB(platform.source_path))
    context.add_output_metadata(stats)
    return stats


@asset(group_name="ingest", deps=[cdc_app_tables], description="Invoices from the billing API, with retries")
def billing_invoices(context: AssetExecutionContext, platform: Platform) -> dict:
    with platform.warehouse() as con:
        seed = int(get_state(con, "cdc_lsn", "0"))  # varies the simulated outages from run to run
        stats = extract_invoices(con, BillingAPI(OperationalDB(platform.source_path), platform.api_failure_rate, seed))
    context.add_output_metadata(stats)
    return stats


@asset(group_name="marts", deps=[cdc_app_tables, billing_invoices], description="Customer health scores")
def customer_health(context: AssetExecutionContext, platform: Platform) -> int:
    with platform.warehouse() as con:
        n = build_health(con)
    context.add_output_metadata({"rows": n})
    return n


@asset(group_name="activation", deps=[customer_health], description="Changed accounts pushed to the CRM")
def crm_sync(context: AssetExecutionContext, platform: Platform) -> dict:
    with platform.warehouse() as con:
        stats = reverse_etl(con, Path(platform.outbox_path))
    context.add_output_metadata(stats)
    return stats


@asset_check(asset=customer_health, description="One row per customer, no orphans")
def customer_health_unique(platform: Platform) -> AssetCheckResult:
    with platform.warehouse() as con:
        dupes = con.execute("select count(*) - count(distinct customer_id) from marts.customer_health").fetchone()[0]
    return AssetCheckResult(passed=dupes == 0, metadata={"duplicates": dupes})


@asset_check(asset=cdc_app_tables, description="Warehouse matches the source after CDC")
def cdc_reconciles(platform: Platform) -> AssetCheckResult:
    src = OperationalDB(platform.source_path).con
    with platform.warehouse() as con:
        diffs = {t: src.execute(f"select count(*) from {t}").fetchone()[0]
                 - con.execute(f"select count(*) from raw.{t}").fetchone()[0] for t in ("customers", "orders")}
    return AssetCheckResult(passed=not any(diffs.values()), metadata=diffs)


ASSETS = [cdc_app_tables, billing_invoices, customer_health, crm_sync]
CHECKS = [customer_health_unique, cdc_reconciles]
hourly = ScheduleDefinition(job=define_asset_job("hourly_refresh", selection="*"), cron_schedule="5 * * * *")

defs = Definitions(assets=ASSETS, asset_checks=CHECKS, schedules=[hourly], resources={"platform": Platform()})
