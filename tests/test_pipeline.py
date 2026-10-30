import duckdb
import pytest

from pipeline.ingest import apply_cdc, extract_invoices, init_warehouse, with_retry
from pipeline.run import run
from pipeline.sources import BillingAPI, OperationalDB, TransientAPIError
from pipeline.transform import build_health, reverse_etl


@pytest.fixture
def env(tmp_path):
    db = OperationalDB(tmp_path / "app.sqlite")
    con = duckdb.connect(str(tmp_path / "wh.duckdb"))
    init_warehouse(con)
    return db, con, tmp_path


def snapshot(db, con, table, pk):
    src = sorted(db.con.execute(f"select * from {table}").fetchall())
    wh = sorted(con.execute(f"select * from raw.{table}").fetchall())
    return src, wh


def test_cdc_replicates_inserts_updates_and_deletes(env):
    db, con, _ = env
    for h in range(4):
        db.simulate_hour(h)
        apply_cdc(con, db)
    for table, pk in (("customers", "customer_id"), ("orders", "order_id")):
        src, wh = snapshot(db, con, table, pk)
        assert [r[0] for r in src] == [r[0] for r in wh]
    deletes = db.con.execute("select count(*) from cdc_log where op = 'd'").fetchone()[0]
    assert deletes > 0


def test_cdc_is_idempotent(env):
    db, con, _ = env
    db.simulate_hour(0)
    first = apply_cdc(con, db)
    second = apply_cdc(con, db)
    assert first["events"] > 0 and second["events"] == 0


def test_retry_recovers_then_gives_up():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise TransientAPIError("503")
        return "ok"

    assert with_retry(flaky, base_delay=0) == ("ok", 2)
    with pytest.raises(TransientAPIError):
        with_retry(lambda: (_ for _ in ()).throw(TransientAPIError("503")), attempts=3, base_delay=0)


def test_api_extraction_paginates_and_is_incremental(env):
    db, con, _ = env
    db.simulate_hour(0)
    db.simulate_hour(1)
    apply_cdc(con, db)
    first = extract_invoices(con, BillingAPI(db, failure_rate=0.3, seed=5))
    again = extract_invoices(con, BillingAPI(db, failure_rate=0.3, seed=6))
    assert first["pages"] >= 2 and first["rows"] > 0
    assert again["rows"] <= first["rows"]          # cursor means only newer rows come back


def test_reverse_etl_sends_only_changes(env):
    db, con, tmp = env
    db.simulate_hour(0)
    apply_cdc(con, db)
    extract_invoices(con, BillingAPI(db, failure_rate=0))
    build_health(con)
    first = reverse_etl(con, tmp / "outbox.jsonl")
    build_health(con)
    second = reverse_etl(con, tmp / "outbox.jsonl")
    assert first["upserts"] > 0 and second["upserts"] == 0


def test_dagster_pipeline_end_to_end(tmp_path):
    res = run(tmp_path / "work", tmp_path / "out", hours=3)
    assert all(r["success"] and r["checks_passed"] for r in res["runs"])
    assert res["noop"] == {"cdc_events": 0, "crm_upserts": 0}
    assert (tmp_path / "out" / "pipeline_report.md").exists()
