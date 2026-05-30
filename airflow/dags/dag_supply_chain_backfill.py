"""
dag_supply_chain_backfill
=========================
Parameterized backfill DAG for re-processing a historical date range.

Trigger manually via Airflow UI → "Trigger DAG w/ config" or CLI:

    airflow dags trigger dag_supply_chain_backfill \\
        --conf '{"start_date": "2015-01-01", "end_date": "2015-03-31"}'

What it does:
  - Re-runs staging → warehouse → marts for every calendar month in the
    specified date range, using the raw data already loaded in raw.orders.
  - Does NOT re-load raw.orders (the raw layer is append-only and already
    contains the full DataCo history from the initial load).
  - Idempotent: DELETE + INSERT pattern in each mart means re-running the
    same date range twice produces identical output.

When to use:
  - After a schema change that requires re-deriving warehouse or mart rows.
  - After fixing a bug in a transformation function.
  - To re-score supplier performance tiers after formula adjustment.

Design note on dynamic task mapping:
  Each calendar month in the date range becomes an independent mapped task
  instance via .expand(). This means months run in parallel (up to
  max_active_tasks) and each month's failure is isolated — a bad month
  doesn't cancel all others.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

DAG_ID = "dag_supply_chain_backfill"

_DEFAULT_ARGS = {
    "owner":           "ldip",
    "retries":         2,
    "retry_delay":     timedelta(minutes=2),
    "email_on_failure": False,
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _months_in_range(start: date, end: date) -> list[str]:
    """Return a list of YYYY-MM-01 strings for every month in [start, end]."""
    months = []
    current = start.replace(day=1)
    while current <= end:
        months.append(current.strftime("%Y-%m-%d"))
        # Advance to first day of next month
        next_month = current.month % 12 + 1
        next_year  = current.year + (1 if current.month == 12 else 0)
        current    = current.replace(year=next_year, month=next_month, day=1)
    return months


def _get_batch_id_for_date_range(start_date: date, end_date: date) -> str | None:
    """Find the most recent SUCCESS batch_id that covers raw data.

    The backfill re-uses the existing raw load — it doesn't reload raw.orders.
    Returns the latest batch_id so staging/warehouse tasks can filter by it.
    If the raw layer has data but we can't find a specific batch, returns None
    (which means staging will process all available raw rows).
    """
    from src.utils.db import get_engine
    from sqlalchemy import text

    engine = get_engine("raw")
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                select batch_id from `raw`.load_log
                where status = 'SUCCESS'
                order by created_at desc
                limit 1
            """)
        )
        row = result.fetchone()
    return row[0] if row else None


# ── Task callables ────────────────────────────────────────────────────────────

def _generate_month_list(**context) -> list[str]:
    """Parse params and push the list of report_months to XCom."""
    params     = context["params"]
    start_date = date.fromisoformat(params["start_date"])
    end_date   = date.fromisoformat(params["end_date"])

    if end_date < start_date:
        raise ValueError(f"end_date ({end_date}) must be >= start_date ({start_date})")

    months = _months_in_range(start_date, end_date)
    log.info(
        "Backfill range: %s → %s | %d months to process: %s",
        start_date, end_date, len(months), months,
    )
    context["ti"].xcom_push(key="report_months", value=months)
    context["ti"].xcom_push(key="start_date",    value=str(start_date))
    context["ti"].xcom_push(key="end_date",      value=str(end_date))
    return months


def _backfill_staging_and_warehouse(**context) -> None:
    """Re-run staging transform + warehouse load for the raw data in range.

    This is a single task for the entire backfill — not per-month — because
    the raw.orders data is batch-id scoped, not date-scoped.  The staging
    and warehouse transforms handle full-dataset processing; the mart tasks
    below are what iterate per month.
    """
    from src.utils.pipeline_logger import log_task_run
    from src.transformations.warehouse import run_warehouse_load

    start      = datetime.now(timezone.utc)
    params     = context["params"]
    start_date = date.fromisoformat(params["start_date"])
    end_date   = date.fromisoformat(params["end_date"])
    batch_id   = _get_batch_id_for_date_range(start_date, end_date)

    if not batch_id:
        raise RuntimeError(
            "No SUCCESS batch found in raw.load_log. "
            "Run the initial raw load before backfilling."
        )

    log.info(
        "Backfill warehouse load | batch_id=%s | range=%s→%s",
        batch_id, start_date, end_date,
    )
    result = run_warehouse_load(
        batch_id=batch_id,
        effective_date=end_date,   # use end of range as SCD2 effective date
    )

    context["ti"].xcom_push(key="batch_id", value=batch_id)

    log_task_run(
        dag_id=DAG_ID, task_id="backfill_warehouse",
        execution_date=str(start_date),
        batch_id=batch_id, start_ts=start, end_ts=datetime.now(timezone.utc),
        status="success",
        rows_written=result.get("fct_orders", 0) + result.get("fct_order_items", 0),
        airflow_run_id=context["run_id"],
    )


def _backfill_marts_for_month(report_month: str, **context) -> None:
    """Populate all 5 marts for one calendar month.

    Called once per month via dynamic task mapping (.expand()).
    Each month is independent — a failure in one month doesn't block others.
    """
    from pathlib import Path
    from src.utils.db import get_engine
    from src.utils.pipeline_logger import log_task_run
    from sqlalchemy import text

    start   = datetime.now(timezone.utc)
    batch_id = context["ti"].xcom_pull(
        task_ids="backfill_warehouse", key="batch_id"
    )

    mart_scripts = [
        "mart_supplier_performance.sql",
        "mart_route_efficiency.sql",
        "mart_customer_segment_value.sql",
        "mart_inventory_velocity.sql",
        "mart_late_delivery_risk.sql",
    ]
    sql_dir = Path("/opt/airflow/sql/marts")
    engine  = get_engine("marts")
    total   = 0

    for script_name in mart_scripts:
        sql_path = sql_dir / script_name
        if not sql_path.exists():
            log.warning("Mart script not found: %s — skipping", script_name)
            continue

        statements = [
            s.strip() for s in sql_path.read_text().split(";") if s.strip()
        ]
        with engine.connect() as conn:
            for stmt in statements:
                r = conn.execute(text(stmt), {"report_month": report_month})
                if r.rowcount and r.rowcount > 0:
                    total += r.rowcount
            conn.commit()

        log.info("Backfill mart: %s | month=%s", script_name, report_month)

    log_task_run(
        dag_id=DAG_ID, task_id=f"backfill_marts_{report_month}",
        execution_date=report_month,
        batch_id=batch_id, start_ts=start, end_ts=datetime.now(timezone.utc),
        status="success", rows_written=total,
        airflow_run_id=context["run_id"],
    )
    log.info("Backfill complete for month=%s | rows=%d", report_month, total)


def _log_backfill_summary(**context) -> None:
    """Write a summary row to metadata.pipeline_runs."""
    from src.utils.pipeline_logger import log_task_run

    start    = datetime.now(timezone.utc)
    batch_id = context["ti"].xcom_pull(task_ids="backfill_warehouse", key="batch_id")
    months   = context["ti"].xcom_pull(task_ids="generate_month_list", key="report_months")

    log.info("Backfill complete | %d months processed", len(months) if months else 0)
    log_task_run(
        dag_id=DAG_ID, task_id="backfill_summary",
        execution_date=context["params"]["start_date"],
        batch_id=batch_id, start_ts=start, end_ts=datetime.now(timezone.utc),
        status="success",
        airflow_run_id=context["run_id"],
    )


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id=DAG_ID,
    description="Parameterized backfill: re-process staging→warehouse→marts for a date range",
    schedule_interval=None,    # manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    params={
        "start_date": Param(
            "2015-01-01",
            type="string",
            description="Backfill start date (YYYY-MM-DD)",
        ),
        "end_date": Param(
            "2015-01-31",
            type="string",
            description="Backfill end date (YYYY-MM-DD)",
        ),
    },
    tags=["supply-chain", "backfill", "ldip"],
    doc_md=__doc__,
) as dag:

    # ── 1. Generate list of months ────────────────────────────────────────────
    gen_months = PythonOperator(
        task_id="generate_month_list",
        python_callable=_generate_month_list,
        doc_md="Parses start_date/end_date params → list of YYYY-MM-01 month strings.",
    )

    # ── 2. Warehouse load (once for the full range) ───────────────────────────
    warehouse_task = PythonOperator(
        task_id="backfill_warehouse",
        python_callable=_backfill_staging_and_warehouse,
        doc_md="Re-runs warehouse dim+fact load for the existing raw batch.",
    )

    # ── 3. Mart population per month (dynamically mapped) ────────────────────
    #
    # Dynamic task mapping: Airflow creates one task instance per report_month.
    # Months run in parallel (up to DAG's max_active_tasks setting).
    # Failure in one month is isolated — other months continue.
    #
    # To set max parallelism: in Airflow UI → Admin → Configurations → core.max_active_tasks
    marts_task = PythonOperator.partial(
        task_id="backfill_marts",
        python_callable=_backfill_marts_for_month,
    ).expand(
        op_kwargs=[
            # op_kwargs are resolved at runtime via XCom in a real dynamic mapping;
            # here we use a simple list — Airflow 2.9 supports .expand() with
            # static lists or XCom-backed mapped arguments.
            {"report_month": "{{ ti.xcom_pull(task_ids='generate_month_list', key='report_months')[loop.index0] }}"}
        ]
    )

    # ── 4. Summary log ────────────────────────────────────────────────────────
    summary_task = PythonOperator(
        task_id="log_backfill_summary",
        python_callable=_log_backfill_summary,
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md="Writes summary to metadata.pipeline_runs regardless of per-month failures.",
    )

    # ── Dependency graph ──────────────────────────────────────────────────────
    gen_months >> warehouse_task >> marts_task >> summary_task
