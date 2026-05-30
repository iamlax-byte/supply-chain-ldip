"""
dag_supply_chain_daily
======================
Main production DAG for the Supply Chain LDIP pipeline.
Runs daily at 06:00 UTC. Processes new CSV data end-to-end:

  FileSensor → raw_load → dq_checks → branch_on_dq
      ├─ (critical failure) → skip_downstream → log_run
      └─ (DQ passed)  → run_staging → load_warehouse → load_marts → log_run

Key design decisions:
  - XComs carry batch_id between tasks — no shared global state.
  - BranchPythonOperator skips warehouse/marts on critical DQ failure so
    bad data never pollutes the gold layer.
  - trigger_rule=ALL_DONE on log_run ensures metadata is always written,
    even after failures.
  - Exponential backoff retries: 30s → 60s → 120s (3 attempts total).
  - SLA alert fires if total DAG exceeds 3 hours (180 min).
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
DAG_ID   = "dag_supply_chain_daily"
CSV_PATH = os.environ.get(
    "DATACO_CSV_PATH",
    "/opt/airflow/data/raw/DataCoSupplyChainDataset.csv",
)

_DEFAULT_ARGS = {
    "owner":                    "ldip",
    "depends_on_past":          False,
    "retries":                  3,
    "retry_delay":              timedelta(seconds=30),
    "retry_exponential_backoff": True,   # delays: 30s → 60s → 120s
    "max_retry_delay":          timedelta(minutes=5),
    "sla":                      timedelta(hours=3),
    "email_on_failure":         False,   # swap to True + set email in Airflow config
    "email_on_retry":           False,
}


# ── Task callables ────────────────────────────────────────────────────────────

def _load_raw(**context) -> str:
    """Load DataCo CSV into raw.orders and push batch_id to XCom."""
    from src.ingestion.load_raw import load_raw_orders
    from src.utils.pipeline_logger import log_task_run

    start = datetime.now(timezone.utc)
    try:
        result  = load_raw_orders(CSV_PATH, force=False)
        batch_id = result["batch_id"]
        status  = "success"
    except RuntimeError as exc:
        # File already loaded — extract existing batch_id from error if present
        # or re-raise to fail the task and let Airflow retry
        log.warning("load_raw raised RuntimeError: %s", exc)
        raise

    context["ti"].xcom_push(key="batch_id",    value=batch_id)
    context["ti"].xcom_push(key="rows_loaded", value=result["rows_loaded"])

    log_task_run(
        dag_id=DAG_ID, task_id="load_raw",
        execution_date=context["ds"],
        batch_id=batch_id, start_ts=start, end_ts=datetime.now(timezone.utc),
        status=status, rows_written=result["rows_loaded"],
        airflow_run_id=context["run_id"],
    )
    return batch_id


def _run_dq_checks(**context) -> None:
    """Execute all DQ rules and push has_critical_failures to XCom."""
    from src.quality.dq_framework import DQFramework
    from src.utils.pipeline_logger import log_task_run

    start    = datetime.now(timezone.utc)
    batch_id = context["ti"].xcom_pull(task_ids="load_raw", key="batch_id")
    run_id   = f"dq-{context['run_id']}"

    framework = DQFramework()
    summary   = framework.run_all(batch_id=batch_id, run_id=run_id)

    context["ti"].xcom_push(
        key="has_critical_failures",
        value=summary.has_critical_failures,
    )
    context["ti"].xcom_push(
        key="dq_pass_rate",
        value=summary.overall_pass_rate,
    )

    log_task_run(
        dag_id=DAG_ID, task_id="run_dq_checks",
        execution_date=context["ds"],
        batch_id=batch_id, start_ts=start, end_ts=datetime.now(timezone.utc),
        status="success",
        dq_pass_rate=summary.overall_pass_rate,
        airflow_run_id=context["run_id"],
    )


def _branch_on_dq(**context) -> str:
    """Return downstream task_id based on DQ result.

    Critical failures route to the skip task — no warehouse/mart writes.
    This keeps bad data out of the gold layer until DQ is resolved.
    """
    has_failures = context["ti"].xcom_pull(
        task_ids="run_dq_checks", key="has_critical_failures"
    )
    if has_failures:
        log.warning("Critical DQ failures detected — skipping warehouse/marts.")
        return "skip_on_critical_dq_failure"
    return "run_staging"


def _run_staging(**context) -> None:
    """Transform raw.orders into all 5 staging tables."""
    from src.transformations.staging import run_staging
    from src.utils.pipeline_logger import log_task_run

    start    = datetime.now(timezone.utc)
    batch_id = context["ti"].xcom_pull(task_ids="load_raw", key="batch_id")

    result = run_staging(batch_id)

    total_written = sum(v for v in result.values() if isinstance(v, int))
    log_task_run(
        dag_id=DAG_ID, task_id="run_staging",
        execution_date=context["ds"],
        batch_id=batch_id, start_ts=start, end_ts=datetime.now(timezone.utc),
        status="success", rows_written=total_written,
        airflow_run_id=context["run_id"],
    )


def _load_warehouse(**context) -> None:
    """Promote staging data to all warehouse dims and facts."""
    from datetime import date
    from src.transformations.warehouse import run_warehouse_load
    from src.utils.pipeline_logger import log_task_run

    start    = datetime.now(timezone.utc)
    batch_id = context["ti"].xcom_pull(task_ids="load_raw", key="batch_id")
    exec_date = date.fromisoformat(context["ds"])

    result = run_warehouse_load(batch_id=batch_id, effective_date=exec_date)

    fact_rows = (
        result.get("fct_orders", 0)
        + result.get("fct_order_items", 0)
        + result.get("fct_shipments", 0)
    )
    log_task_run(
        dag_id=DAG_ID, task_id="load_warehouse",
        execution_date=context["ds"],
        batch_id=batch_id, start_ts=start, end_ts=datetime.now(timezone.utc),
        status="success", rows_written=fact_rows,
        airflow_run_id=context["run_id"],
    )


def _load_marts(**context) -> None:
    """Populate all 5 mart tables for the current execution month."""
    from pathlib import Path
    from src.utils.db import get_engine
    from src.utils.pipeline_logger import log_task_run
    from sqlalchemy import text

    start    = datetime.now(timezone.utc)
    batch_id = context["ti"].xcom_pull(task_ids="load_raw", key="batch_id")

    # report_month = first day of the execution date's calendar month
    exec_date    = datetime.fromisoformat(context["ds"])
    report_month = exec_date.replace(day=1).strftime("%Y-%m-%d")

    mart_scripts = [
        "mart_supplier_performance.sql",
        "mart_route_efficiency.sql",
        "mart_customer_segment_value.sql",
        "mart_inventory_velocity.sql",
        "mart_late_delivery_risk.sql",
    ]
    sql_dir = Path("/opt/airflow/sql/marts")
    engine  = get_engine("marts")
    total_rows = 0

    for script_name in mart_scripts:
        sql_path = sql_dir / script_name
        if not sql_path.exists():
            log.warning("Mart script not found: %s — skipping", script_name)
            continue

        # Split on semicolons; execute each statement separately
        statements = [
            s.strip() for s in sql_path.read_text().split(";") if s.strip()
        ]
        with engine.connect() as conn:
            for stmt in statements:
                result = conn.execute(text(stmt), {"report_month": report_month})
                if result.rowcount and result.rowcount > 0:
                    total_rows += result.rowcount
            conn.commit()

        log.info("Mart loaded: %s | report_month=%s", script_name, report_month)

    log_task_run(
        dag_id=DAG_ID, task_id="load_marts",
        execution_date=context["ds"],
        batch_id=batch_id, start_ts=start, end_ts=datetime.now(timezone.utc),
        status="success", rows_written=total_rows,
        airflow_run_id=context["run_id"],
    )


def _log_final_run(**context) -> None:
    """Write a DAG-level summary row to metadata.pipeline_runs.

    trigger_rule=ALL_DONE ensures this always runs — even after DQ failure
    or skipped branches — so every DAG run has a traceable audit record.
    """
    from src.utils.pipeline_logger import log_task_run

    start    = datetime.now(timezone.utc)
    batch_id = context["ti"].xcom_pull(task_ids="load_raw", key="batch_id")
    dq_rate  = context["ti"].xcom_pull(task_ids="run_dq_checks", key="dq_pass_rate")
    skipped  = context["ti"].xcom_pull(
        task_ids="run_dq_checks", key="has_critical_failures"
    )

    status = "skipped_dq_failure" if skipped else "success"
    log_task_run(
        dag_id=DAG_ID, task_id="dag_summary",
        execution_date=context["ds"],
        batch_id=batch_id, start_ts=start, end_ts=datetime.now(timezone.utc),
        status=status, dq_pass_rate=dq_rate,
        airflow_run_id=context["run_id"],
    )


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id=DAG_ID,
    description="Supply Chain LDIP: daily end-to-end pipeline",
    schedule_interval="0 6 * * *",          # 06:00 UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["supply-chain", "daily", "ldip"],
    doc_md=__doc__,
) as dag:

    # ── 1. Wait for raw file ──────────────────────────────────────────────────
    wait_for_file = FileSensor(
        task_id="wait_for_raw_csv",
        filepath=CSV_PATH,
        poke_interval=60,       # check every 60 seconds
        timeout=3_600,          # fail after 1 hour of waiting
        mode="poke",
        doc_md="Block until the DataCo CSV appears in data/raw/.",
    )

    # ── 2. Load raw layer ─────────────────────────────────────────────────────
    load_raw_task = PythonOperator(
        task_id="load_raw",
        python_callable=_load_raw,
        doc_md="Reads CSV → raw.orders with batch_id metadata. Idempotent.",
    )

    # ── 3. Run DQ checks ──────────────────────────────────────────────────────
    dq_task = PythonOperator(
        task_id="run_dq_checks",
        python_callable=_run_dq_checks,
        doc_md=(
            "Executes all rules in config/dq_rules.yaml against staging tables. "
            "Writes results to staging.dq_results. "
            "Pushes has_critical_failures to XCom."
        ),
    )

    # ── 4. Branch on DQ result ────────────────────────────────────────────────
    branch_task = BranchPythonOperator(
        task_id="branch_on_dq",
        python_callable=_branch_on_dq,
        doc_md=(
            "Routes to skip_on_critical_dq_failure if any critical rule failed. "
            "Routes to run_staging otherwise."
        ),
    )

    # ── 4a. DQ failure path ───────────────────────────────────────────────────
    skip_task = EmptyOperator(
        task_id="skip_on_critical_dq_failure",
        doc_md="No-op. Signals that downstream warehouse/mart tasks were intentionally skipped.",
    )

    # ── 5. Stage transform ────────────────────────────────────────────────────
    staging_task = PythonOperator(
        task_id="run_staging",
        python_callable=_run_staging,
        doc_md="raw.orders → stg_orders, stg_order_items, stg_customers, stg_products, stg_suppliers.",
    )

    # ── 6. Warehouse load ─────────────────────────────────────────────────────
    warehouse_task = PythonOperator(
        task_id="load_warehouse",
        python_callable=_load_warehouse,
        doc_md=(
            "Staging → all warehouse dims (SCD1/SCD2) and facts. "
            "Dimension keys resolved via SQL joins — no Python memory pressure."
        ),
    )

    # ── 7. Marts population ───────────────────────────────────────────────────
    marts_task = PythonOperator(
        task_id="load_marts",
        python_callable=_load_marts,
        doc_md="Runs 5 mart SQL scripts parameterised by report_month.",
    )

    # ── 8. Final metadata log (always runs) ───────────────────────────────────
    log_task = PythonOperator(
        task_id="log_pipeline_run",
        python_callable=_log_final_run,
        trigger_rule=TriggerRule.ALL_DONE,  # runs even after failures/skips
        doc_md="Writes DAG-level summary to metadata.pipeline_runs.",
    )

    # ── Dependency graph ──────────────────────────────────────────────────────
    wait_for_file >> load_raw_task >> dq_task >> branch_task
    branch_task   >> [skip_task, staging_task]
    staging_task  >> warehouse_task >> marts_task >> log_task
    skip_task     >> log_task
