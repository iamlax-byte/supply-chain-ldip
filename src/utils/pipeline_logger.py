"""
Pipeline run metadata logger.

Every Airflow task calls log_task_run() on completion so that
metadata.pipeline_runs captures row counts, duration, DQ pass rate, and
status for every step — providing observability without querying Airflow's
own metadata DB.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Optional

from sqlalchemy import text

from src.utils.db import get_engine

log = logging.getLogger(__name__)


def log_task_run(
    dag_id: str,
    task_id: str,
    execution_date: date,
    batch_id: Optional[str],
    start_ts: datetime,
    end_ts: datetime,
    status: str,
    rows_read: int = 0,
    rows_written: int = 0,
    rows_rejected: int = 0,
    dq_pass_rate: Optional[float] = None,
    airflow_run_id: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    """Insert one row into metadata.pipeline_runs for a completed task.

    Args:
        dag_id:          Airflow DAG ID.
        task_id:         Airflow task ID.
        execution_date:  Logical execution date of the DAG run.
        batch_id:        Pipeline batch ID (from raw load XCom).
        start_ts:        Task start timestamp (UTC).
        end_ts:          Task end timestamp (UTC).
        status:          'success' | 'failed' | 'skipped'.
        rows_read:       Rows consumed by this task.
        rows_written:    Rows produced / committed by this task.
        rows_rejected:   Rows dropped by DQ or dedup.
        dq_pass_rate:    Fraction of DQ rules that passed (0.0–1.0); None if N/A.
        airflow_run_id:  Airflow run_id string (e.g. scheduled__2024-01-15T06:00).
        error_message:   Exception message if status == 'failed'.
    """
    duration = int((end_ts - start_ts).total_seconds())
    engine = get_engine("metadata")

    try:
        with engine.connect() as conn:
            conn.execute(
                text("""
                    insert into metadata.pipeline_runs
                        (dag_id, airflow_run_id, task_id, execution_date,
                         start_ts, end_ts, duration_seconds, status,
                         rows_read, rows_written, rows_rejected, dq_pass_rate,
                         batch_id, error_message)
                    values
                        (:dag_id, :airflow_run_id, :task_id, :execution_date,
                         :start_ts, :end_ts, :duration_seconds, :status,
                         :rows_read, :rows_written, :rows_rejected, :dq_pass_rate,
                         :batch_id, :error_message)
                """),
                {
                    "dag_id":           dag_id,
                    "airflow_run_id":   airflow_run_id,
                    "task_id":          task_id,
                    "execution_date":   execution_date,
                    "start_ts":         start_ts,
                    "end_ts":           end_ts,
                    "duration_seconds": duration,
                    "status":           status,
                    "rows_read":        rows_read,
                    "rows_written":     rows_written,
                    "rows_rejected":    rows_rejected,
                    "dq_pass_rate":     dq_pass_rate,
                    "batch_id":         batch_id,
                    "error_message":    error_message,
                },
            )
            conn.commit()
        log.info("Logged pipeline_run: dag=%s task=%s status=%s duration=%ds",
                 dag_id, task_id, status, duration)
    except Exception as exc:
        # Metadata logging failure must never kill the pipeline task itself
        log.warning("Failed to write pipeline_runs metadata: %s", exc)
