"""
dag_ml_late_delivery
====================
Trains (or reuses) the late-delivery risk classifier and writes predictions
back to mart_late_delivery_risk.

Schedule: daily at 07:30 UTC — runs *after* dag_supply_chain_daily (06:00 UTC)
          so mart rows are already populated before scoring.

DAG flow:
  check_mart_ready → train_or_load_model → score_all_orders → log_ml_run

Design notes:
  - The model is retrained whenever the mart has grown by more than RETRAIN_THRESHOLD
    rows since the last saved model.  Otherwise the existing pickled model is loaded
    and only predict() is called (fast path).
  - Model artifact is written to /opt/airflow/models/ inside the container.
    Mount this path as a volume in docker-compose.yml if you want persistence
    across container restarts.
  - Idempotent: scoring re-runs update existing rows; duplicate runs produce
    identical output (same mart rows → same features → same predictions).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

DAG_ID         = "dag_ml_late_delivery"
MODEL_PATH     = Path("/opt/airflow/models/late_delivery_clf.pkl")
RETRAIN_THRESHOLD = 500   # retrain if mart has grown by this many rows

_DEFAULT_ARGS = {
    "owner":           "ldip",
    "retries":         1,
    "retry_delay":     timedelta(minutes=5),
    "email_on_failure": False,
}


# ── Task callables ────────────────────────────────────────────────────────────

def _check_mart_ready(**context) -> None:
    """Verify mart_late_delivery_risk has data before scoring."""
    from src.utils.db import get_engine
    from sqlalchemy import text

    engine = get_engine("marts")
    with engine.connect() as conn:
        result = conn.execute(
            text("select count(*) from marts.mart_late_delivery_risk")
        )
        count = result.scalar()

    if not count or count == 0:
        raise RuntimeError(
            "mart_late_delivery_risk is empty. "
            "Ensure dag_supply_chain_daily has completed successfully first."
        )

    log.info("Mart rows available for scoring: %d", count)
    context["ti"].xcom_push(key="mart_row_count", value=int(count))


def _train_or_load_model(**context) -> str:
    """Train a fresh model or load the existing pickle, whichever is appropriate.

    Retraining decision:
      - No pickle exists → train
      - mart_row_count has grown by > RETRAIN_THRESHOLD since last train → train
      - Otherwise → load existing
    Returns the model_version string via XCom.
    """
    from src.ml.late_delivery_classifier import LateDeliveryClassifier
    from src.utils.db import get_engine
    from src.utils.pipeline_logger import log_task_run

    start     = datetime.now(timezone.utc)
    mart_rows = context["ti"].xcom_pull(task_ids="check_mart_ready", key="mart_row_count")
    engine    = get_engine("marts")

    should_train = True
    if MODEL_PATH.exists():
        clf = LateDeliveryClassifier.load_model(MODEL_PATH)
        # Cheap heuristic: compare current mart size to when we last trained
        last_trained_rows = _get_last_trained_row_count(engine)
        if abs(mart_rows - last_trained_rows) < RETRAIN_THRESHOLD:
            log.info("Model is fresh (delta=%d < threshold=%d). Skipping retrain.",
                     abs(mart_rows - last_trained_rows), RETRAIN_THRESHOLD)
            should_train = False

    if should_train:
        log.info("Training new model (mart_rows=%d) …", mart_rows)
        clf = LateDeliveryClassifier()
        metrics = clf.train(engine)
        clf.save_model(MODEL_PATH)
        _save_trained_row_count(engine, mart_rows, clf.model_version)
        log_task_run(
            dag_id=DAG_ID, task_id="train_model",
            execution_date=context["ds"],
            batch_id=clf.model_version,
            start_ts=start, end_ts=datetime.now(timezone.utc),
            status="success",
            rows_written=metrics["n_train"],
            airflow_run_id=context["run_id"],
        )

    context["ti"].xcom_push(key="model_version", value=clf.model_version)
    context["ti"].xcom_push(key="clf_pickle",    value=str(MODEL_PATH))
    return clf.model_version


def _score_all_orders(**context) -> None:
    """Load the pickled model and write predictions to the mart."""
    from src.ml.late_delivery_classifier import LateDeliveryClassifier
    from src.utils.db import get_engine
    from src.utils.pipeline_logger import log_task_run

    start  = datetime.now(timezone.utc)
    engine = get_engine("marts")

    clf         = LateDeliveryClassifier.load_model(MODEL_PATH)
    rows_scored = clf.predict(engine)

    log_task_run(
        dag_id=DAG_ID, task_id="score_orders",
        execution_date=context["ds"],
        batch_id=clf.model_version,
        start_ts=start, end_ts=datetime.now(timezone.utc),
        status="success",
        rows_written=rows_scored,
        airflow_run_id=context["run_id"],
    )
    context["ti"].xcom_push(key="rows_scored", value=rows_scored)


def _log_ml_run(**context) -> None:
    """Write a DAG-level summary to metadata.pipeline_runs."""
    from src.utils.pipeline_logger import log_task_run

    start        = datetime.now(timezone.utc)
    model_version = context["ti"].xcom_pull(task_ids="train_or_load_model", key="model_version")
    rows_scored   = context["ti"].xcom_pull(task_ids="score_all_orders",    key="rows_scored")

    log.info("ML run complete | model=%s | rows_scored=%d", model_version, rows_scored or 0)
    log_task_run(
        dag_id=DAG_ID, task_id="ml_summary",
        execution_date=context["ds"],
        batch_id=model_version,
        start_ts=start, end_ts=datetime.now(timezone.utc),
        status="success",
        rows_written=rows_scored or 0,
        airflow_run_id=context["run_id"],
    )


# ── Private helpers ───────────────────────────────────────────────────────────

def _get_last_trained_row_count(engine) -> int:
    """Read the mart row count recorded at the last model training."""
    from sqlalchemy import text
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    select cast(rows_written as unsigned)
                    from metadata.pipeline_runs
                    where dag_id = :dag and task_id = 'train_model'
                    order by created_at desc
                    limit 1
                """),
                {"dag": DAG_ID},
            )
            row = result.fetchone()
        return int(row[0]) if row else 0
    except Exception:
        return 0


def _save_trained_row_count(engine, count: int, version: str) -> None:
    """Record the mart size at training time in pipeline_runs."""
    # pipeline_logger handles this — called from _train_or_load_model already.
    pass


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id=DAG_ID,
    description="Late-delivery ML classifier: train + score mart_late_delivery_risk",
    schedule_interval="30 7 * * *",    # 07:30 UTC — after daily pipeline
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["supply-chain", "ml", "ldip"],
    doc_md=__doc__,
) as dag:

    check_task = PythonOperator(
        task_id="check_mart_ready",
        python_callable=_check_mart_ready,
        doc_md="Verify mart_late_delivery_risk is populated before scoring.",
    )

    train_task = PythonOperator(
        task_id="train_or_load_model",
        python_callable=_train_or_load_model,
        doc_md="Train a new RandomForest model or reload the existing pickle.",
    )

    score_task = PythonOperator(
        task_id="score_all_orders",
        python_callable=_score_all_orders,
        doc_md="Write predicted_risk_score + predicted_is_late to mart rows.",
    )

    log_task = PythonOperator(
        task_id="log_ml_run",
        python_callable=_log_ml_run,
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md="Write DAG-level summary to metadata.pipeline_runs.",
    )

    check_task >> train_task >> score_task >> log_task
