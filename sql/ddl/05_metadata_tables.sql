-- =============================================================================
-- 05_metadata_tables.sql  —  Pipeline Observability
-- Tracks every pipeline run: row counts, duration, DQ pass rate.
-- Required for the "Metadata observability" non-negotiable in CLAUDE.md.
-- =============================================================================

use metadata;

-- ---------------------------------------------------------------------------
-- metadata.pipeline_runs
-- One row per task execution. Written by each Airflow task via PipelineLogger.
-- Queried by the Airflow post-run notification task and the ops dashboard.
-- ---------------------------------------------------------------------------
create table if not exists metadata.pipeline_runs (
    run_id              bigint          auto_increment primary key,
    dag_id              varchar(250)    not null,
    airflow_run_id      varchar(250),   -- Airflow's run_id (e.g. scheduled__2024-01-15T...)
    task_id             varchar(250),
    execution_date      date,
    start_ts            timestamp,
    end_ts              timestamp,
    duration_seconds    int,
    status              varchar(50),    -- 'success' | 'failed' | 'skipped'
    -- Row-level observability
    rows_read           bigint,
    rows_written        bigint,
    rows_rejected       bigint,
    dq_pass_rate        decimal(5,4),   -- fraction of DQ rules that passed (0.0–1.0)
    -- Lineage
    batch_id            varchar(50),
    error_message       text,
    created_at          timestamp       default current_timestamp,

    index idx_dag_id         (dag_id),
    index idx_execution_date (execution_date),
    index idx_status         (status),
    index idx_batch_id       (batch_id)
) engine = InnoDB
  default charset = utf8mb4
  collate = utf8mb4_unicode_ci;
