"""
Generic SCD Type 2 merge for warehouse dimensions.

Design:
  - A row_hash (MD5 of tracked attribute values) detects changes without
    comparing each column individually — O(1) comparison per row.
  - On hash mismatch: close the old version (effective_to = today - 1 day,
    is_current = 0), then insert a new version (effective_from = today,
    effective_to = NULL, is_current = 1).
  - On hash match: no-op — idempotent, re-running produces the same state.
  - New natural keys (never seen before): insert as first version.
  - Batch UPDATE / INSERT to avoid N+1 queries for large datasets.

Called by the warehouse load task for dim_customer and dim_supplier.
"""
from __future__ import annotations

import hashlib
import logging
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)


def compute_row_hash(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    """Return an MD5 hash Series over the concatenated values of *cols*.

    NULL values are represented as the literal string 'NULL' so that a column
    changing from NULL to a real value is detected as a change.
    """
    def _hash_row(row: pd.Series) -> str:
        raw = "|".join(
            "NULL" if pd.isna(v) else str(v).strip().lower()
            for v in row
        )
        return hashlib.md5(raw.encode()).hexdigest()

    return df[cols].apply(_hash_row, axis=1)


def merge_scd2(
    engine: Engine,
    staging_df: pd.DataFrame,
    schema: str,
    table: str,
    natural_key: str,
    surrogate_key: str,
    dim_cols: list[str],
    tracked_cols: list[str],
    effective_date: date,
) -> dict:
    """Perform an SCD Type 2 merge for one dimension table.

    Args:
        engine:         SQLAlchemy engine connected to the warehouse schema.
        staging_df:     DataFrame of incoming staging records. Must contain
                        *natural_key* and all columns in *dim_cols*.
        schema:         MySQL schema name (e.g. 'warehouse').
        table:          Table name (e.g. 'dim_customer').
        natural_key:    Business key column (e.g. 'customer_id').
        surrogate_key:  Auto-increment PK column (e.g. 'customer_key').
        dim_cols:       All non-SCD columns to write on insert.
        tracked_cols:   Subset of dim_cols whose changes trigger a new version.
        effective_date: The calendar date this batch represents (usually today).

    Returns:
        dict with keys: inserted_new, closed_old, unchanged.
    """
    qualified = f"{schema}.{table}"
    yesterday = effective_date - timedelta(days=1)

    # ── 1. Compute hash for incoming staging rows ─────────────────────────────
    staging_df = staging_df.copy()
    staging_df["row_hash"] = compute_row_hash(staging_df, tracked_cols)

    # ── 2. Load current active dimension records ──────────────────────────────
    with engine.connect() as conn:
        current_df = pd.read_sql(
            f"select {surrogate_key}, {natural_key}, row_hash "
            f"from {qualified} where is_current = 1",
            conn,
        )

    # ── 3. Classify each staging row ─────────────────────────────────────────
    merged = staging_df.merge(
        current_df.rename(columns={"row_hash": "existing_hash"}),
        on=natural_key,
        how="left",
    )
    # New record: no existing surrogate key
    is_new     = merged[surrogate_key].isna()
    # Changed: existing record but hash is different
    is_changed = (~is_new) & (merged["row_hash"] != merged["existing_hash"])
    # Unchanged: existing record, same hash
    is_same    = (~is_new) & (merged["row_hash"] == merged["existing_hash"])

    new_rows     = merged[is_new]
    changed_rows = merged[is_changed]
    same_count   = int(is_same.sum())

    log.info(
        "SCD2 %s | new=%d  changed=%d  unchanged=%d",
        table, len(new_rows), len(changed_rows), same_count,
    )

    with engine.connect() as conn:

        # ── 4. Close old versions for changed records ─────────────────────────
        if len(changed_rows) > 0:
            keys_to_close = changed_rows[surrogate_key].dropna().astype(int).tolist()
            # Batch UPDATE — single round-trip regardless of batch size
            conn.execute(
                text(
                    f"update {qualified} "
                    f"set effective_to = :yesterday, is_current = 0 "
                    f"where {surrogate_key} in :keys"
                ),
                {"yesterday": yesterday, "keys": tuple(keys_to_close)},
            )
            log.info("Closed %d old versions in %s", len(keys_to_close), table)

        # ── 5. Insert new versions (new + changed) ────────────────────────────
        to_insert = pd.concat([new_rows, changed_rows], ignore_index=True)

        if len(to_insert) > 0:
            # Build insert rows with SCD2 metadata
            to_insert["effective_from"] = effective_date
            to_insert["effective_to"]   = None       # NULL = currently active
            to_insert["is_current"]     = 1

            insert_cols = dim_cols + ["effective_from", "effective_to", "is_current", "row_hash"]
            placeholders = ", ".join(f":{c}" for c in insert_cols)
            col_list = ", ".join(insert_cols)

            for _, row in to_insert[insert_cols].iterrows():
                conn.execute(
                    text(f"insert into {qualified} ({col_list}) values ({placeholders})"),
                    row.to_dict(),
                )

            log.info("Inserted %d new versions into %s", len(to_insert), table)

        conn.commit()

    return {
        "inserted_new": len(new_rows),
        "closed_old":   len(changed_rows),
        "unchanged":    same_count,
    }
