"""
Deduplication utilities for the staging transform layer.

Each entity in the DataCo dataset has a natural business key. Duplicate rows
can appear when the same source file is re-processed or when the CSV contains
repeated order-level fields across order-item rows.

Strategy: keep the last-seen record per key (latest ingestion_ts wins).
All functions are pure DataFrame → DataFrame with no DB side effects.
"""
from __future__ import annotations

import logging

import pandas as pd

log = logging.getLogger(__name__)


def dedup_by_key(
    df: pd.DataFrame,
    key_col: str,
    sort_col: str | None = None,
    keep: str = "last",
) -> pd.DataFrame:
    """Drop duplicate rows sharing the same *key_col*, keeping *keep* occurrence.

    Args:
        df:       Source DataFrame.
        key_col:  Column name used as the dedup key.
        sort_col: If provided, sort by this column before deduplication so that
                  'last' reliably means 'most recent'.
        keep:     Which duplicate to keep — 'first' or 'last'.

    Returns:
        DataFrame with exactly one row per unique *key_col* value.
    """
    before = len(df)
    if sort_col and sort_col in df.columns:
        df = df.sort_values(sort_col, ascending=True)

    df = df.drop_duplicates(subset=[key_col], keep=keep).reset_index(drop=True)
    dropped = before - len(df)

    if dropped:
        log.info("dedup_by_key(%s): dropped %d duplicates, %d remain", key_col, dropped, len(df))

    return df


def dedup_orders(df: pd.DataFrame) -> pd.DataFrame:
    """One row per order_id.

    The raw dataset repeats order-level fields across every order-item line.
    We take the first occurrence of each order_id — financial totals (Sales,
    Order Profit Per Order) are order-level and identical across all items.
    """
    return dedup_by_key(df, key_col="order_id", sort_col="ingestion_ts", keep="first")


def dedup_order_items(df: pd.DataFrame) -> pd.DataFrame:
    """One row per order_item_id — the natural grain of the raw dataset."""
    return dedup_by_key(df, key_col="order_item_id", sort_col="ingestion_ts", keep="last")


def dedup_customers(df: pd.DataFrame) -> pd.DataFrame:
    """One row per customer_id.

    When a customer appears in multiple batches (e.g. re-loads), the row with
    the latest ingestion_ts wins — this feeds the SCD2 change-detection logic.
    """
    return dedup_by_key(df, key_col="customer_id", sort_col="ingestion_ts", keep="last")


def dedup_products(df: pd.DataFrame) -> pd.DataFrame:
    """One row per product_card_id (SCD1 — always keep latest state)."""
    return dedup_by_key(df, key_col="product_card_id", sort_col="ingestion_ts", keep="last")


def dedup_suppliers(df: pd.DataFrame) -> pd.DataFrame:
    """One row per department_id (supplier natural key in DataCo)."""
    return dedup_by_key(df, key_col="department_id", sort_col="ingestion_ts", keep="last")
