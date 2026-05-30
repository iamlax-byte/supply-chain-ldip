"""
Derived field computations applied during raw → staging transformation.

All functions are pure (DataFrame in → DataFrame out) with no DB side effects,
making them straightforward to unit test.

Derived fields produced here:
  - delivery_delay_days     : actual transit days minus scheduled days (signed)
  - is_late_flag            : 1 when delivery_delay_days > 0
  - profit_margin_pct       : order_profit / sales_amount
  - composite_score         : weighted supplier performance score
  - performance_tier        : 'platinum' | 'gold' | 'silver' | 'bronze'
"""
from __future__ import annotations

import pandas as pd


def compute_delivery_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add delivery_delay_days and is_late_flag columns.

    delivery_delay_days = days_for_shipping_real - days_for_shipment_scheduled
      Positive → late, Negative → early, Zero → exactly on time.

    is_late_flag = 1 when delay > 0, else 0.
    Rows where either days column is NULL get NaN delay and 0 flag.
    """
    df = df.copy()

    real = pd.to_numeric(df["days_for_shipping_real"], errors="coerce")
    sched = pd.to_numeric(df["days_for_shipment_scheduled"], errors="coerce")

    df["delivery_delay_days"] = (real - sched).astype("Int64")
    df["is_late_flag"] = (df["delivery_delay_days"] > 0).astype("Int8")

    return df


def compute_profit_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add profit_margin_pct column.

    profit_margin_pct = order_profit / sales_amount
    Returns NaN where sales_amount is zero or NULL (no division-by-zero errors).
    """
    df = df.copy()

    sales = pd.to_numeric(df["sales_amount"], errors="coerce")
    profit = pd.to_numeric(df["order_profit"], errors="coerce")

    # Replace 0 with NaN (not pd.NA) to keep float64 dtype and allow .round()
    safe_sales = sales.where(sales != 0, other=float("nan"))
    df["profit_margin_pct"] = (profit / safe_sales).round(6)

    return df


def compute_supplier_score(df: pd.DataFrame) -> pd.DataFrame:
    """Compute composite_score and performance_tier per supplier row.

    Expected input columns: on_time_rate, avg_profit_margin.
    Both are fractions (0.0–1.0).

    Composite score formula (weights from CLAUDE.md):
      composite_score = on_time_rate * 0.50
                      + avg_profit_margin * 0.30
                      + volume_score * 0.20
    volume_score is normalised to 0–1 within the batch (max units = 1.0).

    Performance tiers:
      ≥ 0.80 → platinum
      ≥ 0.65 → gold
      ≥ 0.50 → silver
      <  0.50 → bronze
    """
    df = df.copy()

    on_time = pd.to_numeric(df["on_time_rate"], errors="coerce").fillna(0)
    margin = pd.to_numeric(df["avg_profit_margin"], errors="coerce").fillna(0)

    # Normalise volume within this batch (0–1 scale)
    if "total_orders" in df.columns:
        vol = pd.to_numeric(df["total_orders"], errors="coerce").fillna(0)
        max_vol = vol.max()
        volume_score = vol / max_vol if max_vol > 0 else pd.Series(0.0, index=df.index)
    else:
        volume_score = pd.Series(0.0, index=df.index)

    df["composite_score"] = (
        on_time * 0.50 + margin * 0.30 + volume_score * 0.20
    ).round(4)

    df["performance_tier"] = pd.cut(
        df["composite_score"],
        bins=[-0.001, 0.499, 0.649, 0.799, 1.001],
        labels=["bronze", "silver", "gold", "platinum"],
    ).astype(str)

    return df
