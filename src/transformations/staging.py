"""
Raw → Staging transformation layer.

Reads raw.orders for a given batch_id, extracts and type-casts each entity,
applies derived fields, deduplicates, and writes to the 5 staging tables:

  stg_orders, stg_order_items, stg_customers, stg_products, stg_suppliers

Idempotency: existing staging rows for this batch_id are deleted before insert,
so re-running the same batch produces identical staging state.

Usage::

    python -m src.transformations.staging <batch_id>
    # or called programmatically from an Airflow PythonOperator
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text

from src.transformations.dedup import (
    dedup_customers,
    dedup_order_items,
    dedup_orders,
    dedup_products,
    dedup_suppliers,
)
from src.transformations.derived_fields import (
    compute_delivery_metrics,
    compute_profit_metrics,
    compute_supplier_score,
)
from src.utils.db import get_engine

log = logging.getLogger(__name__)

# Date format used by DataCo: '1/31/2015 22:00'
_DATE_FMT = "%m/%d/%Y %H:%M"


def _safe_int(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("Int64")


def _safe_float(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _safe_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, format=_DATE_FMT, errors="coerce")


def _read_raw_batch(batch_id: str) -> pd.DataFrame:
    """Load all raw.orders rows for *batch_id* into a DataFrame."""
    engine = get_engine("raw")
    with engine.connect() as conn:
        df = pd.read_sql(
            "select * from `raw`.orders where load_batch_id = %(batch_id)s",
            conn,
            params={"batch_id": batch_id},
        )
    log.info("Read %d rows from raw.orders for batch_id=%s", len(df), batch_id)
    return df


def _delete_existing(batch_id: str) -> None:
    """Delete any existing staging rows for this batch (idempotency guard)."""
    engine = get_engine("staging")
    tables = [
        "stg_orders", "stg_order_items", "stg_customers",
        "stg_products", "stg_suppliers",
    ]
    with engine.connect() as conn:
        for tbl in tables:
            conn.execute(
                text(f"delete from staging.{tbl} where load_batch_id = :bid"),
                {"bid": batch_id},
            )
        conn.commit()
    log.info("Cleared existing staging rows for batch_id=%s", batch_id)


def _stage_orders(raw: pd.DataFrame, batch_id: str) -> int:
    """Extract order-grain rows and write to staging.stg_orders."""
    df = raw.copy()

    # Type cast all columns
    df["order_id"]                    = _safe_int(df["order_id"])
    df["order_customer_id"]           = _safe_int(df["order_customer_id"])
    df["order_date"]                  = _safe_date(df["order_date"])
    df["shipping_date"]               = _safe_date(df["shipping_date"])
    df["days_for_shipping_real"]      = _safe_int(df["days_for_shipping_real"])
    df["days_for_shipment_scheduled"] = _safe_int(df["days_for_shipment_scheduled"])
    df["late_delivery_risk"]          = _safe_int(df["late_delivery_risk"])
    df["sales_amount"]                = _safe_float(df["sales"])
    df["order_profit"]                = _safe_float(df["order_profit_per_order"])
    df["benefit_per_order"]           = _safe_float(df["benefit_per_order"])
    df["sales_per_customer"]          = _safe_float(df["sales_per_customer"])
    df["latitude"]                    = _safe_float(df["latitude"])
    df["longitude"]                   = _safe_float(df["longitude"])

    # Derived fields
    df = compute_delivery_metrics(df)
    df = compute_profit_metrics(df)

    # Dedup to order grain (one row per order_id)
    df = dedup_orders(df)

    staging = df[[
        "order_id", "order_customer_id", "order_date", "order_status",
        "type", "order_region", "order_city", "order_state", "order_country",
        "order_zipcode", "shipping_date", "shipping_mode",
        "days_for_shipping_real", "days_for_shipment_scheduled",
        "delivery_delay_days", "is_late_flag", "late_delivery_risk", "delivery_status",
        "sales_amount", "benefit_per_order", "sales_per_customer", "order_profit",
        "profit_margin_pct", "market", "latitude", "longitude",
    ]].rename(columns={"type": "order_type"}).copy()

    staging["load_batch_id"] = batch_id
    staging["staged_at"]     = datetime.now(timezone.utc)

    engine = get_engine("staging")
    staging.to_sql(
        "stg_orders", con=engine, schema="staging",
        if_exists="append", index=False, method="multi", chunksize=2000,
    )
    log.info("Staged %d orders", len(staging))
    return len(staging)


def _stage_order_items(raw: pd.DataFrame, batch_id: str) -> int:
    """Extract order-item-grain rows and write to staging.stg_order_items."""
    df = raw.copy()

    df["order_item_id"]            = _safe_int(df["order_item_id"])
    df["order_id"]                 = _safe_int(df["order_id"])
    df["product_card_id"]          = _safe_int(df["product_card_id"])
    df["department_id"]            = _safe_int(df["department_id"])
    df["order_item_quantity"]      = _safe_int(df["order_item_quantity"])
    df["order_item_product_price"] = _safe_float(df["order_item_product_price"])
    df["order_item_discount"]      = _safe_float(df["order_item_discount"])
    df["order_item_discount_rate"] = _safe_float(df["order_item_discount_rate"])
    df["order_item_total"]         = _safe_float(df["order_item_total"])
    df["order_item_profit_ratio"]  = _safe_float(df["order_item_profit_ratio"])
    df["order_item_profit"]        = (
        df["order_item_total"] * df["order_item_profit_ratio"]
    )

    df = dedup_order_items(df)

    staging = df[[
        "order_item_id", "order_id", "product_card_id", "department_id",
        "order_item_quantity", "order_item_product_price", "order_item_discount",
        "order_item_discount_rate", "order_item_total", "order_item_profit_ratio",
        "order_item_profit",
    ]].copy()

    staging["load_batch_id"] = batch_id
    staging["staged_at"]     = datetime.now(timezone.utc)

    engine = get_engine("staging")
    staging.to_sql(
        "stg_order_items", con=engine, schema="staging",
        if_exists="append", index=False, method="multi", chunksize=2000,
    )
    log.info("Staged %d order items", len(staging))
    return len(staging)


def _stage_customers(raw: pd.DataFrame, batch_id: str) -> int:
    """Extract unique customers and write to staging.stg_customers."""
    import hashlib

    df = raw[["customer_id", "customer_fname", "customer_lname", "customer_email",
              "customer_segment", "customer_city", "customer_state",
              "customer_country", "customer_zipcode", "customer_street",
              "ingestion_ts"]].copy()

    df["customer_id"] = _safe_int(df["customer_id"])
    df = dedup_customers(df)

    # Compute row_hash over tracked attributes (mirrors SCD2 change detection)
    tracked = ["customer_segment", "customer_city", "customer_state",
               "customer_country", "customer_zipcode", "customer_street"]

    def _hash(row):
        raw_str = "|".join(
            "NULL" if pd.isna(v) else str(v).strip().lower()
            for v in row
        )
        return hashlib.md5(raw_str.encode()).hexdigest()

    df["row_hash"] = df[tracked].apply(_hash, axis=1)

    staging = df[[
        "customer_id", "customer_fname", "customer_lname", "customer_email",
        "customer_segment", "customer_city", "customer_state", "customer_country",
        "customer_zipcode", "customer_street", "row_hash",
    ]].copy()
    staging["load_batch_id"] = batch_id
    staging["staged_at"]     = datetime.now(timezone.utc)

    engine = get_engine("staging")
    staging.to_sql(
        "stg_customers", con=engine, schema="staging",
        if_exists="append", index=False, method="multi", chunksize=2000,
    )
    log.info("Staged %d customers", len(staging))
    return len(staging)


def _stage_products(raw: pd.DataFrame, batch_id: str) -> int:
    """Extract unique products and write to staging.stg_products."""
    df = raw[["product_card_id", "product_name", "product_price", "product_status",
              "category_id", "category_name", "department_id", "department_name",
              "product_image", "ingestion_ts"]].copy()

    df["product_card_id"] = _safe_int(df["product_card_id"])
    df["product_price"]   = _safe_float(df["product_price"])
    df["category_id"]     = _safe_int(df["category_id"])
    df["department_id"]   = _safe_int(df["department_id"])
    df = dedup_products(df)

    staging = df[[
        "product_card_id", "product_name", "product_price", "product_status",
        "category_id", "category_name", "department_id", "department_name",
        "product_image",
    ]].copy()
    staging["load_batch_id"] = batch_id
    staging["staged_at"]     = datetime.now(timezone.utc)

    engine = get_engine("staging")
    staging.to_sql(
        "stg_products", con=engine, schema="staging",
        if_exists="append", index=False, method="multi", chunksize=2000,
    )
    log.info("Staged %d products", len(staging))
    return len(staging)


def _stage_suppliers(raw: pd.DataFrame, batch_id: str) -> int:
    """Compute supplier performance metrics and write to staging.stg_suppliers."""
    import hashlib

    df = raw.copy()
    df["department_id"]      = _safe_int(df["department_id"])
    df["late_delivery_risk"] = _safe_int(df["late_delivery_risk"])
    df["sales_amount"]       = _safe_float(df["sales"])
    df["order_profit"]       = _safe_float(df["order_profit_per_order"])

    # Aggregate per department — on_time_rate and avg_profit_margin
    agg = df.groupby(["department_id", "department_name", "market"]).agg(
        total_orders=("order_id", "nunique"),
        late_orders=("late_delivery_risk", "sum"),
        avg_profit_margin=("order_profit", "mean"),
    ).reset_index()

    agg["on_time_rate"]     = 1.0 - (agg["late_orders"] / agg["total_orders"].replace(0, 1))
    agg["avg_profit_margin"] = agg["avg_profit_margin"].fillna(0)

    agg = compute_supplier_score(agg)
    agg = dedup_suppliers(agg)

    # Compute row_hash for SCD2 change detection
    tracked = ["performance_tier", "composite_score", "on_time_rate"]

    def _hash(row):
        raw_str = "|".join(
            "NULL" if pd.isna(v) else f"{v:.4f}" if isinstance(v, float) else str(v)
            for v in row
        )
        return hashlib.md5(raw_str.encode()).hexdigest()

    agg["row_hash"] = agg[tracked].apply(_hash, axis=1)

    staging = agg[[
        "department_id", "department_name", "market",
        "on_time_rate", "avg_profit_margin", "composite_score",
        "performance_tier", "row_hash",
    ]].rename(columns={
        "department_id":   "supplier_id",
        "department_name": "supplier_name",
        "market":          "supplier_market",
    }).copy()
    staging["load_batch_id"] = batch_id
    staging["staged_at"]     = datetime.now(timezone.utc)

    engine = get_engine("staging")
    staging.to_sql(
        "stg_suppliers", con=engine, schema="staging",
        if_exists="append", index=False, method="multi", chunksize=2000,
    )
    log.info("Staged %d suppliers", len(staging))
    return len(staging)


def run_staging(batch_id: str) -> dict:
    """Transform raw.orders for *batch_id* into all 5 staging tables.

    Returns a dict with row counts per table and total duration.
    """
    start = datetime.now(timezone.utc)
    log.info("Starting staging transform | batch_id=%s", batch_id)

    raw = _read_raw_batch(batch_id)
    if raw.empty:
        raise ValueError(f"No raw rows found for batch_id={batch_id}")

    _delete_existing(batch_id)

    counts = {
        "stg_orders":       _stage_orders(raw, batch_id),
        "stg_order_items":  _stage_order_items(raw, batch_id),
        "stg_customers":    _stage_customers(raw, batch_id),
        "stg_products":     _stage_products(raw, batch_id),
        "stg_suppliers":    _stage_suppliers(raw, batch_id),
    }

    duration = (datetime.now(timezone.utc) - start).total_seconds()
    log.info("Staging complete | batch_id=%s | duration=%.1fs | %s", batch_id, duration, counts)
    return {"batch_id": batch_id, "duration_seconds": duration, **counts}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
    if len(sys.argv) < 2:
        print("Usage: python -m src.transformations.staging <batch_id>")
        sys.exit(1)
    result = run_staging(sys.argv[1])
    print(result)
