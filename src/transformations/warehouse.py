"""
Staging → Warehouse load.

Populates all dimension and fact tables from the staging layer for a given
batch_id. Every load is idempotent: INSERT IGNORE on facts (duplicate key
= (order_id, order_date_key)) means re-running the same batch is safe.

Load order matters because of foreign key relationships:
  1. dim_date         (ensure coverage for order dates)
  2. dim_shipping_mode (seed — 4 static rows)
  3. dim_geography    (from stg_orders)
  4. dim_product      (SCD1 — from stg_products)
  5. dim_customer     (SCD2 — from stg_customers)
  6. dim_supplier     (SCD2 — from stg_suppliers)
  7. fct_orders       (joins dims 1–6 via SQL subqueries)
  8. fct_order_items  (joins product + supplier dims)
  9. fct_shipments    (joins geography + shipping_mode dims)

SQL-based ETL is used for fact loads (INSERT … SELECT) so MySQL does the
join and no intermediate DataFrame needs to fit in memory.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.transformations.scd2 import compute_row_hash, merge_scd2
from src.transformations.date_dim import load_date_dim
from src.utils.db import get_engine

log = logging.getLogger(__name__)

# Static seed data for dim_shipping_mode — same four values as DataCo source
_SHIPPING_MODES = [
    ("Standard Class", 5, "standard"),
    ("First Class",    2, "express"),
    ("Second Class",   3, "standard"),
    ("Same Day",       0, "same-day"),
]


# ── Dimension loaders ─────────────────────────────────────────────────────────

def seed_dim_shipping_mode(engine: Engine) -> None:
    """Insert the 4 known shipping modes if they don't already exist."""
    with engine.connect() as conn:
        for name, days, tier in _SHIPPING_MODES:
            conn.execute(
                text("""
                    insert ignore into warehouse.dim_shipping_mode
                        (shipping_mode_name, avg_days_committed, service_tier)
                    values (:name, :days, :tier)
                """),
                {"name": name, "days": days, "tier": tier},
            )
        conn.commit()
    log.info("dim_shipping_mode seeded")


def load_dim_geography(engine: Engine, batch_id: str) -> int:
    """Upsert distinct geography records from stg_orders into dim_geography.

    Natural key: (order_country, order_state, order_city) — enforced by a
    unique index on the dimension table.  INSERT IGNORE skips duplicates.
    """
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                insert ignore into warehouse.dim_geography
                    (market, order_region, order_country, order_state,
                     order_city, latitude, longitude)
                select distinct
                    market, order_region, order_country, order_state,
                    order_city,
                    avg(latitude)  over (partition by order_country, order_state, order_city),
                    avg(longitude) over (partition by order_country, order_state, order_city)
                from staging.stg_orders
                where load_batch_id = :batch_id
                  and order_country is not null
                  and order_city    is not null
            """),
            {"batch_id": batch_id},
        )
        conn.commit()
    rows = result.rowcount
    log.info("dim_geography: %d new rows inserted", rows)
    return rows


def load_dim_product(engine: Engine, batch_id: str) -> int:
    """SCD1 upsert for dim_product.

    ON DUPLICATE KEY UPDATE overwrites stale attributes in-place — we only
    care about the current product state, not its history.
    """
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                insert into warehouse.dim_product
                    (product_card_id, product_name, product_price, product_status,
                     category_id, category_name, department_id, department_name,
                     product_image)
                select
                    product_card_id, product_name, product_price, product_status,
                    category_id, category_name, department_id, department_name,
                    product_image
                from staging.stg_products
                where load_batch_id = :batch_id
                on duplicate key update
                    product_name    = values(product_name),
                    product_price   = values(product_price),
                    product_status  = values(product_status),
                    category_name   = values(category_name),
                    department_name = values(department_name),
                    updated_at      = current_timestamp
            """),
            {"batch_id": batch_id},
        )
        conn.commit()
    rows = result.rowcount
    log.info("dim_product: %d rows upserted (SCD1)", rows)
    return rows


def merge_dim_customer(engine: Engine, batch_id: str, effective_date: date) -> dict:
    """SCD2 merge for dim_customer from staging.stg_customers.

    Tracked attributes (change → new dimension version):
      customer_segment, customer_city, customer_state,
      customer_country, customer_zipcode, customer_street

    Non-tracked (updated in-place, no new version):
      customer_fname, customer_lname, customer_email
    """
    with engine.connect() as conn:
        staging_df = pd.read_sql(
            "select * from staging.stg_customers where load_batch_id = %(bid)s",
            conn,
            params={"bid": batch_id},
        )

    if staging_df.empty:
        log.info("dim_customer: no staging rows for batch_id=%s", batch_id)
        return {"inserted_new": 0, "closed_old": 0, "unchanged": 0}

    return merge_scd2(
        engine=engine,
        staging_df=staging_df,
        schema="warehouse",
        table="dim_customer",
        natural_key="customer_id",
        surrogate_key="customer_key",
        dim_cols=[
            "customer_id", "customer_fname", "customer_lname", "customer_email",
            "customer_segment", "customer_city", "customer_state",
            "customer_country", "customer_zipcode", "customer_street",
        ],
        tracked_cols=[
            "customer_segment", "customer_city", "customer_state",
            "customer_country", "customer_zipcode", "customer_street",
        ],
        effective_date=effective_date,
    )


def merge_dim_supplier(engine: Engine, batch_id: str, effective_date: date) -> dict:
    """SCD2 merge for dim_supplier from staging.stg_suppliers.

    Tracked attributes: performance_tier, composite_score, on_time_rate.
    A new version is minted when any of these shift — capturing the supplier's
    performance trajectory over time for historical analysis.
    """
    with engine.connect() as conn:
        staging_df = pd.read_sql(
            "select * from staging.stg_suppliers where load_batch_id = %(bid)s",
            conn,
            params={"bid": batch_id},
        )

    if staging_df.empty:
        log.info("dim_supplier: no staging rows for batch_id=%s", batch_id)
        return {"inserted_new": 0, "closed_old": 0, "unchanged": 0}

    return merge_scd2(
        engine=engine,
        staging_df=staging_df,
        schema="warehouse",
        table="dim_supplier",
        natural_key="supplier_id",
        surrogate_key="supplier_key",
        dim_cols=[
            "supplier_id", "supplier_name", "supplier_market",
            "on_time_rate", "avg_profit_margin", "composite_score", "performance_tier",
        ],
        tracked_cols=["performance_tier", "composite_score", "on_time_rate"],
        effective_date=effective_date,
    )


# ── Fact loaders ──────────────────────────────────────────────────────────────

def load_fct_orders(engine: Engine, batch_id: str) -> int:
    """Insert order-grain rows into warehouse.fct_orders.

    Resolves dimension surrogate keys via SQL joins so no Python-side memory
    is needed for the lookup tables.  INSERT IGNORE is idempotent: the unique
    key (order_id, order_date_key) prevents duplicate rows on re-run.

    Note: MySQL does not support FK constraints on partitioned tables, so
    referential integrity is enforced here by the inner joins — orders whose
    customer, geography, or date cannot be resolved are silently excluded and
    logged as rejected rows.
    """
    with engine.connect() as conn:
        # Count staging rows before load (for metadata logging)
        read_count = conn.execute(
            text("select count(*) from staging.stg_orders where load_batch_id = :bid"),
            {"bid": batch_id},
        ).scalar()

        result = conn.execute(
            text("""
                insert ignore into warehouse.fct_orders (
                    order_id, customer_key, geography_key,
                    order_date_key, shipping_date_key, shipping_mode_key,
                    order_status, order_type, order_region,
                    sales_amount, benefit_per_order, sales_per_customer,
                    order_profit, profit_margin_pct,
                    days_for_shipping_real, days_for_shipment_scheduled,
                    delivery_delay_days, is_late_flag, late_delivery_risk,
                    delivery_status, load_batch_id
                )
                select
                    so.order_id,
                    dc.customer_key,
                    dg.geography_key,
                    -- date_key: YYYYMMDD integer
                    year(so.order_date)   * 10000
                        + month(so.order_date)  * 100
                        + day(so.order_date)    as order_date_key,
                    case when so.shipping_date is not null
                        then year(so.shipping_date)  * 10000
                           + month(so.shipping_date) * 100
                           + day(so.shipping_date)
                        else null
                    end                         as shipping_date_key,
                    dsm.shipping_mode_key,
                    so.order_status,
                    so.order_type,
                    so.order_region,
                    so.sales_amount,
                    so.benefit_per_order,
                    so.sales_per_customer,
                    so.order_profit,
                    so.profit_margin_pct,
                    so.days_for_shipping_real,
                    so.days_for_shipment_scheduled,
                    so.delivery_delay_days,
                    so.is_late_flag,
                    so.late_delivery_risk,
                    so.delivery_status,
                    so.load_batch_id
                from staging.stg_orders so
                -- Inner join: orders without a resolvable customer are excluded
                join warehouse.dim_customer dc
                    on dc.customer_id = so.order_customer_id
                   and dc.is_current  = 1
                -- Inner join: orders without a resolvable geography are excluded
                join warehouse.dim_geography dg
                    on dg.order_country = so.order_country
                   and dg.order_state   = so.order_state
                   and dg.order_city    = so.order_city
                -- Left join: shipping mode may be NULL for some orders
                left join warehouse.dim_shipping_mode dsm
                    on dsm.shipping_mode_name = so.shipping_mode
                where so.load_batch_id = :bid
                  and so.order_date   is not null   -- must have a valid date_key
            """),
            {"bid": batch_id},
        )
        conn.commit()

    written = result.rowcount
    rejected = read_count - written
    log.info(
        "fct_orders: read=%d  written=%d  rejected=%d (unresolvable dims)",
        read_count, written, rejected,
    )
    return written


def load_fct_order_items(engine: Engine, batch_id: str) -> int:
    """Insert order-item-grain rows into warehouse.fct_order_items."""
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                insert ignore into warehouse.fct_order_items (
                    order_item_id, order_id,
                    product_key, supplier_key, order_date_key,
                    order_item_quantity, order_item_product_price,
                    order_item_discount, order_item_discount_rate,
                    order_item_total, order_item_profit_ratio, order_item_profit,
                    load_batch_id
                )
                select
                    si.order_item_id,
                    si.order_id,
                    dp.product_key,
                    ds.supplier_key,
                    year(so.order_date)  * 10000
                        + month(so.order_date) * 100
                        + day(so.order_date)   as order_date_key,
                    si.order_item_quantity,
                    si.order_item_product_price,
                    si.order_item_discount,
                    si.order_item_discount_rate,
                    si.order_item_total,
                    si.order_item_profit_ratio,
                    si.order_item_profit,
                    si.load_batch_id
                from staging.stg_order_items si
                join staging.stg_orders so
                    on so.order_id        = si.order_id
                   and so.load_batch_id   = si.load_batch_id
                join warehouse.dim_product dp
                    on dp.product_card_id = si.product_card_id
                join warehouse.dim_supplier ds
                    on ds.supplier_id     = si.department_id
                   and ds.is_current      = 1
                where si.load_batch_id = :bid
                  and so.order_date   is not null
            """),
            {"bid": batch_id},
        )
        conn.commit()

    rows = result.rowcount
    log.info("fct_order_items: %d rows written", rows)
    return rows


def load_fct_shipments(engine: Engine, batch_id: str) -> int:
    """Insert shipment-event rows into warehouse.fct_shipments."""
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                insert ignore into warehouse.fct_shipments (
                    order_id, shipping_mode_key, shipping_date_key,
                    order_date_key, geography_key,
                    shipping_mode, delivery_status,
                    days_for_shipping_real, days_for_shipment_scheduled,
                    delivery_delay_days, is_late_flag, late_delivery_risk,
                    load_batch_id
                )
                select
                    so.order_id,
                    dsm.shipping_mode_key,
                    case when so.shipping_date is not null
                        then year(so.shipping_date)  * 10000
                           + month(so.shipping_date) * 100
                           + day(so.shipping_date)
                        else null
                    end                                as shipping_date_key,
                    year(so.order_date)  * 10000
                        + month(so.order_date) * 100
                        + day(so.order_date)           as order_date_key,
                    dg.geography_key,
                    so.shipping_mode,
                    so.delivery_status,
                    so.days_for_shipping_real,
                    so.days_for_shipment_scheduled,
                    so.delivery_delay_days,
                    so.is_late_flag,
                    so.late_delivery_risk,
                    so.load_batch_id
                from staging.stg_orders so
                join warehouse.dim_geography dg
                    on dg.order_country = so.order_country
                   and dg.order_state   = so.order_state
                   and dg.order_city    = so.order_city
                left join warehouse.dim_shipping_mode dsm
                    on dsm.shipping_mode_name = so.shipping_mode
                where so.load_batch_id = :bid
                  and so.order_date   is not null
            """),
            {"bid": batch_id},
        )
        conn.commit()

    rows = result.rowcount
    log.info("fct_shipments: %d rows written", rows)
    return rows


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_warehouse_load(
    batch_id: str,
    effective_date: Optional[date] = None,
) -> dict:
    """Run the full staging → warehouse load sequence for one batch.

    Args:
        batch_id:       Identifies which staging rows to promote.
        effective_date: SCD2 effective date (defaults to today UTC).

    Returns:
        dict with row counts per table.
    """
    effective_date = effective_date or datetime.now(timezone.utc).date()
    engine = get_engine("warehouse")
    start = datetime.now(timezone.utc)

    log.info("Starting warehouse load | batch_id=%s | effective_date=%s",
             batch_id, effective_date)

    # Ensure dim_date covers the full DataCo date range on first run
    _ensure_date_dim_coverage(engine)

    seed_dim_shipping_mode(engine)
    geo_rows      = load_dim_geography(engine, batch_id)
    prod_rows     = load_dim_product(engine, batch_id)
    cust_result   = merge_dim_customer(engine, batch_id, effective_date)
    sup_result    = merge_dim_supplier(engine, batch_id, effective_date)
    order_rows    = load_fct_orders(engine, batch_id)
    item_rows     = load_fct_order_items(engine, batch_id)
    ship_rows     = load_fct_shipments(engine, batch_id)

    duration = (datetime.now(timezone.utc) - start).total_seconds()
    counts = {
        "dim_geography":    geo_rows,
        "dim_product":      prod_rows,
        "dim_customer":     cust_result,
        "dim_supplier":     sup_result,
        "fct_orders":       order_rows,
        "fct_order_items":  item_rows,
        "fct_shipments":    ship_rows,
        "duration_seconds": duration,
    }
    log.info("Warehouse load complete | batch_id=%s | duration=%.1fs", batch_id, duration)
    return counts


def _ensure_date_dim_coverage(engine: Engine) -> None:
    """Populate dim_date for 2010–2030 if it is empty or missing dates."""
    with engine.connect() as conn:
        row_count = conn.execute(
            text("select count(*) from warehouse.dim_date")
        ).scalar()

    if row_count < 365:
        log.info("dim_date has %d rows — generating full 2010–2030 range", row_count)
        load_date_dim(date(2010, 1, 1), date(2030, 12, 31))
    else:
        log.debug("dim_date already populated (%d rows)", row_count)
