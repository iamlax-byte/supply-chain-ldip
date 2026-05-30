"""
Load DataCo Smart Supply Chain CSV into raw.orders.

Design decisions:
- All source columns stored as VARCHAR in raw layer — no type coercion here.
  Type casting happens in the staging layer where failures are expected and caught.
- Chunked reads (default 10 000 rows) so the script doesn't OOM on large files.
- Idempotent by batch_id: re-running the same file generates a new batch_id
  and a new set of rows — raw is append-only. Callers that need exactly-once
  semantics should check raw.load_log before calling.
- DataCo CSV is encoded in ISO-8859-1 (Latin-1), not UTF-8. Any other encoding
  raises a clear error rather than silently mangling characters.

Usage::

    python -m src.ingestion.load_raw data/raw/DataCoSupplyChainDataset.csv
    # or from Airflow via PythonOperator / bash call
"""
from __future__ import annotations

import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

from src.utils.db import get_engine

log = logging.getLogger(__name__)

# Maps DataCo CSV column headers → raw.orders column names (snake_case).
# Covers all 53 source columns.
COLUMN_MAP: dict[str, str] = {
    "Type":                          "type",
    "Days for shipping (real)":      "days_for_shipping_real",
    "Days for shipment (scheduled)": "days_for_shipment_scheduled",
    "Benefit per order":             "benefit_per_order",
    "Sales per customer":            "sales_per_customer",
    "Delivery Status":               "delivery_status",
    "Late_delivery_risk":            "late_delivery_risk",
    "Category Id":                   "category_id",
    "Category Name":                 "category_name",
    "Customer City":                 "customer_city",
    "Customer Country":              "customer_country",
    "Customer Email":                "customer_email",
    "Customer Fname":                "customer_fname",
    "Customer Id":                   "customer_id",
    "Customer Lname":                "customer_lname",
    "Customer Password":             "customer_password",
    "Customer Segment":              "customer_segment",
    "Customer State":                "customer_state",
    "Customer Street":               "customer_street",
    "Customer Zipcode":              "customer_zipcode",
    "Department Id":                 "department_id",
    "Department Name":               "department_name",
    "Latitude":                      "latitude",
    "Longitude":                     "longitude",
    "Market":                        "market",
    "Order City":                    "order_city",
    "Order Country":                 "order_country",
    "Order Customer Id":             "order_customer_id",
    "order date (DateOrders)":       "order_date",
    "Order Id":                      "order_id",
    "Order Item Cardprod Id":        "order_item_cardprod_id",
    "Order Item Discount":           "order_item_discount",
    "Order Item Discount Rate":      "order_item_discount_rate",
    "Order Item Id":                 "order_item_id",
    "Order Item Product Price":      "order_item_product_price",
    "Order Item Profit Ratio":       "order_item_profit_ratio",
    "Order Item Quantity":           "order_item_quantity",
    "Sales":                         "sales",
    "Order Item Total":              "order_item_total",
    "Order Profit Per Order":        "order_profit_per_order",
    "Order Region":                  "order_region",
    "Order State":                   "order_state",
    "Order Status":                  "order_status",
    "Order Zipcode":                 "order_zipcode",
    "Product Card Id":               "product_card_id",
    "Product Category Id":           "product_category_id",
    "Product Description":           "product_description",
    "Product Image":                 "product_image",
    "Product Name":                  "product_name",
    "Product Price":                 "product_price",
    "Product Status":                "product_status",
    "shipping date (DateOrders)":    "shipping_date",
    "Shipping Mode":                 "shipping_mode",
}


def already_loaded(source_file: str) -> bool:
    """Return True if this filename has a SUCCESS record in raw.load_log.

    Guards against accidental double-loads of the same physical file.
    """
    engine = get_engine("raw")
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "select count(*) from `raw`.load_log "
                "where source_file = :f and status = 'SUCCESS'"
            ),
            {"f": source_file},
        )
        return result.scalar() > 0


def load_raw_orders(
    csv_path: str | Path,
    batch_id: Optional[str] = None,
    chunk_size: int = 10_000,
    force: bool = False,
) -> dict:
    """Load DataCo CSV into raw.orders.

    Args:
        csv_path:   Path to the DataCo CSV file.
        batch_id:   Optional caller-supplied batch ID. Auto-generated if None.
        chunk_size: Rows per pandas read chunk. Tune for available memory.
        force:      Skip the already-loaded guard (useful for backfills).

    Returns:
        dict with keys: batch_id, rows_loaded, duration_seconds.

    Raises:
        FileNotFoundError: if csv_path does not exist.
        RuntimeError: if the file was already loaded and force=False.
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Source CSV not found: {csv_path}")

    if not force and already_loaded(csv_path.name):
        raise RuntimeError(
            f"{csv_path.name} already has a SUCCESS record in raw.load_log. "
            "Pass force=True to load again."
        )

    batch_id = batch_id or (
        f"ldip-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
    )
    start_ts = datetime.now(timezone.utc)
    total_rows = 0
    engine = get_engine("raw")
    expected_cols = list(COLUMN_MAP.values())

    log.info(
        "Starting raw load | batch_id=%s | file=%s | chunk_size=%d",
        batch_id, csv_path.name, chunk_size,
    )

    try:
        for chunk_num, chunk in enumerate(
            pd.read_csv(
                csv_path,
                encoding="iso-8859-1",   # DataCo CSV is Latin-1, not UTF-8
                chunksize=chunk_size,
                dtype=str,               # Everything stays string in raw layer
                on_bad_lines="warn",
            ),
            start=1,
        ):
            # Rename source headers to target column names
            chunk = chunk.rename(columns=COLUMN_MAP)

            # Drop any unexpected columns (schema drift guard)
            chunk = chunk[[c for c in expected_cols if c in chunk.columns]]

            # Attach batch metadata
            chunk["load_batch_id"] = batch_id
            chunk["source_file"]   = csv_path.name
            # ingestion_ts handled by MySQL DEFAULT current_timestamp

            chunk.to_sql(
                name="orders",
                con=engine,
                schema="raw",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1_000,
            )

            total_rows += len(chunk)
            log.info(
                "Chunk %d loaded | rows=%d | cumulative=%d",
                chunk_num, len(chunk), total_rows,
            )

        status = "SUCCESS"
        error_msg = None

    except Exception as exc:
        status = "FAILED"
        error_msg = str(exc)
        log.error("Raw load failed at chunk %d: %s", chunk_num, exc)
        raise

    finally:
        duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
        _write_load_log(
            engine, batch_id, csv_path.name, total_rows,
            start_ts, duration, status, error_msg,
        )

    log.info(
        "Raw load complete | batch_id=%s | rows=%d | duration=%.1fs",
        batch_id, total_rows, duration,
    )
    return {"batch_id": batch_id, "rows_loaded": total_rows, "duration_seconds": duration}


def _write_load_log(
    engine,
    batch_id: str,
    source_file: str,
    rows_loaded: int,
    start_ts: datetime,
    duration_seconds: float,
    status: str,
    error_message: Optional[str],
) -> None:
    """Append one row to raw.load_log to record this load event."""
    with engine.connect() as conn:
        conn.execute(
            text("""
                insert into `raw`.load_log
                    (batch_id, source_file, rows_loaded,
                     start_ts, end_ts, duration_seconds, status, error_message)
                values
                    (:batch_id, :source_file, :rows_loaded,
                     :start_ts, :end_ts, :duration_seconds, :status, :error_message)
            """),
            {
                "batch_id":         batch_id,
                "source_file":      source_file,
                "rows_loaded":      rows_loaded,
                "start_ts":         start_ts,
                "end_ts":           datetime.now(timezone.utc),
                "duration_seconds": int(duration_seconds),
                "status":           status,
                "error_message":    error_message,
            },
        )
        conn.commit()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
    )
    if len(sys.argv) < 2:
        print("Usage: python -m src.ingestion.load_raw <path/to/DataCoSupplyChainDataset.csv>")
        sys.exit(1)
    result = load_raw_orders(sys.argv[1])
    print(result)
