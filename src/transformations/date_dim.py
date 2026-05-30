"""
Date dimension generator for warehouse.dim_date.

Generates one row per calendar day between start_date and end_date (inclusive).
date_key format: YYYYMMDD integer — compact, fast to join, human-readable.

Usage::

    python -m src.transformations.date_dim 2015-01-01 2025-12-31
    # Generates and loads 10 years of calendar rows into warehouse.dim_date
"""
from __future__ import annotations

import logging
import sys
from datetime import date

import pandas as pd
from sqlalchemy import text

from src.utils.db import get_engine

log = logging.getLogger(__name__)


def generate_date_dim(start_date: date, end_date: date) -> pd.DataFrame:
    """Build a dim_date DataFrame covering every day in [start_date, end_date].

    Returns columns matching the warehouse.dim_date DDL:
      date_key, full_date, day_of_week, day_name, day_of_month, day_of_year,
      week_of_year, month_num, month_name, quarter_num, year_num,
      fiscal_quarter, fiscal_year, is_weekend, is_holiday
    """
    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    df = pd.DataFrame({"full_date": dates})

    df["date_key"]      = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["day_of_week"]   = df["full_date"].dt.dayofweek + 2   # 1=Sun…7=Sat (ISO-like)
    # Adjust: pandas dayofweek is Mon=0…Sun=6; we want Sun=1…Sat=7
    df["day_of_week"]   = df["full_date"].dt.dayofweek.map(
        {0: 2, 1: 3, 2: 4, 3: 5, 4: 6, 5: 7, 6: 1}
    )
    df["day_name"]      = df["full_date"].dt.day_name()
    df["day_of_month"]  = df["full_date"].dt.day
    df["day_of_year"]   = df["full_date"].dt.dayofyear
    df["week_of_year"]  = df["full_date"].dt.isocalendar().week.astype(int)
    df["month_num"]     = df["full_date"].dt.month
    df["month_name"]    = df["full_date"].dt.month_name()
    df["quarter_num"]   = df["full_date"].dt.quarter
    df["year_num"]      = df["full_date"].dt.year
    # Fiscal year = calendar year (adjust offset here if fiscal year differs)
    df["fiscal_quarter"] = df["quarter_num"]
    df["fiscal_year"]    = df["year_num"]
    df["is_weekend"]    = (df["full_date"].dt.dayofweek >= 5).astype(int)
    df["is_holiday"]    = 0   # placeholder — extend with a holiday calendar if needed

    df["full_date"] = df["full_date"].dt.date   # store as date, not datetime

    return df[[
        "date_key", "full_date", "day_of_week", "day_name", "day_of_month",
        "day_of_year", "week_of_year", "month_num", "month_name", "quarter_num",
        "year_num", "fiscal_quarter", "fiscal_year", "is_weekend", "is_holiday",
    ]]


def load_date_dim(
    start_date: date,
    end_date: date,
    replace: bool = False,
) -> int:
    """Generate dim_date rows and upsert into warehouse.dim_date.

    Args:
        start_date: First date to generate.
        end_date:   Last date to generate (inclusive).
        replace:    If True, truncate the table before inserting (full rebuild).

    Returns:
        Number of rows written.
    """
    engine = get_engine("warehouse")
    df = generate_date_dim(start_date, end_date)

    with engine.connect() as conn:
        if replace:
            conn.execute(text("truncate table warehouse.dim_date"))
            log.info("Truncated warehouse.dim_date for full rebuild")
        conn.commit()

    # INSERT IGNORE skips rows whose date_key already exists (idempotent)
    rows_written = 0
    with engine.connect() as conn:
        for _, row in df.iterrows():
            conn.execute(
                text("""
                    insert ignore into warehouse.dim_date
                        (date_key, full_date, day_of_week, day_name, day_of_month,
                         day_of_year, week_of_year, month_num, month_name, quarter_num,
                         year_num, fiscal_quarter, fiscal_year, is_weekend, is_holiday)
                    values
                        (:date_key, :full_date, :day_of_week, :day_name, :day_of_month,
                         :day_of_year, :week_of_year, :month_num, :month_name, :quarter_num,
                         :year_num, :fiscal_quarter, :fiscal_year, :is_weekend, :is_holiday)
                """),
                row.to_dict(),
            )
            rows_written += 1
        conn.commit()

    log.info(
        "dim_date loaded: %d rows (%s → %s)", rows_written,
        start_date.isoformat(), end_date.isoformat(),
    )
    return rows_written


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
    start = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date(2015, 1, 1)
    end   = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else date(2025, 12, 31)
    n = load_date_dim(start, end)
    print(f"Loaded {n} rows into warehouse.dim_date")
