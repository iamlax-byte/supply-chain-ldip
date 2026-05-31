"""
Database connection utilities.

All pipeline code should obtain engines/connections through this module
so that credentials come exclusively from environment variables and never
appear in source code or logs.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv()

# Schemas available in the warehouse
SCHEMAS = ("raw", "staging", "warehouse", "marts", "metadata")


def get_engine(schema: str = "raw") -> Engine:
    """Return a SQLAlchemy engine connected to *schema*.

    Falls back to the 'mysql' system schema when schema is empty so that
    schema-creation DDL can run without needing a pre-existing user schema.
    """
    host = os.environ["MYSQL_HOST"]
    port = os.environ.get("MYSQL_PORT", "3306")
    user = os.environ["MYSQL_USER"]
    password = os.environ["MYSQL_PASSWORD"]
    db_name = schema if schema else "mysql"

    url = (
        f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db_name}"
        "?charset=utf8mb4"
    )
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False,
        future=True,   # SQLAlchemy 2.0-style API — enables conn.commit()/rollback()
                       # Required when running under Airflow 2.9 which pins SQLAlchemy 1.4
    )


@contextmanager
def get_connection(schema: str = "raw") -> Generator:
    """Context manager that yields a SQLAlchemy Connection for *schema*.

    Commits on clean exit; rolls back on exception.

    Example::

        with get_connection("staging") as conn:
            conn.execute(text("select 1"))
    """
    engine = get_engine(schema)
    with engine.connect() as conn:
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
