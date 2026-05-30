"""
Initialize all MySQL schemas by executing DDL scripts in order.

Run once before the first pipeline execution, or any time you need to
recreate schemas from scratch (e.g. after a docker-compose down -v).

Usage::

    python -m src.utils.init_db
    # or
    python src/utils/init_db.py
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

from sqlalchemy import text

from src.utils.db import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
log = logging.getLogger(__name__)

# Resolved relative to this file → works regardless of cwd
DDL_DIR = Path(__file__).parents[2] / "sql" / "ddl"

# Execute in this exact order — later scripts depend on schemas from earlier ones.
DDL_SCRIPTS = [
    "00_create_schemas.sql",
    "01_raw_tables.sql",
    "02_staging_tables.sql",
    "03_warehouse_tables.sql",
    "04_marts_tables.sql",
    "05_metadata_tables.sql",
]


def _split_statements(sql: str) -> list[str]:
    """Split a SQL file on semicolons, skipping blanks and comment-only lines."""
    statements = []
    for raw in sql.split(";"):
        stripped = raw.strip()
        # Skip empty chunks and pure comment blocks
        if stripped and not all(
            line.startswith("--") or not line for line in stripped.splitlines()
        ):
            statements.append(stripped)
    return statements


def run_ddl_script(script_path: Path) -> None:
    """Execute a multi-statement DDL script.

    Connects without specifying a schema so CREATE SCHEMA statements work
    before any user schema exists.
    """
    sql = script_path.read_text(encoding="utf-8")
    statements = _split_statements(sql)

    # Use the system 'mysql' schema so we can run CREATE SCHEMA statements
    engine = get_engine(schema="")
    with engine.connect() as conn:
        for stmt in statements:
            conn.execute(text(stmt))
        conn.commit()

    log.info("Executed DDL script: %s (%d statements)", script_path.name, len(statements))


def init_db() -> None:
    """Run all DDL scripts in order to build the full schema hierarchy."""
    missing = [DDL_DIR / s for s in DDL_SCRIPTS if not (DDL_DIR / s).exists()]
    if missing:
        log.error("Missing DDL scripts: %s", [p.name for p in missing])
        sys.exit(1)

    log.info("Starting database initialization from %s", DDL_DIR)
    for script_name in DDL_SCRIPTS:
        run_ddl_script(DDL_DIR / script_name)

    log.info("Database initialization complete — all schemas ready.")


if __name__ == "__main__":
    init_db()
