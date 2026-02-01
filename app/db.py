# app/db.py
from __future__ import annotations

import os

from sqlmodel import SQLModel, create_engine, Session
from sqlalchemy import inspect, text


DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    # Postgres (Neon)
    engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
else:
    # SQLite fallback (local)
    DB_PATH = os.getenv("INSPECTION_DB", "inspection.db")
    engine = create_engine(
        f"sqlite:///{DB_PATH}",
        echo=False,
        connect_args={"check_same_thread": False},
    )


def _ensure_schema_patches() -> None:
    """
    Minimal migration helper.
    SQLModel's create_all() won't add columns to existing tables, so we patch
    known missing columns here safely.
    """
    insp = inspect(engine)
    tables = set(insp.get_table_names())

    # ---- Patch: mrrreceiving.qty_arrived (missing in your Postgres) ----
    if "mrrreceiving" in tables:
        cols = {c["name"] for c in insp.get_columns("mrrreceiving")}
        if "qty_arrived" not in cols:
            dialect = engine.dialect.name
            if dialect == "postgresql":
                ddl = "ALTER TABLE mrrreceiving ADD COLUMN IF NOT EXISTS qty_arrived DOUBLE PRECISION"
            elif dialect == "sqlite":
                # SQLite supports ADD COLUMN (no IF NOT EXISTS reliably across all versions)
                ddl = "ALTER TABLE mrrreceiving ADD COLUMN qty_arrived REAL"
            else:
                # generic fallback
                ddl = "ALTER TABLE mrrreceiving ADD COLUMN qty_arrived FLOAT"

            with engine.begin() as conn:
                conn.execute(text(ddl))


def create_db_and_tables() -> None:
    # Create any missing tables first
    SQLModel.metadata.create_all(engine)

    # Then patch existing tables (add missing columns)
    _ensure_schema_patches()


def get_session():
    with Session(engine) as session:
        yield session
