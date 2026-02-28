# app/db.py
from __future__ import annotations

import os
from sqlmodel import SQLModel, create_engine, Session
from sqlalchemy import inspect, text


DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
else:
    DB_PATH = os.getenv("INSPECTION_DB", "inspection.db")
    engine = create_engine(
        f"sqlite:///{DB_PATH}",
        echo=False,
        connect_args={"check_same_thread": False},
    )


def _add_column_if_missing(table: str, col: str, ddl_postgres: str, ddl_sqlite: str) -> None:
    insp = inspect(engine)
    if table not in insp.get_table_names():
        return

    cols = {c["name"] for c in insp.get_columns(table)}
    if col in cols:
        return

    dialect = engine.dialect.name
    ddl = ddl_postgres if dialect == "postgresql" else ddl_sqlite

    with engine.begin() as conn:
        conn.execute(text(ddl))


def _ensure_schema_patches() -> None:
    # ✅ mrrreceiving new fields
    _add_column_if_missing(
        "mrrreceiving",
        "qty_arrived",
        "ALTER TABLE mrrreceiving ADD COLUMN IF NOT EXISTS qty_arrived DOUBLE PRECISION",
        "ALTER TABLE mrrreceiving ADD COLUMN qty_arrived REAL",
    )
    _add_column_if_missing(
        "mrrreceiving",
        "qty_unit",
        "ALTER TABLE mrrreceiving ADD COLUMN IF NOT EXISTS qty_unit VARCHAR(10) DEFAULT 'KG'",
        "ALTER TABLE mrrreceiving ADD COLUMN qty_unit TEXT DEFAULT 'KG'",
    )
    _add_column_if_missing(
        "mrrreceiving",
        "is_partial_delivery",
        "ALTER TABLE mrrreceiving ADD COLUMN IF NOT EXISTS is_partial_delivery BOOLEAN DEFAULT FALSE",
        "ALTER TABLE mrrreceiving ADD COLUMN is_partial_delivery INTEGER DEFAULT 0",
    )
    _add_column_if_missing(
        "mrrreceiving",
        "qty_mismatch_reason",
        "ALTER TABLE mrrreceiving ADD COLUMN IF NOT EXISTS qty_mismatch_reason TEXT DEFAULT ''",
        "ALTER TABLE mrrreceiving ADD COLUMN qty_mismatch_reason TEXT DEFAULT ''",
    )

    # ✅ materiallot new fields
    _add_column_if_missing(
        "materiallot",
        "quantity_unit",
        "ALTER TABLE materiallot ADD COLUMN IF NOT EXISTS quantity_unit VARCHAR(10) DEFAULT 'KG'",
        "ALTER TABLE materiallot ADD COLUMN quantity_unit TEXT DEFAULT 'KG'",
    )
    _add_column_if_missing(
        "materiallot",
        "received_total",
        "ALTER TABLE materiallot ADD COLUMN IF NOT EXISTS received_total DOUBLE PRECISION DEFAULT 0",
        "ALTER TABLE materiallot ADD COLUMN received_total REAL DEFAULT 0",
    )

    # --- materiallot ---
    _add_column_if_missing(
        "materiallot",
        "quantity_unit",
        "ALTER TABLE materiallot ADD COLUMN IF NOT EXISTS quantity_unit VARCHAR(10) DEFAULT 'KG'",
        "ALTER TABLE materiallot ADD COLUMN quantity_unit TEXT DEFAULT 'KG'",
    )
    _add_column_if_missing(
        "materiallot",
        "received_total",
        "ALTER TABLE materiallot ADD COLUMN IF NOT EXISTS received_total DOUBLE PRECISION DEFAULT 0",
        "ALTER TABLE materiallot ADD COLUMN received_total REAL DEFAULT 0",
    )
    
    # --- mrrreceivinginspection (shipment based) ---
    _add_column_if_missing(
        "mrrreceivinginspection",
        "created_at",
        "ALTER TABLE mrrreceivinginspection ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()",
        "ALTER TABLE mrrreceivinginspection ADD COLUMN created_at TEXT",
    )
    _add_column_if_missing(
        "mrrreceivinginspection",
        "delivery_note_no",
        "ALTER TABLE mrrreceivinginspection ADD COLUMN IF NOT EXISTS delivery_note_no TEXT DEFAULT ''",
        "ALTER TABLE mrrreceivinginspection ADD COLUMN delivery_note_no TEXT DEFAULT ''",
    )
    _add_column_if_missing(
        "mrrreceivinginspection",
        "qty_arrived",
        "ALTER TABLE mrrreceivinginspection ADD COLUMN IF NOT EXISTS qty_arrived DOUBLE PRECISION DEFAULT 0",
        "ALTER TABLE mrrreceivinginspection ADD COLUMN qty_arrived REAL DEFAULT 0",
    )
    _add_column_if_missing(
        "mrrreceivinginspection",
        "qty_unit",
        "ALTER TABLE mrrreceivinginspection ADD COLUMN IF NOT EXISTS qty_unit VARCHAR(10) DEFAULT 'KG'",
        "ALTER TABLE mrrreceivinginspection ADD COLUMN qty_unit TEXT DEFAULT 'KG'",
    )
    _add_column_if_missing(
        "mrrreceivinginspection",
        "report_no",
        "ALTER TABLE mrrreceivinginspection ADD COLUMN IF NOT EXISTS report_no TEXT DEFAULT ''",
        "ALTER TABLE mrrreceivinginspection ADD COLUMN report_no TEXT DEFAULT ''",
    )

    # --- mrrdocument (attachments) ---
    # New: allow documents to be linked to a specific shipment/inspection.
    _add_column_if_missing(
        "mrrdocument",
        "inspection_id",
        "ALTER TABLE mrrdocument ADD COLUMN IF NOT EXISTS inspection_id INTEGER",
        "ALTER TABLE mrrdocument ADD COLUMN inspection_id INTEGER",
    )
        # --- mrrdocument SAFE DELETE columns ---
    _add_column_if_missing(
        "mrrdocument",
        "is_deleted",
        "ALTER TABLE mrrdocument ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE",
        "ALTER TABLE mrrdocument ADD COLUMN is_deleted INTEGER DEFAULT 0",
    )
    _add_column_if_missing(
        "mrrdocument",
        "deleted_at_utc",
        "ALTER TABLE mrrdocument ADD COLUMN IF NOT EXISTS deleted_at_utc TIMESTAMP NULL",
        "ALTER TABLE mrrdocument ADD COLUMN deleted_at_utc TEXT",
    )
    _add_column_if_missing(
        "mrrdocument",
        "deleted_by_user_id",
        "ALTER TABLE mrrdocument ADD COLUMN IF NOT EXISTS deleted_by_user_id INTEGER NULL",
        "ALTER TABLE mrrdocument ADD COLUMN deleted_by_user_id INTEGER",
    )
    _add_column_if_missing(
        "mrrdocument",
        "deleted_by_user_name",
        "ALTER TABLE mrrdocument ADD COLUMN IF NOT EXISTS deleted_by_user_name TEXT DEFAULT ''",
        "ALTER TABLE mrrdocument ADD COLUMN deleted_by_user_name TEXT DEFAULT ''",
    )
     
    # =========================
    # BURST: BurstAttachment.sample_id
    # =========================
    _add_column_if_missing(
        "burstattachment",
        "sample_id",
        "ALTER TABLE burstattachment ADD COLUMN IF NOT EXISTS sample_id INTEGER",
        "ALTER TABLE burstattachment ADD COLUMN sample_id INTEGER",
    )



def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)
    _ensure_schema_patches()


def get_session():
    with Session(engine) as session:
        yield session

