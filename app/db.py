# app/db.py
from __future__ import annotations

import os
from sqlmodel import SQLModel, create_engine, Session
from sqlalchemy import inspect, text


DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=300,
        pool_size=10,
        max_overflow=20,
    )
else:
    DB_PATH = os.getenv("INSPECTION_DB", "inspection.db")
    engine = create_engine(
        f"sqlite:///{DB_PATH}",
        echo=False,
        connect_args={"check_same_thread": False},
    )


def _load_schema_state() -> tuple[set[str], dict[str, set[str]]]:
    insp = inspect(engine)
    tables = set(insp.get_table_names())
    cols_by_table: dict[str, set[str]] = {}

    for table in tables:
        try:
            cols_by_table[table] = {c["name"] for c in insp.get_columns(table)}
        except Exception:
            cols_by_table[table] = set()

    return tables, cols_by_table


def _add_column_if_missing_cached(
    tables: set[str],
    cols_by_table: dict[str, set[str]],
    table: str,
    col: str,
    ddl_postgres: str,
    ddl_sqlite: str,
) -> None:
    if table not in tables:
        return

    existing_cols = cols_by_table.get(table, set())
    if col in existing_cols:
        return

    dialect = engine.dialect.name
    ddl = ddl_postgres if dialect == "postgresql" else ddl_sqlite

    with engine.begin() as conn:
        conn.execute(text(ddl))

    cols_by_table.setdefault(table, set()).add(col)


def _ensure_schema_patches() -> None:
    tables, cols_by_table = _load_schema_state()

    column_patches = [
        # mrrreceiving
        (
            "mrrreceiving",
            "qty_arrived",
            "ALTER TABLE mrrreceiving ADD COLUMN IF NOT EXISTS qty_arrived DOUBLE PRECISION",
            "ALTER TABLE mrrreceiving ADD COLUMN qty_arrived REAL",
        ),
        (
            "mrrreceiving",
            "qty_unit",
            "ALTER TABLE mrrreceiving ADD COLUMN IF NOT EXISTS qty_unit VARCHAR(10) DEFAULT 'KG'",
            "ALTER TABLE mrrreceiving ADD COLUMN qty_unit TEXT DEFAULT 'KG'",
        ),
        (
            "mrrreceiving",
            "is_partial_delivery",
            "ALTER TABLE mrrreceiving ADD COLUMN IF NOT EXISTS is_partial_delivery BOOLEAN DEFAULT FALSE",
            "ALTER TABLE mrrreceiving ADD COLUMN is_partial_delivery INTEGER DEFAULT 0",
        ),
        (
            "mrrreceiving",
            "qty_mismatch_reason",
            "ALTER TABLE mrrreceiving ADD COLUMN IF NOT EXISTS qty_mismatch_reason TEXT DEFAULT ''",
            "ALTER TABLE mrrreceiving ADD COLUMN qty_mismatch_reason TEXT DEFAULT ''",
        ),

        # burstattachment
        (
            "burstattachment",
            "sample_id",
            "ALTER TABLE burstattachment ADD COLUMN IF NOT EXISTS sample_id INTEGER",
            "ALTER TABLE burstattachment ADD COLUMN sample_id INTEGER",
        ),
        (
            "burstattachment",
            "kind",
            "ALTER TABLE burstattachment ADD COLUMN IF NOT EXISTS kind TEXT DEFAULT ''",
            "ALTER TABLE burstattachment ADD COLUMN kind TEXT DEFAULT ''",
        ),

        # materiallot
        (
            "materiallot",
            "quantity_unit",
            "ALTER TABLE materiallot ADD COLUMN IF NOT EXISTS quantity_unit VARCHAR(10) DEFAULT 'KG'",
            "ALTER TABLE materiallot ADD COLUMN quantity_unit TEXT DEFAULT 'KG'",
        ),
        (
            "materiallot",
            "received_total",
            "ALTER TABLE materiallot ADD COLUMN IF NOT EXISTS received_total DOUBLE PRECISION DEFAULT 0",
            "ALTER TABLE materiallot ADD COLUMN received_total REAL DEFAULT 0",
        ),

        # mrrreceivinginspection
        (
            "mrrreceivinginspection",
            "created_at",
            "ALTER TABLE mrrreceivinginspection ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()",
            "ALTER TABLE mrrreceivinginspection ADD COLUMN created_at TEXT",
        ),
        (
            "mrrreceivinginspection",
            "delivery_note_no",
            "ALTER TABLE mrrreceivinginspection ADD COLUMN IF NOT EXISTS delivery_note_no TEXT DEFAULT ''",
            "ALTER TABLE mrrreceivinginspection ADD COLUMN delivery_note_no TEXT DEFAULT ''",
        ),
        (
            "mrrreceivinginspection",
            "qty_arrived",
            "ALTER TABLE mrrreceivinginspection ADD COLUMN IF NOT EXISTS qty_arrived DOUBLE PRECISION DEFAULT 0",
            "ALTER TABLE mrrreceivinginspection ADD COLUMN qty_arrived REAL DEFAULT 0",
        ),
        (
            "mrrreceivinginspection",
            "qty_unit",
            "ALTER TABLE mrrreceivinginspection ADD COLUMN IF NOT EXISTS qty_unit VARCHAR(10) DEFAULT 'KG'",
            "ALTER TABLE mrrreceivinginspection ADD COLUMN qty_unit TEXT DEFAULT 'KG'",
        ),
        (
            "mrrreceivinginspection",
            "report_no",
            "ALTER TABLE mrrreceivinginspection ADD COLUMN IF NOT EXISTS report_no TEXT DEFAULT ''",
            "ALTER TABLE mrrreceivinginspection ADD COLUMN report_no TEXT DEFAULT ''",
        ),

        # mrrdocument
        (
            "mrrdocument",
            "inspection_id",
            "ALTER TABLE mrrdocument ADD COLUMN IF NOT EXISTS inspection_id INTEGER",
            "ALTER TABLE mrrdocument ADD COLUMN inspection_id INTEGER",
        ),
        (
            "mrrdocument",
            "is_deleted",
            "ALTER TABLE mrrdocument ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE",
            "ALTER TABLE mrrdocument ADD COLUMN is_deleted INTEGER DEFAULT 0",
        ),
        (
            "mrrdocument",
            "deleted_at_utc",
            "ALTER TABLE mrrdocument ADD COLUMN IF NOT EXISTS deleted_at_utc TIMESTAMP NULL",
            "ALTER TABLE mrrdocument ADD COLUMN deleted_at_utc TEXT",
        ),
        (
            "mrrdocument",
            "deleted_by_user_id",
            "ALTER TABLE mrrdocument ADD COLUMN IF NOT EXISTS deleted_by_user_id INTEGER NULL",
            "ALTER TABLE mrrdocument ADD COLUMN deleted_by_user_id INTEGER",
        ),
        (
            "mrrdocument",
            "deleted_by_user_name",
            "ALTER TABLE mrrdocument ADD COLUMN IF NOT EXISTS deleted_by_user_name TEXT DEFAULT ''",
            "ALTER TABLE mrrdocument ADD COLUMN deleted_by_user_name TEXT DEFAULT ''",
        ),


        # rndqualificationspecimen execution detail fields

        (
            "rndattachmentregister",
            "original_filename",
            "ALTER TABLE rndattachmentregister ADD COLUMN IF NOT EXISTS original_filename TEXT DEFAULT ''",
            "ALTER TABLE rndattachmentregister ADD COLUMN original_filename TEXT DEFAULT ''",
        ),
        (
            "rndattachmentregister",
            "stored_filename",
            "ALTER TABLE rndattachmentregister ADD COLUMN IF NOT EXISTS stored_filename TEXT DEFAULT ''",
            "ALTER TABLE rndattachmentregister ADD COLUMN stored_filename TEXT DEFAULT ''",
        ),
        (
            "rndattachmentregister",
            "file_path",
            "ALTER TABLE rndattachmentregister ADD COLUMN IF NOT EXISTS file_path TEXT DEFAULT ''",
            "ALTER TABLE rndattachmentregister ADD COLUMN file_path TEXT DEFAULT ''",
        ),
        (
            "rndattachmentregister",
            "content_type",
            "ALTER TABLE rndattachmentregister ADD COLUMN IF NOT EXISTS content_type TEXT DEFAULT ''",
            "ALTER TABLE rndattachmentregister ADD COLUMN content_type TEXT DEFAULT ''",
        ),
        (
            "rndattachmentregister",
            "file_size_bytes",
            "ALTER TABLE rndattachmentregister ADD COLUMN IF NOT EXISTS file_size_bytes INTEGER",
            "ALTER TABLE rndattachmentregister ADD COLUMN file_size_bytes INTEGER",
        ),
        (
            "rndattachmentregister",
            "source_mode",
            "ALTER TABLE rndattachmentregister ADD COLUMN IF NOT EXISTS source_mode TEXT DEFAULT 'UPLOAD'",
            "ALTER TABLE rndattachmentregister ADD COLUMN source_mode TEXT DEFAULT 'UPLOAD'",
        ),
        (
            "rndattachmentregister",
            "is_signed_copy",
            "ALTER TABLE rndattachmentregister ADD COLUMN IF NOT EXISTS is_signed_copy BOOLEAN DEFAULT FALSE",
            "ALTER TABLE rndattachmentregister ADD COLUMN is_signed_copy INTEGER DEFAULT 0",
        ),
        (
            "rndqualificationspecimen",
            "material_ref",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN IF NOT EXISTS material_ref TEXT DEFAULT ''",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN material_ref TEXT DEFAULT ''",
        ),
        (
            "rndqualificationspecimen",
            "conditioning_required",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN IF NOT EXISTS conditioning_required BOOLEAN DEFAULT FALSE",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN conditioning_required INTEGER DEFAULT 0",
        ),
        (
            "rndqualificationspecimen",
            "planned_pressure_mpa",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN IF NOT EXISTS planned_pressure_mpa DOUBLE PRECISION",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN planned_pressure_mpa REAL",
        ),
        (
            "rndqualificationspecimen",
            "actual_pressure_at_failure_mpa",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN IF NOT EXISTS actual_pressure_at_failure_mpa DOUBLE PRECISION",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN actual_pressure_at_failure_mpa REAL",
        ),
        (
            "rndqualificationspecimen",
            "pressure_at_hold_mpa",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN IF NOT EXISTS pressure_at_hold_mpa DOUBLE PRECISION",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN pressure_at_hold_mpa REAL",
        ),
        (
            "rndqualificationspecimen",
            "failure_time_sec",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN IF NOT EXISTS failure_time_sec DOUBLE PRECISION",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN failure_time_sec REAL",
        ),
        (
            "rndqualificationspecimen",
            "pre_failure_condition",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN IF NOT EXISTS pre_failure_condition TEXT DEFAULT ''",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN pre_failure_condition TEXT DEFAULT ''",
        ),
        (
            "rndqualificationspecimen",
            "pre_failure_visual",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN IF NOT EXISTS pre_failure_visual TEXT DEFAULT ''",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN pre_failure_visual TEXT DEFAULT ''",
        ),
        (
            "rndqualificationspecimen",
            "post_failure_visual",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN IF NOT EXISTS post_failure_visual TEXT DEFAULT ''",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN post_failure_visual TEXT DEFAULT ''",
        ),
        (
            "rndqualificationspecimen",
            "failure_location",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN IF NOT EXISTS failure_location TEXT DEFAULT ''",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN failure_location TEXT DEFAULT ''",
        ),
        (
            "rndqualificationspecimen",
            "failure_description",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN IF NOT EXISTS failure_description TEXT DEFAULT ''",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN failure_description TEXT DEFAULT ''",
        ),
        (
            "rndqualificationspecimen",
            "leak_observation",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN IF NOT EXISTS leak_observation TEXT DEFAULT ''",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN leak_observation TEXT DEFAULT ''",
        ),
        (
            "rndqualificationspecimen",
            "result_status",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN IF NOT EXISTS result_status TEXT DEFAULT 'PENDING'",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN result_status TEXT DEFAULT 'PENDING'",
        ),
        (
            "rndqualificationspecimen",
            "qa_review_status",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN IF NOT EXISTS qa_review_status TEXT DEFAULT 'PENDING'",
            "ALTER TABLE rndqualificationspecimen ADD COLUMN qa_review_status TEXT DEFAULT 'PENDING'",
        ),

        # burstsample
        (
            "burstsample",
            "effective_length_m",
            "ALTER TABLE burstsample ADD COLUMN IF NOT EXISTS effective_length_m DOUBLE PRECISION DEFAULT 0",
            "ALTER TABLE burstsample ADD COLUMN effective_length_m REAL DEFAULT 0",
        ),
        (
            "burstsample",
            "liner_material_grade",
            "ALTER TABLE burstsample ADD COLUMN IF NOT EXISTS liner_material_grade TEXT DEFAULT ''",
            "ALTER TABLE burstsample ADD COLUMN liner_material_grade TEXT DEFAULT ''",
        ),
        (
            "burstsample",
            "liner_thickness_mm",
            "ALTER TABLE burstsample ADD COLUMN IF NOT EXISTS liner_thickness_mm DOUBLE PRECISION DEFAULT 0",
            "ALTER TABLE burstsample ADD COLUMN liner_thickness_mm REAL DEFAULT 0",
        ),
        (
            "burstsample",
            "reinforcement_material_grade",
            "ALTER TABLE burstsample ADD COLUMN IF NOT EXISTS reinforcement_material_grade TEXT DEFAULT ''",
            "ALTER TABLE burstsample ADD COLUMN reinforcement_material_grade TEXT DEFAULT ''",
        ),
        (
            "burstsample",
            "reinforcement_thickness_mm",
            "ALTER TABLE burstsample ADD COLUMN IF NOT EXISTS reinforcement_thickness_mm DOUBLE PRECISION DEFAULT 0",
            "ALTER TABLE burstsample ADD COLUMN reinforcement_thickness_mm REAL DEFAULT 0",
        ),
        (
            "burstsample",
            "cover_material_grade",
            "ALTER TABLE burstsample ADD COLUMN IF NOT EXISTS cover_material_grade TEXT DEFAULT ''",
            "ALTER TABLE burstsample ADD COLUMN cover_material_grade TEXT DEFAULT ''",
        ),
        (
            "burstsample",
            "cover_thickness_mm",
            "ALTER TABLE burstsample ADD COLUMN IF NOT EXISTS cover_thickness_mm DOUBLE PRECISION DEFAULT 0",
            "ALTER TABLE burstsample ADD COLUMN cover_thickness_mm REAL DEFAULT 0",
        ),

        # bursttestreport
        (
            "bursttestreport",
            "purpose",
            "ALTER TABLE bursttestreport ADD COLUMN IF NOT EXISTS purpose TEXT DEFAULT 'BATCH_RELEASE'",
            "ALTER TABLE bursttestreport ADD COLUMN purpose TEXT DEFAULT 'BATCH_RELEASE'",
        ),
        (
            "bursttestreport",
            "technician_name",
            "ALTER TABLE bursttestreport ADD COLUMN IF NOT EXISTS technician_name TEXT DEFAULT ''",
            "ALTER TABLE bursttestreport ADD COLUMN technician_name TEXT DEFAULT ''",
        ),

        # hydrotestrecord
        (
            "hydrotestrecord",
            "assigned_qaqc_user_id",
            "ALTER TABLE hydrotestrecord ADD COLUMN IF NOT EXISTS assigned_qaqc_user_id INTEGER",
            "ALTER TABLE hydrotestrecord ADD COLUMN assigned_qaqc_user_id INTEGER",
        ),
        (
            "hydrotestrecord",
            "assigned_qaqc_username",
            "ALTER TABLE hydrotestrecord ADD COLUMN IF NOT EXISTS assigned_qaqc_username TEXT DEFAULT ''",
            "ALTER TABLE hydrotestrecord ADD COLUMN assigned_qaqc_username TEXT DEFAULT ''",
        ),
        (
            "hydrotestrecord",
            "assigned_qaqc_display_name",
            "ALTER TABLE hydrotestrecord ADD COLUMN IF NOT EXISTS assigned_qaqc_display_name TEXT DEFAULT ''",
            "ALTER TABLE hydrotestrecord ADD COLUMN assigned_qaqc_display_name TEXT DEFAULT ''",
        ),

        # productionrun
        (
            "productionrun",
            "confirmed_total_length_m",
            "ALTER TABLE productionrun ADD COLUMN IF NOT EXISTS confirmed_total_length_m DOUBLE PRECISION DEFAULT 0",
            "ALTER TABLE productionrun ADD COLUMN confirmed_total_length_m REAL DEFAULT 0",
        ),
        (
            "productionrun",
            "confirmed_length_note",
            "ALTER TABLE productionrun ADD COLUMN IF NOT EXISTS confirmed_length_note TEXT DEFAULT ''",
            "ALTER TABLE productionrun ADD COLUMN confirmed_length_note TEXT DEFAULT ''",
        ),

        # finalinspectionphase
        (
            "finalinspectionphase",
            "status",
            "ALTER TABLE finalinspectionphase ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'DRAFT'",
            "ALTER TABLE finalinspectionphase ADD COLUMN status TEXT DEFAULT 'DRAFT'",
        ),
        (
            "finalinspectionphase",
            "submitted_at",
            "ALTER TABLE finalinspectionphase ADD COLUMN IF NOT EXISTS submitted_at TIMESTAMP NULL",
            "ALTER TABLE finalinspectionphase ADD COLUMN submitted_at TIMESTAMP NULL",
        ),
        (
            "finalinspectionphase",
            "approved_at",
            "ALTER TABLE finalinspectionphase ADD COLUMN IF NOT EXISTS approved_at TIMESTAMP NULL",
            "ALTER TABLE finalinspectionphase ADD COLUMN approved_at TIMESTAMP NULL",
        ),
        (
            "finalinspectionphase",
            "approved_by_user_id",
            "ALTER TABLE finalinspectionphase ADD COLUMN IF NOT EXISTS approved_by_user_id INTEGER",
            "ALTER TABLE finalinspectionphase ADD COLUMN approved_by_user_id INTEGER",
        ),
        (
            "finalinspectionphase",
            "approved_by_user_name",
            "ALTER TABLE finalinspectionphase ADD COLUMN IF NOT EXISTS approved_by_user_name TEXT DEFAULT ''",
            "ALTER TABLE finalinspectionphase ADD COLUMN approved_by_user_name TEXT DEFAULT ''",
        ),
        (
            "finalinspectionphase",
            "notes",
            "ALTER TABLE finalinspectionphase ADD COLUMN IF NOT EXISTS notes TEXT DEFAULT ''",
            "ALTER TABLE finalinspectionphase ADD COLUMN notes TEXT DEFAULT ''",
        ),

        # finalinspectionreel
        (
            "finalinspectionreel",
            "phase_id",
            "ALTER TABLE finalinspectionreel ADD COLUMN IF NOT EXISTS phase_id INTEGER",
            "ALTER TABLE finalinspectionreel ADD COLUMN phase_id INTEGER",
        ),
        (
            "finalinspectionreel",
            "liner_thickness_mm",
            "ALTER TABLE finalinspectionreel ADD COLUMN IF NOT EXISTS liner_thickness_mm DOUBLE PRECISION DEFAULT 0",
            "ALTER TABLE finalinspectionreel ADD COLUMN liner_thickness_mm REAL DEFAULT 0",
        ),
        (
            "finalinspectionreel",
            "reinforcement_thickness_mm",
            "ALTER TABLE finalinspectionreel ADD COLUMN IF NOT EXISTS reinforcement_thickness_mm DOUBLE PRECISION DEFAULT 0",
            "ALTER TABLE finalinspectionreel ADD COLUMN reinforcement_thickness_mm REAL DEFAULT 0",
        ),
        (
            "finalinspectionreel",
            "cover_thickness_mm",
            "ALTER TABLE finalinspectionreel ADD COLUMN IF NOT EXISTS cover_thickness_mm DOUBLE PRECISION DEFAULT 0",
            "ALTER TABLE finalinspectionreel ADD COLUMN cover_thickness_mm REAL DEFAULT 0",
        ),
        (
            "finalinspectionreel",
            "wall_thickness_mm",
            "ALTER TABLE finalinspectionreel ADD COLUMN IF NOT EXISTS wall_thickness_mm DOUBLE PRECISION DEFAULT 0",
            "ALTER TABLE finalinspectionreel ADD COLUMN wall_thickness_mm REAL DEFAULT 0",
        ),
        (
            "finalinspectionreel",
            "start_length_m",
            "ALTER TABLE finalinspectionreel ADD COLUMN IF NOT EXISTS start_length_m DOUBLE PRECISION DEFAULT 0",
            "ALTER TABLE finalinspectionreel ADD COLUMN start_length_m REAL DEFAULT 0",
        ),
        (
            "finalinspectionreel",
            "end_length_m",
            "ALTER TABLE finalinspectionreel ADD COLUMN IF NOT EXISTS end_length_m DOUBLE PRECISION DEFAULT 0",
            "ALTER TABLE finalinspectionreel ADD COLUMN end_length_m REAL DEFAULT 0",
        ),

        # user
        (
            "user",
            "department",
            "ALTER TABLE \"user\" ADD COLUMN IF NOT EXISTS department TEXT DEFAULT 'QUALITY'",
            "ALTER TABLE user ADD COLUMN department TEXT DEFAULT 'QUALITY'",
        ),
        (
            "user",
            "access_overrides_json",
            "ALTER TABLE \"user\" ADD COLUMN IF NOT EXISTS access_overrides_json TEXT DEFAULT ''",
            "ALTER TABLE user ADD COLUMN access_overrides_json TEXT DEFAULT ''",
        ),
        (
            "user",
            "email",
            "ALTER TABLE \"user\" ADD COLUMN IF NOT EXISTS email TEXT DEFAULT ''",
            "ALTER TABLE user ADD COLUMN email TEXT DEFAULT ''",
        ),
        (
            "user",
            "is_locked",
            "ALTER TABLE \"user\" ADD COLUMN IF NOT EXISTS is_locked BOOLEAN DEFAULT FALSE",
            "ALTER TABLE user ADD COLUMN is_locked INTEGER DEFAULT 0",
        ),
        (
            "user",
            "must_change_password",
            "ALTER TABLE \"user\" ADD COLUMN IF NOT EXISTS must_change_password BOOLEAN DEFAULT FALSE",
            "ALTER TABLE user ADD COLUMN must_change_password INTEGER DEFAULT 0",
        ),

        # rfirecord
        (
            "rfirecord",
            "batch_no",
            "ALTER TABLE rfirecord ADD COLUMN IF NOT EXISTS batch_no TEXT DEFAULT ''",
            "ALTER TABLE rfirecord ADD COLUMN batch_no TEXT DEFAULT ''",
        ),
        (
            "rfirecord",
            "rfi_no",
            "ALTER TABLE rfirecord ADD COLUMN IF NOT EXISTS rfi_no TEXT DEFAULT ''",
            "ALTER TABLE rfirecord ADD COLUMN rfi_no TEXT DEFAULT ''",
        ),
        (
            "rfirecord",
            "client_name",
            "ALTER TABLE rfirecord ADD COLUMN IF NOT EXISTS client_name TEXT DEFAULT ''",
            "ALTER TABLE rfirecord ADD COLUMN client_name TEXT DEFAULT ''",
        ),
        (
            "rfirecord",
            "inspection_stage",
            "ALTER TABLE rfirecord ADD COLUMN IF NOT EXISTS inspection_stage TEXT DEFAULT 'GENERAL'",
            "ALTER TABLE rfirecord ADD COLUMN inspection_stage TEXT DEFAULT 'GENERAL'",
        ),
        (
            "rfirecord",
            "status",
            "ALTER TABLE rfirecord ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'DRAFT'",
            "ALTER TABLE rfirecord ADD COLUMN status TEXT DEFAULT 'DRAFT'",
        ),
        (
            "rfirecord",
            "linked_run_id",
            "ALTER TABLE rfirecord ADD COLUMN IF NOT EXISTS linked_run_id INTEGER",
            "ALTER TABLE rfirecord ADD COLUMN linked_run_id INTEGER",
        ),
    ]

    for table, col, ddl_postgres, ddl_sqlite in column_patches:
        _add_column_if_missing_cached(
            tables,
            cols_by_table,
            table,
            col,
            ddl_postgres,
            ddl_sqlite,
        )

    try:
        with engine.begin() as conn:
            index_sql = [
                "CREATE INDEX IF NOT EXISTS ix_inspectionentry_run_date ON inspectionentry (run_id, actual_date)",
                "CREATE INDEX IF NOT EXISTS ix_inspectionvalue_entry_param ON inspectionvalue (entry_id, param_key)",
                "CREATE INDEX IF NOT EXISTS ix_productionrun_process_status ON productionrun (process, status)",
                "CREATE INDEX IF NOT EXISTS ix_mrrreceivinginspection_ticket_flags ON mrrreceivinginspection (ticket_id, inspector_confirmed, manager_approved)",
                "CREATE INDEX IF NOT EXISTS ix_finalinspectionphase_batch_status ON finalinspectionphase (batch_no, status)",
                "CREATE INDEX IF NOT EXISTS ix_finalinspectionreel_phase_created ON finalinspectionreel (phase_id, created_at)",
            ]
            for sql in index_sql:
                conn.execute(text(sql))
    except Exception:
        pass


def _ensure_rnd_specimen_defaults() -> None:
    dialect = engine.dialect.name
    statements = [
        "UPDATE rndqualificationspecimen SET material_ref = COALESCE(NULLIF(material_ref, ''), 'FINAL_PRODUCT') WHERE material_ref IS NULL OR material_ref = ''",
        "UPDATE rndqualificationspecimen SET conditioning_required = {} WHERE conditioning_required IS NULL".format('FALSE' if dialect == 'postgresql' else '0'),
    ]
    with engine.begin() as conn:
        for stmt in statements:
            try:
                conn.execute(text(stmt))
            except Exception:
                pass

_SCHEMA_READY = False

def create_db_and_tables() -> None:
    global _SCHEMA_READY

    if _SCHEMA_READY:
        return

    SQLModel.metadata.create_all(engine)
    apply_rnd_schema_patches()
    _ensure_schema_patches()
    _ensure_rnd_specimen_defaults()
    _SCHEMA_READY = True


def get_session():
    with Session(engine) as session:
        yield session


from sqlalchemy import text

def apply_rnd_schema_patches():
    statements = [
        "ALTER TABLE rndqualificationprogram ADD COLUMN IF NOT EXISTS program_type VARCHAR(50) DEFAULT 'API_15S';",
        "ALTER TABLE rndqualificationprogram ADD COLUMN IF NOT EXISTS service_medium VARCHAR(50) DEFAULT 'WATER';",
        "ALTER TABLE rndqualificationprogram ADD COLUMN IF NOT EXISTS service_factor DOUBLE PRECISION DEFAULT 1.0;",
        "ALTER TABLE rndqualificationprogram ADD COLUMN IF NOT EXISTS custom_requirements TEXT DEFAULT '';",
        "ALTER TABLE rndqualificationprogram ADD COLUMN IF NOT EXISTS custom_acceptance_criteria TEXT DEFAULT '';",
        "ALTER TABLE rndqualificationprogram ADD COLUMN IF NOT EXISTS is_archived BOOLEAN DEFAULT FALSE;",
        "ALTER TABLE rndqualificationprogram ADD COLUMN IF NOT EXISTS archived_at TIMESTAMP NULL;",
        "ALTER TABLE rndqualificationprogram ADD COLUMN IF NOT EXISTS archived_by_name TEXT DEFAULT '';",

        "ALTER TABLE rndqualificationtest ADD COLUMN IF NOT EXISTS scope_tag VARCHAR(50) DEFAULT 'BOTH';",
        "ALTER TABLE rndqualificationtest ADD COLUMN IF NOT EXISTS source_standard VARCHAR(50) DEFAULT 'API_15S';",

        "ALTER TABLE rndqualificationspecimen ADD COLUMN IF NOT EXISTS scope_tag VARCHAR(50) DEFAULT 'BOTH';",

        "ALTER TABLE rndattachmentregister ADD COLUMN IF NOT EXISTS scope_tag VARCHAR(50) DEFAULT 'BOTH';",
    ]

    with engine.begin() as conn:
        for sql in statements:
            conn.execute(text(sql))
