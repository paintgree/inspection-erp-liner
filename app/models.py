from __future__ import annotations

from datetime import datetime, date
from typing import Optional, List

from sqlmodel import SQLModel, Field, Relationship


# =========================
# USERS
# =========================

class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    username: str = Field(index=True, unique=True)
    display_name: str
    role: str = Field(default="INSPECTOR")  # MANAGER / INSPECTOR / BOSS / RUN_CREATOR
    password_hash: str
    created_at: datetime = Field(default_factory=datetime.utcnow)

    entries: List["InspectionEntry"] = Relationship(back_populates="inspector")


# =========================
# PRODUCTION RUNS (In-process inspections)
# =========================

class ProductionRun(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    process: str  # LINER / REINFORCEMENT / COVER
    dhtp_batch_no: str = Field(index=True)

    client_name: str
    po_number: str
    itp_number: str
    pipe_specification: str
    raw_material_spec: str

    total_length_m: float = Field(default=0.0)
    status: str = Field(default="OPEN")  # OPEN / CLOSED / APPROVED
    created_at: datetime = Field(default_factory=datetime.utcnow)

    machines: List["RunMachine"] = Relationship(back_populates="run")
    params: List["RunParameter"] = Relationship(back_populates="run")
    entries: List["InspectionEntry"] = Relationship(back_populates="run")


class RunMachine(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    run_id: int = Field(foreign_key="productionrun.id", index=True)
    machine_name: str
    machine_tag: str = ""

    run: "ProductionRun" = Relationship(back_populates="machines")


class RunParameter(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    run_id: int = Field(foreign_key="productionrun.id", index=True)

    param_key: str
    label: str
    unit: str = ""

    # Spec rule
    rule: str = Field(default="RANGE")  # "" / RANGE / MAX_ONLY / MIN_ONLY
    min_value: Optional[float] = None
    max_value: Optional[float] = None

    display_order: int = 0

    run: "ProductionRun" = Relationship(back_populates="params")


class InspectionEntry(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    run_id: int = Field(foreign_key="productionrun.id", index=True)

    actual_date: date
    actual_time: str  # "HH:MM"
    slot_time: str    # "00:00"..."22:00"

    inspector_id: int = Field(foreign_key="user.id", index=True)

    operator_1: str = ""
    operator_2: str = ""
    operator_annular_12: str = ""
    operator_int_ext_34: str = ""

    remarks: str = ""

    # stored for export / trace
    raw_material_batch_no: str = ""

    tool1_name: str = ""
    tool1_serial: str = ""
    tool1_calib_due: str = ""

    tool2_name: str = ""
    tool2_serial: str = ""
    tool2_calib_due: str = ""

    created_at: datetime = Field(default_factory=datetime.utcnow)

    run: "ProductionRun" = Relationship(back_populates="entries")
    inspector: "User" = Relationship(back_populates="entries")
    values: List["InspectionValue"] = Relationship(back_populates="entry")


class InspectionValue(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    entry_id: int = Field(foreign_key="inspectionentry.id", index=True)

    param_key: str
    value: Optional[float] = None

    is_out_of_spec: bool = Field(default=False)
    spec_note: str = Field(default="")

    # pending edit workflow
    pending_value: Optional[float] = None
    pending_status: str = Field(default="")  # "" / PENDING / APPROVED / REJECTED
    pending_by_user_id: Optional[int] = Field(default=None)
    pending_at: Optional[datetime] = None

    entry: "InspectionEntry" = Relationship(back_populates="values")
    audits: List["InspectionValueAudit"] = Relationship(back_populates="value")


class InspectionValueAudit(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    inspection_value_id: int = Field(foreign_key="inspectionvalue.id", index=True)

    action: str  # CREATED / PROPOSED / APPROVED / REJECTED
    old_value: Optional[float] = None
    new_value: Optional[float] = None

    by_user_id: Optional[int] = None
    by_user_name: str = ""
    note: str = ""

    created_at: datetime = Field(default_factory=datetime.utcnow)

    value: "InspectionValue" = Relationship(back_populates="audits")


# =========================
# MRR (Receiving inspection tickets)
# IMPORTANT: MaterialLot IS the "MRR ticket"
# =========================

class MaterialLot(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    lot_type: str = Field(default="RAW", index=True)  # RAW / OUTSOURCED

    batch_no: str = Field(index=True)  # TMP... until manager sets final number if needed
    material_name: str = ""
    supplier_name: str = ""

    # Manager PO (what manager entered when creating ticket)
    po_number: str = ""
    quantity: Optional[float] = None  # PO quantity (planned)

    status: str = "PENDING"  # PENDING / APPROVED / REJECTED
    created_at: datetime = Field(default_factory=datetime.utcnow)


class MrrDocument(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    # link to ticket
    lot_id: int = Field(foreign_key="materiallot.id", index=True)

    # user chooses: PO / DELIVERY_NOTE / COA / RELATED
    doc_type: str = Field(default="RELATED")

    # user typed friendly name
    doc_title: str = Field(default="")

    # optional: a document number (DN number, etc.)
    doc_number: str = Field(default="")

    # where file saved on server
    file_path: str = Field(default="")

    uploaded_by_user_id: Optional[int] = Field(default=None, index=True)
    uploaded_by_user_name: str = Field(default="")
    uploaded_by_role: str = Field(default="")  # MANAGER / INSPECTOR


class MrrReceiving(SQLModel, table=True):
    """
    Inspector documentation step (PO match, delivery note, qty arrived, confirmation)
    """
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    lot_id: int = Field(foreign_key="materiallot.id", index=True)

    inspector_po_number: str = Field(default="")
    delivery_note_no: str = Field(default="")
    qty_arrived: Optional[float] = None

    # Documentation status gate
    docs_status: str = Field(default="PENDING")  # PENDING / CLEARED / NEED_MANAGER_APPROVAL

    confirmed_by_inspector_id: Optional[int] = Field(default=None, index=True)
    confirmed_by_manager_id: Optional[int] = Field(default=None, index=True)

    remarks: str = Field(default="")


class MrrInspection(SQLModel, table=True):
    """
    Receiving inspection form (template-based).
    Keep it flexible using JSON string to avoid DB changes when template changes.
    """
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    lot_id: int = Field(foreign_key="materiallot.id", index=True)

    template_used: str = Field(default="RAW")  # RAW / OUTSOURCED
    form_json: str = Field(default="{}")


# =========================
# Linking RAW lots to production runs (in-process trace)
# =========================

class MaterialUseEvent(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    run_id: int = Field(index=True)
    day: date = Field(index=True)
    slot_time: str = Field(index=True)  # "00:00".."22:00"

    lot_id: int = Field(index=True)

    created_by_user_id: Optional[int] = Field(default=None, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
