from __future__ import annotations

from datetime import datetime, date
from typing import Optional

from sqlmodel import SQLModel, Field

from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field

class RunApproval(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: int = Field(index=True)

    approved_by_name: str
    approved_at_utc: datetime = Field(index=True)

# =========================
# USERS
# =========================

class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    username: str = Field(index=True, unique=True)
    display_name: str = Field(default="")
    role: str = Field(default="INSPECTOR")  # MANAGER / INSPECTOR / BOSS / RUN_CREATOR
    password_hash: str = Field(default="")
    created_at: datetime = Field(default_factory=datetime.utcnow)


# =========================
# PRODUCTION RUNS
# =========================

class ProductionRun(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    process: str = Field(default="LINER")   # LINER / REINFORCEMENT / COVER
    dhtp_batch_no: str = Field(default="", index=True)

    client_name: str = Field(default="")
    po_number: str = Field(default="")
    itp_number: str = Field(default="")
    pipe_specification: str = Field(default="")
    raw_material_spec: str = Field(default="")

    total_length_m: float = Field(default=0.0)
    status: str = Field(default="OPEN")  # OPEN / CLOSED / APPROVED
      # ✅ Approval audit fields
    approved_by_user_id: Optional[int] = Field(default=None, index=True)
    approved_by_user_name: str = Field(default="")
    approved_at_utc: Optional[datetime] = Field(default=None)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class RunMachine(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    run_id: int = Field(foreign_key="productionrun.id", index=True)
    machine_name: str = Field(default="")
    machine_tag: str = Field(default="")


class RunParameter(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    run_id: int = Field(foreign_key="productionrun.id", index=True)

    param_key: str = Field(default="")
    label: str = Field(default="")
    unit: str = Field(default="")

    rule: str = Field(default="RANGE")  # "" / RANGE / MAX_ONLY / MIN_ONLY
    min_value: Optional[float] = Field(default=None)
    max_value: Optional[float] = Field(default=None)

    display_order: int = Field(default=0)


class InspectionEntry(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    run_id: int = Field(foreign_key="productionrun.id", index=True)

    actual_date: date
    actual_time: str = Field(default="")  # "HH:MM"
    slot_time: str = Field(default="", index=True)  # "00:00".."22:00"

    inspector_id: int = Field(foreign_key="user.id", index=True)

    operator_1: str = Field(default="")
    operator_2: str = Field(default="")
    operator_annular_12: str = Field(default="")
    operator_int_ext_34: str = Field(default="")

    remarks: str = Field(default="")

    raw_material_batch_no: str = Field(default="")

    tool1_name: str = Field(default="")
    tool1_serial: str = Field(default="")
    tool1_calib_due: str = Field(default="")

    tool2_name: str = Field(default="")
    tool2_serial: str = Field(default="")
    tool2_calib_due: str = Field(default="")

    created_at: datetime = Field(default_factory=datetime.utcnow)


class InspectionValue(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    entry_id: int = Field(foreign_key="inspectionentry.id", index=True)

    param_key: str = Field(default="", index=True)
    value: Optional[float] = Field(default=None)

    is_out_of_spec: bool = Field(default=False)
    spec_note: str = Field(default="")

    pending_value: Optional[float] = Field(default=None)
    pending_status: str = Field(default="")  # "" / PENDING / APPROVED / REJECTED
    pending_by_user_id: Optional[int] = Field(default=None)
    pending_at: Optional[datetime] = Field(default=None)


class InspectionValueAudit(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    inspection_value_id: int = Field(foreign_key="inspectionvalue.id", index=True)

    action: str = Field(default="")  # CREATED / PROPOSED / APPROVED / REJECTED
    old_value: Optional[float] = Field(default=None)
    new_value: Optional[float] = Field(default=None)

    by_user_id: Optional[int] = Field(default=None)
    by_user_name: str = Field(default="")
    note: str = Field(default="")

    created_at: datetime = Field(default_factory=datetime.utcnow)


# =========================
# MATERIAL LOT = MRR TICKET
# =========================

class MaterialLot(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    lot_type: str = Field(default="RAW", index=True)  # RAW / OUTSOURCED

    batch_no: str = Field(default="", index=True)
    material_name: str = Field(default="")
    supplier_name: str = Field(default="")

    po_number: str = Field(default="")

    # ✅ PO qty is REQUIRED (manager must fill)
    quantity: float = Field(default=0.0)

    # ✅ PO unit (manager chooses)
    quantity_unit: str = Field(default="KG")  # KG / T / PCS

    # ✅ how much received so far (normalized for KG/T, or PCS)
    received_total: float = Field(default=0.0)

    status: str = Field(default="PENDING")  # PENDING / PARTIAL / APPROVED / REJECTED
    created_at: datetime = Field(default_factory=datetime.utcnow)



class MaterialUseEvent(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    run_id: int = Field(index=True)
    day: date = Field(index=True)
    slot_time: str = Field(default="", index=True)

    lot_id: int = Field(index=True)

    created_by_user_id: Optional[int] = Field(default=None, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)


# =========================
# MRR DOCUMENTS + STEPS
# =========================

class MrrDocument(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    ticket_id: int
    inspection_id: Optional[int] = None  # null for ticket-level docs, set for shipment-level docs

    doc_type: str
    doc_number: Optional[str] = None
    doc_name: str
    file_path: str

    uploaded_by_user_id: Optional[int] = None
    uploaded_by_user_name: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

    # ✅ SAFE DELETE (TRASH)
    is_deleted: bool = Field(default=False)
    deleted_at_utc: Optional[datetime] = None
    deleted_by_user_id: Optional[int] = None
    deleted_by_user_name: Optional[str] = None


class MrrReceiving(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    ticket_id: int = Field(index=True)

    delivery_note_no: str = Field(default="")
    inspector_po_number: str = Field(default="")

    po_match: bool = Field(default=True)
    inspector_confirmed_po: bool = Field(default=False)
    manager_confirmed_po: bool = Field(default=False)

    received_date: date = Field(default_factory=date.today)
    remarks: str = Field(default="")

    # ✅ Quantity entered ONCE here, and reused in inspection page
    qty_arrived: Optional[float] = Field(default=None)
    qty_unit: str = Field(default="KG")  # KG / T

    # ✅ Partial delivery logic
    is_partial_delivery: bool = Field(default=False)
    qty_mismatch_reason: str = Field(default="")




class MrrReceivingInspection(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    ticket_id: int = Field(index=True)

    inspector_id: int = Field(index=True)
    inspector_name: str = Field(default="")

    # ✅ Shipment-specific header
    delivery_note_no: str = Field(default="")
    qty_arrived: float = Field(default=0.0)
    qty_unit: str = Field(default="KG")  # KG / T / PCS

    report_no: str = Field(default="")   # auto
    template_type: str = Field(default="RAW")

    inspection_json: str = Field(default="{}")

    inspector_confirmed: bool = Field(default=False)
    manager_approved: bool = Field(default=False)

    remarks: str = Field(default="")


# =========================
# MRR INSPECTION PHOTOS
# =========================
class MrrInspectionPhoto(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    ticket_id: int = Field(index=True)
    inspection_id: int = Field(index=True)

    group_name: str = Field(default="General")   # e.g., Packaging / Labeling / Damage
    caption: str = Field(default="")            # optional

    file_path: str = Field(default="")

    uploaded_by_user_id: Optional[int] = Field(default=None, index=True)
    uploaded_by_user_name: str = Field(default="")


# =========================
# COMPATIBILITY ALIAS
# =========================
# Your main.py imports MrrInspection, but the real model you use everywhere
# is MrrReceivingInspection. This alias fixes the import error cleanly.
MrrInspection = MrrReceivingInspection

# =========================
# BURST TESTING
# =========================

class BurstTestReport(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    # Link / Identity
    batch_no: str = Field(default="", index=True)
    linked_run_id: Optional[int] = Field(default=None, foreign_key="productionrun.id")  # COVER run only
    is_unlinked: bool = Field(default=False)

    # Finished pipe length
    total_length_m: float = Field(default=0.0)

    # Sample: start & length
    sample_start_m: float = Field(default=0.0)
    sample_length_m: float = Field(default=0.0)

    # -------------------------
    # TEMPLATE: TEST DETAILS (auto-filled when linked)
    # -------------------------
    client_name: str = Field(default="")
    client_po: str = Field(default="")
    pipe_specification: str = Field(default="")  # "Specimen Specification"

    reference_standard: str = Field(default="")       # manual
    reference_dhtp_procedure: str = Field(default="") # manual
    system_max_pressure: str = Field(default="")      # manual (string to allow "100 MPa")
    laboratory_temperature: str = Field(default="")   # manual
    testing_medium: str = Field(default="Water")
    total_no_of_specimens: int = Field(default=1)

    # -------------------------
    # TEMPLATE: SPECIMENS DETAILS
    # -------------------------
    effective_length_m: str = Field(default="")  # manual (string to allow "700mm" etc)

    # Material grades (auto-filled when linked)
    liner_material_grade: str = Field(default="")
    reinforcement_material_grade: str = Field(default="")
    cover_material_grade: str = Field(default="")

    # Wall thicknesses (manual)
    liner_thickness: str = Field(default="")
    reinforcement_thickness: str = Field(default="")
    cover_thickness: str = Field(default="")

    # -------------------------
    # TEMPLATE: TEST RESULTS
    # -------------------------
    sample_serial_number: str = Field(default="")  # manual
    target_pressure_psi: float = Field(default=0.0)
    actual_burst_psi: float = Field(default=0.0)
    pressurization_time_s: str = Field(default="")  # manual
    test_result: str = Field(default="")            # PASS / FAIL
    failure_mode: str = Field(default="")
    test_temp_c: float = Field(default=0.0)

    notes: str = Field(default="")

    # -------------------------
    # TEMPLATE: SIGNATURES
    # -------------------------
    qa_qc_officer_name: str = Field(default="")
    testing_operator_name: str = Field(default="")


    # Workflow
    is_locked: bool = Field(default=False)
    locked_at: Optional[datetime] = Field(default=None)
    locked_by_user_id: Optional[int] = Field(default=None)
    locked_by_user_name: str = Field(default="")
    
    # Audit
    tested_at: datetime = Field(default_factory=datetime.utcnow)
    created_by_user_id: Optional[int] = Field(default=None)
    created_by_user_name: str = Field(default="")
    created_at: datetime = Field(default_factory=datetime.utcnow)


class BurstAttachment(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    report_id: int = Field(foreign_key="bursttestreport.id", index=True)

    # CHART / FULL / SIDE_A / SIDE_B
    kind: str = Field(default="PHOTO", index=True)
    caption: str = Field(default="")

    file_path: str = Field(default="")

    uploaded_by_user_id: Optional[int] = Field(default=None)
    uploaded_by_user_name: str = Field(default="")
    uploaded_at: datetime = Field(default_factory=datetime.utcnow)

class BurstSample(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    report_id: int = Field(foreign_key="bursttestreport.id", index=True)

    sample_start_m: float = Field(default=0.0)
    sample_length_m: float = Field(default=0.0)

    # Optional per-sample results (useful when multiple specimens)
    sample_serial_number: str = Field(default="")
    actual_burst_psi: float = Field(default=0.0)
    pressurization_time_s: str = Field(default="")
    failure_mode: str = Field(default="")
    test_result: str = Field(default="")  # PASS/FAIL

    created_at: datetime = Field(default_factory=datetime.utcnow)


class BurstAuditLog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    report_id: int = Field(foreign_key="bursttestreport.id", index=True)

    action: str = Field(default="")  # LOCK / REOPEN / EDIT
    note: str = Field(default="")

    user_id: Optional[int] = Field(default=None)
    user_name: str = Field(default="")
    created_at: datetime = Field(default_factory=datetime.utcnow)







