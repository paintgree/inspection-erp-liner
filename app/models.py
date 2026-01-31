from datetime import datetime, date
from typing import Optional
from sqlmodel import SQLModel, Field, Relationship
from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime, date

class MrrTicket(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    status: str = Field(default="OPEN")  # OPEN, IN_PROGRESS, CLOSED, APPROVED

    material_category: str = Field(default="RAW")  # RAW or OUTSOURCED
    material_type: str = Field(default="")         # e.g. PE100, Resin name, Flange type...
    supplier_name: str = Field(default="")
    quantity: str = Field(default="")              # keep string to support "10 bags", "5 tons"

    po_number: str = Field(default="")             # planned PO from manager
    notes: str = Field(default="")

class MrrDocument(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    ticket_id: int = Field(index=True)

    doc_name: str = Field(default="")              # required
    doc_number: str = Field(default="")            # optional
    file_path: str = Field(default="")             # stored file path

    uploaded_by_user_id: Optional[int] = Field(default=None, index=True)
    uploaded_by_user_name: str = Field(default="")

    doc_type: str = Field(default="GENERAL")       # GENERAL, PO, DELIVERY_NOTE, COA, etc.

class MrrReceiving(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    ticket_id: int = Field(index=True)

    delivery_note_no: str = Field(default="")
    inspector_po_number: str = Field(default="")

    po_match: bool = Field(default=True)           # computed/updated at save time
    inspector_confirmed_po: bool = Field(default=False)
    manager_confirmed_po: bool = Field(default=False)

    received_date: date = Field(default_factory=date.today)
    remarks: str = Field(default="")

class MrrInspection(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    ticket_id: int = Field(index=True)
    template_used: str = Field(default="RAW")      # RAW / OUTSOURCED

    # you can store form fields as columns OR JSON.
    # simplest: store JSON string so you can change template later without DB changes.
    form_json: str = Field(default="{}")


class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(index=True, unique=True)
    display_name: str
    role: str = Field(default="INSPECTOR")  # MANAGER / INSPECTOR
    password_hash: str
    created_at: datetime = Field(default_factory=datetime.utcnow)

    entries: list["InspectionEntry"] = Relationship(back_populates="inspector")


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

    machines: list["RunMachine"] = Relationship(back_populates="run")
    params: list["RunParameter"] = Relationship(back_populates="run")
    entries: list["InspectionEntry"] = Relationship(back_populates="run")


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

    rule: str = Field(default="RANGE")  # RANGE / MAX_ONLY / MIN_ONLY
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
    values: list["InspectionValue"] = Relationship(back_populates="entry")


class InspectionValue(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    entry_id: int = Field(foreign_key="inspectionentry.id", index=True)

    param_key: str
    value: Optional[float] = None

    # ✅ limit check result (used for highlighting)
    is_out_of_spec: bool = Field(default=False)
    spec_note: str = Field(default="")  # optional message

    # ✅ pending edit workflow (inspector can propose, manager approves)
    pending_value: Optional[float] = None
    pending_status: str = Field(default="")  # "" / "PENDING" / "APPROVED" / "REJECTED"
    pending_by_user_id: Optional[int] = Field(default=None)
    pending_at: Optional[datetime] = None

    entry: "InspectionEntry" = Relationship(back_populates="values")
    audits: list["InspectionValueAudit"] = Relationship(back_populates="value")


# ✅ NEW: full audit trail table (keeps history forever)
class InspectionValueAudit(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    inspection_value_id: int = Field(foreign_key="inspectionvalue.id", index=True)

    action: str  # PROPOSED / APPROVED / REJECTED
    old_value: Optional[float] = None
    new_value: Optional[float] = None

    by_user_id: Optional[int] = None
    by_user_name: str = ""
    note: str = ""  # optional reason

    created_at: datetime = Field(default_factory=datetime.utcnow)

    value: "InspectionValue" = Relationship(back_populates="audits")


from datetime import datetime, date
from typing import Optional
from sqlmodel import SQLModel, Field


class MaterialLot(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    batch_no: str = Field(index=True)
    material_name: str = ""
    supplier_name: str = ""

    status: str = "PENDING"  # PENDING / APPROVED / REJECTED
    created_at: datetime = Field(default_factory=datetime.utcnow)


class MaterialUseEvent(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    run_id: int = Field(index=True)
    day: date = Field(index=True)
    slot_time: str = Field(index=True)  # "00:00".."22:00"

    lot_id: int = Field(index=True)

    created_by_user_id: Optional[int] = Field(default=None, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)



