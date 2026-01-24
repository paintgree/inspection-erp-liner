from typing import Optional
from datetime import datetime, time, date
from sqlmodel import SQLModel, Field

class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    email: str = Field(index=True, unique=True)
    name: str
    role: str   # MANAGER / INSPECTOR
    password_hash: str

class ProductionRun(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    process: str                 # LINER / REINFORCEMENT / COVER
    dhtp_batch_no: str = Field(index=True)   # run reference
    status: str = "OPEN"         # OPEN / CLOSED / APPROVED

    client_name: str
    po_number: str
    itp_number: str
    pipe_specification: str
    raw_material_spec: str
    raw_material_batch_no: str

    created_by: int
    created_at: datetime = Field(default_factory=datetime.utcnow)

class RunMachine(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: int = Field(index=True)
    machine_name: str
    tag: Optional[str] = None

class RunParameter(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: int = Field(index=True)

    param_key: str = Field(index=True)
    label: str
    unit: str
    rule: str               # RANGE / MAX_ONLY / MIN_ONLY / INFO_ONLY
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    display_order: int = 0

class InspectionEntry(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: int = Field(index=True)

    actual_date: date
    actual_time: time
    slot_time: time

    inspector_user_id: int

    operator1: Optional[str] = None
    operator2: Optional[str] = None
    remark: Optional[str] = None

    created_at: datetime = Field(default_factory=datetime.utcnow)

class InspectionValue(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    entry_id: int = Field(index=True)
    param_key: str = Field(index=True)
    value: float

class AuditLog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: int = Field(index=True)
    actor_user_id: int
    action: str
    reason: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
