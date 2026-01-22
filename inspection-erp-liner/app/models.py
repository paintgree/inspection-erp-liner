from __future__ import annotations
from typing import Optional
from datetime import datetime, time
from sqlmodel import SQLModel, Field

class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    email: str = Field(index=True, unique=True)
    name: str
    role: str  # "MANAGER" or "INSPECTOR"
    password_hash: str
    created_at: datetime = Field(default_factory=datetime.utcnow)

class ProductionRun(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    run_code: str = Field(index=True, unique=True)

    status: str = Field(default="OPEN")  # OPEN / CLOSED / APPROVED

    # Master data (simplified MVP)
    client_name: str
    po_number: str
    dhtp_batch_no: str
    pipe_specification: str
    raw_material_spec: str
    raw_material_batch_no_current: str
    itp_number: str

    validation_mode: str = Field(default="SOFT")  # SOFT / HARD

    created_by: int
    created_at: datetime = Field(default_factory=datetime.utcnow)

class RunParameter(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    production_run_id: int = Field(index=True)
    param_key: str = Field(index=True)
    label: str
    unit: str
    rule: str  # RANGE / MAX_ONLY / MIN_ONLY / INFO_ONLY
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    is_active: bool = True
    display_order: int = 0

class InspectionEntry(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    production_run_id: int = Field(index=True)

    actual_time: time
    slot_time: time

    entered_by: int
    entered_at: datetime = Field(default_factory=datetime.utcnow)

    remark: Optional[str] = None
