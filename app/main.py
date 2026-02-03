from __future__ import annotations

import os
import mimetypes
import traceback
from datetime import datetime, date, time as dtime, timedelta
from io import BytesIO
from typing import Dict, List, Optional, Tuple

from typing import Dict, List, Optional, Tuple
from pathlib import Path
import subprocess
import tempfile
import json
import os
from datetime import datetime

import openpyxl
from pypdf import PdfWriter, PdfReader, Transformation

from fastapi import (
    FastAPI,
    Depends,
    Request,
    Form,
    HTTPException,
    UploadFile,
    File,
)
from fastapi.responses import (
    HTMLResponse,
    RedirectResponse,
    StreamingResponse,
    Response,
    FileResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from sqlmodel import Session, select




from .auth import hash_password, verify_password
from .db import create_db_and_tables, get_session
from .models import (
    User,
    ProductionRun,
    RunMachine,
    RunParameter,
    InspectionEntry,
    InspectionValue,
    InspectionValueAudit,
    MaterialLot,
    MaterialUseEvent,
    MrrDocument,
    MrrReceiving,
    MrrReceivingInspection,  # ✅ required because your code uses this name
    MrrInspection,           # ✅ now works (alias in models.py)
)
SLOTS = [
    "00:00", "02:00", "04:00", "06:00",
    "08:00", "10:00", "12:00", "14:00",
    "16:00", "18:00", "20:00", "22:00"
]

# =========================
# MRR helpers (units + report no)
# =========================

UNIT_MULTIPLIER = {
    "KG": 1.0,
    "T": 1000.0,   # 1 Ton = 1000 KG
}

def normalize_qty_to_kg(qty: float, unit: str) -> float:
    u = (unit or "KG").upper().strip()
    if u in ["T", "TON", "TONNE"]:
        return float(qty) * 1000.0
    if u in ["KG", "KGS"]:
        return float(qty)
    # For PCS we cannot convert to KG; keep as-is (handled by units_compatible)
    return float(qty)

def dn_doc_exists(session: Session, lot_id: int, dn_number: str) -> bool:
    dn_number = (dn_number or "").strip()
    if not dn_number:
        return False
    doc = session.exec(
        select(MrrDocument).where(
            (MrrDocument.ticket_id == lot_id) &
            (MrrDocument.doc_type == "DELIVERY_NOTE") &
            (MrrDocument.doc_number == dn_number)
        )
    ).first()
    return bool(doc)

def get_submitted_shipment_by_dn(session: Session, lot_id: int, dn_number: str):
    dn_number = (dn_number or "").strip()
    if not dn_number:
        return None
    # DN is "consumed" ONLY if shipment was submitted (inspector_confirmed True) or approved
    return session.exec(
        select(MrrReceivingInspection).where(
            (MrrReceivingInspection.ticket_id == lot_id) &
            (MrrReceivingInspection.delivery_note_no == dn_number) &
            (
                (MrrReceivingInspection.inspector_confirmed == True) |
                (MrrReceivingInspection.manager_approved == True)
            )
        )
    ).first()

def get_draft_shipment_by_dn(session: Session, lot_id: int, dn_number: str):
    dn_number = (dn_number or "").strip()
    if not dn_number:
        return None
    # Draft = created but NOT submitted yet
    return session.exec(
        select(MrrReceivingInspection).where(
            (MrrReceivingInspection.ticket_id == lot_id) &
            (MrrReceivingInspection.delivery_note_no == dn_number) &
            (MrrReceivingInspection.inspector_confirmed == False) &
            (MrrReceivingInspection.manager_approved == False)
        )
    ).first()

def get_latest_draft_shipment(session: Session, lot_id: int):
    return session.exec(
        select(MrrReceivingInspection).where(
            (MrrReceivingInspection.ticket_id == lot_id) &
            (MrrReceivingInspection.inspector_confirmed == False) &
            (MrrReceivingInspection.manager_approved == False)
        ).order_by(MrrReceivingInspection.created_at.desc())
    ).first()


def units_compatible(po_unit: str, arrived_unit: str) -> bool:
    pu = (po_unit or "").upper().strip()
    au = (arrived_unit or "").upper().strip()
    # KG and T compatible, PCS only with PCS
    if pu == "PCS" or au == "PCS":
        return pu == au
    return True

def generate_report_no(ticket_id: int, seq: int) -> str:
    # MRR-YYYY-MM-<ticket>-<shipment>
    now = datetime.utcnow()
    return f"MRR-{now.year}-{now.month:02d}-{ticket_id:04d}-{seq:02d}"


app = FastAPI()

BASE_DIR = os.path.dirname(__file__)

# =========================
# Upload storage (local FS)
# =========================
DATA_DIR = os.environ.get("DATA_DIR", "/tmp/inspection_erp_data")
MRR_UPLOAD_DIR = os.path.join(DATA_DIR, "mrr_uploads")
os.makedirs(MRR_UPLOAD_DIR, exist_ok=True)


templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
# =========================
# File upload directories
# =========================
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")



IMAGE_MAP = {
    "LINER": "/static/images/liner.png",
    "REINFORCEMENT": "/static/images/reinforcement.png",
    "COVER": "/static/images/cover.png",
}

PAPER_BG_MAP = {
    "LINER": os.path.join(BASE_DIR, "static", "papers", "liner_bg.pdf"),
    "REINFORCEMENT": os.path.join(BASE_DIR, "static", "papers", "reinforcement_bg.pdf"),
    "COVER": os.path.join(BASE_DIR, "static", "papers", "cover_bg.pdf"),
}

TEMPLATE_XLSX_MAP = {
    "LINER": os.path.join(BASE_DIR, "templates", "templates_xlsx", "liner.xlsx"),
    "REINFORCEMENT": os.path.join(BASE_DIR, "templates", "templates_xlsx", "reinforcement.xlsx"),
    "COVER": os.path.join(BASE_DIR, "templates", "templates_xlsx", "cover.xlsx"),
}
def resolve_mrr_doc_path(p: str) -> str:
    """Resolve stored MRR document path to an existing file on disk.

    Backward compatible with rows that stored absolute paths, relative paths, or just filenames.
    """
    if not p:
        return ""

    p_norm = p.replace("\\", "/").lstrip("/")

    # absolute
    try:
        if os.path.isabs(p) and os.path.exists(p):
            return p
    except Exception:
        pass

    candidates = [
        p_norm,
        os.path.join(BASE_DIR, p_norm) if 'BASE_DIR' in globals() else p_norm,
        os.path.join(DATA_DIR, p_norm) if 'DATA_DIR' in globals() else p_norm,
        os.path.join(MRR_UPLOAD_DIR, p_norm),
        os.path.join(MRR_UPLOAD_DIR, os.path.basename(p_norm)),
    ]

    for c in candidates:
        if c and os.path.exists(c):
            return c

    return ""



@app.get("/health")
def health():
    return {"ok": True}


@app.on_event("startup")
def on_startup():
    create_db_and_tables()
    ensure_default_users()


def ensure_default_users():
    from .db import engine
    with Session(engine) as session:
        u = session.exec(select(User).where(User.username == "manager")).first()
        if not u:
            session.add(User(
                username="manager",
                display_name="Manager",
                role="MANAGER",
                password_hash=hash_password("manager123"),
            ))

        i = session.exec(select(User).where(User.username == "inspector")).first()
        if not i:
            session.add(User(
                username="inspector",
                display_name="Inspector",
                role="INSPECTOR",
                password_hash=hash_password("inspector"),
            ))

        b = session.exec(select(User).where(User.username == "boss")).first()
        if not b:
            session.add(User(
                username="boss",
                display_name="Boss",
                role="BOSS",
                password_hash=hash_password("boss123"),
            ))

        session.commit()



def get_current_user(request: Request, session: Session) -> User:
    username = request.cookies.get("user")
    if not username:
        raise HTTPException(401, "Not logged in")
    user = session.exec(select(User).where(User.username == username)).first()
    if not user:
        raise HTTPException(401, "Invalid user")
    return user

def require_manager(user: User):
    if user.role != "MANAGER":
        raise HTTPException(403, "Manager only")

def forbid_boss(user: User):
    if user.role == "BOSS":
        raise HTTPException(403, "Read-only user")


@app.post("/users/{username}/update")
def users_update(
    username: str,
    request: Request,
    session: Session = Depends(get_session),
    display_name: str = Form(""),
    role: str = Form(""),
    password: str = Form(""),
):
    user = get_current_user(request, session)
    require_manager(user)

    target = session.exec(select(User).where(User.username == username)).first()
    if not target:
        raise HTTPException(404, "User not found")

    if display_name.strip():
        target.display_name = display_name.strip()

    if role.strip():
        r = role.strip().upper()
        if r not in ["INSPECTOR", "MANAGER", "BOSS", "RUN_CREATOR"]:
            raise HTTPException(400, "Invalid role")
        target.role = r

    if password.strip():
        if len(password.strip()) < 4:
            raise HTTPException(400, "Password too short")
        target.password_hash = hash_password(password.strip())

    session.add(target)
    session.commit()
    return RedirectResponse("/users", status_code=302)


@app.post("/users/{username}/delete")
def users_delete(
    username: str,
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    require_manager(user)

    # safety: do not delete yourself
    if user.username == username:
        raise HTTPException(400, "You cannot delete your own account")

    target = session.exec(select(User).where(User.username == username)).first()
    if not target:
        raise HTTPException(404, "User not found")

    session.delete(target)
    session.commit()
    return RedirectResponse("/users", status_code=302)


@app.get("/", response_class=HTMLResponse)
def home(request: Request, session: Session = Depends(get_session)):
    # If logged in -> dashboard, else -> login (so root stays "healthy")
    username = request.cookies.get("user")
    if username:
        u = session.exec(select(User).where(User.username == username)).first()
        if u:
            return RedirectResponse("/dashboard", status_code=302)
    return RedirectResponse("/login", status_code=302)



@app.get("/login", response_class=HTMLResponse)
def login_get(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": ""})


@app.post("/login")
def login_post(
    request: Request,
    session: Session = Depends(get_session),
    username: str = Form(...),
    password: str = Form(...),
):
    user = session.exec(select(User).where(User.username == username)).first()
    if not user or not verify_password(password, user.password_hash):
        return templates.TemplateResponse("login.html", {"request": request, "error": "Invalid credentials"})
    resp = RedirectResponse("/dashboard", status_code=302)
    resp.set_cookie("user", user.username, httponly=True)
    return resp


@app.get("/logout")
def logout():
    resp = RedirectResponse("/login", status_code=302)
    resp.delete_cookie("user")
    return resp


def get_days_for_run(session: Session, run_id: int) -> List[date]:
    days = session.exec(
        select(InspectionEntry.actual_date)
        .where(InspectionEntry.run_id == run_id)
        .distinct()
        .order_by(InspectionEntry.actual_date)
    ).all()
    return list(days)



@app.get("/users", response_class=HTMLResponse)
def users_get(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)

    users = session.exec(select(User).order_by(User.username)).all()
    return templates.TemplateResponse(
        "users.html",
        {"request": request, "user": user, "users": users, "error": ""},
    )


@app.post("/users")
def users_post(
    request: Request,
    session: Session = Depends(get_session),
    username: str = Form(...),
    display_name: str = Form(...),
    role: str = Form(...),
    password: str = Form(...),
):
    user = get_current_user(request, session)
    require_manager(user)

    username = username.strip().lower()
    display_name = display_name.strip()
    role = role.strip().upper()

    if role not in ["INSPECTOR", "MANAGER", "BOSS"]:
        users = session.exec(select(User).order_by(User.username)).all()
        return templates.TemplateResponse(
            "users.html",
            {"request": request, "user": user, "users": users, "error": "Invalid role"},
        )

    existing = session.exec(select(User).where(User.username == username)).first()
    if existing:
        users = session.exec(select(User).order_by(User.username)).all()
        return templates.TemplateResponse(
            "users.html",
            {"request": request, "user": user, "users": users, "error": "Username already exists"},
        )

    if len(password.strip()) < 4:
        users = session.exec(select(User).order_by(User.username)).all()
        return templates.TemplateResponse(
            "users.html",
            {"request": request, "user": user, "users": users, "error": "Password too short (min 4)"},
        )

    session.add(User(
        username=username,
        display_name=display_name,
        role=role,
        password_hash=hash_password(password),
    ))
    session.commit()

    return RedirectResponse("/users", status_code=302)


def slot_from_time_str(t: str) -> tuple[str, int]:
    """
    HARD RULE (2-hour slots):
    - HH:00 .. HH+1:30  -> HH:00
    - HH+1:31 .. HH+2:00 -> HH+2:00

    Special rule for end of day:
    - If the calculated next slot becomes 24:00, return ("00:00", 1) meaning next day.
    """
    parts = t.split(":")
    hh = int(parts[0])
    mm = int(parts[1]) if len(parts) > 1 else 0
    total_min = hh * 60 + mm

    # base even hour (00,02,04,...,22)
    base_h = (hh // 2) * 2
    base_min = base_h * 60
    delta = total_min - base_min

    # exact even hour always stays in its slot
    if delta == 0:
        slot_min = base_min
    # 0..90 minutes => same slot
    elif 0 < delta <= 90:
        slot_min = base_min
    # 91..120 => next slot
    else:
        slot_min = base_min + 120

    # If we rolled past midnight (24:00), move to next day 00:00
    if slot_min >= 24 * 60:
        return "00:00", 1

    # Normal clamp low end (shouldn't happen, but safe)
    if slot_min < 0:
        slot_min = 0

    return f"{slot_min // 60:02d}:00", 0



def get_progress_percent(session: Session, run: ProductionRun) -> int:
    if run.total_length_m <= 0:
        return 0
    max_len = 0.0
    entries = session.exec(select(InspectionEntry).where(InspectionEntry.run_id == run.id)).all()
    for e in entries:
        vals = session.exec(
            select(InspectionValue).where(
                InspectionValue.entry_id == e.id,
                InspectionValue.param_key == "length_m"
            )
        ).all()
        for v in vals:
            if v.value is not None and v.value > max_len:
                max_len = v.value
    return int(min(100.0, (max_len / run.total_length_m) * 100.0))


def get_day_latest_trace(session: Session, run_id: int, day: date) -> dict:
    entries = session.exec(
        select(InspectionEntry)
        .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date == day)
        .order_by(InspectionEntry.created_at)
    ).all()

    raw_batches = get_day_material_batches(session, run_id, day)

    tool1 = (None, None, None)
    tool2 = (None, None, None)
    for e in entries:
        if (e.tool1_name or e.tool1_serial or e.tool1_calib_due):
            tool1 = (e.tool1_name, e.tool1_serial, e.tool1_calib_due)
        if (e.tool2_name or e.tool2_serial or e.tool2_calib_due):
            tool2 = (e.tool2_name, e.tool2_serial, e.tool2_calib_due)

    tools = []
    if any(tool1):
        tools.append(tool1)
    if any(tool2):
        tools.append(tool2)

    return {"entries": entries, "raw_batches": raw_batches, "tools": tools}


def get_last_known_trace_before_day(session: Session, run_id: int, day: date) -> dict:
    # ✅ Get last known RAW batch from MaterialUseEvent (not from entries)
    raw = ""

    last_ev = session.exec(
        select(MaterialUseEvent)
        .where(
            MaterialUseEvent.run_id == run_id,
            MaterialUseEvent.day < day,
        )
        .order_by(
            MaterialUseEvent.day.desc(),
            MaterialUseEvent.slot_time.desc(),
            MaterialUseEvent.created_at.desc(),
        )
    ).first()

    if last_ev:
        lot = session.get(MaterialLot, last_ev.lot_id)
        if lot and lot.batch_no:
            raw = lot.batch_no

    # Keep existing tool carry-forward logic
    entries = session.exec(
        select(InspectionEntry)
        .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date < day)
        .order_by(InspectionEntry.actual_date, InspectionEntry.created_at)
    ).all()

    tool1 = (None, None, None)
    tool2 = (None, None, None)

    for e in entries:
        if (e.tool1_name or e.tool1_serial or e.tool1_calib_due):
            tool1 = (e.tool1_name, e.tool1_serial, e.tool1_calib_due)
        if (e.tool2_name or e.tool2_serial or e.tool2_calib_due):
            tool2 = (e.tool2_name, e.tool2_serial, e.tool2_calib_due)

    tools = []
    if any(tool1):
        tools.append(tool1)
    if any(tool2):
        tools.append(tool2)

    return {"raw": raw, "tools": tools}


def get_current_material_lot_for_slot(session: Session, run_id: int, day: date, slot_time: str):
    """
    Returns MaterialLot or None.
    Rule:
      - Find latest MaterialUseEvent for this run/day with slot_time <= current slot_time
      - If none: fallback to latest event before this day
    """
    # events for same day up to this slot
    ev = session.exec(
        select(MaterialUseEvent)
        .where(
            MaterialUseEvent.run_id == run_id,
            MaterialUseEvent.day == day,
            MaterialUseEvent.slot_time <= slot_time,
        )
        .order_by(MaterialUseEvent.slot_time.desc(), MaterialUseEvent.created_at.desc())
    ).first()

    if not ev:
        # fallback: last event before this day
        ev = session.exec(
            select(MaterialUseEvent)
            .where(
                MaterialUseEvent.run_id == run_id,
                MaterialUseEvent.day < day,
            )
            .order_by(MaterialUseEvent.day.desc(), MaterialUseEvent.slot_time.desc(), MaterialUseEvent.created_at.desc())
        ).first()

    if not ev:
        return None

    return session.get(MaterialLot, ev.lot_id)


def get_day_material_batches(session: Session, run_id: int, day: date) -> list[str]:
    """
    Returns unique batch_no used in THIS day.
    First tries MaterialUseEvent; if none, falls back to InspectionEntry.raw_material_batch_no.
    """

    # 1) Events for this day
    events = session.exec(
        select(MaterialUseEvent)
        .where(MaterialUseEvent.run_id == run_id, MaterialUseEvent.day == day)
        .order_by(MaterialUseEvent.slot_time, MaterialUseEvent.created_at)
    ).all()

    batch_nos: list[str] = []
    seen = set()

    for ev in events:
        lot = session.get(MaterialLot, ev.lot_id)
        bn = (lot.batch_no or "").strip() if lot else ""
        if bn and bn not in seen:
            seen.add(bn)
            batch_nos.append(bn)

    # 2) Fallback: if no events found, read from entries
    if not batch_nos:
        entries = session.exec(
            select(InspectionEntry)
            .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date == day)
            .order_by(InspectionEntry.created_at)
        ).all()

        for e in entries:
            bn = (e.raw_material_batch_no or "").strip()
            if bn and bn not in seen:
                seen.add(bn)
                batch_nos.append(bn)

    return batch_nos


def format_spec_for_export(rule: str, mn: float | None, mx: float | None):
    """
    Returns (set_value, tolerance_text)
    """
    rule = (rule or "").upper()

    if rule == "RANGE" and mn is not None and mx is not None:
        set_value = (mn + mx) / 2.0
        tol = abs(mx - mn) / 2.0
        return set_value, f"±{tol:g}"

    if rule == "MAX_ONLY" and mx is not None:
        return mx, "max"   # you requested: set=35, tol=max (like temperature max 35C)

    if rule == "MIN_ONLY" and mn is not None:
        return mn, "min"

    # fallback (no spec)
    return None, ""


def _safe_float(x: Optional[str]) -> Optional[float]:
    if x is None:
        return None
    x = str(x).strip()
    if x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None
# =========================
# Excel row maps (template coordinates)
# =========================
# These map param_key -> Excel row number in the template (the row where the value goes).
# You MUST match your template files exactly.

ROW_MAP_LINER_COVER = {
    # مثال / Example:
    # "length_m": 23,
    # "id_mm": 24,
    # "od_mm": 25,
}

ROW_MAP_REINF = {
    # Example:
    # "length_m": 22,
    # "angle_deg": 23,
}


def apply_specs_to_template(ws, run: ProductionRun, session: Session):
    params = session.exec(select(RunParameter).where(RunParameter.run_id == run.id)).all()

    # pick row map per process
    row_map = ROW_MAP_REINF if run.process == "REINFORCEMENT" else ROW_MAP_LINER_COVER

    # columns per template
    if run.process in ["LINER", "COVER"]:
        SPEC_COL = "C"
        TOL_COL = "D"
    else:  # REINFORCEMENT
        SPEC_COL = "D"
        TOL_COL = "E"

    for p in params:
        r = row_map.get(p.param_key)
        if not r:
            continue

        set_val, tol_txt = format_spec_for_export(p.rule, p.min_value, p.max_value)

        _set_cell_safe(ws, f"{SPEC_COL}{r}", set_val if set_val is not None else "")
        _set_cell_safe(ws, f"{TOL_COL}{r}", tol_txt)

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)

    # Production runs
    runs = session.exec(select(ProductionRun).order_by(ProductionRun.created_at.desc())).all()

    grouped: Dict[str, List[ProductionRun]] = {}
    for r in runs:
        grouped.setdefault(r.dhtp_batch_no, []).append(r)

    progress_map = {r.id: get_progress_percent(session, r) for r in runs}

    # ✅ MRR status summary (simple + useful)
    lots = session.exec(select(MaterialLot).where(MaterialLot.status != MRR_CANCELED_STATUS).order_by(MaterialLot.created_at.desc())).all()
    lot_ids = [l.id for l in lots if l and l.id is not None]

    receiving_map = {}
    inspection_map = {}

    if lot_ids:
        receivings = session.exec(
            select(MrrReceiving).where(MrrReceiving.ticket_id.in_(lot_ids))
        ).all()
        receiving_map = {r.ticket_id: r for r in receivings}

        inspections = session.exec(
            select(MrrReceivingInspection).where(MrrReceivingInspection.ticket_id.in_(lot_ids))
        ).all()
        inspection_map = {i.ticket_id: i for i in inspections}

    mrr_pending_docs = 0
    mrr_docs_cleared = 0
    mrr_insp_submitted = 0
    mrr_final_approved = 0

    for lot in lots:
        rec = receiving_map.get(lot.id)
        insp = inspection_map.get(lot.id)

        docs_ok = bool(rec and (rec.inspector_confirmed_po or rec.manager_confirmed_po))
        insp_submitted = bool(insp and insp.inspector_confirmed)
        insp_ok = bool(insp and insp.manager_approved)

        if lot.status == "APPROVED":
            mrr_final_approved += 1
        else:
            if not docs_ok:
                mrr_pending_docs += 1
            else:
                mrr_docs_cleared += 1

            if insp_submitted and not insp_ok:
                mrr_insp_submitted += 1

    mrr_stats = {
        "pending_docs": mrr_pending_docs,
        "docs_cleared": mrr_docs_cleared,
        "insp_submitted": mrr_insp_submitted,
        "final_approved": mrr_final_approved,
    }

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "user": user,
            "grouped": grouped,
            "progress_map": progress_map,
            "mrr_stats": mrr_stats,
        },
    )

@app.get("/mrr", response_class=HTMLResponse)
def mrr_list(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)

    lots = session.exec(
        select(MaterialLot).where(MaterialLot.status != MRR_CANCELED_STATUS).order_by(MaterialLot.created_at.desc())
    ).all()

    lot_ids = [l.id for l in lots if l and l.id is not None]

    receiving_map = {}
    inspection_map = {}

    if lot_ids:
        receivings = session.exec(
            select(MrrReceiving).where(MrrReceiving.ticket_id.in_(lot_ids))
        ).all()
        receiving_map = {r.ticket_id: r for r in receivings}

        inspections = session.exec(
            select(MrrReceivingInspection).where(MrrReceivingInspection.ticket_id.in_(lot_ids))
        ).all()
        inspection_map = {i.ticket_id: i for i in inspections}

    return templates.TemplateResponse(
        "mrr_list.html",
        {
            "request": request,
            "user": user,
            "lots": lots,
            "receiving_map": receiving_map,
            "inspection_map": inspection_map,
            "error": "",
        },
    )



@app.get("/mrr/canceled", response_class=HTMLResponse)
def mrr_list_canceled(request: Request, session: Session = Depends(get_session)):
    """Show canceled (soft-deleted) MRR tickets for audit/review."""
    user = get_current_user(request, session)
    forbid_guest(user)

    lots = session.exec(
        select(MaterialLot)
        .where(func.upper(MaterialLot.status) == "CANCELED")
        .order_by(MaterialLot.id.desc())
    ).all()

    return templates.TemplateResponse(
        "mrr_list.html",
        {"request": request, "user": user, "lots": lots, "showing_canceled": True},
    )


@app.post("/mrr/new")
async def mrr_new(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)

    form = await request.form()

    tmp_batch = "TMP-" + datetime.utcnow().strftime("%Y%m%d-%H%M%S")

    material_name = str(form.get("material_name", "")).strip()
    supplier_name = str(form.get("supplier_name", "")).strip()

    lot_type = str(form.get("lot_type", "RAW")).strip().upper()
    if lot_type not in ["RAW", "OUTSOURCED"]:
        lot_type = "RAW"

    po_number = str(form.get("po_number", "")).strip()

    qty_raw = str(form.get("quantity", "")).strip()
    if qty_raw == "":
        raise HTTPException(400, "PO Quantity is required")
    try:
        quantity = float(qty_raw)
    except Exception:
        raise HTTPException(400, "Invalid PO Quantity")

    quantity_unit = str(form.get("quantity_unit", "KG")).strip().upper()
    if quantity_unit not in ["KG", "T", "PCS"]:
        quantity_unit = "KG"

    lot = MaterialLot(
        lot_type=lot_type,
        batch_no=tmp_batch,
        material_name=material_name,
        supplier_name=supplier_name,
        po_number=po_number,
        quantity=quantity,
        quantity_unit=quantity_unit,
        received_total=0.0,
        status="PENDING",
    )

    session.add(lot)
    session.commit()

    return RedirectResponse("/mrr", status_code=303)

@app.post("/mrr/{lot_id}/approve")
def mrr_approve(lot_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    # ✅ Do NOT approve the lot for production here anymore.
    # This button now only confirms the ticket is valid to proceed (still not production-approved).
    # We'll mark it as PENDING and rely on Receiving Inspection approval to set APPROVED.
    lot.status = "PENDING"
    session.add(lot)
    session.commit()

    return RedirectResponse(f"/mrr/{lot_id}", status_code=303)



@app.post("/mrr/{lot_id}/reject")
def mrr_reject(lot_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    lot.status = "REJECTED"
    session.add(lot)
    session.commit()

    return RedirectResponse(f"/mrr/{lot_id}", status_code=303)



@app.post("/mrr/{lot_id}/cancel")
def mrr_cancel(lot_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    lot.status = MRR_CANCELED_STATUS
    session.add(lot)
    session.commit()

    return RedirectResponse("/mrr", status_code=303)





    
@app.get("/mrr/{lot_id}", response_class=HTMLResponse)
def mrr_view(lot_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")
    block_if_mrr_canceled(lot)

    docs = session.exec(
        select(MrrDocument)
        .where(MrrDocument.ticket_id == lot_id)
        .order_by(MrrDocument.created_at.desc())
    ).all()

    receiving = session.exec(
        select(MrrReceiving)
        .where(MrrReceiving.ticket_id == lot_id)
    ).first()

    inspection = session.exec(
        select(MrrReceivingInspection)
        .where(MrrReceivingInspection.ticket_id == lot_id)
        .order_by(MrrReceivingInspection.created_at.desc())
    ).first()

    docs_ok = bool(receiving and (receiving.inspector_confirmed_po or receiving.manager_confirmed_po))
    insp_submitted = bool(inspection and getattr(inspection, 'inspector_confirmed', False))
    insp_ok = bool(inspection and getattr(inspection, 'manager_approved', False))

    return templates.TemplateResponse(
        "mrr_view.html",
        {
            "request": request,
            "user": user,
            "lot": lot,
            "docs": docs,
            "receiving": receiving,
            "docs_ok": docs_ok,
            "insp_submitted": insp_submitted,
            "insp_ok": insp_ok,
            "inspection": inspection,
            "error": request.query_params.get("error", ""),
        },
    )


@app.post("/mrr/{lot_id}/docs/upload")
async def mrr_doc_upload(
    lot_id: int,
    request: Request,
    session: Session = Depends(get_session),
    doc_type: str = Form(...),
    doc_title: str = Form(""),
    doc_number: str = Form(...),
    file: UploadFile = File(...),
):
    user = get_current_user(request, session)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")
    block_if_mrr_canceled(lot)

    

    safe_original = os.path.basename(file.filename or "upload.bin")
    filename = f"{lot_id}_{int(datetime.utcnow().timestamp())}_{safe_original}"
    abs_path = os.path.join(MRR_UPLOAD_DIR, filename)

    # write file
    with open(abs_path, "wb") as f:
        f.write(await file.read())

    dt = (doc_type or "").strip().upper()

    # Auto doc name unless RELATED
    title = (doc_title or "").strip()
    if dt != "RELATED":
        title = {
            "PO": "PO Copy",
            "DELIVERY_NOTE": "Delivery Note",
            "COA": "COA / Lab Test",
        }.get(dt, dt)

    if dt == "RELATED" and not title:
        raise HTTPException(400, "Document Name is required when type is RELATED")

    # ✅ store RELATIVE path (portable)
    rel_path = os.path.relpath(abs_path, BASE_DIR)

    doc = MrrDocument(
        ticket_id=lot_id,
        doc_type=dt,
        doc_name=title,
        doc_number=(doc_number or "").strip(),
        file_path=rel_path,
        uploaded_by_user_id=user.id,
        uploaded_by_user_name=user.display_name,
    )

    session.add(doc)
    session.commit()

    return RedirectResponse(f"/mrr/{lot_id}", status_code=303)





@app.get("/mrr/docs/{doc_id}/download")
def mrr_doc_download(doc_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)

    doc = session.get(MrrDocument, doc_id)
    if not doc:
        raise HTTPException(404, "File not found")

    real_path = resolve_mrr_doc_path(doc.file_path)
    if not real_path:
        raise HTTPException(404, "File not found")

    return FileResponse(
        real_path,
        filename=os.path.basename(real_path),
    )

@app.get("/mrr/docs/{doc_id}/inline")
def mrr_doc_inline(doc_id: int, request: Request, session: Session = Depends(get_session)):
    """
    Open document in browser (inline). Used by the "View" button in templates.

    Important:
    - Always resolve the stored file_path via resolve_mrr_doc_path(), because old records might store
      absolute paths from a different machine, or relative paths.
    """
    user = get_current_user(request, session)
    forbid_none(user)

    doc = session.get(MrrDocument, doc_id)
    if not doc:
        raise HTTPException(404, "Document not found")

    resolved = resolve_mrr_doc_path(doc.file_path)
    if not resolved or not os.path.exists(resolved):
        raise HTTPException(404, "File not found")

    media_type, _ = mimetypes.guess_type(resolved)
    media_type = media_type or "application/octet-stream"
    return FileResponse(
        resolved,
        media_type=media_type,
        filename=os.path.basename(resolved),
        headers={"Content-Disposition": f'inline; filename="{os.path.basename(resolved)}"'},
    )


@app.get("/mrr/docs/{doc_id}/view")
def mrr_doc_view(doc_id: int, request: Request, session: Session = Depends(get_session)):
    """
    View an uploaded MRR document in the browser (PDF/images inline).
    Falls back to download for other file types.

    NOTE: We intentionally do NOT rely on the `filename=` parameter here because
    some Starlette versions behave differently on mobile Safari when combined with
    inline Content-Disposition.
    """
    user = get_current_user(request, session)
    require_login(user)

    doc = session.get(MrrDocument, doc_id)
    if not doc:
        raise HTTPException(404, "Document not found")

    resolved = resolve_mrr_doc_path(doc.file_path or "")
    if not resolved or not os.path.exists(resolved):
        # Keep same behavior as download: explicit 404
        raise HTTPException(404, "File not found")

    # Guess content-type
    ctype, _ = mimetypes.guess_type(resolved)
    ctype = ctype or "application/octet-stream"

    # Inline only for types browsers can render nicely
    is_inline = (
        ctype.startswith("image/") or
        ctype in ("application/pdf",)
    )

    safe_name = os.path.basename(resolved)

    headers = {}
    if is_inline:
        headers["Content-Disposition"] = f'inline; filename="{safe_name}"'
    else:
        headers["Content-Disposition"] = f'attachment; filename="{safe_name}"'

    try:
        return FileResponse(resolved, media_type=ctype, headers=headers)
    except Exception as e:
        # If FileResponse fails for any environment-specific reason, stream it.
        def file_iter():
            with open(resolved, "rb") as f:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    yield chunk

        return StreamingResponse(file_iter(), media_type=ctype, headers=headers)

@app.post("/mrr/{lot_id}/docs/submit")
def mrr_docs_submit(
    lot_id: int,
    request: Request,
    session: Session = Depends(get_session),
    inspector_po_number: str = Form(...),
):
    user = get_current_user(request, session)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")
    block_if_mrr_canceled(lot)

    # create or load receiving record (this is ONLY for PO verification now)
    receiving = session.exec(
        select(MrrReceiving).where(MrrReceiving.ticket_id == lot_id)
    ).first()

    if not receiving:
        receiving = MrrReceiving(ticket_id=lot_id)

    receiving.inspector_po_number = inspector_po_number.strip()

    # PO match check
    receiving.po_match = (receiving.inspector_po_number == (lot.po_number or "").strip())

    session.add(receiving)
    session.commit()

    return RedirectResponse(f"/mrr/{lot_id}", status_code=303)


@app.post("/mrr/{lot_id}/docs/confirm")
def mrr_docs_confirm(lot_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    forbid_boss(user)

    lot = session.get(MaterialLot, lot_id)
    rec = session.exec(select(MrrReceiving).where(MrrReceiving.ticket_id == lot_id)).first()
    docs = session.exec(select(MrrDocument).where(MrrDocument.ticket_id == lot_id)).all()

    if not rec:
        raise HTTPException(400, "Documentation not saved")

    has_po_doc = any(d.doc_type == "PO" for d in docs)
    po_match = rec.inspector_po_number.strip() == (lot.po_number or "").strip()

    rec.po_match = po_match

    if has_po_doc and po_match:
        rec.inspector_confirmed_po = True
    else:
        rec.inspector_confirmed_po = False
        rec.manager_confirmed_po = False

    session.add(rec)
    session.commit()

    return RedirectResponse(f"/mrr/{lot_id}", status_code=303)

@app.post("/mrr/{lot_id}/docs/approve")
def mrr_docs_manager_approve(lot_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)

    rec = session.exec(select(MrrReceiving).where(MrrReceiving.ticket_id == lot_id)).first()
    if not rec:
        raise HTTPException(404, "Receiving record not found")

    rec.manager_confirmed_po = True
    rec.inspector_confirmed_po = True

    session.add(rec)
    session.commit()

    return RedirectResponse(f"/mrr/{lot_id}", status_code=303)

def mrr_docs_are_cleared(session: Session, lot_id: int) -> bool:
    rec = session.exec(
        select(MrrReceiving).where(MrrReceiving.ticket_id == lot_id)
    ).first()

    if not rec:
        return False


# =========================
# MRR Cancel helpers (soft delete)
# =========================
MRR_CANCELED_STATUS = "CANCELED"

def is_mrr_canceled(lot):
    return bool(lot and (getattr(lot, "status", "") or "").upper() == MRR_CANCELED_STATUS)

def block_if_mrr_canceled(lot):
    """Guard helper: prevent actions on canceled MRR tickets."""
    if is_mrr_canceled(lot):
        raise HTTPException(status_code=403, detail="This MRR Ticket is canceled")




def mrr_inspection_approve(lot_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)

    insp = session.exec(
        select(MrrReceivingInspection)
        .where(MrrReceivingInspection.ticket_id == lot_id)
    ).first()

    if not insp:
        raise HTTPException(404, "Receiving Inspection not found")

    # ✅ Manager approves the receiving inspection
    insp.manager_approved = True
    session.add(insp)

    # ✅ THIS is the ONLY place the batch becomes usable in production
    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")
    block_if_mrr_canceled(lot)

    lot.status = "APPROVED"   # <-- makes it appear in production dropdown
    session.add(lot)

    session.commit()
    return RedirectResponse(f"/mrr/{lot_id}", status_code=303)

@app.get("/mrr/pending", response_class=HTMLResponse)
def mrr_pending(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)

    # Find inspections submitted by inspector but not yet manager-approved
    pending = session.exec(
        select(MrrReceivingInspection)
        .where(MrrReceivingInspection.inspector_confirmed == True)
        .where(MrrReceivingInspection.manager_approved == False)
        .order_by(MrrReceivingInspection.created_at.desc())
    ).all()

    # Load lots to show ticket info
    lot_ids = [p.ticket_id for p in pending]
    lots = []
    lot_map = {}
    if lot_ids:
        lots = session.exec(select(MaterialLot).where(MaterialLot.id.in_(lot_ids))).all()
        lot_map = {l.id: l for l in lots}

    return templates.TemplateResponse(
        "mrr_pending.html",
        {
            "request": request,
            "user": user,
            "pending": pending,
            "lot_map": lot_map,
        },
    )

@app.get("/mrr/{lot_id}/inspection/new", response_class=HTMLResponse)
def new_shipment_inspection_page(
    lot_id: int,
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    # Remaining qty (kg/t compatible)
    po_unit = (lot.quantity_unit or "KG").upper().strip()
    po_qty = float(lot.quantity or 0.0)

    if po_unit in ["PC", "PCS"]:
        # PCS balance uses raw numbers
        received_total = float(lot.received_total or 0.0)
        remaining = max(po_qty - received_total, 0.0)
        remaining_label = f"{remaining:g} {po_unit}"
    else:
        po_kg = normalize_qty_to_kg(po_qty, po_unit)
        received_total = float(lot.received_total or 0.0)  # we store as KG normalized
        remaining_kg = max(po_kg - received_total, 0.0)
        remaining_label = f"{remaining_kg:g} KG (normalized)"

    # Shipment counter (how many shipments exist already)
    shipments = session.exec(
        select(MrrReceivingInspection).where(MrrReceivingInspection.ticket_id == lot_id)
    ).all()
    next_seq = len(shipments) + 1

    # Draft resume (best UX)
    draft = get_latest_draft_shipment(session, lot_id)

    error = request.query_params.get("error", "").strip()

    return templates.TemplateResponse(
        "mrr_new_shipment.html",
        {
            "request": request,
            "user": user,
            "lot": lot,
            "remaining_label": remaining_label,
            "next_seq": next_seq,
            "draft": draft,
            "error": error,
        },
    )

@app.post("/mrr/{lot_id}/inspection/{inspection_id}")
async def shipment_inspection_submit(
    lot_id: int,
    inspection_id: int,
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    forbid_boss(user)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    insp = session.get(MrrReceivingInspection, inspection_id)
    if not insp or insp.ticket_id != lot_id:
        raise HTTPException(404, "Shipment inspection not found")

    # If already submitted, don't double-submit
    if insp.inspector_confirmed:
        return RedirectResponse(f"/mrr/{lot_id}", status_code=303)

    form = await request.form()

    # DN must match the shipment DN (no editing here)
    dn = (insp.delivery_note_no or "").strip()
    if not dn:
        return RedirectResponse(f"/mrr/{lot_id}/inspection/{inspection_id}", status_code=303)

    # Block only if ANOTHER submitted shipment already used this DN
    used = session.exec(
        select(MrrReceivingInspection).where(
            (MrrReceivingInspection.ticket_id == lot_id) &
            (MrrReceivingInspection.id != inspection_id) &
            (MrrReceivingInspection.delivery_note_no == dn) &
            (
                (MrrReceivingInspection.inspector_confirmed == True) |
                (MrrReceivingInspection.manager_approved == True)
            )
        )
    ).first()
    if used:
        return RedirectResponse(
            f"/mrr/{lot_id}/inspection/{inspection_id}?error=This%20Delivery%20Note%20was%20already%20used%20in%20another%20submitted%20shipment",
            status_code=303
        )

    # Update JSON with all inspection fields (store everything)
    try:
        data = json.loads(insp.inspection_json or "{}")
    except Exception:
        data = {}

    # Keep header fields from shipment as truth
    data["report_no"] = insp.report_no
    data["delivery_note_no"] = insp.delivery_note_no
    data["qty_arrived"] = insp.qty_arrived
    data["qty_unit"] = insp.qty_unit

    # Store the rest of inspection form inputs
    for k in form.keys():
        data[k] = str(form.get(k) or "").strip()

    insp.inspection_json = json.dumps(data)
    insp.inspector_confirmed = True
    session.add(insp)

    # ✅ NOW we consume qty into received_total (only on SUBMIT)
    po_unit = (lot.quantity_unit or "KG").upper().strip()
    received_total = float(lot.received_total or 0.0)

    if po_unit in ["PC", "PCS"]:
        arrived_norm = float(insp.qty_arrived or 0.0)
        lot.received_total = received_total + arrived_norm
        lot.status = "PARTIAL" if lot.received_total < float(lot.quantity or 0.0) else "PENDING"
    else:
        arrived_norm = normalize_qty_to_kg(float(insp.qty_arrived or 0.0), insp.qty_unit or "KG")
        lot.received_total = received_total + float(arrived_norm)

        po_kg = normalize_qty_to_kg(float(lot.quantity or 0.0), po_unit)
        lot.status = "PARTIAL" if lot.received_total < po_kg else "PENDING"

    session.add(lot)
    session.commit()

    return RedirectResponse(f"/mrr/{lot_id}", status_code=303)

@app.post("/mrr/{lot_id}/inspection/new")
def create_shipment_inspection(
    lot_id: int,
    request: Request,
    session: Session = Depends(get_session),
    delivery_note_no: str = Form(...),
    qty_arrived: float = Form(...),
    qty_unit: str = Form(...),
):
    user = get_current_user(request, session)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    dn = (delivery_note_no or "").strip()
    if not dn:
        return RedirectResponse(
            f"/mrr/{lot_id}/inspection/new?error=Delivery%20Note%20number%20is%20required",
            status_code=303
        )

    qty_unit = (qty_unit or "KG").upper().strip()
    arrived_qty = float(qty_arrived or 0.0)
    if arrived_qty <= 0:
        return RedirectResponse(
            f"/mrr/{lot_id}/inspection/new?error=Arrived%20quantity%20must%20be%20greater%20than%200",
            status_code=303
        )

    # 1) DN document must exist (uploaded as DELIVERY_NOTE with same doc_number)
    if not dn_doc_exists(session, lot_id, dn):
        return RedirectResponse(
            f"/mrr/{lot_id}/inspection/new?error=Upload%20Delivery%20Note%20document%20first%20(type%20DELIVERY_NOTE)%20with%20Document%20Number%20exactly%20matching%20this%20DN",
            status_code=303,
        )

    # 2) If there is a DRAFT shipment with same DN => resume it
    draft = get_draft_shipment_by_dn(session, lot_id, dn)
    if draft:
        return RedirectResponse(f"/mrr/{lot_id}/inspection/{draft.id}", status_code=303)

    # 3) If there is a SUBMITTED shipment with same DN => block
    used = get_submitted_shipment_by_dn(session, lot_id, dn)
    if used:
        return RedirectResponse(
            f"/mrr/{lot_id}/inspection/new?error=This%20Delivery%20Note%20was%20already%20used%20in%20a%20previous%20submitted%20shipment",
            status_code=303,
        )

    # 4) Unit compatibility
    po_unit = (lot.quantity_unit or "KG").upper().strip()
    if not units_compatible(po_unit, qty_unit):
        return RedirectResponse(
            f"/mrr/{lot_id}/inspection/new?error=Unit%20mismatch:%20PO%20is%20{po_unit},%20arrived%20is%20{qty_unit}",
            status_code=303,
        )

    # 5) Remaining balance validation (IMPORTANT: use CURRENT received_total, do NOT change it here)
    po_qty = float(lot.quantity or 0.0)
    received_total = float(lot.received_total or 0.0)

    if po_unit in ["PC", "PCS"]:
        remaining = max(po_qty - received_total, 0.0)
        if arrived_qty > remaining:
            return RedirectResponse(
                f"/mrr/{lot_id}/inspection/new?error=Arrived%20exceeds%20remaining%20balance%20({remaining:g}%20{po_unit})",
                status_code=303,
            )
        arrived_norm = arrived_qty
    else:
        po_kg = normalize_qty_to_kg(po_qty, po_unit)
        remaining_kg = max(po_kg - received_total, 0.0)
        arrived_norm = normalize_qty_to_kg(arrived_qty, qty_unit)
        if arrived_norm > remaining_kg:
            return RedirectResponse(
                f"/mrr/{lot_id}/inspection/new?error=Arrived%20exceeds%20remaining%20balance%20({remaining_kg:g}%20KG%20normalized)",
                status_code=303,
            )

    # 6) Shipment sequence
    existing = session.exec(
        select(MrrReceivingInspection).where(MrrReceivingInspection.ticket_id == lot_id)
    ).all()
    seq = len(existing) + 1

    # ✅ Create shipment as DRAFT (DO NOT change lot.received_total here)
    insp = MrrReceivingInspection(
        ticket_id=lot_id,
        inspector_id=user.id,
        inspector_name=user.display_name,
        delivery_note_no=dn,
        qty_arrived=float(arrived_qty),
        qty_unit=qty_unit,
        report_no=generate_report_no(lot_id, seq),
        template_type=lot.lot_type or "RAW",
        inspection_json=json.dumps({
            "delivery_note_no": dn,
            "qty_arrived": float(arrived_qty),
            "qty_unit": qty_unit,
            "arrived_norm": float(arrived_norm),
        }),
        inspector_confirmed=False,
        manager_approved=False,
    )
    session.add(insp)
    session.commit()
    session.refresh(insp)

    return RedirectResponse(f"/mrr/{lot_id}/inspection/{insp.id}", status_code=303)





@app.get("/runs/new", response_class=HTMLResponse)
def run_new_get(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    if (user.role or "").upper() not in ["MANAGER", "RUN_CREATOR"]:
        raise HTTPException(403, "Manager only")

    return templates.TemplateResponse("run_new.html", {"request": request, "user": user, "error": ""})

@app.get("/mrr/{lot_id}/inspection/{inspection_id}", response_class=HTMLResponse)
def shipment_inspection_form(
    lot_id: int,
    inspection_id: int,
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    inspection = session.get(MrrReceivingInspection, inspection_id)
    if not inspection or inspection.ticket_id != lot_id:
        raise HTTPException(404, "Shipment inspection not found")

    # Header source of truth:
    # - PO = ticket
    # - Report/DN/Qty/Unit = shipment record
    import json
    try:
        data = json.loads(inspection.inspection_json or "{}")
    except Exception:
        data = {}

    # Ensure report_no exists even for older drafts
    if not getattr(inspection, "report_no", None):
        # fallback (should not happen if your model has report_no)
        data["report_no"] = data.get("report_no") or generate_report_no(lot_id, 1)
    else:
        data["report_no"] = inspection.report_no

    # Mirror shipment fields into json (template reads inspection + data)
    data["delivery_note_no"] = inspection.delivery_note_no or ""
    data["qty_arrived"] = inspection.qty_arrived if inspection.qty_arrived is not None else ""
    data["qty_unit"] = inspection.qty_unit or "KG"

    return templates.TemplateResponse(
        "mrr_inspection.html",
        {
            "request": request,
            "user": user,
            "lot": lot,
            "inspection": inspection,
            "inspection_data": data,
        },
    )

@app.get("/mrr/{lot_id}/inspection")
def old_inspection_redirect(lot_id: int):
    return RedirectResponse(f"/mrr/{lot_id}/inspection/new", status_code=302)

@app.post("/runs/new")
def run_new_post(
    request: Request,
    session: Session = Depends(get_session),
    process: str = Form(...),
    dhtp_batch_no: str = Form(...),
    client_name: str = Form(...),
    po_number: str = Form(...),
    itp_number: str = Form(...),
    pipe_specification: str = Form(...),
    raw_material_spec: str = Form(...),
    total_length_m: float = Form(0.0),
    allow_duplicate: str = Form(""),
):
    user = get_current_user(request, session)
    if (user.role or "").upper() not in ["MANAGER", "RUN_CREATOR"]:
        raise HTTPException(403, "Manager only")

     

    process = process.upper().strip()
    if process not in PROCESS_PARAMS:
        return templates.TemplateResponse("run_new.html", {"request": request, "user": user, "error": "Invalid process"})

    existing_open = session.exec(
        select(ProductionRun).where(
            ProductionRun.dhtp_batch_no == dhtp_batch_no,
            ProductionRun.process == process,
            ProductionRun.status == "OPEN",
        )
    ).first()
    if existing_open and allow_duplicate != "1":
        return templates.TemplateResponse(
            "run_new.html",
            {
                "request": request,
                "user": user,
                "error": f"There is already an OPEN {process} run for Batch {dhtp_batch_no}. If you really need a second line, tick 'Allow duplicate line'."
            },
        )

    run = ProductionRun(
        process=process,
        dhtp_batch_no=dhtp_batch_no,
        client_name=client_name,
        po_number=po_number,
        itp_number=itp_number,
        pipe_specification=pipe_specification,
        raw_material_spec=raw_material_spec,
        total_length_m=total_length_m or 0.0,
    )
    session.add(run)
    session.commit()
    session.refresh(run)

    for idx, (key, label, unit) in enumerate(PROCESS_PARAMS[process]):
        session.add(RunParameter(run_id=run.id, param_key=key, label=label, unit=unit, display_order=idx))
    session.commit()

    return RedirectResponse("/dashboard", status_code=302)


@app.get("/runs/{run_id}", response_class=HTMLResponse)
def run_view(run_id: int, request: Request, session: Session = Depends(get_session)):
    try:
        user = get_current_user(request, session)

        run = session.get(ProductionRun, run_id)
        if not run:
            raise HTTPException(404, "Run not found")

        machines = session.exec(select(RunMachine).where(RunMachine.run_id == run_id)).all()
        params = session.exec(
            select(RunParameter).where(RunParameter.run_id == run_id).order_by(RunParameter.display_order)
        ).all()

        days = get_days_for_run(session, run_id)

        selected_day = None
        day_q = request.query_params.get("day")
        if day_q:
            try:
                selected_day = date.fromisoformat(day_q)
            except Exception:
                selected_day = None

        if selected_day is None:
            selected_day = days[-1] if days else date.today()

        entries = session.exec(
            select(InspectionEntry)
            .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date == selected_day)
            .order_by(InspectionEntry.created_at)
        ).all()
        
        # ✅ NEW: for the UI to know which slots have an InspectionEntry
        slot_entry_ids: Dict[str, int] = {}
        for e in entries:
            if not e.slot_time or e.slot_time not in SLOTS:
                continue
            slot_entry_ids[e.slot_time] = e.id



        users = session.exec(select(User)).all()
        user_map = {u.id: u for u in users}

        slot_inspectors: Dict[str, str] = {s: "" for s in SLOTS}
        grid: Dict[str, Dict[str, dict]] = {s: {} for s in SLOTS}

        for e in entries:
            if not e.slot_time or e.slot_time not in SLOTS:
                continue

            if e.inspector_id and e.inspector_id in user_map:
                slot_inspectors[e.slot_time] = user_map[e.inspector_id].display_name or ""
            else:
                slot_inspectors[e.slot_time] = ""

            vals = session.exec(select(InspectionValue).where(InspectionValue.entry_id == e.id)).all()
            for v in vals:
                grid[e.slot_time][v.param_key] = {
                    "value_id": v.id,
                    "value": v.value,
                    "out": bool(v.is_out_of_spec),
                    "note": v.spec_note or "",
                    "pending_value": v.pending_value,
                    "pending_status": v.pending_status or "",
                }

        trace_today = get_day_latest_trace(session, run_id, selected_day)
        carry = get_last_known_trace_before_day(session, run_id, selected_day)

        raw_batches = trace_today["raw_batches"] or ([carry["raw"]] if carry["raw"] else [])
        tools = trace_today["tools"] or carry["tools"]

        progress = get_progress_percent(session, run)

        return templates.TemplateResponse(
            "run_view.html",
            {
                "request": request,
                "user": user,
                "run": run,
                "machines": machines,
                "params": params,
                "days": days,
                "selected_day": selected_day,
                "grid": grid,
                "slot_inspectors": slot_inspectors,
                "slot_entry_ids": slot_entry_ids, 
                "image_url": IMAGE_MAP.get(run.process, ""),
                "progress": progress,
                "raw_batches": raw_batches,
                "tools": tools,
               
            },
        )

    except Exception:
        # TEMP DEBUG: show the real error on the page
        return HTMLResponse(
            "<pre style='white-space:pre-wrap;font-size:14px'>"
            + traceback.format_exc()
            + "</pre>",
            status_code=500,
        )



@app.get("/runs/{run_id}/edit", response_class=HTMLResponse)
def run_edit_get(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)
        

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    machines = session.exec(select(RunMachine).where(RunMachine.run_id == run_id)).all()
    params = session.exec(
        select(RunParameter).where(RunParameter.run_id == run_id).order_by(RunParameter.display_order)
    ).all()

    return templates.TemplateResponse(
        "run_edit.html",
        {"request": request, "user": user, "run": run, "machines": machines, "params": params, "error": ""},
    )


@app.post("/runs/{run_id}/edit")
async def run_edit_post(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)
        

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    form = await request.form()

    run.client_name = str(form.get("client_name", "")).strip()
    run.po_number = str(form.get("po_number", "")).strip()
    run.itp_number = str(form.get("itp_number", "")).strip()
    run.pipe_specification = str(form.get("pipe_specification", "")).strip()
    run.raw_material_spec = str(form.get("raw_material_spec", "")).strip()
    try:
        run.total_length_m = float(form.get("total_length_m") or 0.0)
    except Exception:
        run.total_length_m = 0.0

    session.add(run)
    session.commit()

    existing = session.exec(select(RunMachine).where(RunMachine.run_id == run_id)).all()
    for m in existing:
        session.delete(m)
    session.commit()

    def _m(name_key: str, tag_key: str):
        name = str(form.get(name_key, "")).strip()
        tag = str(form.get(tag_key, "")).strip()
        if name:
            session.add(RunMachine(run_id=run_id, machine_name=name, machine_tag=tag))

    _m("machine1_name", "machine1_tag")
    _m("machine2_name", "machine2_tag")
    _m("machine3_name", "machine3_tag")
    _m("machine4_name", "machine4_tag")
    _m("machine5_name", "machine5_tag")
    session.commit()

    params = session.exec(select(RunParameter).where(RunParameter.run_id == run_id)).all()
    for p in params:
        new_label = str(form.get(f"label_{p.param_key}", "")).strip()
        if new_label:
            p.label = new_label

        rule = str(form.get(f"rule_{p.param_key}", "")).strip().upper()
        if rule not in ["", "RANGE", "MIN_ONLY", "MAX_ONLY"]:
            rule = ""
        p.rule = rule

        set_raw = form.get(f"set_{p.param_key}", "")
        tol_raw = form.get(f"tol_{p.param_key}", "")
        
        set_v = _safe_float(set_raw)
        tol_v = _safe_float(tol_raw)
        
        # convert Set/Tolerance to Min/Max (keeps old system working)
        if p.rule == "RANGE":
            if set_v is None or tol_v is None:
                p.min_value = None
                p.max_value = None
            else:
                t = abs(tol_v)
                p.min_value = set_v - t
                p.max_value = set_v + t
        
        elif p.rule == "MAX_ONLY":
            # your example: temperature max 35 -> set=35, tol can be empty
            p.min_value = None
            p.max_value = set_v
        
        elif p.rule == "MIN_ONLY":
            p.min_value = set_v
            p.max_value = None
        
        else:
            p.min_value = None
            p.max_value = None


        session.add(p)

    session.commit()
    return RedirectResponse(f"/runs/{run.id}", status_code=302)


@app.get("/runs/{run_id}/entry/new", response_class=HTMLResponse)
def entry_new_get(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    params = session.exec(
        select(RunParameter).where(RunParameter.run_id == run_id).order_by(RunParameter.display_order)
    ).all()

    has_any = session.exec(select(InspectionEntry.id).where(InspectionEntry.run_id == run_id)).first() is not None
    error = request.query_params.get("error", "")

    approved_lots = session.exec(
        select(MaterialLot)
        .where(MaterialLot.status == "APPROVED", MaterialLot.lot_type == "RAW")
        .order_by(MaterialLot.batch_no)
    ).all()

    # ✅ TRUE check: does the run already have any batch event?
    has_any_event = session.exec(
        select(MaterialUseEvent.id).where(MaterialUseEvent.run_id == run_id).limit(1)
    ).first() is not None

    # ✅ Show current batch as the latest batch in the run (not today 00:00)
    today_lot = get_latest_material_lot_for_run(session, run_id)

    return templates.TemplateResponse(
        "entry_new.html",
        {
            "request": request,
            "user": user,
            "run": run,
            "params": params,
            "has_any": has_any,
            "error": error,
            "approved_lots": approved_lots,
            "has_any_event": has_any_event,
            "current_lot_preview": today_lot,
        },
    )


def get_latest_material_lot_for_run(session: Session, run_id: int):
    last_ev = session.exec(
        select(MaterialUseEvent)
        .where(MaterialUseEvent.run_id == run_id)
        .order_by(
            MaterialUseEvent.day.desc(),
            MaterialUseEvent.slot_time.desc(),
            MaterialUseEvent.created_at.desc(),
        )
    ).first()

    if not last_ev:
        return None
    return session.get(MaterialLot, last_ev.lot_id)


def apply_spec_check(param, v):
    """
    Returns (is_oos, note)
    - is_oos: True if value is out of spec
    - note: human-readable explanation
    """

    # Handle empty / missing values safely
    if v is None or (isinstance(v, str) and v.strip() == ""):
        return False, ""

    # Convert value to float if possible (common for measurements)
    try:
        val = float(v)
    except (TypeError, ValueError):
        # If it's not numeric, we can't spec-check it here
        return False, ""

    # Extract limits from param in a flexible way (dict or object)
    def get_attr(obj, *names):
        if obj is None:
            return None
        # dict style
        if isinstance(obj, dict):
            for n in names:
                if n in obj and obj[n] is not None:
                    return obj[n]
        # object style
        for n in names:
            if hasattr(obj, n):
                x = getattr(obj, n)
                if x is not None:
                    return x
        return None

    min_v = get_attr(param, "min_value", "min", "lower", "low_limit")
    max_v = get_attr(param, "max_value", "max", "upper", "high_limit")

    # Try to convert min/max if they exist
    try:
        min_v = float(min_v) if min_v is not None else None
    except (TypeError, ValueError):
        min_v = None
    try:
        max_v = float(max_v) if max_v is not None else None
    except (TypeError, ValueError):
        max_v = None

    # Spec checks
    if min_v is not None and val < min_v:
        return True, f"Below min ({val} < {min_v})"
    if max_v is not None and val > max_v:
        return True, f"Above max ({val} > {max_v})"

    return False, ""
   
@app.post("/runs/{run_id}/entry/new")
async def entry_new_post(
    run_id: int,
    request: Request,
    session: Session = Depends(get_session),
    actual_date: str = Form(...),
    actual_time: str = Form(...),
    operator_1: str = Form(""),
    operator_2: str = Form(""),
    operator_annular_12: str = Form(""),
    operator_int_ext_34: str = Form(""),
    remarks: str = Form(""),
    tool1_name: str = Form(""),
    tool1_serial: str = Form(""),
    tool1_calib_due: str = Form(""),
    tool2_name: str = Form(""),
    tool2_serial: str = Form(""),
    tool2_calib_due: str = Form(""),
    start_lot_id: str = Form(""),   # ✅ for first ever entry
):
    user = get_current_user(request, session)
    forbid_boss(user)

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    if run.status in ["CLOSED", "APPROVED"] and user.role != "MANAGER":
        raise HTTPException(403, "Run is not open")

    slot_time, day_add = slot_from_time_str(actual_time)
    day_obj = date.fromisoformat(actual_date) + timedelta(days=day_add)

    # prevent duplicate slot entry
    existing_for_slot = session.exec(
        select(InspectionEntry).where(
            InspectionEntry.run_id == run_id,
            InspectionEntry.actual_date == day_obj,
            InspectionEntry.slot_time == slot_time
        )
    ).first()
    if existing_for_slot:
        msg = "This timing slot is already inspected. Please confirm the time, or use Edit to change the existing record."
        return RedirectResponse(f"/runs/{run_id}/entry/new?error={msg}", status_code=302)

    form = await request.form()

    # batch-change checkbox + selected lot
    batch_changed = str(form.get("batch_changed", "")).strip() == "1"
    new_lot_id_raw = str(form.get("new_lot_id", "")).strip()
    
    # ✅ If user selected a new lot from dropdown, treat it as batch changed (even if checkbox not ticked)
    if new_lot_id_raw.isdigit():
        batch_changed = True


    # check if run has ANY material event yet
    has_any_event = session.exec(
        select(MaterialUseEvent.id).where(MaterialUseEvent.run_id == run_id).limit(1)
    ).first() is not None

    # If this is the FIRST entry EVER (no event exists), require start_lot_id
    if not has_any_event:
        if not str(start_lot_id).isdigit():
            msg = "Please select the STARTING approved RAW batch (first entry only)."
            return RedirectResponse(f"/runs/{run_id}/entry/new?error={msg}", status_code=302)

        lot = session.get(MaterialLot, int(start_lot_id))
        if (not lot) or (lot.status != "APPROVED") or (getattr(lot, "lot_type", "RAW") != "RAW"):
            msg = "Selected starting batch is not an APPROVED RAW batch."
            return RedirectResponse(f"/runs/{run_id}/entry/new?error={msg}", status_code=302)
            

        # Create the inspection entry first (NO batch yet)
    entry = InspectionEntry(
        run_id=run_id,
        actual_date=day_obj,
        actual_time=actual_time,
        slot_time=slot_time,
        inspector_id=user.id,
        operator_1=operator_1,
        operator_2=operator_2,
        operator_annular_12=operator_annular_12,
        operator_int_ext_34=operator_int_ext_34,
        remarks=remarks,
        tool1_name=tool1_name,
        tool1_serial=tool1_serial,
        tool1_calib_due=tool1_calib_due,
        tool2_name=tool2_name,
        tool2_serial=tool2_serial,
        tool2_calib_due=tool2_calib_due,
    )
    session.add(entry)
    session.commit()
    session.refresh(entry)

    # ✅ FIRST entry: create starting MaterialUseEvent at this slot
    if not has_any_event:
        session.add(MaterialUseEvent(
            run_id=run_id,
            day=day_obj,
            slot_time=slot_time,
            lot_id=int(start_lot_id),
            created_by_user_id=user.id,
        ))
        session.commit()

    # ✅ Batch changed: create new event
    if batch_changed:
        if not new_lot_id_raw.isdigit():
            msg = "Please select the NEW approved RAW batch when 'batch changed' is checked."
            return RedirectResponse(f"/runs/{run_id}/entry/new?error={msg}", status_code=302)

        new_lot = session.get(MaterialLot, int(new_lot_id_raw))
        if (not new_lot) or (new_lot.status != "APPROVED") or (getattr(new_lot, "lot_type", "RAW") != "RAW"):
            msg = "Selected NEW batch is not an APPROVED RAW batch."
            return RedirectResponse(f"/runs/{run_id}/entry/new?error={msg}", status_code=302)

        session.add(MaterialUseEvent(
            run_id=run_id,
            day=day_obj,
            slot_time=slot_time,
            lot_id=int(new_lot_id_raw),
            created_by_user_id=user.id,
        ))
        session.commit()

    # ✅ NOW we can read the current batch (because the events exist) and save it into the entry
    current_lot = get_current_material_lot_for_slot(session, run_id, day_obj, slot_time)
    entry.raw_material_batch_no = (current_lot.batch_no or "").strip() if current_lot else ""
    session.add(entry)
    session.commit()


    # save values (unchanged logic)
    params = session.exec(select(RunParameter).where(RunParameter.run_id == run_id)).all()
    by_key = {p.param_key: p for p in params}

    for key, param in by_key.items():
        raw = form.get(f"v_{key}")
        if raw in [None, ""]:
            continue
        v = _safe_float(raw)
        if v is None:
            continue
        is_oos, note = apply_spec_check(param, v)
        session.add(InspectionValue(
            entry_id=entry.id,
            param_key=key,
            value=v,
            is_out_of_spec=is_oos,
            spec_note=note,
        ))

    session.commit()
    return RedirectResponse(f"/runs/{run_id}?day={entry.actual_date.isoformat()}", status_code=302)


@app.get("/runs/{run_id}/entry/{slot_time}/fill/{param_key}", response_class=HTMLResponse)
def fill_missing_value_get(
    run_id: int,
    slot_time: str,
    param_key: str,
    request: Request,
    day: str,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    forbid_boss(user)

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    # permissions
    if run.status in ["CLOSED", "APPROVED"] and (user.role or "").upper() != "MANAGER":
        raise HTTPException(403, "Run is closed")

    day_obj = date.fromisoformat(day)

    entry = session.exec(
        select(InspectionEntry).where(
            InspectionEntry.run_id == run_id,
            InspectionEntry.actual_date == day_obj,
            InspectionEntry.slot_time == slot_time,
        )
    ).first()
    if not entry:
        raise HTTPException(404, "No inspection entry for this slot/day")

    # param
    param = session.exec(
        select(RunParameter).where(RunParameter.run_id == run_id, RunParameter.param_key == param_key)
    ).first()
    if not param:
        raise HTTPException(404, "Parameter not found")

    # already exists? send to normal edit
    existing = session.exec(
        select(InspectionValue).where(InspectionValue.entry_id == entry.id, InspectionValue.param_key == param_key)
    ).first()
    if existing:
        return RedirectResponse(f"/values/{existing.id}/edit", status_code=302)

    return templates.TemplateResponse(
        "value_fill.html",
        {"request": request, "user": user, "run": run, "entry": entry, "param": param, "error": ""},
    )

@app.post("/runs/{run_id}/entry/{slot_time}/fill/{param_key}")
async def fill_missing_value_post(
    run_id: int,
    slot_time: str,
    param_key: str,
    request: Request,
    day: str = Form(...),
    new_value: str = Form(...),
    note: str = Form(""),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    forbid_boss(user)

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    # permissions
    if run.status in ["CLOSED", "APPROVED"] and (user.role or "").upper() != "MANAGER":
        raise HTTPException(403, "Run is closed")

    day_obj = date.fromisoformat(day)

    entry = session.exec(
        select(InspectionEntry).where(
            InspectionEntry.run_id == run_id,
            InspectionEntry.actual_date == day_obj,
            InspectionEntry.slot_time == slot_time,
        )
    ).first()
    if not entry:
        raise HTTPException(404, "No inspection entry for this slot/day")

    param = session.exec(
        select(RunParameter).where(RunParameter.run_id == run_id, RunParameter.param_key == param_key)
    ).first()
    if not param:
        raise HTTPException(404, "Parameter not found")

    # prevent duplicates
    existing = session.exec(
        select(InspectionValue).where(InspectionValue.entry_id == entry.id, InspectionValue.param_key == param_key)
    ).first()
    if existing:
        return RedirectResponse(f"/runs/{run_id}?day={day_obj.isoformat()}", status_code=302)

    v = _safe_float(new_value)
    if v is None:
        return RedirectResponse(f"/runs/{run_id}?day={day_obj.isoformat()}", status_code=302)

    is_oos, spec_note = apply_spec_check(param, v)

    new_iv = InspectionValue(
        entry_id=entry.id,
        param_key=param_key,
        value=v,
        is_out_of_spec=is_oos,
        spec_note=spec_note,
    )
    session.add(new_iv)
    session.commit()
    session.refresh(new_iv)

    # optional: store audit (if your audit table allows it)
    session.add(InspectionValueAudit(
        inspection_value_id=new_iv.id,
        action="CREATED",
        old_value=None,
        new_value=v,
        by_user_id=user.id,
        by_user_name=user.display_name,
        note=note or "",
    ))
    session.commit()

    return RedirectResponse(f"/runs/{run_id}?day={day_obj.isoformat()}", status_code=302)


# ===== VALUE EDIT + APPROVAL (kept as your working logic) =====
@app.get("/values/{value_id}/edit", response_class=HTMLResponse)
def value_edit_get(value_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)

    v = session.get(InspectionValue, value_id)
    if not v:
        raise HTTPException(404, "Value not found")
    entry = session.get(InspectionEntry, v.entry_id)
    run = session.get(ProductionRun, entry.run_id) if entry else None
    if not entry or not run:
        raise HTTPException(404, "Run/Entry not found")

    if run.status in ["CLOSED", "APPROVED"] and user.role != "MANAGER":
        raise HTTPException(403, "Run is not open")

    param = session.exec(
        select(RunParameter).where(RunParameter.run_id == run.id, RunParameter.param_key == v.param_key)
    ).first()

    return templates.TemplateResponse(
        "value_edit.html",
        {"request": request, "user": user, "run": run, "entry": entry, "param": param, "v": v, "error": ""},
    )


@app.post("/values/{value_id}/edit")
async def value_edit_post(
    value_id: int,
    request: Request,
    session: Session = Depends(get_session),
    new_value: str = Form(...),
    note: str = Form(""),
):
    user = get_current_user(request, session)
    forbid_boss(user)

    v = session.get(InspectionValue, value_id)
    if not v:
        raise HTTPException(404, "Value not found")
    entry = session.get(InspectionEntry, v.entry_id)
    run = session.get(ProductionRun, entry.run_id) if entry else None
    if not entry or not run:
        raise HTTPException(404, "Run/Entry not found")

    if run.status in ["CLOSED", "APPROVED"] and user.role != "MANAGER":
        raise HTTPException(403, "Run is not open")

    nv = _safe_float(new_value)
    if nv is None:
        return RedirectResponse(f"/values/{value_id}/edit", status_code=302)

    v.pending_value = nv
    v.pending_status = "PENDING"
    v.pending_by_user_id = user.id
    v.pending_at = datetime.utcnow()
    session.add(v)

    session.add(InspectionValueAudit(
        inspection_value_id=v.id,
        action="PROPOSED",
        old_value=v.value,
        new_value=nv,
        by_user_id=user.id,
        by_user_name=user.display_name,
        note=note or "",
    ))

    session.commit()
    return RedirectResponse(f"/runs/{run.id}?day={entry.actual_date.isoformat()}", status_code=302)


@app.get("/runs/{run_id}/pending", response_class=HTMLResponse)
def pending_list(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)
        

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    pending_items = []
    entries = session.exec(select(InspectionEntry).where(InspectionEntry.run_id == run_id)).all()
    entry_ids = [e.id for e in entries]
    if entry_ids:
        vals = session.exec(
            select(InspectionValue).where(
                InspectionValue.entry_id.in_(entry_ids),
                InspectionValue.pending_status == "PENDING"
            )
        ).all()

        params = session.exec(select(RunParameter).where(RunParameter.run_id == run_id)).all()
        pmap = {p.param_key: p for p in params}
        emap = {e.id: e for e in entries}

        for v in vals:
            pending_items.append({"value": v, "entry": emap.get(v.entry_id), "param": pmap.get(v.param_key)})

    return templates.TemplateResponse(
        "pending_list.html",
        {"request": request, "user": user, "run": run, "items": pending_items},
    )


@app.post("/values/{value_id}/approve")
def value_approve(value_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)
        

    v = session.get(InspectionValue, value_id)
    if not v:
        raise HTTPException(404, "Value not found")
    entry = session.get(InspectionEntry, v.entry_id)
    run = session.get(ProductionRun, entry.run_id) if entry else None
    if not entry or not run:
        raise HTTPException(404, "Run/Entry not found")

    if v.pending_status != "PENDING" or v.pending_value is None:
        return RedirectResponse(f"/runs/{run.id}/pending", status_code=302)

    old = v.value
    new = v.pending_value

    v.value = new
    v.pending_status = "APPROVED"
    session.add(v)

    session.add(InspectionValueAudit(
        inspection_value_id=v.id,
        action="APPROVED",
        old_value=old,
        new_value=new,
        by_user_id=user.id,
        by_user_name=user.display_name,
        note="",
    ))

    v.pending_value = None
    v.pending_status = ""
    v.pending_by_user_id = None
    v.pending_at = None
    session.add(v)

    session.commit()
    return RedirectResponse(f"/runs/{run.id}/pending", status_code=302)


@app.post("/values/{value_id}/reject")
def value_reject(value_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)
       

    v = session.get(InspectionValue, value_id)
    if not v:
        raise HTTPException(404, "Value not found")
    entry = session.get(InspectionEntry, v.entry_id)
    run = session.get(ProductionRun, entry.run_id) if entry else None
    if not entry or not run:
        raise HTTPException(404, "Run/Entry not found")

    if v.pending_status != "PENDING":
        return RedirectResponse(f"/runs/{run.id}/pending", status_code=302)

    session.add(InspectionValueAudit(
        inspection_value_id=v.id,
        action="REJECTED",
        old_value=v.value,
        new_value=v.pending_value,
        by_user_id=user.id,
        by_user_name=user.display_name,
        note="",
    ))

    v.pending_value = None
    v.pending_status = ""
    v.pending_by_user_id = None
    v.pending_at = None
    session.add(v)

    session.commit()
    return RedirectResponse(f"/runs/{run.id}/pending", status_code=302)
from openpyxl.cell.cell import MergedCell
import subprocess
import tempfile
from pathlib import Path
from fastapi.responses import Response


def _set_cell_safe(ws, addr: str, value, number_format: str | None = None):
    """
    Write value to Excel, even if the target address is part of a merged range.
    Also optionally apply number_format to the actual written cell.
    """
    target_addr = addr
    for rng in ws.merged_cells.ranges:
        if addr in rng:
            target_addr = rng.coord.split(":")[0]  # top-left
            break

    c = ws[target_addr]
    c.value = value
    if number_format:
        c.number_format = number_format



def _clone_sheet_no_drawings(wb, src_ws, title: str):
    """
    Clone a worksheet WITHOUT drawings/images (prevents crash).
    Preserves:
      - values
      - styles
      - merged cells
      - row/col dimensions
    """
    dst = wb.create_sheet(title=title)

    for col, dim in src_ws.column_dimensions.items():
        dst.column_dimensions[col].width = dim.width
    for row, dim in src_ws.row_dimensions.items():
        dst.row_dimensions[row].height = dim.height

    for merged_range in list(src_ws.merged_cells.ranges):
        dst.merge_cells(str(merged_range))

    for row in src_ws.iter_rows():
        for cell in row:
            new_cell = dst.cell(row=cell.row, column=cell.col_idx, value=cell.value)
            if cell.has_style:
                new_cell._style = cell._style
                new_cell.number_format = cell.number_format
                new_cell.font = cell.font
                new_cell.border = cell.border
                new_cell.fill = cell.fill
                new_cell.alignment = cell.alignment
                new_cell.protection = cell.protection

    return dst


def convert_xlsx_bytes_to_pdf_bytes(xlsx_bytes: bytes) -> bytes:
    """
    Uses LibreOffice headless to convert XLSX -> PDF.
    Requires 'soffice' available in the runtime image.
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        xlsx_path = tmpdir / "report.xlsx"
        out_dir = tmpdir / "out"
        out_dir.mkdir(parents=True, exist_ok=True)

        xlsx_path.write_bytes(xlsx_bytes)

        cmd = [
            "soffice",
            "--headless",
            "--nologo",
            "--nofirststartwizard",
            "--invisible",
            "--convert-to", "pdf",
            "--outdir", str(out_dir),
            str(xlsx_path),
        ]
        # capture output for debugging
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except FileNotFoundError:
            raise RuntimeError("PDF export failed: LibreOffice 'soffice' is not installed / not found on this server.")
        except subprocess.CalledProcessError as e:
            out = (e.stdout or b"").decode("utf-8", errors="ignore")
            err = (e.stderr or b"").decode("utf-8", errors="ignore")
            raise RuntimeError(
                "PDF export failed: LibreOffice conversion error.\n\n"
                f"CMD: {' '.join(cmd)}\n\n"
                f"STDOUT:\n{out}\n\n"
                f"STDERR:\n{err}\n"
            )

def build_one_day_workbook_bytes(run_id: int, day: date, session: Session) -> bytes:
    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    template_path = TEMPLATE_XLSX_MAP.get(run.process)
    if not template_path or not os.path.exists(template_path):
        raise HTTPException(404, f"Template not found: {template_path}")

    # Load the template fresh (images/logos stay)
    wb = openpyxl.load_workbook(template_path)
    ws = wb.worksheets[0]

    # ✅ Print setup (keep 1-page)
    apply_pdf_page_setup(ws)
    apply_specs_to_template(ws, run, session)
    

    # ✅ Per-process coordinates (THIS was missing and causing crashes)
    if run.process in ["LINER", "COVER"]:
        col_start = 5  # E
        date_row = 20
        time_row = 21
        inspector_row = 38
        op1_row = 39
        op2_row = 40
        row_map = ROW_MAP_LINER_COVER

        # Header cells (LINER/COVER)
        _set_cell_safe(ws, "E5", run.dhtp_batch_no)
        _set_cell_safe(ws, "I5", run.client_name)
        _set_cell_safe(ws, "I6", run.po_number)
        _set_cell_safe(ws, "E6", run.pipe_specification)
        _set_cell_safe(ws, "E7", run.raw_material_spec)
        _set_cell_safe(ws, "E9", run.itp_number)

    else:  # REINFORCEMENT
        col_start = 6   # F
        date_row = 19
        time_row = 20
        inspector_row = 35
        op1_row = 36
        op2_row = 37
        row_map = ROW_MAP_REINF

        # ✅ IMPORTANT:
        # Put ONLY the reinforcement header cells here (do NOT write E5/I5 again)
        # Use the real cells from your reinforcement.xlsx template
        _set_cell_safe(ws, "D4", run.dhtp_batch_no)
        _set_cell_safe(ws, "I4", run.client_name)
        _set_cell_safe(ws, "I5", run.po_number)
        _set_cell_safe(ws, "D5", run.pipe_specification)
        _set_cell_safe(ws, "D6", run.raw_material_spec)
        _set_cell_safe(ws, "D8", run.itp_number)

    # Machines Used
    machines = session.exec(select(RunMachine).where(RunMachine.run_id == run_id)).all()
    for idx in range(5):
        r = 4 + idx
        name = machines[idx].machine_name if idx < len(machines) else ""
        tag = machines[idx].machine_tag if (idx < len(machines) and machines[idx].machine_tag) else ""
        _set_cell_safe(ws, f"M{r}", name)
        _set_cell_safe(ws, f"P{r}", tag)
    
    # ✅ Date row (IMPORTANT: format as DATE so it doesn't show 12:00 AM)
    for slot_idx, slot in enumerate(SLOTS):
        col = openpyxl.utils.get_column_letter(col_start + slot_idx)
        _set_cell_safe(ws, f"{col}{date_row}", day, number_format="m/d/yyyy")
    
    # ✅ Time row (format as time)
    for slot_idx, slot in enumerate(SLOTS):
        col = openpyxl.utils.get_column_letter(col_start + slot_idx)
        hh, mm = slot.split(":")
        _set_cell_safe(ws, f"{col}{time_row}", dtime(int(hh), int(mm)), number_format="h:mm")


    # Trace for THIS day: raw batch + tools
    trace_today = get_day_latest_trace(session, run_id, day)
    carry = get_last_known_trace_before_day(session, run_id, day)

    raw_batches = trace_today["raw_batches"] or ([carry["raw"]] if carry["raw"] else [])
    raw_str = ", ".join([x for x in raw_batches if x])
    if raw_str:
        if run.process in ["LINER", "COVER"]:
            _set_cell_safe(ws, "E8", raw_str)   # LINER/COVER: Raw Material Batch No.
        else:
            _set_cell_safe(ws, "D7", raw_str)   # REINFORCEMENT (keep as-is)


    tools = trace_today["tools"] or carry["tools"]
    for t_idx in range(2):
        r = 8 + t_idx
        if t_idx < len(tools):
            name, serial, calib = tools[t_idx]
            _set_cell_safe(ws, f"G{r}", name or "")
            _set_cell_safe(ws, f"I{r}", serial or "")
            _set_cell_safe(ws, f"K{r}", calib or "")

    apply_specs_to_template(ws, run, session)

    # Fill inspector/operators per slot + values
    day_entries = session.exec(
        select(InspectionEntry)
        .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date == day)
        .order_by(InspectionEntry.created_at)
    ).all()

    user_map = {u.id: u for u in session.exec(select(User)).all()}

    for e in day_entries:
        if e.slot_time not in SLOTS:
            continue
        slot_idx = SLOTS.index(e.slot_time)
        col = openpyxl.utils.get_column_letter(col_start + slot_idx)

        inspector_name = user_map.get(e.inspector_id).display_name if e.inspector_id in user_map else ""
        _set_cell_safe(ws, f"{col}{inspector_row}", inspector_name)

        if run.process in ["LINER", "COVER"]:
            _set_cell_safe(ws, f"{col}{op1_row}", e.operator_1 or "")
            _set_cell_safe(ws, f"{col}{op2_row}", e.operator_2 or "")
        else:
            _set_cell_safe(ws, f"{col}{op1_row}", e.operator_annular_12 or "")
            _set_cell_safe(ws, f"{col}{op2_row}", e.operator_int_ext_34 or "")

        vals = session.exec(select(InspectionValue).where(InspectionValue.entry_id == e.id)).all()
        for v in vals:
            r = row_map.get(v.param_key)
            if not r:
                continue
            _set_cell_safe(ws, f"{col}{r}", v.value)

    out = BytesIO()
    wb.save(out)
    out.seek(0)
    return out.getvalue()


from pypdf import PdfReader, PdfWriter
from pypdf import Transformation
from io import BytesIO
import os

def stamp_background_pdf(data_pdf_bytes: bytes, background_pdf_path: str, y_shift_pts: float = -50) -> bytes:
    """
    Stamp background UNDER the page, then place the data on top,
    but shift the data DOWN by y_shift_pts points.

    y_shift_pts:
      -18 means move down ~6.35mm (good starting point)
      try -12, -18, -24 until perfect.
    """
    if not background_pdf_path or not os.path.exists(background_pdf_path):
        return data_pdf_bytes

    data_reader = PdfReader(BytesIO(data_pdf_bytes))
    bg_reader = PdfReader(background_pdf_path)

    writer = PdfWriter()
    bg_pages = bg_reader.pages
    bg_count = len(bg_pages)

    for i, data_page in enumerate(data_reader.pages):
        bg_page = bg_pages[i] if (bg_count > 1 and i < bg_count) else bg_pages[0]

        w = float(data_page.mediabox.width)
        h = float(data_page.mediabox.height)
        new_page = writer.add_blank_page(width=w, height=h)

        # Background first
        new_page.merge_page(bg_page)

        # Data second, shifted DOWN
        t = Transformation().translate(tx=0, ty=y_shift_pts)
        new_page.merge_transformed_page(data_page, t)

    out = BytesIO()
    writer.write(out)
    out.seek(0)
    return out.getvalue()





def build_export_xlsx_bytes(run_id: int, request: Request, session: Session) -> tuple[bytes, str]:
    """
    Build the XLSX export in memory and return (bytes, filename_base).
    This is used by BOTH /export/xlsx and /export/pdf.
    """
    user = get_current_user(request, session)

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    template_path = TEMPLATE_XLSX_MAP.get(run.process)
    if not template_path or not os.path.exists(template_path):
        raise HTTPException(404, f"Template not found: {template_path}")

    days = get_days_for_run(session, run_id)
    if not days:
        raise HTTPException(400, "No entries to export")

    base_wb = openpyxl.load_workbook(template_path)
    base_ws = base_wb.worksheets[0]

    # per process coordinates
    if run.process in ["LINER", "COVER"]:
        col_start = 5  # E
        date_row = 20
        inspector_row = 38
        op1_row = 39
        op2_row = 40
        row_map = ROW_MAP_LINER_COVER
    else:
        col_start = 6  # F
        date_row = 20
        inspector_row = 36
        op1_row = 37
        op2_row = 38
        row_map = ROW_MAP_REINF

    machines = session.exec(select(RunMachine).where(RunMachine.run_id == run_id)).all()
    user_map = {u.id: u for u in session.exec(select(User)).all()}

    for i, day in enumerate(days):
        title = f"Day {i+1} ({day.isoformat()})"

        if i == 0:
            ws = base_ws
            ws.title = title
        else:
            ws = _clone_sheet_no_drawings(base_wb, base_ws, title)

        # ----- HEADER (fixed cells) -----
        _set_cell_safe(ws, "E5", run.dhtp_batch_no)      # Batch
        _set_cell_safe(ws, "I5", run.client_name)        # Client
        _set_cell_safe(ws, "I6", run.po_number)          # PO
        _set_cell_safe(ws, "E6", run.pipe_specification) # Pipe Spec
        _set_cell_safe(ws, "E7", run.raw_material_spec)  # Raw Spec
        _set_cell_safe(ws, "E9", run.itp_number)         # ITP

        # ----- Machines (M4:P8) -----
        for idx in range(5):
            r = 4 + idx
            name = machines[idx].machine_name if idx < len(machines) else ""
            tag = machines[idx].machine_tag if (idx < len(machines) and machines[idx].machine_tag) else ""
            _set_cell_safe(ws, f"M{r}", name)
            _set_cell_safe(ws, f"P{r}", tag)

        # ----- Day trace (raw batch + tools) -----
        trace_today = get_day_latest_trace(session, run_id, day)
        carry = get_last_known_trace_before_day(session, run_id, day)

        raw_batches = trace_today["raw_batches"] or ([carry["raw"]] if carry["raw"] else [])
        raw_str = ", ".join([x for x in raw_batches if x])
        if raw_str:
            if run.process in ["LINER", "COVER"]:
                _set_cell_safe(ws, "E8", raw_str)
            else:
                _set_cell_safe(ws, "D7", raw_str)


        tools = trace_today["tools"] or carry["tools"]
        for t_idx in range(2):
            r = 8 + t_idx
            if t_idx < len(tools):
                name, serial, calib = tools[t_idx]
                _set_cell_safe(ws, f"G{r}", name or "")
                _set_cell_safe(ws, f"I{r}", serial or "")
                _set_cell_safe(ws, f"K{r}", calib or "")

        # ----- Date row for each time slot -----
        for slot_idx, slot in enumerate(SLOTS):
            col = openpyxl.utils.get_column_letter(col_start + slot_idx)
            _set_cell_safe(ws, f"{col}{date_row}", day)

        # ----- Time header row (optional) -----
        time_row = 21
        for slot_idx, slot in enumerate(SLOTS):
            col = openpyxl.utils.get_column_letter(col_start + slot_idx)
            hh, mm = slot.split(":")
            cell_addr = f"{col}{time_row}"
            for rng in ws.merged_cells.ranges:
                if cell_addr in rng:
                    cell_addr = rng.coord.split(":")[0]
                    break
            ws[cell_addr].value = dtime(int(hh), int(mm))
            ws[cell_addr].number_format = "h:mm"

        # ✅ print setup (important for PDF export)
        apply_pdf_page_setup(ws)
        

        
        # ----- Fill per-slot inspector/operators + values -----
        day_entries = session.exec(
            select(InspectionEntry)
            .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date == day)
            .order_by(InspectionEntry.created_at)
        ).all()

        for e in day_entries:
            if e.slot_time not in SLOTS:
                continue

            slot_idx = SLOTS.index(e.slot_time)
            col = openpyxl.utils.get_column_letter(col_start + slot_idx)

            inspector_name = user_map.get(e.inspector_id).display_name if e.inspector_id in user_map else ""
            _set_cell_safe(ws, f"{col}{inspector_row}", inspector_name)

            if run.process in ["LINER", "COVER"]:
                _set_cell_safe(ws, f"{col}{op1_row}", e.operator_1 or "")
                _set_cell_safe(ws, f"{col}{op2_row}", e.operator_2 or "")
            else:
                _set_cell_safe(ws, f"{col}{op1_row}", e.operator_annular_12 or "")
                _set_cell_safe(ws, f"{col}{op2_row}", e.operator_int_ext_34 or "")

            vals = session.exec(select(InspectionValue).where(InspectionValue.entry_id == e.id)).all()
            for v in vals:
                r = row_map.get(v.param_key)
                if not r:
                    continue
                _set_cell_safe(ws, f"{col}{r}", v.value)

    out = BytesIO()
    base_wb.save(out)
    out.seek(0)

    filename_base = f"{run.process}_{run.dhtp_batch_no}_ALL_DAYS"
    return out.getvalue(), filename_base
    

# =========================
# MRR EXPORT (per MaterialLot)
# =========================

def build_mrr_xlsx_bytes(lot_id: int, session: Session, template_kind: str = "RAW") -> bytes:
    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "Lot not found")

    template_path = MRR_TEMPLATE_XLSX_MAP.get(template_kind)
    if not template_path or not os.path.exists(template_path):
        raise HTTPException(404, f"MRR template not found: {template_path}")

    wb = openpyxl.load_workbook(template_path)
    ws = wb.worksheets[0]

    # IMPORTANT: You MUST adjust these cell addresses to match your MRR template
    # (These are EXAMPLES so export works immediately and you can change them later.)
    _set_cell_safe(ws, "C6", lot.batch_no or "")
    _set_cell_safe(ws, "C7", lot.material_name or "")
    _set_cell_safe(ws, "C8", lot.supplier_name or "")
    _set_cell_safe(ws, "C9", lot.status or "")
    _set_cell_safe(ws, "C10", (lot.created_at.date().isoformat() if getattr(lot, "created_at", None) else ""))

    apply_pdf_page_setup(ws)

    out = BytesIO()
    wb.save(out)
    out.seek(0)
    return out.getvalue()


@app.get("/mrr/{lot_id}/export/xlsx")
def mrr_export_xlsx(lot_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)

    # for now default RAW (later we add a field)
    template_kind = "RAW"

    xlsx_bytes = build_mrr_xlsx_bytes(lot_id, session, template_kind=template_kind)
    filename = f"MRR_{template_kind}_{lot_id}.xlsx"

    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/mrr/{lot_id}/export/pdf")
def mrr_export_pdf(lot_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)

    template_kind = "RAW"

    # 1) Build XLSX
    xlsx_bytes = build_mrr_xlsx_bytes(lot_id, session, template_kind=template_kind)

    # 2) Convert to PDF
    pdf_bytes = convert_xlsx_bytes_to_pdf_bytes(xlsx_bytes)

    # 3) Stamp MRR background (NOT the production PAPER_BG_MAP)
    bg_path = MRR_PAPER_BG_MAP.get(template_kind, "")
    if bg_path:
        pdf_bytes = stamp_background_pdf(pdf_bytes, bg_path, y_shift_pts=-30)

    filename = f"MRR_{template_kind}_{lot_id}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/runs/{run_id}/export/xlsx")
def export_xlsx(run_id: int, request: Request, session: Session = Depends(get_session)):
    xlsx_bytes, filename_base = build_export_xlsx_bytes(run_id, request, session)

    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename_base}.xlsx"'},
    )




@app.get("/runs/{run_id}/export/pdf")
def export_pdf(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    days = get_days_for_run(session, run_id)
    if not days:
        raise HTTPException(400, "No entries to export")

    writer = PdfWriter()

    # ✅ pick correct paper background by process
    background_path = PAPER_BG_MAP.get(run.process, "")

    for day in days:
        # 1) Build 1-day excel
        xlsx_bytes = build_one_day_workbook_bytes(run_id, day, session)

        # 2) Convert to PDF
        pdf_bytes = convert_xlsx_bytes_to_pdf_bytes(xlsx_bytes)

        # ✅ 3) Stamp background UNDER the page
        if background_path:
            pdf_bytes = stamp_background_pdf(pdf_bytes, background_path)

        # 4) Merge into final output
        reader = PdfReader(BytesIO(pdf_bytes))
        for page in reader.pages:
            writer.add_page(page)

    out = BytesIO()
    writer.write(out)
    out.seek(0)

    filename = f"{run.process}_{run.dhtp_batch_no}_ALL_DAYS.pdf"
    return Response(
        content=out.getvalue(),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )



def apply_pdf_page_setup(ws):
    # A4 Portrait
    ws.page_setup.orientation = ws.ORIENTATION_PORTRAIT
    ws.page_setup.paperSize = ws.PAPERSIZE_A4

    # ✅ Back to "everything in 1 page"
    ws.page_setup.fitToWidth = 1
    ws.page_setup.fitToHeight = 1
    ws.sheet_properties.pageSetUpPr.fitToPage = True

    # Margins (keep as before)
    ws.page_margins.left = 0.25
    ws.page_margins.right = 0.25
    ws.page_margins.top = 0.35
    ws.page_margins.bottom = 0.70









