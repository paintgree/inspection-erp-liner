from __future__ import annotations

import os
from datetime import datetime, date, time as dtime
from io import BytesIO
from typing import Dict, List, Optional, Tuple

import openpyxl
from fastapi import FastAPI, Depends, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlmodel import Session, select
from fastapi.responses import Response
from pypdf import PdfWriter, PdfReader, Transformation
import subprocess
import tempfile
from pathlib import Path


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
)

app = FastAPI()

BASE_DIR = os.path.dirname(__file__)
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

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


SLOTS = ["00:00","02:00","04:00","06:00","08:00","10:00","12:00","14:00","16:00","18:00","20:00","22:00"]

LINER_COVER_PARAMS = [
    ("length_m", "Length (Mtr)", "m"),
    ("od_mm", "OD (mm)", "mm"),
    ("wall_thickness_mm", "Wall Thickness (mm)", "mm"),
    ("cooling_water_c", "Cooling Water (°C)", "°C"),
    ("line_speed_m_min", "Line Speed (m/min)", "m/min"),
    ("tractor_pressure_mpa", "Tractor Pressure (MPa)", "MPa"),
    ("body_temp_zone_1_c", "Body Temp Zone 1 (°C)", "°C"),
    ("body_temp_zone_2_c", "Body Temp Zone 2 (°C)", "°C"),
    ("body_temp_zone_3_c", "Body Temp Zone 3 (°C)", "°C"),
    ("body_temp_zone_4_c", "Body Temp Zone 4 (°C)", "°C"),
    ("body_temp_zone_5_c", "Body Temp Zone 5 (°C)", "°C"),
    ("noising_temp_zone_1_c", "Noising Temp Zone 1 (°C)", "°C"),
    ("noising_temp_zone_2_c", "Noising Temp Zone 2 (°C)", "°C"),
    ("noising_temp_zone_3_c", "Noising Temp Zone 3 (°C)", "°C"),
    ("noising_temp_zone_4_c", "Noising Temp Zone 4 (°C)", "°C"),
    ("noising_temp_zone_5_c", "Noising Temp Zone 5 (°C)", "°C"),
]

REINF_PARAMS = [
    ("length_m", "Length (Mtr)", "m"),
    ("annular_od_70_1", "Annular OD (mm) (∠ 70°) #1", "mm"),
    ("annular_od_70_2", "Annular OD (mm) (∠ 70°) #2", "mm"),
    ("annular_od_45_3", "Annular OD (mm) (∠ 45°) #3", "mm"),
    ("annular_od_45_4", "Annular OD (mm) (∠ 45°) #4", "mm"),
    ("core_mould_dia_mm", "Core Mould Dia. (mm)", "mm"),
    ("annular_width_1_mm", "Annular Width (mm) #1", "mm"),
    ("annular_width_2_mm", "Annular Width (mm) #2", "mm"),
    ("screw_yarn_width_1_mm", "Screw Yarn Width (mm) #1", "mm"),
    ("screw_yarn_width_2_mm", "Screw Yarn Width (mm) #2", "mm"),
    ("tractor_speed_m_min", "Tractor Speed (m/min)", "m/min"),
    ("clamping_gas_p1_mpa", "Clamping Gas Pressure (MPa) #1", "MPa"),
    ("clamping_gas_p2_mpa", "Clamping Gas Pressure (MPa) #2", "MPa"),
    ("thrust_gas_p_mpa", "Thrust Gas Pressure (MPa)", "MPa"),
]

PROCESS_PARAMS = {"LINER": LINER_COVER_PARAMS, "COVER": LINER_COVER_PARAMS, "REINFORCEMENT": REINF_PARAMS}

ROW_MAP_LINER_COVER = {
    "length_m": 22, "od_mm": 23, "wall_thickness_mm": 24, "cooling_water_c": 25,
    "line_speed_m_min": 26, "tractor_pressure_mpa": 27, "body_temp_zone_1_c": 28,
    "body_temp_zone_2_c": 29, "body_temp_zone_3_c": 30, "body_temp_zone_4_c": 31,
    "body_temp_zone_5_c": 32, "noising_temp_zone_1_c": 33, "noising_temp_zone_2_c": 34,
    "noising_temp_zone_3_c": 35, "noising_temp_zone_4_c": 36, "noising_temp_zone_5_c": 37,
}

ROW_MAP_REINF = {
    "length_m": 21, "annular_od_70_1": 22, "annular_od_70_2": 23, "annular_od_45_3": 24,
    "annular_od_45_4": 25, "core_mould_dia_mm": 26, "annular_width_1_mm": 27,
    "annular_width_2_mm": 28, "screw_yarn_width_1_mm": 29, "screw_yarn_width_2_mm": 30,
    "tractor_speed_m_min": 31, "clamping_gas_p1_mpa": 32, "clamping_gas_p2_mpa": 33,
    "thrust_gas_p_mpa": 34,
}

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


def slot_from_time_str(t: str) -> str:
    """
    HARD RULE (2-hour slots):
    - HH:00 .. HH+1:30  -> HH:00
    - HH+1:31 .. HH+2:00 -> HH+2:00
    Example:
      02:00–03:30 -> 02:00
      03:31–04:00 -> 04:00
      07:00 -> 06:00
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

    # clamp to valid range 00:00..22:00
    if slot_min < 0:
        slot_min = 0
    if slot_min > 22 * 60:
        slot_min = 22 * 60

    return f"{slot_min // 60:02d}:00"


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

    raw_batches = [e.raw_material_batch_no for e in entries if e.raw_material_batch_no]

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
    entries = session.exec(
        select(InspectionEntry)
        .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date < day)
        .order_by(InspectionEntry.actual_date, InspectionEntry.created_at)
    ).all()

    raw = ""
    tool1 = (None, None, None)
    tool2 = (None, None, None)

    for e in entries:
        if e.raw_material_batch_no:
            raw = e.raw_material_batch_no
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
    runs = session.exec(select(ProductionRun).order_by(ProductionRun.created_at.desc())).all()

    grouped: Dict[str, List[ProductionRun]] = {}
    for r in runs:
        grouped.setdefault(r.dhtp_batch_no, []).append(r)

    progress_map = {r.id: get_progress_percent(session, r) for r in runs}

    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "user": user, "grouped": grouped, "progress_map": progress_map},
    )


@app.get("/runs/new", response_class=HTMLResponse)
def run_new_get(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    if (user.role or "").upper() not in ["MANAGER", "RUN_CREATOR"]:
        raise HTTPException(403, "Manager only")

    return templates.TemplateResponse("run_new.html", {"request": request, "user": user, "error": ""})


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


    # --- NEW: inspector name per slot ---
    users = session.exec(select(User)).all()
    user_map = {u.id: u for u in users}
    slot_inspectors: Dict[str, str] = {s: "" for s in SLOTS}

    
    grid: Dict[str, Dict[str, dict]] = {s: {} for s in SLOTS}
    for e in entries:
        slot_inspectors[e.slot_time] = user_map.get(e.inspector_id).display_name if e.inspector_id in user_map else ""

        vals = session.exec(select(InspectionValue).where(InspectionValue.entry_id == e.id)).all()
        for v in vals:
            grid.setdefault(e.slot_time, {})[v.param_key] = {
                "value_id": v.id,
                "value": v.value,
                "out": bool(v.is_out_of_spec),
                "note": v.spec_note or "",
                "pending_value": v.pending_value,
                "pending_status": v.pending_status or "",
            }
    # ✅ Daily Trace (Selected Day): raw batch + tools
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
            "image_url": IMAGE_MAP.get(run.process, ""),
            "progress": progress,
            "raw_batches": raw_batches,
            "tools": tools,

        },
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

    return templates.TemplateResponse(
        "entry_new.html",
        {"request": request, "user": user, "run": run, "params": params, "has_any": has_any, "error": error},
    )


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
    raw_material_batch_no: str = Form(""),
    tool1_name: str = Form(""),
    tool1_serial: str = Form(""),
    tool1_calib_due: str = Form(""),
    tool2_name: str = Form(""),
    tool2_serial: str = Form(""),
    tool2_calib_due: str = Form(""),
):
    user = get_current_user(request, session)
    run = session.get(ProductionRun, run_id)
    forbid_boss(user)

    if not run:
        raise HTTPException(404, "Run not found")

    if run.status in ["CLOSED", "APPROVED"] and user.role != "MANAGER":
        raise HTTPException(403, "Run is not open")

    slot_time = slot_from_time_str(actual_time)
    day_obj = date.fromisoformat(actual_date)

    existing_for_slot = session.exec(
        select(InspectionEntry)
        .where(
            InspectionEntry.run_id == run_id,
            InspectionEntry.actual_date == day_obj,
            InspectionEntry.slot_time == slot_time
        )
    ).first()

    if existing_for_slot:
        msg = "This timing slot is already inspected. Please confirm the time, or use Edit to change the existing record."
        return RedirectResponse(f"/runs/{run_id}/entry/new?error={msg}", status_code=302)

    form = await request.form()

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
        raw_material_batch_no=raw_material_batch_no,
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
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        pdfs = list(out_dir.glob("*.pdf"))
        if not pdfs:
            raise RuntimeError("PDF conversion failed: no output produced")
        return pdfs[0].read_bytes()

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
        _set_cell_safe(ws, "D7", raw_str)  # if reinforcement differs, change this cell too

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
            _set_cell_safe(ws, "E8", raw_str)

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



















































