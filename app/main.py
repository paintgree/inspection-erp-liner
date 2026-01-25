from __future__ import annotations

import os
from datetime import date, time as dtime
from io import BytesIO
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, Depends, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from sqlmodel import Session, select

from .db import create_db_and_tables, get_session
from .models import (
    User, ProductionRun, RunMachine, RunParameter,
    InspectionEntry, InspectionValue
)
from .auth import hash_password, verify_password

import openpyxl


app = FastAPI()

BASE_DIR = os.path.dirname(__file__)
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")


# -----------------------------
# CONFIG: templates + images
# -----------------------------
IMAGE_MAP = {
    "LINER": "/static/images/liner.png",
    "REINFORCEMENT": "/static/images/reinforcement.png",
    "COVER": "/static/images/cover.png",
}

TEMPLATE_XLSX_MAP = {
    "LINER": os.path.join("app", "templates", "templates_xlsx", "liner.xlsx"),
    "REINFORCEMENT": os.path.join("app", "templates", "templates_xlsx", "reinforcement.xlsx"),
    "COVER": os.path.join("app", "templates", "templates_xlsx", "cover.xlsx"),
}

SLOTS = ["00:00","02:00","04:00","06:00","08:00","10:00","12:00","14:00","16:00","18:00","20:00","22:00"]


# -----------------------------
# PARAM DEFINITIONS
# -----------------------------
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

PROCESS_PARAMS = {
    "LINER": LINER_COVER_PARAMS,
    "COVER": LINER_COVER_PARAMS,
    "REINFORCEMENT": REINF_PARAMS,
}


# -----------------------------
# EXPORT ROW MAPPING
# -----------------------------
ROW_MAP_LINER_COVER = {
    "length_m": 22,
    "od_mm": 23,
    "wall_thickness_mm": 24,
    "cooling_water_c": 25,
    "line_speed_m_min": 26,
    "tractor_pressure_mpa": 27,
    "body_temp_zone_1_c": 28,
    "body_temp_zone_2_c": 29,
    "body_temp_zone_3_c": 30,
    "body_temp_zone_4_c": 31,
    "body_temp_zone_5_c": 32,
    "noising_temp_zone_1_c": 33,
    "noising_temp_zone_2_c": 34,
    "noising_temp_zone_3_c": 35,
    "noising_temp_zone_4_c": 36,
    "noising_temp_zone_5_c": 37,
}

ROW_MAP_REINF = {
    "length_m": 22,
    "annular_od_70_1": 23,
    "annular_od_70_2": 24,
    "annular_od_45_3": 25,
    "annular_od_45_4": 26,
    "core_mould_dia_mm": 27,
    "annular_width_1_mm": 28,
    "annular_width_2_mm": 29,
    "screw_yarn_width_1_mm": 30,
    "screw_yarn_width_2_mm": 31,
    "tractor_speed_m_min": 32,
    "clamping_gas_p1_mpa": 33,
    "clamping_gas_p2_mpa": 34,
    "thrust_gas_p_mpa": 35,
}


# -----------------------------
# STARTUP
# -----------------------------
@app.on_event("startup")
def on_startup():
    create_db_and_tables()
    ensure_default_users()


# -----------------------------
# USERS / AUTH
# -----------------------------
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
        session.commit()


def get_current_user(request: Request, session: Session) -> User:
    username = request.cookies.get("user")
    if not username:
        raise HTTPException(401, "Not logged in")
    user = session.exec(select(User).where(User.username == username)).first()
    if not user:
        raise HTTPException(401, "Invalid user")
    return user


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return RedirectResponse("/dashboard", status_code=302)


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


# -----------------------------
# HELPERS
# -----------------------------
def get_days_for_run(session: Session, run_id: int) -> List[date]:
    days = session.exec(
        select(InspectionEntry.actual_date)
        .where(InspectionEntry.run_id == run_id)
        .distinct()
        .order_by(InspectionEntry.actual_date)
    ).all()
    return list(days)


def slot_from_time_str(t: str) -> str:
    hh = int(t.split(":")[0])
    slot_h = (hh // 2) * 2
    return f"{slot_h:02d}:00"


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

    raw_batches = []
    tools = []
    if entries:
        raw_batches = [e.raw_material_batch_no for e in entries if e.raw_material_batch_no]
        last = entries[-1]
        tools = [
            (last.tool1_name, last.tool1_serial, last.tool1_calib_due),
            (last.tool2_name, last.tool2_serial, last.tool2_calib_due),
        ]

    return {"entries": entries, "raw_batches": raw_batches, "tools": tools}


def get_last_known_trace_before_day(session: Session, run_id: int, day: date) -> dict:
    entries = session.exec(
        select(InspectionEntry)
        .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date < day)
        .order_by(InspectionEntry.actual_date, InspectionEntry.created_at)
    ).all()
    if not entries:
        return {"raw": "", "tools": []}
    last = entries[-1]
    return {
        "raw": last.raw_material_batch_no or "",
        "tools": [
            (last.tool1_name, last.tool1_serial, last.tool1_calib_due),
            (last.tool2_name, last.tool2_serial, last.tool2_calib_due),
        ],
    }


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


def apply_spec_check(param: RunParameter, value: Optional[float]) -> Tuple[bool, str]:
    if value is None:
        return False, ""
    rule = (param.rule or "").upper()
    mn = param.min_value
    mx = param.max_value

    if rule == "RANGE":
        if mn is not None and value < mn:
            return True, f"Below min {mn}"
        if mx is not None and value > mx:
            return True, f"Above max {mx}"
        return False, ""
    if rule == "MAX_ONLY":
        if mx is not None and value > mx:
            return True, f"Above max {mx}"
        return False, ""
    if rule == "MIN_ONLY":
        if mn is not None and value < mn:
            return True, f"Below min {mn}"
        return False, ""
    return False, ""


# -----------------------------
# DASHBOARD + RUNS
# -----------------------------
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
    if user.role != "MANAGER":
        raise HTTPException(403, "Manager only")

    return templates.TemplateResponse(
        "run_new.html",
        {"request": request, "user": user, "error": "", "param_defs": [], "process": ""},
    )


@app.get("/runs/new/{process}", response_class=HTMLResponse)
def run_new_get_process(process: str, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    if user.role != "MANAGER":
        raise HTTPException(403, "Manager only")

    p = process.upper().strip()
    if p not in PROCESS_PARAMS:
        raise HTTPException(404, "Invalid process")

    return templates.TemplateResponse(
        "run_new.html",
        {"request": request, "user": user, "error": "", "param_defs": PROCESS_PARAMS[p], "process": p},
    )


@app.post("/runs/new")
async def run_new_post(
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
):
    user = get_current_user(request, session)
    if user.role != "MANAGER":
        raise HTTPException(403, "Manager only")

    p = process.upper().strip()
    if p not in PROCESS_PARAMS:
        return templates.TemplateResponse(
            "run_new.html",
            {"request": request, "user": user, "error": "Invalid process", "param_defs": [], "process": ""},
        )

    form = await request.form()

    run = ProductionRun(
        process=p,
        dhtp_batch_no=dhtp_batch_no,
        client_name=client_name,
        po_number=po_number,
        itp_number=itp_number,
        pipe_specification=pipe_specification,
        raw_material_spec=raw_material_spec,
        total_length_m=total_length_m or 0.0,
        status="OPEN",
    )
    session.add(run)
    session.commit()
    session.refresh(run)

    # ✅ machines at creation (machine1_name..machine5_name)
    for i in range(1, 6):
        nm = (form.get(f"machine{i}_name") or "").strip()
        tg = (form.get(f"machine{i}_tag") or "").strip()
        if nm:
            session.add(RunMachine(run_id=run.id, machine_name=nm, machine_tag=tg))
    session.commit()

    # ✅ parameters + ranges (rule_key/min_key/max_key)
    for idx, (key, label, unit) in enumerate(PROCESS_PARAMS[p]):
        rp = RunParameter(run_id=run.id, param_key=key, label=label, unit=unit, display_order=idx)

        rule = (form.get(f"rule_{key}") or "").strip().upper()
        if rule in ["RANGE", "MIN_ONLY", "MAX_ONLY"]:
            rp.rule = rule

        rp.min_value = _safe_float(form.get(f"min_{key}"))
        rp.max_value = _safe_float(form.get(f"max_{key}"))

        session.add(rp)

    session.commit()
    return RedirectResponse("/dashboard", status_code=302)


@app.get("/runs/{run_id}/edit", response_class=HTMLResponse)
def run_edit_get(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    if user.role != "MANAGER":
        raise HTTPException(403, "Manager only")

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
async def run_edit_post(
    run_id: int,
    request: Request,
    session: Session = Depends(get_session),
    client_name: str = Form(...),
    po_number: str = Form(...),
    itp_number: str = Form(...),
    pipe_specification: str = Form(...),
    raw_material_spec: str = Form(...),
    total_length_m: float = Form(0.0),
):
    user = get_current_user(request, session)
    if user.role != "MANAGER":
        raise HTTPException(403, "Manager only")

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    form = await request.form()

    run.client_name = client_name
    run.po_number = po_number
    run.itp_number = itp_number
    run.pipe_specification = pipe_specification
    run.raw_material_spec = raw_material_spec
    run.total_length_m = total_length_m or 0.0
    session.add(run)
    session.commit()

    # update machines: replace list
    existing = session.exec(select(RunMachine).where(RunMachine.run_id == run_id)).all()
    for m in existing:
        session.delete(m)
    session.commit()

    for i in range(1, 6):
        nm = (form.get(f"machine{i}_name") or "").strip()
        tg = (form.get(f"machine{i}_tag") or "").strip()
        if nm:
            session.add(RunMachine(run_id=run_id, machine_name=nm, machine_tag=tg))
    session.commit()

    # update parameter ranges
    params = session.exec(select(RunParameter).where(RunParameter.run_id == run_id)).all()
    for p in params:
        rule = (form.get(f"rule_{p.param_key}") or "").strip().upper()
        if rule in ["RANGE", "MIN_ONLY", "MAX_ONLY"]:
            p.rule = rule
        p.min_value = _safe_float(form.get(f"min_{p.param_key}"))
        p.max_value = _safe_float(form.get(f"max_{p.param_key}"))
        session.add(p)
    session.commit()

    return RedirectResponse(f"/runs/{run.id}", status_code=302)


# -----------------------------
# CLOSE / APPROVE / REOPEN (enforced)
# -----------------------------
@app.post("/runs/{run_id}/close")
def run_close(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    if user.role != "MANAGER":
        raise HTTPException(403, "Manager only")

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    if run.status != "OPEN":
        return RedirectResponse(f"/runs/{run_id}", status_code=302)

    run.status = "CLOSED"
    session.add(run)
    session.commit()
    return RedirectResponse(f"/runs/{run_id}", status_code=302)


@app.post("/runs/{run_id}/approve")
def run_approve(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    if user.role != "MANAGER":
        raise HTTPException(403, "Manager only")

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    if run.status != "CLOSED":
        return RedirectResponse(f"/runs/{run_id}", status_code=302)

    run.status = "APPROVED"
    session.add(run)
    session.commit()
    return RedirectResponse(f"/runs/{run_id}", status_code=302)


@app.post("/runs/{run_id}/reopen")
def run_reopen(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    if user.role != "MANAGER":
        raise HTTPException(403, "Manager only")

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    if run.status not in ["CLOSED", "APPROVED"]:
        return RedirectResponse(f"/runs/{run_id}", status_code=302)

    run.status = "OPEN"
    session.add(run)
    session.commit()
    return RedirectResponse(f"/runs/{run_id}", status_code=302)


# -----------------------------
# RUN VIEW
# -----------------------------
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

    # grid[slot][param_key] = {"value":..., "out":..., "note":..., "pending_value":..., "pending_status":...}
    grid: Dict[str, Dict[str, dict]] = {s: {} for s in SLOTS}

    for e in entries:
        vals = session.exec(select(InspectionValue).where(InspectionValue.entry_id == e.id)).all()
        for v in vals:
            grid.setdefault(e.slot_time, {})[v.param_key] = {
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
            "image_url": IMAGE_MAP.get(run.process, ""),
            "raw_batches": raw_batches,
            "tools": tools,
            "progress": progress,
        },
    )


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

    return templates.TemplateResponse(
        "entry_new.html",
        {"request": request, "user": user, "run": run, "params": params, "has_any": has_any},
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
    if not run:
        raise HTTPException(404, "Run not found")

    # block inspector when closed/approved
    if run.status in ["CLOSED", "APPROVED"] and user.role != "MANAGER":
        raise HTTPException(403, "Run is not open")

    slot_time = slot_from_time_str(actual_time)
    form = await request.form()

    entry = InspectionEntry(
        run_id=run_id,
        actual_date=date.fromisoformat(actual_date),
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

    # ✅ accept BOTH input names: v_key OR param_key
    for key, param in by_key.items():
        raw = form.get(f"v_{key}")
        if raw in [None, ""]:
            raw = form.get(f"param_{key}")
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


# -----------------------------
# EXPORT
# -----------------------------
@app.get("/runs/{run_id}/export/xlsx")
def export_xlsx(run_id: int, request: Request, session: Session = Depends(get_session)):
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

    wb = openpyxl.load_workbook(template_path)
    base_ws = wb.worksheets[0]

    for i, day in enumerate(days):
        if i == 0:
            ws = base_ws
            ws.title = f"Day {i+1} ({day.isoformat()})"
        else:
            ws = wb.copy_worksheet(base_ws)
            ws.title = f"Day {i+1} ({day.isoformat()})"

        # header
        ws["D5"].value = run.dhtp_batch_no
        ws["I5"].value = run.client_name
        ws["I6"].value = run.po_number
        ws["D7"].value = run.raw_material_spec
        ws["D9"].value = run.itp_number

        # column rules
        if run.process in ["LINER", "COVER"]:
            col_start = 5  # E
            date_row = 20
            time_row = 21
            inspector_row = 38
            op1_row = 39
            op2_row = 40
            row_map = ROW_MAP_LINER_COVER
        else:
            col_start = 6  # F
            date_row = 20
            time_row = 21
            inspector_row = 36
            op1_row = 37
            op2_row = 38
            row_map = ROW_MAP_REINF

        # ✅ DATE in every slot column
        for idx in range(len(SLOTS)):
            col = openpyxl.utils.get_column_letter(col_start + idx)
            ws[f"{col}{date_row}"].value = day
            ws[f"{col}{date_row}"].number_format = "mm-dd-yy"

        # ✅ TIME in every slot column
        for idx, slot in enumerate(SLOTS):
            col = openpyxl.utils.get_column_letter(col_start + idx)
            hh, mm = slot.split(":")
            ws[f"{col}{time_row}"].value = dtime(int(hh), int(mm))
            ws[f"{col}{time_row}"].number_format = "h:mm"

        # daily trace
        trace_today = get_day_latest_trace(session, run_id, day)
        carry = get_last_known_trace_before_day(session, run_id, day)

        raw_batches = trace_today["raw_batches"] or ([carry["raw"]] if carry["raw"] else [])
        raw_str = ", ".join(raw_batches)
        if raw_str:
            ws["D8"].value = raw_str

        tools = trace_today["tools"] or carry["tools"]
        for t_idx in range(2):
            r = 8 + t_idx
            if t_idx < len(tools):
                name, serial, calib = tools[t_idx]
                if name:
                    ws[f"G{r}"].value = name
                if serial:
                    ws[f"I{r}"].value = serial
                if calib:
                    ws[f"K{r}"].value = calib

        # ✅ Fill VALUES + Inspector/Operators per SLOT column
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

            inspector_name = session.get(User, e.inspector_id).display_name if e.inspector_id else ""
            ws[f"{col}{inspector_row}"].value = inspector_name

            if run.process in ["LINER", "COVER"]:
                ws[f"{col}{op1_row}"].value = e.operator_1 or ""
                ws[f"{col}{op2_row}"].value = e.operator_2 or ""
            else:
                ws[f"{col}{op1_row}"].value = e.operator_annular_12 or ""
                ws[f"{col}{op2_row}"].value = e.operator_int_ext_34 or ""

            vals = session.exec(select(InspectionValue).where(InspectionValue.entry_id == e.id)).all()
            for v in vals:
                r = row_map.get(v.param_key)
                if not r:
                    continue
                ws[f"{col}{r}"].value = v.value

    out = BytesIO()
    wb.save(out)
    out.seek(0)

    filename = f"{run.process}_{run.dhtp_batch_no}_ALL_DAYS.xlsx"
    return StreamingResponse(
        out,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
