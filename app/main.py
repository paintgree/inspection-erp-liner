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

TEMPLATE_XLSX_MAP = {
    "LINER": os.path.join("app", "templates", "templates_xlsx", "liner.xlsx"),
    "REINFORCEMENT": os.path.join("app", "templates", "templates_xlsx", "reinforcement.xlsx"),
    "COVER": os.path.join("app", "templates", "templates_xlsx", "cover.xlsx"),
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
    "length_m": 22, "annular_od_70_1": 23, "annular_od_70_2": 24, "annular_od_45_3": 25,
    "annular_od_45_4": 26, "core_mould_dia_mm": 27, "annular_width_1_mm": 28,
    "annular_width_2_mm": 29, "screw_yarn_width_1_mm": 30, "screw_yarn_width_2_mm": 31,
    "tractor_speed_m_min": 32, "clamping_gas_p1_mpa": 33, "clamping_gas_p2_mpa": 34,
    "thrust_gas_p_mpa": 35,
}


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


def get_days_for_run(session: Session, run_id: int) -> List[date]:
    days = session.exec(
        select(InspectionEntry.actual_date)
        .where(InspectionEntry.run_id == run_id)
        .distinct()
        .order_by(InspectionEntry.actual_date)
    ).all()
    return list(days)


def slot_from_time_str(t: str) -> str:
    parts = t.split(":")
    hh = int(parts[0])
    mm = int(parts[1]) if len(parts) > 1 else 0
    total_min = hh * 60 + mm
    slot_min = int(round(total_min / 120.0) * 120)
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
    if user.role != "MANAGER":
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

    grid: Dict[str, Dict[str, dict]] = {s: {} for s in SLOTS}
    for e in entries:
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
async def run_edit_post(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    if user.role != "MANAGER":
        raise HTTPException(403, "Manager only")

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

    # machines: replace
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

    # parameter rules + ✅ label rename
    params = session.exec(select(RunParameter).where(RunParameter.run_id == run_id)).all()
    for p in params:
        # ✅ allow manager rename label
        new_label = str(form.get(f"label_{p.param_key}", "")).strip()
        if new_label:
            p.label = new_label

        rule = str(form.get(f"rule_{p.param_key}", "")).strip().upper()
        if rule not in ["", "RANGE", "MIN_ONLY", "MAX_ONLY"]:
            rule = ""
        p.rule = rule

        mn_raw = form.get(f"min_{p.param_key}", "")
        mx_raw = form.get(f"max_{p.param_key}", "")

        try:
            p.min_value = float(mn_raw) if str(mn_raw).strip() != "" else None
        except Exception:
            p.min_value = None

        try:
            p.max_value = float(mx_raw) if str(mx_raw).strip() != "" else None
        except Exception:
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

    # ✅ clean UI message (no JSON)
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


# ====== VALUE EDIT + APPROVAL (same as your working logic) ======
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
    if user.role != "MANAGER":
        raise HTTPException(403, "Manager only")

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
    if user.role != "MANAGER":
        raise HTTPException(403, "Manager only")

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
    if user.role != "MANAGER":
        raise HTTPException(403, "Manager only")

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


# ✅ EXPORT: Machines in M4:P9 + (values already)
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

    base_wb = openpyxl.load_workbook(template_path)
    base_ws = base_wb.worksheets[0]

    machines = session.exec(select(RunMachine).where(RunMachine.run_id == run_id)).all()

    for i, day in enumerate(days):
        if i == 0:
            ws = base_ws
            ws.title = f"Day {i+1} ({day.isoformat()})"
        else:
            ws = base_wb.copy_worksheet(base_ws)
            ws.title = f"Day {i+1} ({day.isoformat()})"

        # header
        ws["D5"].value = run.dhtp_batch_no
        ws["I5"].value = run.client_name
        ws["I6"].value = run.po_number
        ws["D7"].value = run.raw_material_spec
        ws["D9"].value = run.itp_number

        # ✅ machines used mapping M4:P9 (write rows 5..9)
        # name in M, tag in P (template likely merges M:O for name)
        start_row = 5
        for idx in range(5):
            r = start_row + idx
            if idx < len(machines):
                ws[f"M{r}"].value = machines[idx].machine_name
                ws[f"P{r}"].value = machines[idx].machine_tag
            else:
                ws[f"M{r}"].value = ""
                ws[f"P{r}"].value = ""

        # date + time row
        if run.process in ["LINER", "COVER"]:
            ws["E20"].value = day
            for idx, slot in enumerate(SLOTS):
                col = openpyxl.utils.get_column_letter(5 + idx)
                cell = ws[f"{col}21"]
                hh, mm = slot.split(":")
                cell.value = dtime(int(hh), int(mm))
                cell.number_format = "h:mm"
        else:
            ws["F20"].value = day
            for idx, slot in enumerate(SLOTS):
                col = openpyxl.utils.get_column_letter(6 + idx)
                cell = ws[f"{col}21"]
                hh, mm = slot.split(":")
                cell.value = dtime(int(hh), int(mm))
                cell.number_format = "h:mm"

        # trace + tools
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

        col_start = 5 if run.process in ["LINER", "COVER"] else 6
        row_map = ROW_MAP_LINER_COVER if run.process in ["LINER", "COVER"] else ROW_MAP_REINF

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

            vals = session.exec(select(InspectionValue).where(InspectionValue.entry_id == e.id)).all()
            for v in vals:
                r = row_map.get(v.param_key)
                if not r:
                    continue
                ws[f"{col}{r}"].value = v.value

    out = BytesIO()
    base_wb.save(out)
    out.seek(0)

    filename = f"{run.process}_{run.dhtp_batch_no}_ALL_DAYS.xlsx"
    return StreamingResponse(
        out,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
