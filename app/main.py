from __future__ import annotations

import os
from datetime import datetime, date, time as dtime
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

# Your folder (as you showed)
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
    ("annular_od_70_1", "Annular OD (∠70°) #1 (mm)", "mm"),
    ("annular_od_70_2", "Annular OD (∠70°) #2 (mm)", "mm"),
    ("annular_od_45_3", "Annular OD (∠45°) #3 (mm)", "mm"),
    ("annular_od_45_4", "Annular OD (∠45°) #4 (mm)", "mm"),
    ("tractor_speed_m_min", "Tractor Speed (m/min)", "m/min"),
    ("clamping_gas_p1_mpa", "Clamping Gas Pressure #1 (MPa)", "MPa"),
    ("clamping_gas_p2_mpa", "Clamping Gas Pressure #2 (MPa)", "MPa"),
    ("thrust_gas_p_mpa", "Thrust Gas Pressure (MPa)", "MPa"),
]

PROCESS_PARAMS = {
    "LINER": LINER_COVER_PARAMS,
    "COVER": LINER_COVER_PARAMS,
    "REINFORCEMENT": REINF_PARAMS,
}


# -----------------------------
# XLSX ROW MAPS (fixes your mismatch + makes reinforcement export work)
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

    # Seed users once (if empty)
    from sqlmodel import select
    with next(get_session()) as session:
        existing = session.exec(select(User)).first()
        if not existing:
            manager = User(
                username="manager",
                display_name="Manager",
                role="MANAGER",
                password_hash=hash_password("manager123"),
            )
            inspector = User(
                username="inspector",
                display_name="Inspector",
                role="INSPECTOR",
                password_hash=hash_password("inspector123"),
            )
            session.add(manager)
            session.add(inspector)
            session.commit()


# -----------------------------
# SIMPLE AUTH (cookie)
# -----------------------------
def get_current_user(request: Request, session: Session) -> User:
    uname = request.cookies.get("user")
    if not uname:
        raise HTTPException(status_code=401, detail="Not logged in")
    user = session.exec(select(User).where(User.username == uname)).first()
    if not user:
        raise HTTPException(status_code=401, detail="Invalid user")
    return user


def require_manager(user: User):
    if user.role != "MANAGER":
        raise HTTPException(status_code=403, detail="Manager only")


# -----------------------------
# HELPERS
# -----------------------------
def slot_for_time(hhmm: str) -> str:
    """
    Fixed slots 00:00,02:00,...22:00
    Rule: cutoff 30 min before next slot
    """
    h, m = hhmm.split(":")
    minutes = int(h) * 60 + int(m)

    slot_minutes = [i * 120 for i in range(12)]  # 0..1320
    for idx, sm in enumerate(slot_minutes):
        next_sm = slot_minutes[idx + 1] if idx + 1 < len(slot_minutes) else 24 * 60
        cutoff = next_sm - 30
        if minutes < cutoff:
            return SLOTS[idx]
    return "22:00"


def group_runs_by_batch(runs: List[ProductionRun]) -> Dict[str, List[ProductionRun]]:
    grouped: Dict[str, List[ProductionRun]] = {}
    for r in runs:
        grouped.setdefault(r.dhtp_batch_no, []).append(r)
    # stable order
    for k in grouped:
        grouped[k] = sorted(grouped[k], key=lambda x: x.process)
    return dict(sorted(grouped.items(), key=lambda kv: kv[0]))


def get_days_for_run(session: Session, run_id: int) -> List[date]:
    days = session.exec(
        select(InspectionEntry.actual_date)
        .where(InspectionEntry.run_id == run_id)
        .distinct()
        .order_by(InspectionEntry.actual_date)
    ).all()
    return list(days)


def get_day_latest_trace(session: Session, run_id: int, day: date) -> dict:
    """
    For this day: build "what tools/raw batch were used" without overwriting history.
    - raw_material_batch_no: unique values used that day (if none -> "")
    - tools: unique tool lines used that day (if none -> [])
    """
    entries = session.exec(
        select(InspectionEntry)
        .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date == day)
        .order_by(InspectionEntry.created_at)
    ).all()

    raw_batches = []
    tools = []

    for e in entries:
        if e.raw_material_batch_no and e.raw_material_batch_no not in raw_batches:
            raw_batches.append(e.raw_material_batch_no)

        t1 = (e.tool1_name.strip(), e.tool1_serial.strip(), e.tool1_calib_due.strip())
        t2 = (e.tool2_name.strip(), e.tool2_serial.strip(), e.tool2_calib_due.strip())
        for t in [t1, t2]:
            if any(t) and t not in tools:
                tools.append(t)

    return {
        "raw_batches": raw_batches,
        "tools": tools,
        "entries": entries,
    }


def get_last_known_trace_before_day(session: Session, run_id: int, day: date) -> dict:
    """
    If a day has no tools/raw batch provided, we carry forward last known (so the day sheet is still filled).
    """
    all_entries = session.exec(
        select(InspectionEntry)
        .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date <= day)
        .order_by(InspectionEntry.actual_date, InspectionEntry.created_at)
    ).all()

    last_raw = ""
    last_tools: List[Tuple[str,str,str]] = []
    for e in all_entries:
        if e.raw_material_batch_no:
            last_raw = e.raw_material_batch_no
        tlist = []
        t1 = (e.tool1_name.strip(), e.tool1_serial.strip(), e.tool1_calib_due.strip())
        t2 = (e.tool2_name.strip(), e.tool2_serial.strip(), e.tool2_calib_due.strip())
        for t in [t1, t2]:
            if any(t):
                tlist.append(t)
        if tlist:
            last_tools = tlist
    return {"raw": last_raw, "tools": last_tools}


def get_progress_percent(session: Session, run: ProductionRun) -> int:
    """
    Progress = max recorded length / total_length_m
    Uses parameter "length_m" values.
    """
    if run.total_length_m <= 0:
        return 0
    vals = session.exec(
        select(InspectionValue.value)
        .join(InspectionEntry, InspectionValue.entry_id == InspectionEntry.id)
        .where(InspectionEntry.run_id == run.id, InspectionValue.param_key == "length_m")
    ).all()
    nums = [v for v in vals if isinstance(v, (int, float))]
    if not nums:
        return 0
    pct = int(round((max(nums) / run.total_length_m) * 100))
    return max(0, min(100, pct))


# -----------------------------
# ROUTES
# -----------------------------
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return RedirectResponse("/login", status_code=302)


@app.get("/login", response_class=HTMLResponse)
def login_get(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@app.post("/login")
def login_post(
    request: Request,
    session: Session = Depends(get_session),
    username: str = Form(...),
    password: str = Form(...),
):
    user = session.exec(select(User).where(User.username == username)).first()
    if not user or not verify_password(password, user.password_hash):
        return templates.TemplateResponse("login.html", {"request": request, "error": "Invalid login"})
    resp = RedirectResponse("/dashboard", status_code=302)
    resp.set_cookie("user", user.username, httponly=True)
    return resp


@app.get("/logout")
def logout():
    resp = RedirectResponse("/login", status_code=302)
    resp.delete_cookie("user")
    return resp


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    runs = session.exec(select(ProductionRun).order_by(ProductionRun.created_at.desc())).all()

    grouped = group_runs_by_batch(runs)

    # progress per run for small dashboard indicator
    progress = {r.id: get_progress_percent(session, r) for r in runs}

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "user": user,
            "grouped": grouped,
            "progress": progress,
        },
    )


@app.get("/runs/new/{process}", response_class=HTMLResponse)
def run_new_get(process: str, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)
    process = process.upper()
    if process not in PROCESS_PARAMS:
        raise HTTPException(404, "Invalid process")
    return templates.TemplateResponse("run_new.html", {"request": request, "user": user, "process": process})


@app.post("/runs/new/{process}")
def run_new_post(
    process: str,
    request: Request,
    session: Session = Depends(get_session),
    dhtp_batch_no: str = Form(...),
    client_name: str = Form(...),
    po_number: str = Form(...),
    itp_number: str = Form(...),
    pipe_specification: str = Form(...),
    raw_material_spec: str = Form(...),
    total_length_m: float = Form(0.0),

    # machines (up to 5 simple pairs)
    machine1_name: str = Form(""),
    machine1_tag: str = Form(""),
    machine2_name: str = Form(""),
    machine2_tag: str = Form(""),
    machine3_name: str = Form(""),
    machine3_tag: str = Form(""),
    machine4_name: str = Form(""),
    machine4_tag: str = Form(""),
    machine5_name: str = Form(""),
    machine5_tag: str = Form(""),
):
    user = get_current_user(request, session)
    require_manager(user)

    process = process.upper()
    if process not in PROCESS_PARAMS:
        raise HTTPException(404, "Invalid process")

    run = ProductionRun(
        process=process,
        dhtp_batch_no=dhtp_batch_no.strip(),
        client_name=client_name.strip(),
        po_number=po_number.strip(),
        itp_number=itp_number.strip(),
        pipe_specification=pipe_specification.strip(),
        raw_material_spec=raw_material_spec.strip(),
        total_length_m=float(total_length_m or 0),
    )
    session.add(run)
    session.commit()
    session.refresh(run)

    machines = [
        (machine1_name, machine1_tag),
        (machine2_name, machine2_tag),
        (machine3_name, machine3_tag),
        (machine4_name, machine4_tag),
        (machine5_name, machine5_tag),
    ]
    for mn, mt in machines:
        if mn.strip():
            session.add(RunMachine(run_id=run.id, machine_name=mn.strip(), machine_tag=mt.strip()))
    session.commit()

    # Default params created (manager can edit in UI later)
    defs = PROCESS_PARAMS[process]
    for idx, (k, label, unit) in enumerate(defs):
        session.add(RunParameter(run_id=run.id, param_key=k, label=label, unit=unit, display_order=idx))
    session.commit()

    return RedirectResponse(f"/runs/{run.id}", status_code=302)


@app.get("/runs/{run_id}/edit", response_class=HTMLResponse)
def run_edit_get(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    return templates.TemplateResponse("run_edit.html", {"request": request, "user": user, "run": run})


@app.post("/runs/{run_id}/edit")
def run_edit_post(
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
    require_manager(user)

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    run.client_name = client_name.strip()
    run.po_number = po_number.strip()
    run.itp_number = itp_number.strip()
    run.pipe_specification = pipe_specification.strip()
    run.raw_material_spec = raw_material_spec.strip()
    run.total_length_m = float(total_length_m or 0)

    session.add(run)
    session.commit()

    return RedirectResponse(f"/runs/{run.id}", status_code=302)


@app.get("/runs/{run_id}", response_class=HTMLResponse)
def run_view(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    machines = session.exec(select(RunMachine).where(RunMachine.run_id == run_id)).all()
    params = session.exec(select(RunParameter).where(RunParameter.run_id == run_id).order_by(RunParameter.display_order)).all()

    days = get_days_for_run(session, run_id)
    selected_day = days[-1] if days else date.today()

    # build day grid
    entries = session.exec(
        select(InspectionEntry)
        .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date == selected_day)
        .order_by(InspectionEntry.created_at)
    ).all()

    # slot -> param -> value
    grid: Dict[str, Dict[str, Optional[float]]] = {s: {} for s in SLOTS}
    for e in entries:
        vals = session.exec(select(InspectionValue).where(InspectionValue.entry_id == e.id)).all()
        for v in vals:
            grid.setdefault(e.slot_time, {})[v.param_key] = v.value

    # daily trace display
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

    params = session.exec(select(RunParameter).where(RunParameter.run_id == run_id).order_by(RunParameter.display_order)).all()

    # do we already have any entries? if not, tools + raw batch are required on first entry
    has_any = session.exec(select(InspectionEntry.id).where(InspectionEntry.run_id == run_id)).first() is not None

    return templates.TemplateResponse(
        "entry_new.html",
        {
            "request": request,
            "user": user,
            "run": run,
            "params": params,
            "has_any": has_any,
        },
    )


@app.post("/runs/{run_id}/entry/new")
def entry_new_post(
    run_id: int,
    request: Request,
    session: Session = Depends(get_session),
    actual_date: str = Form(...),
    actual_time: str = Form(...),

    # operators
    operator_1: str = Form(""),
    operator_2: str = Form(""),
    operator_annular_12: str = Form(""),
    operator_int_ext_34: str = Form(""),

    remarks: str = Form(""),

    # trace updates (optional after first entry)
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

    # first entry MUST include tools + raw batch (your requirement)
    has_any = session.exec(select(InspectionEntry.id).where(InspectionEntry.run_id == run_id)).first() is not None
    if not has_any:
        if not raw_material_batch_no.strip():
            raise HTTPException(400, "First entry must include Raw Material Batch No.")
        if not (tool1_name.strip() or tool2_name.strip()):
            raise HTTPException(400, "First entry must include Inspection Tools")

    # assign slot automatically
    slot = slot_for_time(actual_time.strip())
    day = datetime.strptime(actual_date, "%Y-%m-%d").date()

    entry = InspectionEntry(
        run_id=run_id,
        actual_date=day,
        actual_time=actual_time.strip(),
        slot_time=slot,
        inspector_id=user.id,
        operator_1=operator_1.strip(),
        operator_2=operator_2.strip(),
        operator_annular_12=operator_annular_12.strip(),
        operator_int_ext_34=operator_int_ext_34.strip(),
        remarks=remarks.strip(),
        raw_material_batch_no=raw_material_batch_no.strip(),
        tool1_name=tool1_name.strip(),
        tool1_serial=tool1_serial.strip(),
        tool1_calib_due=tool1_calib_due.strip(),
        tool2_name=tool2_name.strip(),
        tool2_serial=tool2_serial.strip(),
        tool2_calib_due=tool2_calib_due.strip(),
    )
    session.add(entry)
    session.commit()
    session.refresh(entry)

    # values: read all run params from form (param_<key>)
    params = session.exec(select(RunParameter).where(RunParameter.run_id == run_id)).all()
    for p in params:
        raw = (await_form_value(request, f"param_{p.param_key}"))
        if raw is None or raw == "":
            continue
        try:
            val = float(raw)
        except:
            continue
        session.add(InspectionValue(entry_id=entry.id, param_key=p.param_key, value=val))
    session.commit()

    return RedirectResponse(f"/runs/{run_id}", status_code=302)


async def await_form_value(request: Request, key: str) -> Optional[str]:
    form = await request.form()
    return form.get(key)


@app.post("/runs/{run_id}/close")
def run_close(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)
    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404)
    run.status = "CLOSED"
    session.add(run)
    session.commit()
    return RedirectResponse(f"/runs/{run_id}", status_code=302)


@app.post("/runs/{run_id}/approve")
def run_approve(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)
    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404)
    run.status = "APPROVED"
    session.add(run)
    session.commit()
    return RedirectResponse(f"/runs/{run_id}", status_code=302)


@app.post("/runs/{run_id}/reopen")
def run_reopen(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)
    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404)
    run.status = "OPEN"
    session.add(run)
    session.commit()
    return RedirectResponse(f"/runs/{run_id}", status_code=302)


# -----------------------------
# EXPORT EXCEL (ALL DAYS -> ONE XLSX)  ✅ fills header/date/time/tools/operators + reinforcement works
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

    # open template workbook (keeps image inside template)
    base_wb = openpyxl.load_workbook(template_path)
    base_ws = base_wb.worksheets[0]
    base_title = base_ws.title

    # we will make one sheet per production day (copy template sheet)
    # first sheet becomes Day 1
    for i, day in enumerate(days):
        if i == 0:
            ws = base_ws
            ws.title = f"Day {i+1} ({day.isoformat()})"
        else:
            ws = base_wb.copy_worksheet(base_ws)
            ws.title = f"Day {i+1} ({day.isoformat()})"

        # ---- Fill header (same coordinates in all 3 templates)
        ws["D5"].value = run.dhtp_batch_no
        ws["I5"].value = run.client_name
        ws["I6"].value = run.po_number
        ws["D7"].value = run.raw_material_spec
        ws["D9"].value = run.itp_number

        # ---- Date + time header
        if run.process in ["LINER", "COVER"]:
            ws["E20"].value = day
            # time header row 21 starts E..P
            for idx, slot in enumerate(SLOTS):
                col = openpyxl.utils.get_column_letter(5 + idx)  # E=5
                cell = ws[f"{col}21"]
                hh, mm = slot.split(":")
                cell.value = dtime(int(hh), int(mm))
                cell.number_format = "h:mm"
        else:  # REINFORCEMENT
            ws["F20"].value = day
            # time header row 21 starts F..Q
            for idx, slot in enumerate(SLOTS):
                col = openpyxl.utils.get_column_letter(6 + idx)  # F=6
                cell = ws[f"{col}21"]
                hh, mm = slot.split(":")
                cell.value = dtime(int(hh), int(mm))
                cell.number_format = "h:mm"

        # ---- Per-day traceability (tools + raw batch)
        trace_today = get_day_latest_trace(session, run_id, day)
        carry = get_last_known_trace_before_day(session, run_id, day)

        raw_batches = trace_today["raw_batches"] or ([carry["raw"]] if carry["raw"] else [])
        raw_str = ", ".join(raw_batches)
        if raw_str:
            ws["D8"].value = raw_str  # daily raw batch

        tools = trace_today["tools"] or carry["tools"]
        # template has tools on rows 8 and 9: G8 tool name, I8 serial, K8 calib
        # we fill up to 2 tool lines
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

        # ---- Inspector + operators + remarks
        entries = trace_today["entries"]
        if entries:
            last = entries[-1]
            inspector_name = session.get(User, last.inspector_id).display_name

            if run.process in ["LINER", "COVER"]:
                ws["B38"].value = inspector_name
                ws["B39"].value = last.operator_1
                ws["B40"].value = last.operator_2
                # remarks box exists in cover, liner may have it
                # safe: try common location
                if "Remarks" in str(ws["A41"].value or ""):
                    ws["B41"].value = last.remarks
            else:
                ws["B36"].value = inspector_name
                ws["B37"].value = last.operator_annular_12
                ws["B38"].value = last.operator_int_ext_34

        # ---- Fill inspection values into correct slot column
        # slot column start differs by template:
        col_start = 5 if run.process in ["LINER", "COVER"] else 6  # E or F
        row_map = ROW_MAP_LINER_COVER if run.process in ["LINER", "COVER"] else ROW_MAP_REINF

        # get entries of this day
        day_entries = session.exec(
            select(InspectionEntry)
            .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date == day)
            .order_by(InspectionEntry.created_at)
        ).all()

        for e in day_entries:
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
