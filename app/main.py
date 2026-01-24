from __future__ import annotations

from datetime import datetime, date, time as dtime
from typing import Dict, List, Optional, Tuple

import os
import tempfile

from fastapi import FastAPI, Request, Depends
from fastapi.responses import RedirectResponse, HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from itsdangerous import URLSafeSerializer
from sqlmodel import Session, select

from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

from app.db import create_db_and_tables, get_session
from app.models import (
    User, ProductionRun, RunParameter, InspectionEntry, InspectionValue,
    RunMachine, AuditLog
)
from app.auth import hash_password, verify_password

app = FastAPI()
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

SECRET = "CHANGE_ME_RANDOM"
ser = URLSafeSerializer(SECRET, salt="session")

SLOTS = [dtime(h, 0) for h in range(0, 24, 2)]  # fixed slots

PROCESS_LIST = ["LINER", "REINFORCEMENT", "COVER"]

DEFAULTS = {
    "LINER": [
        ("od_mm", "OD (mm)", "mm", "RANGE", 105.0, 106.0),
        ("wall_thickness_mm", "Wall Thickness (mm)", "mm", "RANGE", 7.0, 7.4),
        ("cooling_water_c", "Cooling Water (°C)", "°C", "MAX_ONLY", None, 35.0),
        ("line_speed_m_min", "Line Speed (m/min)", "m/min", "MAX_ONLY", None, 710.0),
        ("tractor_pressure_mpa", "Tractor Pressure (MPa)", "MPa", "RANGE", 0.2, 0.4),
        ("body_temp_z1_c", "Body Temp Zone 1 (°C)", "°C", "RANGE", 150, 160),
        ("body_temp_z2_c", "Body Temp Zone 2 (°C)", "°C", "RANGE", 170, 180),
        ("body_temp_z3_c", "Body Temp Zone 3 (°C)", "°C", "RANGE", 175, 185),
        ("body_temp_z4_c", "Body Temp Zone 4 (°C)", "°C", "RANGE", 190, 200),
        ("body_temp_z5_c", "Body Temp Zone 5 (°C)", "°C", "RANGE", 195, 205),
        ("noising_temp_z1_c", "Noising Temp Zone 1 (°C)", "°C", "RANGE", 200, 210),
        ("noising_temp_z2_c", "Noising Temp Zone 2 (°C)", "°C", "RANGE", 200, 210),
        ("noising_temp_z3_c", "Noising Temp Zone 3 (°C)", "°C", "RANGE", 200, 210),
    ],
    "REINFORCEMENT": [
        ("wrap_angle_deg", "Wrap Angle (deg)", "deg", "RANGE", 50, 70),
        ("tape_tension_n", "Tape Tension (N)", "N", "RANGE", 10, 30),
        ("line_speed_m_min", "Line Speed (m/min)", "m/min", "MAX_ONLY", None, 500),
        ("overlap_pct", "Overlap (%)", "%", "RANGE", 5, 20),
        ("ambient_temp_c", "Ambient Temp (°C)", "°C", "RANGE", 15, 40),
    ],
    "COVER": [
        ("od_cover_mm", "OD Cover (mm)", "mm", "RANGE", 110, 120),
        ("cover_thickness_mm", "Cover Thickness (mm)", "mm", "RANGE", 2, 5),
        ("cooling_water_c", "Cooling Water (°C)", "°C", "MAX_ONLY", None, 35),
        ("line_speed_m_min", "Line Speed (m/min)", "m/min", "MAX_ONLY", None, 700),
        ("surface_finish_ok", "Surface Finish OK (1=OK)", "", "INFO_ONLY", None, None),
    ],
}

IMAGE_MAP = {
    "LINER": "/static/images/liner.png",
    "REINFORCEMENT": "/static/images/reinforcement.png",
    "COVER": "/static/images/cover.png",
}

TEMPLATE_XLSX_MAP = {
    "LINER": os.path.join("app", "templates_xlsx", "liner.xlsx"),
    "REINFORCEMENT": os.path.join("app", "templates_xlsx", "reinforcement.xlsx"),
    "COVER": os.path.join("app", "templates_xlsx", "cover.xlsx"),
}

def compute_slot(actual: dtime) -> dtime:
    """
    2-hour slots with 30-min cutoff before next slot.
    """
    base_hour = actual.hour - (actual.hour % 2)
    prev_slot = dtime(base_hour, 0)
    idx = SLOTS.index(prev_slot) if prev_slot in SLOTS else 0
    next_slot = SLOTS[(idx + 1) % len(SLOTS)]

    def mins(t: dtime) -> int:
        return t.hour * 60 + t.minute

    a = mins(actual)
    n = mins(next_slot)
    if prev_slot == dtime(22, 0) and next_slot == dtime(0, 0):
        n = 24 * 60

    cutoff = n - 30
    if a > cutoff:
        return dtime(0, 0) if next_slot == dtime(0, 0) else next_slot
    return prev_slot

def get_current_user(request: Request, session: Session) -> Optional[User]:
    cookie = request.cookies.get("erp_session")
    if not cookie:
        return None
    try:
        data = ser.loads(cookie)
        uid = int(data.get("uid"))
        return session.get(User, uid)
    except Exception:
        return None

def parse_float(x: str) -> Optional[float]:
    x = (x or "").strip()
    if x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None

def get_production_days(session: Session, run_id: int) -> List[date]:
    """
    Production days are NOT calendar counted unless entries exist.
    We define a production day as a unique actual_date having >=1 entry.
    Day 1 = first date with any entry.
    """
    days = session.exec(
        select(InspectionEntry.actual_date)
        .where(InspectionEntry.run_id == run_id)
    ).all()
    uniq = sorted(set(days))
    return uniq

@app.on_event("startup")
def on_startup():
    create_db_and_tables()
    with Session(next(get_session()).get_bind()) as s:
        if not s.exec(select(User)).first():
            s.add(User(email="manager@demo.com", name="Manager", role="MANAGER", password_hash=hash_password("manager123")))
            s.add(User(email="inspector@demo.com", name="Inspector", role="INSPECTOR", password_hash=hash_password("inspector123")))
            s.commit()

# ---------------- AUTH ----------------

@app.get("/", response_class=HTMLResponse)
def root(request: Request, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    return RedirectResponse("/dashboard" if u else "/login", status_code=302)

@app.get("/login", response_class=HTMLResponse)
def login_get(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": None})

@app.post("/login")
async def login_post(request: Request, session: Session = Depends(get_session)):
    form = await request.form()
    email = (form.get("email") or "").strip()
    password = (form.get("password") or "").strip()
    user = session.exec(select(User).where(User.email == email)).first()
    if not user or not verify_password(password, user.password_hash):
        return templates.TemplateResponse("login.html", {"request": request, "error": "Invalid login"})

    cookie = ser.dumps({"uid": user.id})
    resp = RedirectResponse("/dashboard", status_code=302)
    resp.set_cookie("erp_session", cookie, httponly=True, samesite="lax")
    return resp

@app.get("/logout")
def logout():
    resp = RedirectResponse("/login", status_code=302)
    resp.delete_cookie("erp_session")
    return resp

# ---------------- DASHBOARD ----------------

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)

    runs = session.exec(select(ProductionRun).order_by(ProductionRun.created_at.desc())).all()
    return templates.TemplateResponse("dashboard.html", {"request": request, "user": u, "runs": runs})

# ---------------- RUN CREATE ----------------

@app.get("/runs/new", response_class=HTMLResponse)
def run_new_get(request: Request, process: str = "LINER", session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)
    if u.role != "MANAGER":
        return RedirectResponse("/dashboard", status_code=302)

    process = process.upper()
    if process not in PROCESS_LIST:
        process = "LINER"
    defaults = DEFAULTS[process]

    return templates.TemplateResponse(
        "run_new.html",
        {"request": request, "user": u, "process": process, "process_list": PROCESS_LIST, "defaults": defaults, "error": None},
    )

@app.post("/runs/new")
async def run_new_post(request: Request, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)
    if u.role != "MANAGER":
        return RedirectResponse("/dashboard", status_code=302)

    form = await request.form()
    process = (form.get("process") or "LINER").upper()
    if process not in PROCESS_LIST:
        process = "LINER"

    dhtp_batch_no = (form.get("dhtp_batch_no") or "").strip()
    if not dhtp_batch_no:
        return templates.TemplateResponse(
            "run_new.html",
            {"request": request, "user": u, "process": process, "process_list": PROCESS_LIST,
             "defaults": DEFAULTS[process], "error": "DHTP Batch No is required"},
        )

    run = ProductionRun(
        process=process,
        dhtp_batch_no=dhtp_batch_no,
        client_name=(form.get("client_name") or "").strip(),
        po_number=(form.get("po_number") or "").strip(),
        itp_number=(form.get("itp_number") or "").strip(),
        pipe_specification=(form.get("pipe_specification") or "").strip(),
        raw_material_spec=(form.get("raw_material_spec") or "").strip(),
        raw_material_batch_no=(form.get("raw_material_batch_no") or "").strip(),
        created_by=u.id,
    )
    session.add(run)
    session.commit()
    session.refresh(run)

    # Machines
    for idx in [1, 2, 3, 4]:
        mn = (form.get(f"m{idx}_name") or "").strip()
        tg = (form.get(f"m{idx}_tag") or "").strip()
        if mn or tg:
            session.add(RunMachine(run_id=run.id, machine_name=mn, tag=tg or None))
    session.commit()

    # Save parameters from arrays (editable)
    p_keys = form.getlist("p_key")
    p_labels = form.getlist("p_label")
    p_units = form.getlist("p_unit")
    p_rules = form.getlist("p_rule")
    p_mins = form.getlist("p_min")
    p_maxs = form.getlist("p_max")

    for i, key in enumerate(p_keys):
        key = (key or "").strip()
        if not key:
            continue
        rp = RunParameter(
            run_id=run.id,
            param_key=key,
            label=(p_labels[i] if i < len(p_labels) else key),
            unit=(p_units[i] if i < len(p_units) else ""),
            rule=(p_rules[i] if i < len(p_rules) else "RANGE"),
            min_value=parse_float(p_mins[i] if i < len(p_mins) else ""),
            max_value=parse_float(p_maxs[i] if i < len(p_maxs) else ""),
            display_order=i,
        )
        session.add(rp)
    session.commit()

    session.add(AuditLog(run_id=run.id, actor_user_id=u.id, action="CREATE_RUN"))
    session.commit()

    return RedirectResponse(f"/runs/{run.id}", status_code=302)

# ---------------- RUN VIEW (per production day page) ----------------

@app.get("/runs/{run_id}", response_class=HTMLResponse)
def run_view(request: Request, run_id: int, day: Optional[int] = None, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)

    run = session.get(ProductionRun, run_id)
    if not run:
        return RedirectResponse("/dashboard", status_code=302)

    params = session.exec(select(RunParameter).where(RunParameter.run_id == run_id).order_by(RunParameter.display_order)).all()
    machines = session.exec(select(RunMachine).where(RunMachine.run_id == run_id)).all()

    production_days = get_production_days(session, run_id)  # list[date]
    # Choose day index
    if not production_days:
        selected_date = None
        day_index = 1
    else:
        if day is None:
            day_index = len(production_days)  # default last day
        else:
            day_index = max(1, min(int(day), len(production_days)))
        selected_date = production_days[day_index - 1]

    # Entries for selected day
    if selected_date:
        entries = session.exec(
            select(InspectionEntry)
            .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date == selected_date)
            .order_by(InspectionEntry.created_at)
        ).all()
    else:
        entries = []

    slot_map: Dict[str, List[InspectionEntry]] = {s.strftime("%H:%M"): [] for s in SLOTS}
    for e in entries:
        slot_map[e.slot_time.strftime("%H:%M")].append(e)

    entry_ids = [e.id for e in entries]
    entry_slot = {e.id: e.slot_time.strftime("%H:%M") for e in entries}
    value_map: Dict[str, Dict[str, float]] = {}
    if entry_ids:
        vals = session.exec(select(InspectionValue).where(InspectionValue.entry_id.in_(entry_ids))).all()
    else:
        vals = []

    for v in vals:
        slot_str = entry_slot.get(v.entry_id)
        if not slot_str:
            continue
        value_map.setdefault(v.param_key, {})[slot_str] = v.value

    img_url = IMAGE_MAP.get(run.process, IMAGE_MAP["LINER"])
    reopen_error = request.query_params.get("reopen_error") == "1"

    return templates.TemplateResponse(
        "run_view.html",
        {
            "request": request,
            "user": u,
            "run": run,
            "params": params,
            "machines": machines,
            "slots": [s.strftime("%H:%M") for s in SLOTS],
            "slot_map": slot_map,
            "value_map": value_map,
            "img_url": img_url,
            "production_days": production_days,
            "day_index": day_index,
            "selected_date": selected_date,
            "reopen_error": reopen_error,
        },
    )

# ---------------- ENTRY CREATE ----------------

@app.get("/runs/{run_id}/entries/new", response_class=HTMLResponse)
def entry_new_get(request: Request, run_id: int, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)

    run = session.get(ProductionRun, run_id)
    if not run:
        return RedirectResponse("/dashboard", status_code=302)

    if run.status != "OPEN":
        return RedirectResponse(f"/runs/{run_id}", status_code=302)

    params = session.exec(select(RunParameter).where(RunParameter.run_id == run_id).order_by(RunParameter.display_order)).all()
    today = date.today().isoformat()

    return templates.TemplateResponse("entry_new.html", {"request": request, "user": u, "run": run, "params": params, "today": today})

@app.post("/runs/{run_id}/entries/new")
async def entry_new_post(request: Request, run_id: int, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)

    run = session.get(ProductionRun, run_id)
    if not run or run.status != "OPEN":
        return RedirectResponse(f"/runs/{run_id}", status_code=302)

    form = await request.form()

    actual_date_str = (form.get("actual_date") or "").strip()  # YYYY-MM-DD
    actual_time_str = (form.get("actual_time") or "").strip()  # HH:MM

    try:
        ad = datetime.strptime(actual_date_str, "%Y-%m-%d").date()
    except Exception:
        ad = date.today()

    if ":" not in actual_time_str:
        return RedirectResponse(f"/runs/{run_id}", status_code=302)
    hh, mm = actual_time_str.split(":")
    at = dtime(int(hh), int(mm))
    slot = compute_slot(at)

    entry = InspectionEntry(
        run_id=run_id,
        actual_date=ad,
        actual_time=at,
        slot_time=slot,
        inspector_user_id=u.id,
        operator1=(form.get("operator1") or "").strip() or None,
        operator2=(form.get("operator2") or "").strip() or None,
        remark=(form.get("remark") or "").strip() or None,
    )
    session.add(entry)
    session.commit()
    session.refresh(entry)

    params = session.exec(select(RunParameter).where(RunParameter.run_id == run_id)).all()
    for p in params:
        raw = (form.get(f"val_{p.param_key}") or "").strip()
        if raw == "":
            continue
        try:
            v = float(raw)
        except Exception:
            continue
        session.add(InspectionValue(entry_id=entry.id, param_key=p.param_key, value=v))
    session.commit()

    session.add(AuditLog(run_id=run_id, actor_user_id=u.id, action="NEW_ENTRY"))
    session.commit()

    # Redirect to the production-day page where this entry belongs
    production_days = get_production_days(session, run_id)
    day_index = production_days.index(ad) + 1 if ad in production_days else 1
    return RedirectResponse(f"/runs/{run_id}?day={day_index}", status_code=302)

# ---------------- WORKFLOW ----------------

@app.post("/runs/{run_id}/close")
def run_close(request: Request, run_id: int, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)
    if u.role != "MANAGER":
        return RedirectResponse(f"/runs/{run_id}", status_code=302)

    run = session.get(ProductionRun, run_id)
    if run and run.status == "OPEN":
        run.status = "CLOSED"
        session.add(run)
        session.add(AuditLog(run_id=run_id, actor_user_id=u.id, action="CLOSE_RUN"))
        session.commit()

    return RedirectResponse(f"/runs/{run_id}", status_code=302)

@app.post("/runs/{run_id}/approve")
def run_approve(request: Request, run_id: int, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)
    if u.role != "MANAGER":
        return RedirectResponse(f"/runs/{run_id}", status_code=302)

    run = session.get(ProductionRun, run_id)
    if run and run.status == "CLOSED":
        run.status = "APPROVED"
        session.add(run)
        session.add(AuditLog(run_id=run_id, actor_user_id=u.id, action="APPROVE_RUN"))
        session.commit()

    return RedirectResponse(f"/runs/{run_id}", status_code=302)

@app.post("/runs/{run_id}/reopen")
async def run_reopen(request: Request, run_id: int, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)
    if u.role != "MANAGER":
        return RedirectResponse(f"/runs/{run_id}", status_code=302)

    run = session.get(ProductionRun, run_id)
    if not run:
        return RedirectResponse("/dashboard", status_code=302)

    form = await request.form()
    reason = (form.get("reason") or "").strip()

    if run.status == "APPROVED" and not reason:
        return RedirectResponse(f"/runs/{run_id}?reopen_error=1", status_code=302)

    if run.status in ("CLOSED", "APPROVED"):
        run.status = "OPEN"
        session.add(run)
        session.add(AuditLog(run_id=run_id, actor_user_id=u.id, action="REOPEN_RUN", reason=reason or None))
        session.commit()

    return RedirectResponse(f"/runs/{run_id}", status_code=302)

# ---------------- EXPORT XLSX (ALL PRODUCTION DAYS IN ONE FILE) ----------------

@app.get("/runs/{run_id}/export-xlsx")
def export_xlsx(request: Request, run_id: int, day: Optional[int] = None, session: Session = Depends(get_session)):
    """
    Export ONE XLSX that contains all production days as separate sheets:
      Day 1, Day 2, ... (only days where entries exist)
    Optional: ?day=2 to export one day only.
    """
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)

    run = session.get(ProductionRun, run_id)
    if not run:
        return RedirectResponse("/dashboard", status_code=302)

    template_path = TEMPLATE_XLSX_MAP.get(run.process, TEMPLATE_XLSX_MAP["LINER"])
    if not os.path.exists(template_path):
        return HTMLResponse(f"Template not found: <b>{template_path}</b>", status_code=500)

    base_wb = load_workbook(template_path)
    base_ws = base_wb.worksheets[0]  # master page only (first sheet)

    production_days = get_production_days(session, run_id)
    if not production_days:
        # export just blank Day 1
        production_days = []

    # If exporting only one day
    if production_days and day is not None:
        di = max(1, min(int(day), len(production_days)))
        production_days = [production_days[di - 1]]

    # We will build the output inside base_wb by copying base_ws
    # First: rename base_ws to Day 1 if needed; if no production days, keep as Day 1 blank.
    # Then create additional day sheets by copying.
    # Finally remove extra original sheets (if template had more).

    # Remove extra template sheets except first, to avoid confusion
    while len(base_wb.worksheets) > 1:
        base_wb.remove(base_wb.worksheets[-1])

    if not production_days:
        base_ws.title = "Day 1"
        day_sheets: List[Tuple[int, date, object]] = [(1, date.today(), base_ws)]
    else:
        base_ws.title = "Day 1"
        day_sheets = [(1, production_days[0], base_ws)]
        for i in range(2, len(production_days) + 1):
            new_ws = base_wb.copy_worksheet(base_ws)
            new_ws.title = f"Day {i}"
            day_sheets.append((i, production_days[i - 1], new_ws))

    machines = session.exec(select(RunMachine).where(RunMachine.run_id == run_id)).all()
    params = session.exec(select(RunParameter).where(RunParameter.run_id == run_id).order_by(RunParameter.display_order)).all()

    # Helper: fill a sheet
    def fill_sheet(ws, day_date: date):
        # Header fill (best-effort positions; matches your liner template)
        ws["D5"] = run.dhtp_batch_no
        ws["I5"] = run.client_name
        ws["I6"] = run.po_number
        ws["D6"] = run.pipe_specification
        ws["D7"] = run.raw_material_spec
        ws["D8"] = run.raw_material_batch_no
        ws["D9"] = run.itp_number

        # Put date somewhere visible (safe place)
        ws["I7"] = day_date.isoformat()

        # Machines (M5..)
        for r in range(5, 12):
            ws[f"M{r}"] = ""  # clear
        for i, m in enumerate(machines[:6]):
            ws[f"M{5+i}"] = f"{m.machine_name}{f' ({m.tag})' if m.tag else ''}"

        # Fill time headers row 22 columns E..P
        for i, sl in enumerate(SLOTS):
            col = get_column_letter(5 + i)  # E=5
            ws[f"{col}22"] = sl.strftime("%H:%M")

        # Entries for this day
        entries = session.exec(
            select(InspectionEntry)
            .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date == day_date)
            .order_by(InspectionEntry.created_at)
        ).all()

        # Fill inspector/operators from last entry of the day
        if entries:
            last_entry = entries[-1]
            inspector = session.get(User, last_entry.inspector_user_id)
            ws["B38"] = inspector.name if inspector else ""
            ws["B39"] = last_entry.operator1 or ""
            ws["B40"] = last_entry.operator2 or ""
        else:
            ws["B38"] = ""
            ws["B39"] = ""
            ws["B40"] = ""

        # Values map for this day
        entry_ids = [e.id for e in entries]
        entry_slot = {e.id: e.slot_time.strftime("%H:%M") for e in entries}
        value_map: Dict[str, Dict[str, float]] = {}
        if entry_ids:
            vals = session.exec(select(InspectionValue).where(InspectionValue.entry_id.in_(entry_ids))).all()
        else:
            vals = []

        for v in vals:
            slot_str = entry_slot.get(v.entry_id)
            if not slot_str:
                continue
            value_map.setdefault(v.param_key, {})[slot_str] = v.value

        # Fill grid values starting row 23, label column B, times E..P
        start_row = 23
        for r_i, p in enumerate(params):
            row = start_row + r_i
            if ws[f"B{row}"].value in (None, ""):
                ws[f"B{row}"] = p.label

            for s_i, slot_str in enumerate([s.strftime("%H:%M") for s in SLOTS]):
                v = value_map.get(p.param_key, {}).get(slot_str)
                if v is None:
                    continue
                col = get_column_letter(5 + s_i)
                ws[f"{col}{row}"] = v

    # Fill all sheets
    for (di, dday, ws) in day_sheets:
        fill_sheet(ws, dday)

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    tmp_path = tmp.name
    tmp.close()
    base_wb.save(tmp_path)

    if day is None:
        filename = f"{run.process}_{run.dhtp_batch_no}_ALL_DAYS.xlsx"
    else:
        filename = f"{run.process}_{run.dhtp_batch_no}_DAY_{day}.xlsx"

    return FileResponse(tmp_path, filename=filename, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
