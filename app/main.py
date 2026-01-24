from __future__ import annotations

from datetime import datetime, time as dtime
from typing import Dict, List, Optional

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

SECRET = "CHANGE_ME_TO_SOMETHING_RANDOM"
ser = URLSafeSerializer(SECRET, salt="session")

SLOTS = [dtime(h, 0) for h in range(0, 24, 2)]  # 00:00..22:00

PROCESS_LIST = ["LINER", "REINFORCEMENT", "COVER"]

# Default parameters (starter pack). Manager can edit per run at creation.
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
        ("tape_tension", "Tape Tension", "N", "RANGE", 10, 30),
        ("wrap_angle", "Wrap Angle", "deg", "RANGE", 50, 70),
        ("line_speed", "Line Speed", "m/min", "MAX_ONLY", None, 500),
        ("overlap", "Overlap", "%", "RANGE", 5, 20),
        ("ambient_temp", "Ambient Temp", "°C", "RANGE", 15, 40),
        ("remark_only", "Process Notes", "", "INFO_ONLY", None, None),
    ],
    "COVER": [
        ("od_cover", "OD (Cover)", "mm", "RANGE", 110, 120),
        ("cover_thickness", "Cover Thickness", "mm", "RANGE", 2, 5),
        ("cooling_water", "Cooling Water", "°C", "MAX_ONLY", None, 35),
        ("line_speed_cover", "Line Speed", "m/min", "MAX_ONLY", None, 700),
        ("surface_finish", "Surface Finish OK", "", "INFO_ONLY", None, None),
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
    base_hour = actual.hour - (actual.hour % 2)
    prev_slot = dtime(base_hour, 0)

    idx = SLOTS.index(prev_slot) if prev_slot in SLOTS else 0
    next_slot = SLOTS[(idx + 1) % len(SLOTS)]

    def mins(t: dtime) -> int:
        return t.hour * 60 + t.minute

    a = mins(actual)
    p = mins(prev_slot)
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

@app.on_event("startup")
def on_startup():
    create_db_and_tables()
    # Seed demo users only once
    with Session(next(get_session()).get_bind()) as s:
        exists = s.exec(select(User)).first()
        if not exists:
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

    # Machines (MVP)
    for idx in [1, 2, 3, 4]:
        mn = (form.get(f"m{idx}_name") or "").strip()
        tg = (form.get(f"m{idx}_tag") or "").strip()
        if mn or tg:
            session.add(RunMachine(run_id=run.id, machine_name=mn, tag=tg or None))
    session.commit()

    # Parameters editable arrays
    p_keys = form.getlist("p_key")
    p_labels = form.getlist("p_label")
    p_units = form.getlist("p_unit")
    p_rules = form.getlist("p_rule")
    p_mins = form.getlist("p_min")
    p_maxs = form.getlist("p_max")

    def parse_float(x):
        x = (x or "").strip()
        if x == "":
            return None
        try:
            return float(x)
        except Exception:
            return None

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
            min_value=parse_float(p_mins[i] if i < len(p_mins) else None),
            max_value=parse_float(p_maxs[i] if i < len(p_maxs) else None),
            display_order=i,
        )
        session.add(rp)
    session.commit()

    session.add(AuditLog(run_id=run.id, actor_user_id=u.id, action="CREATE_RUN", reason=None))
    session.commit()

    return RedirectResponse(f"/runs/{run.id}", status_code=302)

# ---------------- RUN VIEW ----------------

@app.get("/runs/{run_id}", response_class=HTMLResponse)
def run_view(request: Request, run_id: int, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)

    run = session.get(ProductionRun, run_id)
    if not run:
        return RedirectResponse("/dashboard", status_code=302)

    params = session.exec(select(RunParameter).where(RunParameter.run_id == run_id).order_by(RunParameter.display_order)).all()
    machines = session.exec(select(RunMachine).where(RunMachine.run_id == run_id)).all()
    entries = session.exec(select(InspectionEntry).where(InspectionEntry.run_id == run_id).order_by(InspectionEntry.created_at)).all()

    slot_map: Dict[str, List[InspectionEntry]] = {s.strftime("%H:%M"): [] for s in SLOTS}
    for e in entries:
        slot_map[e.slot_time.strftime("%H:%M")].append(e)

    # Build values map: value_map[param_key][slot_str] = value (latest entry wins)
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

    return templates.TemplateResponse("entry_new.html", {"request": request, "user": u, "run": run, "params": params})

@app.post("/runs/{run_id}/entries/new")
async def entry_new_post(request: Request, run_id: int, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)

    run = session.get(ProductionRun, run_id)
    if not run or run.status != "OPEN":
        return RedirectResponse(f"/runs/{run_id}", status_code=302)

    form = await request.form()
    actual_time_str = (form.get("actual_time") or "").strip()  # HH:MM

    if ":" not in actual_time_str:
        return RedirectResponse(f"/runs/{run_id}", status_code=302)

    hh, mm = actual_time_str.split(":")
    at = dtime(int(hh), int(mm))
    slot = compute_slot(at)

    entry = InspectionEntry(
        run_id=run_id,
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
    session.add(AuditLog(run_id=run_id, actor_user_id=u.id, action="NEW_ENTRY", reason=None))
    session.commit()

    return RedirectResponse(f"/runs/{run_id}", status_code=302)

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
        session.add(AuditLog(run_id=run_id, actor_user_id=u.id, action="CLOSE_RUN", reason=None))
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
        session.add(AuditLog(run_id=run_id, actor_user_id=u.id, action="APPROVE_RUN", reason=None))
        session.commit()

    return RedirectResponse(f"/runs/{run_id}", status_code=302)

@app.post("/runs/{run_id}/reopen")
async def run_reopen(request: Request, run_id: int, session: Session = Depends(get_session)):
    """
    Manager can reopen from CLOSED or APPROVED -> OPEN (requires reason if APPROVED).
    """
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

# ---------------- EXPORT XLSX ----------------

@app.get("/runs/{run_id}/export-xlsx")
def export_xlsx(request: Request, run_id: int, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)

    run = session.get(ProductionRun, run_id)
    if not run:
        return RedirectResponse("/dashboard", status_code=302)

    template_path = TEMPLATE_XLSX_MAP.get(run.process, TEMPLATE_XLSX_MAP["LINER"])
    if not os.path.exists(template_path):
        return HTMLResponse(
            f"Template not found at <b>{template_path}</b>. Upload the XLSX there and redeploy.",
            status_code=500,
        )

    wb = load_workbook(template_path)
    ws = wb.active  # assume first sheet

    # Header fill (best-effort, matches your liner template positions)
    # If other templates differ, header still writes in these positions safely.
    ws["D5"] = run.dhtp_batch_no
    ws["I5"] = run.client_name
    ws["I6"] = run.po_number
    ws["D6"] = run.pipe_specification
    ws["D7"] = run.raw_material_spec
    ws["D8"] = run.raw_material_batch_no
    ws["D9"] = run.itp_number

    machines = session.exec(select(RunMachine).where(RunMachine.run_id == run_id)).all()
    for i, m in enumerate(machines[:6]):
        ws[f"M{5+i}"] = f"{m.machine_name}{f' ({m.tag})' if m.tag else ''}"

    # Fill last inspector/operators (rows 38-40)
    last_entry = session.exec(
        select(InspectionEntry).where(InspectionEntry.run_id == run_id).order_by(InspectionEntry.created_at.desc())
    ).first()
    if last_entry:
        inspector = session.get(User, last_entry.inspector_user_id)
        ws["B38"] = inspector.name if inspector else ""
        ws["B39"] = last_entry.operator1 or ""
        ws["B40"] = last_entry.operator2 or ""

    # Try to write times header (row 22 col E..P)
    for i, sl in enumerate(SLOTS):
        col = get_column_letter(5 + i)  # E=5
        ws[f"{col}22"] = sl.strftime("%H:%M")

    # Fill grid values: parameters assumed in column B rows 23+
    params = session.exec(select(RunParameter).where(RunParameter.run_id == run_id).order_by(RunParameter.display_order)).all()
    entries = session.exec(select(InspectionEntry).where(InspectionEntry.run_id == run_id)).all()
    entry_ids = [e.id for e in entries]
    entry_slot = {e.id: e.slot_time.strftime("%H:%M") for e in entries}

    vals = session.exec(select(InspectionValue).where(InspectionValue.entry_id.in_(entry_ids))).all() if entry_ids else []
    # latest wins
    value_map: Dict[str, Dict[str, float]] = {}
    for v in vals:
        slot = entry_slot.get(v.entry_id)
        if not slot:
            continue
        value_map.setdefault(v.param_key, {})[slot] = v.value

    start_row = 23
    for r_i, p in enumerate(params):
        row = start_row + r_i
        # write label if empty
        if ws[f"B{row}"].value in (None, ""):
            ws[f"B{row}"] = p.label

        for s_i, slot_str in enumerate([s.strftime("%H:%M") for s in SLOTS]):
            v = value_map.get(p.param_key, {}).get(slot_str)
            if v is None:
                continue
            col = get_column_letter(5 + s_i)
            ws[f"{col}{row}"] = v

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    tmp_path = tmp.name
    tmp.close()
    wb.save(tmp_path)

    filename = f"{run.process}_{run.dhtp_batch_no}_{run.status}.xlsx"
    return FileResponse(tmp_path, filename=filename, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
