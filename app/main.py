from __future__ import annotations

import os
import tempfile
from copy import copy as _copy
from datetime import datetime, date, time as dtime, timedelta
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, Request, Depends
from fastapi.responses import RedirectResponse, HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from itsdangerous import URLSafeSerializer
from sqlmodel import Session, select

from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.drawing.image import Image as XLImage

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

SLOTS = [dtime(h, 0) for h in range(0, 24, 2)]  # 00:00..22:00
PROCESS_LIST = ["LINER", "REINFORCEMENT", "COVER"]


def parse_float(x: str) -> Optional[float]:
    x = (x or "").strip()
    if x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None


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


def compute_slot_and_date(actual_date: date, actual_time: dtime) -> Tuple[date, dtime]:
    """
    Slots fixed: 00:00, 02:00, ... 22:00
    Cutoff is 30 minutes before the next slot
    23:31–24:00 goes to 00:00 of NEXT DAY
    """
    base_hour = actual_time.hour - (actual_time.hour % 2)
    prev_slot = dtime(base_hour, 0)
    idx = SLOTS.index(prev_slot) if prev_slot in SLOTS else 0
    next_slot = SLOTS[(idx + 1) % len(SLOTS)]

    def mins(t: dtime) -> int:
        return t.hour * 60 + t.minute

    a = mins(actual_time)
    n = mins(next_slot)

    wrap_next_day = (prev_slot == dtime(22, 0) and next_slot == dtime(0, 0))
    if wrap_next_day:
        n = 24 * 60

    cutoff = n - 30
    if a > cutoff:
        if wrap_next_day:
            return (actual_date + timedelta(days=1), dtime(0, 0))
        return (actual_date, next_slot)
    return (actual_date, prev_slot)


def get_production_days(session: Session, run_id: int) -> List[date]:
    days = session.exec(
        select(InspectionEntry.actual_date).where(InspectionEntry.run_id == run_id)
    ).all()
    return sorted(set(days))


# ---------- DEFAULT PARAMETERS ----------
# IMPORTANT: Liner + Cover are identical and MUST include Length
LINER_PARAMS = [
    ("length_m", "Length ( Mtr )", "m", "INFO_ONLY", None, None),
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
]

REINF_PARAMS = [
    ("outer_diameter_mm", "Outer Diameter (mm)", "mm", "INFO_ONLY", None, None),
    ("length_m", "Length ( Mtr )", "m", "INFO_ONLY", None, None),

    ("annular_od_70_1", 'Annular OD (∠ 70°) 1', "mm", "RANGE", 113.0, 114.0),
    ("annular_od_70_2", 'Annular OD (∠ 70°) 2', "mm", "RANGE", 113.0, 114.0),

    ("annular_od_45_3", 'Annular OD (∠ 45°) 3', "mm", "RANGE", 117.2, 117.8),
    ("annular_od_45_4", 'Annular OD (∠ 45°) 4', "mm", "RANGE", 121.2, 121.8),

    ("annular_od_extra_5", "Annular OD (Extra) 5", "mm", "INFO_ONLY", None, None),
    ("annular_od_extra_6", "Annular OD (Extra) 6", "mm", "INFO_ONLY", None, None),

    ("tractor_speed_m_min", "Tractor Speed (m/min)", "m/min", "INFO_ONLY", None, None),
    ("clamping_gas_p1_mpa", "Clamping Gas Pressure (MPa) 1", "MPa", "RANGE", 0.2, 0.3),
    ("clamping_gas_p2_mpa", "Clamping Gas Pressure (MPa) 2", "MPa", "RANGE", 0.2, 0.3),
    ("thrust_gas_p_mpa", "Thrust Gas Pressure (MPa)", "MPa", "RANGE", 0.2, 0.5),
]

DEFAULTS = {
    "LINER": LINER_PARAMS,
    "COVER": LINER_PARAMS,
    "REINFORCEMENT": REINF_PARAMS,
}

IMAGE_MAP = {
    "LINER": "/static/images/liner.png",
    "REINFORCEMENT": "/static/images/reinforcement.png",
    "COVER": "/static/images/cover.png",
}

# Your exact template folder location
TEMPLATE_XLSX_MAP = {
    "LINER": os.path.join("app", "templates", "templates_xlsx", "liner.xlsx"),
    "REINFORCEMENT": os.path.join("app", "templates", "templates_xlsx", "reinforcement.xlsx"),
    "COVER": os.path.join("app", "templates", "templates_xlsx", "cover.xlsx"),
}

# Liner/Cover Excel rows from your screenshot
LINER_ROW_MAP = {
    "length_m": 22,
    "od_mm": 23,
    "wall_thickness_mm": 24,
    "cooling_water_c": 25,
    "line_speed_m_min": 26,
    "tractor_pressure_mpa": 27,
    "body_temp_z1_c": 28,
    "body_temp_z2_c": 29,
    "body_temp_z3_c": 30,
    "body_temp_z4_c": 31,
    "body_temp_z5_c": 32,
    "noising_temp_z1_c": 33,
    "noising_temp_z2_c": 34,
    "noising_temp_z3_c": 35,
}


def _slot_col_letter(slot_index: int) -> str:
    # slot_index 0..11 -> E..P
    return get_column_letter(5 + slot_index)


def _copy_sheet_images(src_ws, dst_ws):
    # Preserve images from template sheet into copied sheet
    if not getattr(src_ws, "_images", None):
        return
    for img in src_ws._images:
        new_img = XLImage(img.ref)
        new_img.anchor = _copy(img.anchor)
        dst_ws.add_image(new_img)


@app.on_event("startup")
def on_startup():
    create_db_and_tables()
    # create demo users if none exist
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


# ---------------- DASHBOARD (with progress) ----------------

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)

    runs = session.exec(select(ProductionRun).order_by(ProductionRun.created_at.desc())).all()

    def run_progress_percent(run: ProductionRun) -> Optional[int]:
        if not run.total_pipe_length_m or run.total_pipe_length_m <= 0:
            return None

        entry_ids = session.exec(select(InspectionEntry.id).where(InspectionEntry.run_id == run.id)).all()
        if not entry_ids:
            return 0

        vals = session.exec(
            select(InspectionValue.value)
            .where(InspectionValue.entry_id.in_(entry_ids), InspectionValue.param_key == "length_m")
        ).all()
        if not vals:
            return 0

        mx = max(vals)
        pct = int(min(100, max(0, (mx / run.total_pipe_length_m) * 100)))
        return pct

    grouped = {}
    for r in runs:
        grouped.setdefault(r.dhtp_batch_no, []).append({"run": r, "percent": run_progress_percent(r)})

    order = {"LINER": 1, "REINFORCEMENT": 2, "COVER": 3}
    for b in grouped:
        grouped[b].sort(key=lambda x: order.get(x["run"].process, 99))

    return templates.TemplateResponse("dashboard.html", {"request": request, "user": u, "grouped": grouped})


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

    return templates.TemplateResponse(
        "run_new.html",
        {"request": request, "user": u, "process": process, "defaults": DEFAULTS[process], "error": None},
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
            {"request": request, "user": u, "process": process, "defaults": DEFAULTS[process], "error": "DHTP Batch No is required"},
        )

    # ✅ THIS IS WHERE total length is saved (you asked where exactly)
    total_len = parse_float(form.get("total_pipe_length_m") or "")

    run = ProductionRun(
        process=process,
        dhtp_batch_no=dhtp_batch_no,
        client_name=(form.get("client_name") or "").strip(),
        po_number=(form.get("po_number") or "").strip(),
        itp_number=(form.get("itp_number") or "").strip(),
        pipe_specification=(form.get("pipe_specification") or "").strip(),
        raw_material_spec=(form.get("raw_material_spec") or "").strip(),
        raw_material_batch_no=(form.get("raw_material_batch_no") or "").strip(),
        total_pipe_length_m=total_len,  # ✅ stored here
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

    # Params (limits)
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


# ---------------- RUN VIEW ----------------

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

    production_days = get_production_days(session, run_id)
    if not production_days:
        selected_date = None
        day_index = 1
    else:
        if day is None:
            day_index = len(production_days)
        else:
            day_index = max(1, min(int(day), len(production_days)))
        selected_date = production_days[day_index - 1]

    if selected_date:
        entries = session.exec(
            select(InspectionEntry)
            .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date == selected_date)
            .order_by(InspectionEntry.created_at)
        ).all()
    else:
        entries = []

    entry_ids = [e.id for e in entries]
    entry_slot = {e.id: e.slot_time.strftime("%H:%M") for e in entries}
    value_map: Dict[str, Dict[str, float]] = {}

    vals = session.exec(select(InspectionValue).where(InspectionValue.entry_id.in_(entry_ids))).all() if entry_ids else []
    for v in vals:
        slot_str = entry_slot.get(v.entry_id)
        if not slot_str:
            continue
        value_map.setdefault(v.param_key, {})[slot_str] = v.value

    img_url = IMAGE_MAP.get(run.process, IMAGE_MAP["LINER"])

    return templates.TemplateResponse(
        "run_view.html",
        {
            "request": request,
            "user": u,
            "run": run,
            "params": params,
            "machines": machines,
            "slots": [s.strftime("%H:%M") for s in SLOTS],
            "value_map": value_map,
            "img_url": img_url,
            "production_days": production_days,
            "day_index": day_index,
            "selected_date": selected_date,
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

    actual_date_str = (form.get("actual_date") or "").strip()
    actual_time_str = (form.get("actual_time") or "").strip()

    try:
        ad = datetime.strptime(actual_date_str, "%Y-%m-%d").date()
    except Exception:
        ad = date.today()

    hh, mm = actual_time_str.split(":")
    at = dtime(int(hh), int(mm))

    slot_date, slot_time = compute_slot_and_date(ad, at)

    entry = InspectionEntry(
        run_id=run_id,
        actual_date=slot_date,
        actual_time=at,
        slot_time=slot_time,
        inspector_user_id=u.id,
        operator1=(form.get("operator1") or "").strip() or None,
        operator2=(form.get("operator2") or "").strip() or None,
        operator_annular12=(form.get("operator_annular12") or "").strip() or None,
        operator_intext34=(form.get("operator_intext34") or "").strip() or None,
        remark=(form.get("remark") or "").strip() or None,
    )
    session.add(entry)
    session.commit()
    session.refresh(entry)

    # ✅ Save Pipe Length into "length_m"
    pl = parse_float(form.get("pipe_length_m") or "")
    if pl is not None:
        session.add(InspectionValue(entry_id=entry.id, param_key="length_m", value=pl))
        session.commit()

    # Save readings
    params = session.exec(select(RunParameter).where(RunParameter.run_id == run_id)).all()
    for p in params:
        if p.param_key == "length_m":
            continue
        raw = (form.get(f"val_{p.param_key}") or "").strip()
        if raw == "":
            continue
        try:
            v = float(raw)
        except Exception:
            continue
        session.add(InspectionValue(entry_id=entry.id, param_key=p.param_key, value=v))
    session.commit()

    production_days = get_production_days(session, run_id)
    day_index = production_days.index(slot_date) + 1 if slot_date in production_days else 1
    return RedirectResponse(f"/runs/{run_id}?day={day_index}", status_code=302)


# ---------------- EXPORT XLSX ----------------

@app.get("/runs/{run_id}/export-xlsx")
def export_xlsx(run_id: int, request: Request, day: int | None = None, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)

    run = session.get(ProductionRun, run_id)
    if not run:
        return HTMLResponse("Run not found", status_code=404)

    template_path = TEMPLATE_XLSX_MAP.get(run.process)
    if not template_path or not os.path.exists(template_path):
        return HTMLResponse(f"Template not found: {template_path}", status_code=500)

    wb = load_workbook(template_path)
    ws = wb.worksheets[0]

    # Keep images in the first sheet
    _copy_sheet_images(ws, ws)

    # Get production days
    production_days = get_production_days(session, run_id)
    if production_days and day is not None:
        di = max(1, min(int(day), len(production_days)))
        production_days = [production_days[di - 1]]

    # Keep one sheet then copy for days
    while len(wb.worksheets) > 1:
        wb.remove(wb.worksheets[-1])

    if not production_days:
        ws.title = "Day 1"
        day_sheets = [(1, date.today(), ws)]
    else:
        ws.title = "Day 1"
        day_sheets = [(1, production_days[0], ws)]
        for i in range(2, len(production_days) + 1):
            new_ws = wb.copy_worksheet(ws)
            new_ws.title = f"Day {i}"
            _copy_sheet_images(ws, new_ws)
            day_sheets.append((i, production_days[i - 1], new_ws))

    params = session.exec(select(RunParameter).where(RunParameter.run_id == run_id).order_by(RunParameter.display_order)).all()
    slot_strs = [s.strftime("%H:%M") for s in SLOTS]

    def fill_sheet(ws2, day_date: date):
        # read entries for that day
        entries = session.exec(
            select(InspectionEntry)
            .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date == day_date)
            .order_by(InspectionEntry.created_at)
        ).all()

        entry_ids = [e.id for e in entries]
        entry_slot = {e.id: e.slot_time.strftime("%H:%M") for e in entries}

        vals = session.exec(select(InspectionValue).where(InspectionValue.entry_id.in_(entry_ids))).all() if entry_ids else []

        value_map: dict[str, dict[str, float]] = {}
        for v in vals:
            slot_str = entry_slot.get(v.entry_id)
            if slot_str:
                value_map.setdefault(v.param_key, {})[slot_str] = v.value

        # Liner/Cover mapping (exact rows)
        if run.process in ("LINER", "COVER"):
            for p in params:
                row = LINER_ROW_MAP.get(p.param_key)
                if not row:
                    continue
                for s_i, ss in enumerate(slot_strs):
                    v = value_map.get(p.param_key, {}).get(ss)
                    if v is None:
                        continue
                    col = _slot_col_letter(s_i)
                    ws2[f"{col}{row}"] = v

    for (_, dday, ws2) in day_sheets:
        fill_sheet(ws2, dday)

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    tmp_path = tmp.name
    tmp.close()
    wb.save(tmp_path)

    filename = f"{run.process}_{run.dhtp_batch_no}_ALL_DAYS.xlsx" if day is None else f"{run.process}_{run.dhtp_batch_no}_DAY_{day}.xlsx"
    return FileResponse(tmp_path, filename=filename,
                        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
