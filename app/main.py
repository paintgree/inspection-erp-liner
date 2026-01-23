from __future__ import annotations

from datetime import datetime, time as dtime
from typing import Dict, List, Optional

from fastapi import FastAPI, Request, Depends, Form
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from itsdangerous import URLSafeSerializer
from sqlmodel import Session, select

from openpyxl import load_workbook
from fastapi.responses import FileResponse
import os
import tempfile

from app.db import create_db_and_tables, get_session
from app.models import User, ProductionRun, RunParameter, InspectionEntry, RunMachine
from app.auth import hash_password, verify_password


app = FastAPI()
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

# Session cookie signing (prototype)
SECRET = "CHANGE_ME_TO_A_RANDOM_SECRET"
ser = URLSafeSerializer(SECRET, salt="session")

# Fixed 2-hour slots
SLOTS = [dtime(h, 0) for h in range(0, 24, 2)]  # 00:00..22:00

# Defaults (Manager can modify at run creation)
LINER_DEFAULT_PARAMS = [
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


def compute_slot(actual: dtime) -> dtime:
    """
    Slot assignment (2-hour slots) with 30-min cutoff before the NEXT slot.
    - Each slot is a 2-hour window.
    - If time is within last 30 mins before next slot => next slot.
    """
    base_hour = actual.hour - (actual.hour % 2)
    prev_slot = dtime(base_hour, 0)

    # Find next slot with wrap
    try:
        idx = SLOTS.index(prev_slot)
    except ValueError:
        idx = 0
    next_slot = SLOTS[(idx + 1) % len(SLOTS)]

    def to_min(t: dtime) -> int:
        return t.hour * 60 + t.minute

    a = to_min(actual)
    p = to_min(prev_slot)
    n = to_min(next_slot)

    # handle wrap 22:00 -> 24:00
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

    # Seed demo users (only if no users exist)
    with Session(next(get_session()).get_bind()) as s:  # safe quick session
        existing = s.exec(select(User)).first()
        if not existing:
            manager = User(
                email="manager@demo.com",
                name="Manager",
                role="MANAGER",
                password_hash=hash_password("manager123"),
            )
            inspector = User(
                email="inspector@demo.com",
                name="Inspector",
                role="INSPECTOR",
                password_hash=hash_password("inspector123"),
            )
            s.add(manager)
            s.add(inspector)
            s.commit()


# ---------------------- AUTH ----------------------

@app.get("/", response_class=HTMLResponse)
def root(request: Request, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)
    return RedirectResponse("/dashboard", status_code=302)


@app.get("/login", response_class=HTMLResponse)
def login_get(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/login")
def login_post(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    session: Session = Depends(get_session),
):
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


# ---------------------- DASHBOARD ----------------------

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)

    runs = session.exec(select(ProductionRun).order_by(ProductionRun.created_at.desc())).all()
    open_count = len([r for r in runs if r.status == "OPEN"])
    closed_count = len([r for r in runs if r.status == "CLOSED"])
    approved_count = len([r for r in runs if r.status == "APPROVED"])

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "user": u,
            "runs": runs,
            "open_count": open_count,
            "closed_count": closed_count,
            "approved_count": approved_count,
        },
    )


# ---------------------- RUN CREATE ----------------------

@app.get("/runs/new", response_class=HTMLResponse)
def run_new_get(request: Request, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)
    if u.role != "MANAGER":
        return RedirectResponse("/dashboard", status_code=302)

    return templates.TemplateResponse(
        "run_new.html",
        {"request": request, "user": u, "defaults": LINER_DEFAULT_PARAMS},
    )


@app.post("/runs/new")
async def run_new_post(request: Request, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)
    if u.role != "MANAGER":
        return RedirectResponse("/dashboard", status_code=302)

    form = await request.form()

    # DHTP Batch No is the run reference (unique)
    dhtp_batch_no = (form.get("dhtp_batch_no") or "").strip()
    if not dhtp_batch_no:
        # fallback: return page with error
        return templates.TemplateResponse(
            "run_new.html",
            {"request": request, "user": u, "defaults": LINER_DEFAULT_PARAMS, "error": "DHTP Batch No is required"},
        )

    # Prevent duplicates
    existing = session.exec(select(ProductionRun).where(ProductionRun.dhtp_batch_no == dhtp_batch_no)).first()
    if existing:
        return templates.TemplateResponse(
            "run_new.html",
            {"request": request, "user": u, "defaults": LINER_DEFAULT_PARAMS, "error": "Batch already exists"},
        )

    run = ProductionRun(
        dhtp_batch_no=dhtp_batch_no,
        client_name=(form.get("client_name") or "").strip(),
        po_number=(form.get("po_number") or "").strip(),
        pipe_specification=(form.get("pipe_specification") or "").strip(),
        raw_material_spec=(form.get("raw_material_spec") or "").strip(),
        raw_material_batch_no_current=(form.get("raw_material_batch_no_current") or "").strip(),
        itp_number=(form.get("itp_number") or "").strip(),
        validation_mode=(form.get("validation_mode") or "SOFT"),
        created_by=u.id,
    )

    session.add(run)
    session.commit()
    session.refresh(run)

    # Save machines (MVP: m1/m2/m3)
    for idx in [1, 2, 3]:
        mn = (form.get(f"m{idx}_name") or "").strip()
        tg = (form.get(f"m{idx}_tag") or "").strip()
        if mn or tg:
            session.add(RunMachine(production_run_id=run.id, machine_name=mn, tag=tg))
    session.commit()

    # Save run parameters from editable table arrays
    # The template uses repeated input names: p_key, p_rule, p_min, p_max, p_unit, p_label
    p_keys = form.getlist("p_key")
    p_rules = form.getlist("p_rule")
    p_mins = form.getlist("p_min")
    p_maxs = form.getlist("p_max")
    p_units = form.getlist("p_unit")
    p_labels = form.getlist("p_label")

    for i, key in enumerate(p_keys):
        key = (key or "").strip()
        if not key:
            continue

        rule = (p_rules[i] if i < len(p_rules) else "RANGE").strip()
        unit = (p_units[i] if i < len(p_units) else "").strip()
        label = (p_labels[i] if i < len(p_labels) else key).strip()

        def parse_float(x):
            x = (x or "").strip()
            if x == "":
                return None
            try:
                return float(x)
            except Exception:
                return None

        mn = parse_float(p_mins[i] if i < len(p_mins) else None)
        mx = parse_float(p_maxs[i] if i < len(p_maxs) else None)

        rp = RunParameter(
            production_run_id=run.id,
            param_key=key,
            label=label,
            unit=unit,
            rule=rule,
            min_value=mn,
            max_value=mx,
            is_active=True,
            display_order=i,
        )
        session.add(rp)

    session.commit()

    return RedirectResponse(f"/runs/{run.id}", status_code=302)


# ---------------------- RUN VIEW ----------------------

@app.get("/runs/{run_id}", response_class=HTMLResponse)
def run_view(request: Request, run_id: int, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)

    run = session.get(ProductionRun, run_id)
    if not run:
        return RedirectResponse("/dashboard", status_code=302)

    params = session.exec(
        select(RunParameter).where(RunParameter.production_run_id == run_id).order_by(RunParameter.display_order)
    ).all()

    machines = session.exec(select(RunMachine).where(RunMachine.production_run_id == run_id)).all()

    entries = session.exec(
        select(InspectionEntry).where(InspectionEntry.production_run_id == run_id).order_by(InspectionEntry.entered_at)
    ).all()

    slot_map: Dict[str, List[InspectionEntry]] = {s.strftime("%H:%M"): [] for s in SLOTS}
    for e in entries:
        slot_map[e.slot_time.strftime("%H:%M")].append(e)

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
        },
    )


# ---------------------- ENTRY CREATE ----------------------

@app.get("/runs/{run_id}/entries/new", response_class=HTMLResponse)
def entry_new_get(request: Request, run_id: int, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)

    run = session.get(ProductionRun, run_id)
    if not run or run.status != "OPEN":
        return RedirectResponse(f"/runs/{run_id}", status_code=302)

    return templates.TemplateResponse(
        "entry_new.html",
        {"request": request, "user": u, "run": run},
    )


@app.post("/runs/{run_id}/entries/new")
async def entry_new_post(request: Request, run_id: int, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)

    run = session.get(ProductionRun, run_id)
    if not run or run.status != "OPEN":
        return RedirectResponse(f"/runs/{run_id}", status_code=302)

    form = await request.form()

    actual_time = (form.get("actual_time") or "").strip()  # "HH:MM"
    if ":" not in actual_time:
        return RedirectResponse(f"/runs/{run_id}", status_code=302)

    hh, mm = actual_time.split(":")
    at = dtime(int(hh), int(mm))
    slot = compute_slot(at)

    remark = (form.get("remark") or "").strip() or None
    op1 = (form.get("op1") or "").strip() or None
    op2 = (form.get("op2") or "").strip() or None

    entry = InspectionEntry(
        production_run_id=run_id,
        actual_time=at,
        slot_time=slot,
        inspector_user_id=u.id,  # auto from login
        operator_hopper_extruder=op1,
        operator_cooling_accumulator=op2,
        remark=remark,
    )

    session.add(entry)
    session.commit()

    return RedirectResponse(f"/runs/{run_id}", status_code=302)


# ---------------------- WORKFLOW (CLOSE / APPROVE / REOPEN) ----------------------

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
        session.commit()

    return RedirectResponse(f"/runs/{run_id}", status_code=302)


@app.post("/runs/{run_id}/reopen_closed")
def run_reopen_closed(request: Request, run_id: int, session: Session = Depends(get_session)):
    """
    CLOSED -> OPEN (Manager)
    """
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)
    if u.role != "MANAGER":
        return RedirectResponse(f"/runs/{run_id}", status_code=302)

    run = session.get(ProductionRun, run_id)
    if run and run.status == "CLOSED":
        run.status = "OPEN"
        session.add(run)
        session.commit()

    return RedirectResponse(f"/runs/{run_id}", status_code=302)


@app.post("/runs/{run_id}/reopen_approved")
async def run_reopen_approved(request: Request, run_id: int, session: Session = Depends(get_session)):
    """
    APPROVED -> OPEN (Manager only) with required reason.
    (Audit table not implemented fully yet — but reason is required.)
    """
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)
    if u.role != "MANAGER":
        return RedirectResponse(f"/runs/{run_id}", status_code=302)

    run = session.get(ProductionRun, run_id)
    if not run or run.status != "APPROVED":
        return RedirectResponse(f"/runs/{run_id}", status_code=302)

    form = await request.form()
    reason = (form.get("reason") or "").strip()
    if not reason:
        # stay on run view, show a hint via query param
        return RedirectResponse(f"/runs/{run_id}?reopen_error=1", status_code=302)

    # reopen
    run.status = "OPEN"
    session.add(run)
    session.commit()

    # Minimal audit: store reason in the latest entry remark if exists (temporary)
    # Proper audit log table can be added next iteration.
    latest_entry = session.exec(
        select(InspectionEntry).where(InspectionEntry.production_run_id == run_id).order_by(InspectionEntry.entered_at.desc())
    ).first()
    if latest_entry:
        latest_entry.remark = (latest_entry.remark or "") + f"\n[REOPEN APPROVED by {u.name} at {datetime.utcnow().isoformat()}] Reason: {reason}"
        session.add(latest_entry)
        session.commit()

    return RedirectResponse(f"/runs/{run_id}", status_code=302)


# ---------------------- EXPORT (XLSX - OFFICIAL TEMPLATE) ----------------------

@app.get("/runs/{run_id}/export-xlsx")
def export_xlsx(request: Request, run_id: int, session: Session = Depends(get_session)):
    """
    Export using the exact official Excel template layout.
    For now: fills header + machines + inspector/operator names from last entry.
    (Next iteration: fill the measurement grid values per slot exactly.)
    """
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)

    run = session.get(ProductionRun, run_id)
    if not run:
        return RedirectResponse("/dashboard", status_code=302)

    machines = session.exec(select(RunMachine).where(RunMachine.production_run_id == run_id)).all()
    last_entry = session.exec(
        select(InspectionEntry).where(InspectionEntry.production_run_id == run_id).order_by(InspectionEntry.entered_at.desc())
    ).first()

    template_path = os.path.join("app", "templates_xlsx", "liner_template.xlsx")
    if not os.path.exists(template_path):
        # If template missing, show a friendly HTML message
        return HTMLResponse(
            content=(
                "Template not found. Upload your official liner template to: "
                "<b>app/templates_xlsx/liner_template.xlsx</b> and redeploy."
            ),
            status_code=500,
        )

    wb = load_workbook(template_path)
    # Use Day1 sheet as export base (keeps approved formatting)
    sheet_name = "In-process (Liner) Day1"
    ws = wb[sheet_name] if sheet_name in wb.sheetnames else wb.active

    # ---- Fill header cells (based on your official template cell positions)
    # Labels found in template:
    # A5 DHTP Batch -> value in D5
    # G5 Client -> value in I5
    # G6 PO -> value in I6
    # A6 Pipe Spec -> value in D6
    # A7 Raw Mat Spec -> value in D7
    # A8 Raw Mat Batch -> value in D8
    # A9 ITP -> value in D9

    ws["D5"] = run.dhtp_batch_no
    ws["I5"] = run.client_name
    ws["I6"] = run.po_number
    ws["D6"] = run.pipe_specification
    ws["D7"] = run.raw_material_spec
    ws["D8"] = run.raw_material_batch_no_current
    ws["D9"] = run.itp_number

    # ---- Fill machines used (template list appears around M5..)
    # We'll fill sequentially starting at M5 (name) and keep tag in P column if exists later.
    # To keep it simple: write "Name - Tag" in machine rows.
    start_row = 5
    col = "M"
    for i, m in enumerate(machines[:6]):  # limit to 6 rows
        r = start_row + i
        text = (m.machine_name or "").strip()
        if m.tag:
            text = f"{text} ({m.tag})" if text else f"{m.tag}"
        ws[f"{col}{r}"] = text

    # ---- Fill inspector/operator names (rows 38-40)
    # We'll write values to B column (safe and readable)
    if last_entry:
        inspector = session.get(User, last_entry.inspector_user_id)
        ws["B38"] = inspector.name if inspector else "Inspector"
        ws["B39"] = last_entry.operator_hopper_extruder or ""
        ws["B40"] = last_entry.operator_cooling_accumulator or ""
    else:
        ws["B38"] = ""
        ws["B39"] = ""
        ws["B40"] = ""

    # NOTE: Next iteration will fill measurement values into the correct grid cells per slot
    # using your “system decides where it goes” rule.

    # Save to temp file and return
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    tmp_path = tmp.name
    tmp.close()
    wb.save(tmp_path)

    filename = f"LINER_{run.dhtp_batch_no}_{run.status}.xlsx"
    return FileResponse(tmp_path, filename=filename, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
