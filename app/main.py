from __future__ import annotations

from datetime import datetime, time as dtime
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, Request, Depends, Form
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from itsdangerous import URLSafeSerializer
from sqlmodel import Session, select

from app.db import create_db_and_tables, get_session
from app.models import User, ProductionRun, RunParameter, InspectionEntry
from app.auth import hash_password, verify_password

app = FastAPI()
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

# Session cookie signing (prototype)
SECRET = "CHANGE_ME_TO_A_RANDOM_SECRET"
ser = URLSafeSerializer(SECRET, salt="session")

SLOTS = [dtime(h, 0) for h in range(0, 24, 2)]  # 00:00..22:00

LINER_DEFAULT_PARAMS = [
    ("od_mm", "OD (mm)", "mm", "RANGE", 105.0, 106.0),
    ("wall_thickness_mm", "Wall Thickness (mm)", "mm", "RANGE", 7.0, 7.4),  # treat as range
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
    Rule:
      Each slot is a 2-hour window.
      If time is within last 30 mins before next slot => next slot.
    """
    # Find the previous slot (floor to even hour)
    base_hour = actual.hour - (actual.hour % 2)
    prev_slot = dtime(base_hour, 0)

    # Next slot
    next_index = (SLOTS.index(prev_slot) + 1) if prev_slot in SLOTS else 0
    if next_index >= len(SLOTS):
        next_slot = dtime(0, 0)
    else:
        next_slot = SLOTS[next_index]

    # Cutoff = next_slot - 00:30 (handle wrap)
    # We'll compute in minutes since midnight
    def to_min(t: dtime) -> int:
        return t.hour * 60 + t.minute

    a = to_min(actual)
    p = to_min(prev_slot)
    n = to_min(next_slot)
    if next_slot == dtime(0, 0) and prev_slot == dtime(22, 0):
        n = 24 * 60  # treat midnight as 24:00 for cutoff check

    cutoff = n - 30
    # If actual is after cutoff => use next slot
    if a > cutoff:
        # wrap midnight
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

def require_login(request: Request, session: Session) -> User:
    u = get_current_user(request, session)
    if not u:
        raise RuntimeError("Not logged in")
    return u

@app.on_event("startup")
def on_startup():
    create_db_and_tables()

    # seed users once
    from sqlmodel import Session as _S
    from app.db import engine
    with _S(engine) as s:
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

@app.get("/runs/new", response_class=HTMLResponse)
def run_new_get(request: Request, session: Session = Depends(get_session)):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)
    if u.role != "MANAGER":
        return RedirectResponse("/dashboard", status_code=302)

    return templates.TemplateResponse("run_new.html", {"request": request, "user": u, "defaults": LINER_DEFAULT_PARAMS})

@app.post("/runs/new")
def run_new_post(
    request: Request,
    run_code: str = Form(...),
    client_name: str = Form(...),
    po_number: str = Form(...),
    dhtp_batch_no: str = Form(...),
    pipe_specification: str = Form(...),
    raw_material_spec: str = Form(...),
    raw_material_batch_no_current: str = Form(...),
    itp_number: str = Form(...),
    validation_mode: str = Form("SOFT"),
    # limits are posted as arrays
    session: Session = Depends(get_session),
):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)
    if u.role != "MANAGER":
        return RedirectResponse("/dashboard", status_code=302)

    run = ProductionRun(
        run_code=run_code,
        client_name=client_name,
        po_number=po_number,
        dhtp_batch_no=dhtp_batch_no,
        pipe_specification=pipe_specification,
        raw_material_spec=raw_material_spec,
        raw_material_batch_no_current=raw_material_batch_no_current,
        itp_number=itp_number,
        validation_mode=validation_mode,
        created_by=u.id,
    )
    session.add(run)
    session.commit()
    session.refresh(run)

    # Create run parameters using defaults (manager can edit later; MVP uses defaults)
    for i, (key, label, unit, rule, mn, mx) in enumerate(LINER_DEFAULT_PARAMS):
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

    entries = session.exec(
        select(InspectionEntry).where(InspectionEntry.production_run_id == run_id).order_by(InspectionEntry.entered_at)
    ).all()

    # Build grid: param rows x slot columns (simple: show entry count per slot + last entry time)
    # MVP: show slot entries list
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
            "slots": [s.strftime("%H:%M") for s in SLOTS],
            "slot_map": slot_map,
        },
    )

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
def entry_new_post(
    request: Request,
    run_id: int,
    actual_time: str = Form(...),  # "HH:MM"
    remark: str = Form(""),
    session: Session = Depends(get_session),
):
    u = get_current_user(request, session)
    if not u:
        return RedirectResponse("/login", status_code=302)

    run = session.get(ProductionRun, run_id)
    if not run or run.status != "OPEN":
        return RedirectResponse(f"/runs/{run_id}", status_code=302)

    hh, mm = actual_time.split(":")
    at = dtime(int(hh), int(mm))
    slot = compute_slot(at)

    entry = InspectionEntry(
        production_run_id=run_id,
        actual_time=at,
        slot_time=slot,
        entered_by=u.id,
        remark=remark.strip() or None,
    )
    session.add(entry)
    session.commit()

    return RedirectResponse(f"/runs/{run_id}", status_code=302)

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

@app.post("/runs/{run_id}/reopen")
def run_reopen(request: Request, run_id: int, session: Session = Depends(get_session)):
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
