from __future__ import annotations

import json
import math
import os
from datetime import date, datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Field, SQLModel, Session, select

from .db import get_session
from .models import User

router = APIRouter(prefix="/rnd", tags=["R&D Qualification"])
TEMPLATES = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))
RCRT_HOURS = 175000.0
CYCLIC_BASIS_CYCLES = 1_000_000.0
DESIGN_FACTOR_NONMETALLIC = 0.67


class RndQualificationProgram(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    program_code: str = Field(default="", index=True)
    title: str = Field(default="", index=True)
    product_family: str = Field(default="LLRTP-PE-RT-PET-PE100")
    qualification_standard: str = Field(default="API 15S R3")
    reinforcement_type: str = Field(default="NONMETALLIC")

    nominal_size_in: float = Field(default=4.0, index=True)
    npr_mpa: float = Field(default=10.0)
    maot_c: float = Field(default=65.0)
    laot_c: float = Field(default=0.0)
    design_life_hours: float = Field(default=RCRT_HOURS)

    liner_material: str = Field(default="PE-RT")
    reinforcement_material: str = Field(default="Polyester Fiber")
    cover_material: str = Field(default="PE100")

    pfr_or_pv: str = Field(default="PFR")
    parent_program_id: Optional[int] = Field(default=None, index=True)
    pfr_reference_code: str = Field(default="")

    intended_service: str = Field(default="Static water service")
    status: str = Field(default="DRAFT", index=True)
    notes: str = Field(default="")
    created_by_name: str = Field(default="")


class RndQualificationTest(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    program_id: int = Field(index=True)
    sort_order: int = Field(default=0)
    clause_ref: str = Field(default="")
    code: str = Field(default="")
    title: str = Field(default="")
    description: str = Field(default="")
    specimen_requirement: str = Field(default="")
    applicability: str = Field(default="")
    status: str = Field(default="PLANNED", index=True)
    result_summary: str = Field(default="")


class RndQualificationSpecimen(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    program_id: int = Field(index=True)
    test_id: Optional[int] = Field(default=None, index=True)

    specimen_id: str = Field(default="", index=True)
    test_type: str = Field(default="STATIC_REGRESSION", index=True)
    sample_date: date = Field(default_factory=date.today)

    nominal_size_in: float = Field(default=0.0)
    pressure_mpa: float = Field(default=0.0)
    temperature_c: float = Field(default=0.0)
    failure_hours: Optional[float] = Field(default=None)
    failure_cycles: Optional[float] = Field(default=None)

    failure_mode: str = Field(default="")
    permissible_failure: bool = Field(default=True)
    is_runout: bool = Field(default=False)
    include_in_regression: bool = Field(default=True)

    fitting_type: str = Field(default="Field fitting")
    lab_name: str = Field(default="")
    witness_name: str = Field(default="")
    notes: str = Field(default="")


class RndMaterialQualification(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    program_id: int = Field(index=True)
    component: str = Field(default="LINER", index=True)
    material_name: str = Field(default="")
    supplier_name: str = Field(default="")
    grade_name: str = Field(default="")
    certificate_ref: str = Field(default="")
    batch_ref: str = Field(default="")
    status: str = Field(default="PLANNED", index=True)
    notes: str = Field(default="")


class RndAttachmentRegister(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    program_id: int = Field(index=True)
    category: str = Field(default="REPORT")
    title: str = Field(default="")
    reference_no: str = Field(default="")
    file_note: str = Field(default="")


def _require_user(session: Session = Depends(get_session)) -> User:
    user = session.exec(select(User).order_by(User.id.asc())).first()
    if not user:
        raise HTTPException(status_code=401, detail="No users found.")
    return user


def _touch_program(program: RndQualificationProgram) -> None:
    program.updated_at = datetime.utcnow()


def _touch_row(row) -> None:
    row.updated_at = datetime.utcnow()


def _default_test_matrix(pfr_or_pv: str) -> list[dict]:
    base = [
        {"code": "MPR_REG", "title": "Long-term hydrostatic regression", "description": "PFR regression basis for nonmetallic reinforcement. Use ASTM D2992 Procedure B logic, exclude points below 10 h, and calculate the lower confidence basis at 175,000 h.", "specimen_requirement": "18 long-term points", "clause_ref": "API 15S 5.3.2.3 / Annex E / Annex G", "applicability": "PFR"},
        {"code": "PV_1000H", "title": "PV 1000-hour constant pressure confirmation", "description": "For a product variant under a qualified family. Run a 1000 h confirmation against the parent PFR relationship.", "specimen_requirement": "2 specimens", "clause_ref": "API 15S 5.3.4.2", "applicability": "PV"},
        {"code": "TEMP_ELEV", "title": "Elevated temperature test", "description": "Confirm elevated-temperature behavior at the selected qualification temperature.", "specimen_requirement": "1 specimen", "clause_ref": "API 15S 5.3.5", "applicability": "ALL"},
        {"code": "TEMP_CYCLE", "title": "Temperature cycling", "description": "Thermal cycling confirmation for the qualified size and rating combination.", "specimen_requirement": "2 specimens", "clause_ref": "API 15S 5.3.6", "applicability": "ALL"},
        {"code": "RAPID_DECOMP", "title": "Rapid decompression", "description": "Required only for gas or multiphase service routes.", "specimen_requirement": "1 specimen", "clause_ref": "API 15S 5.3.7 / Annex B", "applicability": "SERVICE_DEP"},
        {"code": "OPERATING_MBR", "title": "Operating MBR / respooling", "description": "Confirm operating minimum bend radius and respooling capability.", "specimen_requirement": "2 specimens", "clause_ref": "API 15S 5.3.8", "applicability": "ALL"},
        {"code": "AXIAL_LOAD", "title": "Axial load capability", "description": "Confirm maximum allowable axial load and post-load performance.", "specimen_requirement": "2 specimens", "clause_ref": "API 15S 5.3.9", "applicability": "ALL"},
        {"code": "CRUSH", "title": "External load / crush", "description": "Crush resistance under the defined loading method.", "specimen_requirement": "3 specimens", "clause_ref": "API 15S 5.3.10", "applicability": "RANGE_DEP"},
        {"code": "LAOT", "title": "Lowest allowable operating temperature", "description": "Confirm low-temperature behavior at the selected LAOT.", "specimen_requirement": "2 specimens", "clause_ref": "API 15S 5.3.11", "applicability": "ALL"},
        {"code": "IMPACT", "title": "Impact resistance", "description": "Impact followed by post-impact confirmation.", "specimen_requirement": "2 specimens", "clause_ref": "API 15S 5.3.12", "applicability": "ALL"},
        {"code": "TEC", "title": "Thermal expansion coefficient", "description": "Measure axial thermal expansion, and hoop expansion where relevant.", "specimen_requirement": "2 specimens", "clause_ref": "API 15S 5.3.13", "applicability": "ALL"},
        {"code": "GROWTH", "title": "Growth / shrinkage under pressure", "description": "Measure elongation and dimensional response under pressure.", "specimen_requirement": "2 specimens", "clause_ref": "API 15S 5.3.14", "applicability": "ALL"},
        {"code": "CYCLIC_REG", "title": "Cyclic pressure regression", "description": "For cyclic service, build a cyclic regression basis and lower confidence result.", "specimen_requirement": "18 cyclic points", "clause_ref": "API 15S 5.3.16 / Annex D", "applicability": "SERVICE_DEP"},
    ]
    items = []
    for row in base:
        if row["applicability"] == "PFR" and pfr_or_pv != "PFR":
            continue
        if row["applicability"] == "PV" and pfr_or_pv != "PV":
            continue
        items.append(row)
    return items


def _seed_test_matrix(session: Session, program: RndQualificationProgram) -> None:
    existing = session.exec(select(RndQualificationTest).where(RndQualificationTest.program_id == program.id)).all()
    if not existing:
        for idx, item in enumerate(_default_test_matrix(program.pfr_or_pv), start=1):
            session.add(RndQualificationTest(
                program_id=program.id,
                sort_order=idx,
                clause_ref=item["clause_ref"],
                code=item["code"],
                title=item["title"],
                description=item["description"],
                specimen_requirement=item["specimen_requirement"],
                applicability=item["applicability"],
            ))
    material_rows = session.exec(select(RndMaterialQualification).where(RndMaterialQualification.program_id == program.id)).all()
    if not material_rows:
        for component, material in [("LINER", program.liner_material), ("REINFORCEMENT", program.reinforcement_material), ("COVER", program.cover_material)]:
            session.add(RndMaterialQualification(program_id=program.id, component=component, material_name=material))
    session.commit()


def _t_critical_975(df: int) -> float:
    table = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228, 11: 2.201, 12: 2.179, 13: 2.16, 14: 2.145, 15: 2.131, 16: 2.12, 17: 2.11, 18: 2.101, 19: 2.093, 20: 2.086, 25: 2.06, 30: 2.042, 40: 2.021, 60: 2.0, 120: 1.98}
    if df <= 0:
        return 0.0
    if df in table:
        return table[df]
    if df > 120:
        return 1.96
    keys = sorted(table)
    low = max(k for k in keys if k < df)
    high = min(k for k in keys if k > df)
    return table[low] + (table[high] - table[low]) * ((df - low) / (high - low))


def _regression_from_specimens(specimens: List[RndQualificationSpecimen], mode: str, target_npr_mpa: float | None = None) -> dict:
    result = {
        "mode": mode,
        "count": 0,
        "required_minimum": 18,
        "excluded_count": 0,
        "excluded_ids": [],
        "warning": None,
        "points": [],
        "chart_points": [],
        "design_factor": DESIGN_FACTOR_NONMETALLIC if mode == "STATIC_REGRESSION" else 1.0,
        "formula_text": None,
        "pass_status": None,
        "margin_mpa": None,
    }
    candidates = [s for s in specimens if (s.test_type or "").upper() == mode]
    valid = []
    excluded = []
    for s in candidates:
        if not s.include_in_regression or not s.permissible_failure:
            excluded.append(s)
            continue
        if mode == "STATIC_REGRESSION":
            if s.is_runout or not s.failure_hours or s.failure_hours < 10 or not s.pressure_mpa:
                excluded.append(s)
                continue
            valid.append(s)
        else:
            if s.is_runout or not s.failure_cycles or s.failure_cycles <= 0 or not s.pressure_mpa:
                excluded.append(s)
                continue
            valid.append(s)

    result["excluded_count"] = len(excluded)
    result["excluded_ids"] = [s.specimen_id for s in excluded]
    result["count"] = len(valid)
    if len(valid) < 2:
        result["warning"] = "Need at least two valid points before the system can calculate a regression line."
        return result

    xs = [math.log10(s.failure_hours if mode == "STATIC_REGRESSION" else s.failure_cycles) for s in valid]
    ys = [math.log10(s.pressure_mpa) for s in valid]
    n = len(xs)
    x_bar = sum(xs) / n
    y_bar = sum(ys) / n
    sxx = sum((x - x_bar) ** 2 for x in xs)
    if sxx == 0:
        result["warning"] = "All valid points sit at the same time basis. Add spread before using regression."
        return result
    sxy = sum((x - x_bar) * (y - y_bar) for x, y in zip(xs, ys))
    slope = sxy / sxx
    intercept = y_bar - slope * x_bar
    residuals = [y - (intercept + slope * x) for x, y in zip(xs, ys)]
    syx = math.sqrt(sum(r * r for r in residuals) / max(n - 2, 1))
    tcrit = _t_critical_975(max(n - 2, 1))

    def _predict(x_val: float) -> tuple[float, float, float]:
        mean_y = intercept + slope * x_val
        if n <= 2:
            delta = syx
        else:
            root_term = math.sqrt(1 + 1 / n + ((x_val - x_bar) ** 2) / sxx)
            delta = tcrit * syx * root_term
        lcl_y = mean_y - delta
        lpl_y = mean_y - delta
        return mean_y, lcl_y, lpl_y

    basis_x = math.log10(RCRT_HOURS if mode == "STATIC_REGRESSION" else CYCLIC_BASIS_CYCLES)
    mean_y, lcl_y, lpl_y = _predict(basis_x)
    mean_basis_mpa = 10 ** mean_y
    lcl_basis_mpa = 10 ** lcl_y
    lpl_basis_mpa = 10 ** lpl_y
    design_factor = DESIGN_FACTOR_NONMETALLIC if mode == "STATIC_REGRESSION" else 1.0
    mpr_mpa = lcl_basis_mpa * design_factor if mode == "STATIC_REGRESSION" else lcl_basis_mpa
    margin_mpa = mpr_mpa - target_npr_mpa if target_npr_mpa else None
    pass_status = None if not target_npr_mpa else ("PASS" if mpr_mpa >= target_npr_mpa else "FAIL")

    chart_points = []
    x_min = min(xs)
    x_max = max(max(xs), basis_x)
    steps = 24
    for i in range(steps + 1):
        x_val = x_min + (x_max - x_min) * i / steps
        mean_curve_y, lcl_curve_y, lpl_curve_y = _predict(x_val)
        chart_points.append({
            "x": x_val,
            "time_or_cycles": round(10 ** x_val, 3),
            "mean_pressure": round(10 ** mean_curve_y, 4),
            "lcl_pressure": round(10 ** lcl_curve_y, 4),
            "lpl_pressure": round(10 ** lpl_curve_y, 4),
        })

    result.update({
        "points": [{"id": s.id, "specimen_id": s.specimen_id, "x": x, "y": y, "pressure_mpa": s.pressure_mpa, "failure_hours": s.failure_hours, "failure_cycles": s.failure_cycles, "failure_mode": s.failure_mode} for s, x, y in zip(valid, xs, ys)],
        "slope": slope,
        "intercept": intercept,
        "syx": syx,
        "tcrit": tcrit,
        "x_bar": x_bar,
        "y_bar": y_bar,
        "x_basis": basis_x,
        "rcrt_hours": RCRT_HOURS,
        "cyclic_basis_cycles": CYCLIC_BASIS_CYCLES,
        "mean_rcrt_mpa": mean_basis_mpa,
        "lcl_rcrt_mpa": lcl_basis_mpa,
        "lpl_rcrt_mpa": lpl_basis_mpa,
        "chart_points": chart_points,
        "design_factor": design_factor,
        "mpr_mpa": mpr_mpa,
        "target_npr_mpa": target_npr_mpa,
        "margin_mpa": margin_mpa,
        "pass_status": pass_status,
        "formula_text": "log10(P) = intercept + slope × log10(time)",
        "provisional": n < result["required_minimum"],
    })
    if n < result["required_minimum"]:
        result["warning"] = "Regression is calculated, but the dataset is below the API 15S readiness target for a full PFR qualification basis. Treat all LCL and MPR values as provisional."
    return result


def _program_answers(program: RndQualificationProgram) -> dict:
    raw = (program.notes or "").strip()
    if raw.startswith("__RNDJSON__"):
        try:
            return json.loads(raw[len("__RNDJSON__"):])
        except Exception:
            return {}
    return {}


def _save_program_answers(program: RndQualificationProgram, answers: dict) -> None:
    program.notes = "__RNDJSON__" + json.dumps(answers, ensure_ascii=False)
    _touch_program(program)


def _wizard_state(program: RndQualificationProgram) -> dict:
    answers = _program_answers(program)
    launch_size = answers.get("launch_size_in") or f"{program.nominal_size_in:g}"
    sister_size = answers.get("sister_size_in") or "6"
    service_route = answers.get("service_route") or ("gas_multiphase" if "gas" in (program.intended_service or "").lower() else "static_liquid")
    cyclic_service = answers.get("cyclic_service", "no")
    decision = {
        "launch_size": launch_size,
        "sister_size": sister_size,
        "service_route": service_route,
        "cyclic_service": cyclic_service,
        "family_decision": f"Use {launch_size} in as the full PFR and handle {sister_size} in as a PV only if materials, pressure class, and construction remain matched.",
        "service_decision": "Rapid decompression is required because the selected route includes gas or multiphase service." if service_route == "gas_multiphase" else "Rapid decompression is not required for the current static-liquid route.",
        "cyclic_decision": "Cyclic regression route is required for the selected duty." if cyclic_service == "yes" else "Cyclic route stays inactive unless the field duty crosses the API cyclic trigger.",
        "temperature_decision": f"Qualification temperature must be at least the claimed MAOT of {program.maot_c:g} °C. A higher claim later needs its own basis.",
        "wizard_complete": True,
    }
    return {"answers": answers, "decision": decision}


def _material_screening_state(materials: List[RndMaterialQualification]) -> dict:
    rows = []
    all_ready = True
    for material in materials:
        missing = []
        for key, label in [("material_name", "material name"), ("supplier_name", "supplier"), ("grade_name", "grade"), ("certificate_ref", "certificate ref"), ("batch_ref", "batch ref"), ("notes", "screening note")]:
            if not getattr(material, key, ""):
                missing.append(label)
        ready = not missing
        all_ready = all_ready and ready
        notes_lower = (material.notes or "").lower()
        evidence_hint = "Supplier certificate with actual values plus screening note is usually the minimum practical starting point."
        if "compatible" in notes_lower or "pass" in notes_lower:
            evidence_hint = "Current note suggests acceptable compatibility, but keep the supplier cert and any lab report in the dossier."
        rows.append({"row": material, "missing": missing, "ready": ready, "evidence_hint": evidence_hint})
    return {
        "rows": rows,
        "complete": all_ready and len(rows) >= 3,
        "status_label": "Accepted" if all_ready and len(rows) >= 3 else "More evidence needed",
        "headline": "Record traceable grade, supplier, batch, actual certificate reference, and a short engineering note for liner, reinforcement, and cover before structural testing starts.",
        "standard_basis": "API 15S places material responsibility on the manufacturer. Use supplier certificates, lot traceability, and compatibility evidence as the material gate before qualification testing.",
    }


def _burst_threshold(program: RndQualificationProgram) -> float:
    if program.npr_mpa <= 0:
        return 0.0
    return round(program.npr_mpa / DESIGN_FACTOR_NONMETALLIC, 3)


def _burst_threshold_explainer(program: RndQualificationProgram) -> dict:
    threshold = _burst_threshold(program)
    return {
        "threshold_mpa": threshold,
        "formula": f"Planning burst screen = NPR / Fd = {program.npr_mpa:.3f} / {DESIGN_FACTOR_NONMETALLIC:.2f} = {threshold:.3f} MPa",
        "why": "This is used here only as an internal planning screen so weak burst results are flagged early. It is not the final API 15S nonmetallic qualification basis by itself.",
        "standard_note": "For nonmetallic reinforcement, the real qualification backbone is long-term hydrostatic regression and the lower confidence result at 175,000 h, then application of the design factor.",
    }


def _burst_state(program: RndQualificationProgram, specimens: List[RndQualificationSpecimen]) -> dict:
    burst_rows = [s for s in specimens if (s.test_type or "").upper() == "BURST_QUALIFICATION"]
    threshold = _burst_threshold(program)
    evaluated = []
    accepted = 0
    review_needed = 0
    for specimen in burst_rows:
        flags = []
        if specimen.pressure_mpa <= 0:
            flags.append("Burst pressure is missing.")
        if specimen.temperature_c and abs(float(specimen.temperature_c) - float(program.maot_c)) > 5.0:
            flags.append(f"Test temperature {specimen.temperature_c:g} °C is outside the ±5 °C window around the qualification basis.")
        mode = (specimen.failure_mode or "").strip().lower()
        if mode and mode not in {"burst", "rupture"}:
            flags.append("Failure mode is not a clear burst/rupture and needs engineering review.")
        if specimen.pressure_mpa and specimen.pressure_mpa < threshold:
            flags.append(f"Burst pressure is below the planning burst screen of {threshold:.3f} MPa.")
        status = "ACCEPTED" if not flags else "REVIEW"
        if status == "ACCEPTED":
            accepted += 1
        else:
            review_needed += 1
        evaluated.append({"specimen": specimen, "flags": flags, "status": status})
    required_count = 5
    complete = accepted >= required_count and review_needed == 0
    return {
        "threshold_mpa": threshold,
        "required_count": required_count,
        "accepted_count": accepted,
        "review_count": review_needed,
        "rows": evaluated,
        "complete": complete,
        "headline": "Run burst testing first as a design screen. The system only unlocks the next step once five acceptable burst specimens are recorded.",
        "explainer": _burst_threshold_explainer(program),
    }


def _active_stage(program: RndQualificationProgram, materials: List[RndMaterialQualification], specimens: List[RndQualificationSpecimen]) -> dict:
    wizard = _wizard_state(program)
    material_state = _material_screening_state(materials)
    burst_state = _burst_state(program, specimens)
    static_reg = _regression_from_specimens(specimens, "STATIC_REGRESSION", program.npr_mpa)
    if not material_state["complete"]:
        current = "materials"
        percent = 30
    elif not burst_state["complete"]:
        current = "burst"
        percent = 58
    elif static_reg["count"] < static_reg["required_minimum"]:
        current = "regression"
        percent = 80
    else:
        current = "review"
        percent = 100
    return {"wizard": wizard, "materials": material_state, "burst": burst_state, "static_reg": static_reg, "current": current, "progress_pct": percent}


def _qualification_guide(program: Optional[RndQualificationProgram] = None) -> dict:
    size = f"{program.nominal_size_in:g} in" if program else "4 in"
    npr = f"{program.npr_mpa:g} MPa" if program else "10 MPa"
    maot = f"{program.maot_c:g} °C" if program else "65 °C"
    return {
        "summary": f"This workspace organizes API 15S qualification for LLRTP with PE-RT liner, polyester fiber reinforcement, and PE100 cover. It guides the team through product definition, test matrix, specimen tracking, and regression review for {size} / {npr} / {maot}.",
        "steps": [
            {"title": "1. Define the qualification basis", "text": "Create the program as PFR or PV, set size, NPR, MAOT, service, and material stack. Use the most demanding representative as the PFR when possible."},
            {"title": "2. Lock the material system", "text": "Record liner, reinforcement, and cover grade, supplier, batch, and certificate references before test execution."},
            {"title": "3. Build the specimen plan", "text": "Prepare static regression specimens, cyclic specimens if cyclic service applies, and the rest of the API 15S matrix such as temperature cycling, MBR, impact, axial load, and decompression when applicable."},
            {"title": "4. Run regression correctly", "text": "For static regression, record pressure, temperature, time to failure, and failure mode. Exclude invalid failures and any point below 10 h from the regression dataset."},
            {"title": "5. Review the lower confidence basis", "text": "Use the lower confidence result at 175,000 h for nonmetallic reinforcement, then apply the design factor to compare against the target NPR."},
            {"title": "6. Close the program only with full evidence", "text": "A program is ready to close only when the matrix is complete, materials are traceable, exclusions are justified, and the final qualification package is signed off."},
        ],
        "observe": [
            "Use the correct end fittings so fitting failures do not corrupt the pipe qualification dataset.",
            "Keep test temperature stable and recorded for every specimen.",
            "Keep a clear reason whenever a point is excluded from regression.",
            "Do not use average pressure alone for acceptance; review LCL and MPR basis.",
        ],
        "avoid": [
            "Do not mix different designs or reinforcement constructions in one regression set.",
            "Do not include points below 10 h in static regression.",
            "Do not treat the software as a substitute for engineering review or third-party witness requirements.",
            "Do not close a qualification with missing raw records, certificates, or failure descriptions.",
        ],
        "formula_examples": [
            {"label": "Regression line", "expr": "log10(P) = intercept + slope × log10(time)"},
            {"label": "Lower confidence at design life", "expr": "LCL_175000h = lower confidence pressure at 175,000 h"},
            {"label": "Nonmetallic MPR", "expr": "MPR = LCL_175000h × 0.67"},
            {"label": "PV helper", "expr": "PPV1000 = PPFR1000 × (NPR_PV / NPR_PFR)"},
        ],
    }


def _test_rulebook(program: RndQualificationProgram, test: RndQualificationTest) -> dict:
    generic = {
        "objective": test.description,
        "why": "This test remains in the qualification matrix because it supports the API 15S route for the selected product family.",
        "how": [
            "Prepare specimens representative of the qualified construction.",
            "Record the setup, temperature, pressure, and failure/result details.",
            "Keep raw test records and witness evidence for the dossier.",
        ],
        "acceptance": ["Use the program-specific test method and engineering review before closing this item."],
        "report_fields": ["Specimen IDs", "Test setup", "Measured result", "Conclusion"],
    }
    mapping = {
        "MPR_REG": {
            "objective": "Establish the long-term nonmetallic regression basis for the PFR.",
            "why": "API 15S uses the lower confidence result at 175,000 h for nonmetallic reinforced PFRs, then applies the design factor to obtain MPR.",
            "how": [
                "Run a spread of static hydrostatic specimens across several pressure levels.",
                "Record time to failure, pressure, temperature, failure mode, and whether the point is valid.",
                "Exclude invalid points and any point below 10 h from the static regression basis.",
            ],
            "acceptance": [
                "Target at least 18 valid long-term points for the PFR basis.",
                "Review provisional LCL and MPR only as trend indicators until the dataset is mature.",
                f"Compare MPR against the intended NPR of {program.npr_mpa:.3f} MPa.",
            ],
            "report_fields": ["Regression chart", "Valid points", "Excluded points", "LCL @ 175,000 h", "MPR", "Pass/fail comment"],
        },
        "PV_1000H": {
            "objective": "Confirm a product variant under a qualified family instead of rerunning a full new PFR program.",
            "why": "API 15S allows PV handling within the qualified family when the family relationship remains valid.",
            "how": [
                "Confirm that the PV remains within the parent family logic.",
                "Run the 1000 h confirmation per the selected pressure relationship.",
            ],
            "acceptance": ["Keep the parent PFR reference and the PV pressure ratio in the report."],
            "report_fields": ["Parent PFR", "PV pressure basis", "1000 h outcome"],
        },
        "RAPID_DECOMP": {
            "objective": "Confirm resistance to rapid decompression for gas or multiphase service.",
            "why": "This item is conditional. It only applies when the selected service route includes gas or multiphase use.",
            "how": ["Prepare gas/multiphase representative specimens.", "Record decompression cycle conditions and visible damage assessment."],
            "acceptance": ["Keep this test inactive when the service route remains static liquid only."],
            "report_fields": ["Service route", "Cycle conditions", "Damage observation", "Conclusion"],
        },
        "CYCLIC_REG": {
            "objective": "Establish a cyclic regression basis when the service duty is cyclic.",
            "why": "Cyclic pressure duty can require a different qualification basis than purely static duty.",
            "how": ["Log cycles to failure, peak/mean conditions, and failure mode.", "Build the cyclic regression chart and lower basis."],
            "acceptance": ["Do not activate this route unless cyclic duty is part of the selected qualification basis."],
            "report_fields": ["Cycle counts", "Regression chart", "Lower cyclic basis", "Decision"],
        },
    }
    data = mapping.get(test.code, generic)
    applicability_note = test.applicability
    if test.applicability == "SERVICE_DEP":
        route = _wizard_state(program)["decision"]["service_decision"]
        applicability_note = f"Conditional service-dependent item. {route}"
    elif test.applicability == "RANGE_DEP":
        applicability_note = "Range-dependent item. Keep active unless engineering review waives it for the selected envelope."
    elif test.applicability == "ALL":
        applicability_note = "Applies to the selected route unless specifically waived by engineering review."
    elif test.applicability == "PFR":
        applicability_note = "Applies to the main PFR route."
    elif test.applicability == "PV":
        applicability_note = "Applies only when the program is treated as a PV."
    data["applicability_note"] = applicability_note
    data["clause_ref"] = test.clause_ref
    data["specimen_requirement"] = test.specimen_requirement
    return data


def _matrix_status_for_test(program: RndQualificationProgram, test: RndQualificationTest, flow: dict) -> str:
    if test.code == "MPR_REG":
        return "IN_PROGRESS" if flow["current"] in {"regression", "review"} else "PLANNED"
    if test.code == "PV_1000H" and program.pfr_or_pv != "PV":
        return "NOT_APPLICABLE"
    if test.code in {"RAPID_DECOMP", "CYCLIC_REG"}:
        answers = flow["wizard"]["decision"]
        if test.code == "RAPID_DECOMP" and answers["service_route"] != "gas_multiphase":
            return "NOT_APPLICABLE"
        if test.code == "CYCLIC_REG" and answers["cyclic_service"] != "yes":
            return "NOT_APPLICABLE"
    if flow["current"] == "materials":
        return "UPCOMING"
    if flow["current"] == "burst" and test.code == "MPR_REG":
        return "BLOCKED"
    return test.status or "PLANNED"


def _program_context(session: Session, program_id: int) -> dict:
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, "Program not found")
    _seed_test_matrix(session, program)
    materials = session.exec(select(RndMaterialQualification).where(RndMaterialQualification.program_id == program_id).order_by(RndMaterialQualification.id.asc())).all()
    specimens = session.exec(select(RndQualificationSpecimen).where(RndQualificationSpecimen.program_id == program_id).order_by(RndQualificationSpecimen.created_at.desc())).all()
    tests = session.exec(select(RndQualificationTest).where(RndQualificationTest.program_id == program_id).order_by(RndQualificationTest.sort_order.asc(), RndQualificationTest.id.asc())).all()
    flow = _active_stage(program, materials, specimens)
    matrix_rows = []
    for test in tests:
        matrix_rows.append({"test": test, "ui_status": _matrix_status_for_test(program, test, flow), "rulebook": _test_rulebook(program, test)})
    return {
        "program": program,
        "materials": materials,
        "specimens": specimens,
        "tests": tests,
        "flow": flow,
        "wizard": flow["wizard"],
        "material_state": flow["materials"],
        "burst_state": flow["burst"],
        "static_reg": flow["static_reg"],
        "threshold_mpa": _burst_threshold(program),
        "rcrt_hours": RCRT_HOURS,
        "design_factor_nonmetallic": DESIGN_FACTOR_NONMETALLIC,
        "matrix_rows": matrix_rows,
        "guide": _qualification_guide(program),
    }


@router.get("")
def rnd_home() -> RedirectResponse:
    return RedirectResponse(url="/rnd/qualifications", status_code=303)


@router.get("/qualifications")
def rnd_dashboard(request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    programs = session.exec(select(RndQualificationProgram).order_by(RndQualificationProgram.updated_at.desc())).all()
    dashboard = []
    for program in programs:
        materials = session.exec(select(RndMaterialQualification).where(RndMaterialQualification.program_id == program.id).order_by(RndMaterialQualification.id.asc())).all()
        specimens = session.exec(select(RndQualificationSpecimen).where(RndQualificationSpecimen.program_id == program.id).order_by(RndQualificationSpecimen.created_at.desc())).all()
        flow = _active_stage(program, materials, specimens)
        dashboard.append({"program": program, "flow": flow})
    return TEMPLATES.TemplateResponse(request=request, name="rnd_dashboard.html", context={"request": request, "user": user, "dashboard": dashboard, "guide": _qualification_guide()})


@router.get("/qualifications/new")
def rnd_new_program_form(request: Request, user: User = Depends(_require_user)):
    return TEMPLATES.TemplateResponse(request=request, name="rnd_program_form.html", context={"request": request, "user": user})


@router.post("/qualifications/new")
def rnd_create_program(session: Session = Depends(get_session), user: User = Depends(_require_user), title: str = Form(...), program_code: str = Form(...), nominal_size_in: float = Form(...), npr_mpa: float = Form(...), maot_c: float = Form(...), laot_c: float = Form(0.0), pfr_or_pv: str = Form("PFR"), parent_program_id: Optional[int] = Form(None), intended_service: str = Form("Static water service"), notes: str = Form("")):
    program = RndQualificationProgram(
        program_code=(program_code or "").strip().upper(),
        title=(title or "").strip(),
        nominal_size_in=nominal_size_in,
        npr_mpa=npr_mpa,
        maot_c=maot_c,
        laot_c=laot_c,
        pfr_or_pv=(pfr_or_pv or "PFR").strip().upper(),
        parent_program_id=parent_program_id,
        intended_service=intended_service,
        notes="",
        created_by_name=(getattr(user, "display_name", "") or getattr(user, "username", "") or ""),
    )
    session.add(program)
    session.commit()
    session.refresh(program)
    if program.parent_program_id:
        parent = session.get(RndQualificationProgram, program.parent_program_id)
        if parent:
            program.pfr_reference_code = parent.program_code
            _touch_program(program)
            session.add(program)
            session.commit()
    _seed_test_matrix(session, program)
    _save_program_answers(program, {"free_notes": notes})
    session.add(program)
    session.commit()
    return RedirectResponse(url=f"/rnd/qualifications/{program.id}", status_code=303)


@router.get("/qualifications/{program_id}")
def rnd_program_view(program_id: int, request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    context = _program_context(session, program_id)
    context.update({"request": request, "user": user})
    return TEMPLATES.TemplateResponse(request=request, name="rnd_program_view.html", context=context)


@router.post("/qualifications/{program_id}/wizard/save")
def rnd_save_wizard(program_id: int, session: Session = Depends(get_session), user: User = Depends(_require_user), launch_size_in: str = Form(...), sister_size_in: str = Form("6"), service_route: str = Form("static_liquid"), cyclic_service: str = Form("no"), intended_service: str = Form("Static water service"), npr_mpa: float = Form(...), maot_c: float = Form(...), pfr_or_pv: str = Form("PFR")):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, "Program not found")
    program.nominal_size_in = float(launch_size_in)
    program.npr_mpa = npr_mpa
    program.maot_c = maot_c
    program.pfr_or_pv = (pfr_or_pv or "PFR").upper()
    program.intended_service = intended_service
    answers = _program_answers(program)
    answers.update({"launch_size_in": launch_size_in, "sister_size_in": sister_size_in, "service_route": service_route, "cyclic_service": cyclic_service})
    _save_program_answers(program, answers)
    session.add(program)
    session.commit()
    return RedirectResponse(url=f"/rnd/qualifications/{program_id}", status_code=303)


@router.post("/qualifications/{program_id}/materials/{material_id}")
def rnd_update_material(program_id: int, material_id: int, material_name: str = Form(""), supplier_name: str = Form(""), grade_name: str = Form(""), certificate_ref: str = Form(""), batch_ref: str = Form(""), status: str = Form("PLANNED"), notes: str = Form(""), session: Session = Depends(get_session), user: User = Depends(_require_user)):
    row = session.get(RndMaterialQualification, material_id)
    if not row or row.program_id != program_id:
        raise HTTPException(404, "Material row not found")
    row.material_name = material_name or row.material_name
    row.supplier_name = supplier_name or ""
    row.grade_name = grade_name or ""
    row.certificate_ref = certificate_ref or ""
    row.batch_ref = batch_ref or ""
    row.status = (status or "PLANNED").strip().upper()
    row.notes = notes or ""
    _touch_row(row)
    session.add(row)
    program = session.get(RndQualificationProgram, program_id)
    if program:
        _touch_program(program)
        session.add(program)
    session.commit()
    return RedirectResponse(url=f"/rnd/qualifications/{program_id}", status_code=303)


@router.post("/qualifications/{program_id}/burst/add")
def rnd_add_burst_result(program_id: int, specimen_id: str = Form(...), pressure_mpa: float = Form(...), temperature_c: float = Form(...), failure_mode: str = Form(...), notes: str = Form(""), session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, "Program not found")
    specimen = RndQualificationSpecimen(
        program_id=program_id,
        specimen_id=(specimen_id or "").strip().upper(),
        test_type="BURST_QUALIFICATION",
        sample_date=date.today(),
        nominal_size_in=program.nominal_size_in,
        pressure_mpa=pressure_mpa,
        temperature_c=temperature_c,
        failure_mode=failure_mode,
        permissible_failure=(failure_mode or "").strip().lower() in {"burst", "rupture"},
        include_in_regression=False,
        notes=notes,
    )
    session.add(specimen)
    _touch_program(program)
    session.add(program)
    session.commit()
    return RedirectResponse(url=f"/rnd/qualifications/{program_id}", status_code=303)


@router.post("/qualifications/{program_id}/specimens/new")
def rnd_add_specimen(program_id: int, session: Session = Depends(get_session), user: User = Depends(_require_user), specimen_id: str = Form(...), test_type: str = Form(...), test_id: Optional[int] = Form(None), sample_date: date = Form(...), nominal_size_in: float = Form(0.0), pressure_mpa: float = Form(0.0), temperature_c: float = Form(0.0), failure_hours: Optional[float] = Form(None), failure_cycles: Optional[float] = Form(None), failure_mode: str = Form(""), permissible_failure: Optional[str] = Form(None), is_runout: Optional[str] = Form(None), include_in_regression: Optional[str] = Form(None), fitting_type: str = Form("Field fitting"), lab_name: str = Form(""), witness_name: str = Form(""), notes: str = Form("")):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, "Program not found")
    specimen = RndQualificationSpecimen(
        program_id=program_id,
        test_id=test_id,
        specimen_id=(specimen_id or "").strip().upper(),
        test_type=(test_type or "STATIC_REGRESSION").strip().upper(),
        sample_date=sample_date,
        nominal_size_in=nominal_size_in or program.nominal_size_in,
        pressure_mpa=pressure_mpa,
        temperature_c=temperature_c,
        failure_hours=failure_hours,
        failure_cycles=failure_cycles,
        failure_mode=failure_mode,
        permissible_failure=bool(permissible_failure),
        is_runout=bool(is_runout),
        include_in_regression=bool(include_in_regression),
        fitting_type=fitting_type,
        lab_name=lab_name,
        witness_name=witness_name,
        notes=notes,
    )
    session.add(specimen)
    _touch_program(program)
    session.add(program)
    session.commit()
    return RedirectResponse(url=f"/rnd/qualifications/{program_id}/regression", status_code=303)


@router.post("/qualifications/{program_id}/specimens/{specimen_id}/delete")
def rnd_delete_specimen(program_id: int, specimen_id: int, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    specimen = session.get(RndQualificationSpecimen, specimen_id)
    if not specimen or specimen.program_id != program_id:
        raise HTTPException(404, "Specimen not found")
    session.delete(specimen)
    program = session.get(RndQualificationProgram, program_id)
    if program:
        _touch_program(program)
        session.add(program)
    session.commit()
    return RedirectResponse(url=f"/rnd/qualifications/{program_id}/regression", status_code=303)


@router.get("/qualifications/{program_id}/tests/{test_id}")
def rnd_test_detail(program_id: int, test_id: int, request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    context = _program_context(session, program_id)
    test = session.get(RndQualificationTest, test_id)
    if not test or test.program_id != program_id:
        raise HTTPException(404, "Test not found")
    context.update({"request": request, "user": user, "test": test, "rulebook": _test_rulebook(context["program"], test)})
    return TEMPLATES.TemplateResponse(request=request, name="rnd_test_detail.html", context=context)


@router.get("/qualifications/{program_id}/regression")
def rnd_regression_view(program_id: int, request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    context = _program_context(session, program_id)
    program = context["program"]
    specimens = context["specimens"]
    static_reg = _regression_from_specimens(specimens, "STATIC_REGRESSION", program.npr_mpa)
    cyclic_reg = _regression_from_specimens(specimens, "CYCLIC_REGRESSION", program.npr_mpa)
    pv_formula = None
    if program.pfr_or_pv == "PV" and program.parent_program_id:
        parent = session.get(RndQualificationProgram, program.parent_program_id)
        if parent:
            ratio = (program.npr_mpa / parent.npr_mpa) if parent.npr_mpa else None
            pv_formula = {
                "pfr_code": parent.program_code,
                "npr_pv": program.npr_mpa,
                "npr_pfr": parent.npr_mpa,
                "formula": "PPV1000 = PPFR1000 × (NPR_PV / NPR_PFR)",
                "ratio": ratio,
            }
    explainers = [
        {"label": "Why 175,000 h?", "text": "API 15S uses 175,000 hours as the regression curve reference time for defining the long-term rating basis."},
        {"label": "What is LCL?", "text": "LCL is the lower-confidence pressure value at the reference time. For nonmetallic reinforcement, that lower basis feeds the MPR calculation."},
        {"label": "Why can LCL look much higher than NPR?", "text": "NPR is the commercial claim. LCL is the modelled long-term pressure basis before the design factor is applied. With too few points, the extrapolated LCL can look unrealistically high, so the software marks it as provisional."},
        {"label": "What is MPR?", "text": f"For static nonmetallic qualification, MPR = LCL × {DESIGN_FACTOR_NONMETALLIC:.2f}. NPR should not exceed this long-term basis."},
    ]
    context.update({"request": request, "user": user, "static_reg": static_reg, "cyclic_reg": cyclic_reg, "pv_formula": pv_formula, "explainers": explainers})
    return TEMPLATES.TemplateResponse(request=request, name="rnd_regression_view.html", context=context)


@router.get("/qualifications/{program_id}/reports/materials")
def rnd_materials_report(program_id: int, request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    context = _program_context(session, program_id)
    context.update({"request": request, "user": user, "report_title": "Material qualification report"})
    return TEMPLATES.TemplateResponse(request=request, name="rnd_report_materials.html", context=context)


@router.get("/qualifications/{program_id}/reports/burst")
def rnd_burst_report(program_id: int, request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    context = _program_context(session, program_id)
    context.update({"request": request, "user": user, "report_title": "Burst screening report"})
    return TEMPLATES.TemplateResponse(request=request, name="rnd_report_burst.html", context=context)


@router.get("/qualifications/{program_id}/reports/regression")
def rnd_regression_report(program_id: int, request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    context = _program_context(session, program_id)
    context.update({"request": request, "user": user, "report_title": "Regression report"})
    return TEMPLATES.TemplateResponse(request=request, name="rnd_report_regression.html", context=context)


@router.get("/qualifications/{program_id}/reports/final")
def rnd_final_report(program_id: int, request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    context = _program_context(session, program_id)
    context.update({"request": request, "user": user, "report_title": "Final qualification dossier"})
    return TEMPLATES.TemplateResponse(request=request, name="rnd_report_final.html", context=context)
