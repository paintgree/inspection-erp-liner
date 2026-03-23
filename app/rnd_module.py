from __future__ import annotations

import csv
import math
import os
from datetime import datetime, date
from io import StringIO
from typing import Optional, List

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import SQLModel, Field, Session, select

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


class RndWizardAnswer(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    program_id: int = Field(index=True)
    section: str = Field(default="GENERAL", index=True)
    key: str = Field(default="", index=True)
    label: str = Field(default="")
    value: str = Field(default="")


class RndChecklistItem(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    program_id: int = Field(index=True)
    sort_order: int = Field(default=0)
    stage: str = Field(default="SCOPING", index=True)
    code: str = Field(default="", index=True)
    title: str = Field(default="")
    why_this_matters: str = Field(default="")
    acceptance_rule: str = Field(default="")
    evidence_required: str = Field(default="")
    api_reference: str = Field(default="")
    applicability: str = Field(default="REQUIRED")
    status: str = Field(default="PENDING", index=True)
    owner_hint: str = Field(default="R&D")
    notes: str = Field(default="")


WIZARD_FIELDS = [
    ("BUSINESS", "claim_pressure_mpa", "What pressure rating do you want to claim?"),
    ("BUSINESS", "claim_temperature_c", "What maximum operating temperature do you want to claim?"),
    ("BUSINESS", "launch_size_in", "Which size do you want to launch first?"),
    ("FAMILY", "sister_size_in", "Do you have another nearby size in the same family?"),
    ("FAMILY", "family_same_construction", "Is the second size the same construction and pressure class?"),
    ("SERVICE", "service_medium", "What service is the product intended for?"),
    ("SERVICE", "service_is_cyclic", "Is the product intended for cyclic service?"),
    ("SERVICE", "service_requires_gas", "Will this product be sold for gas or multiphase service?"),
    ("MATERIALS", "liner_screened", "Has the liner material already passed screening?"),
    ("MATERIALS", "reinforcement_screened", "Has the reinforcement system passed screening?"),
    ("MATERIALS", "cover_screened", "Has the cover material already passed screening?"),
    ("MATERIALS", "end_fitting_ready", "Is the end fitting concept frozen?"),
    ("EVIDENCE", "has_burst_data", "Do you already have short-term burst data?"),
    ("EVIDENCE", "has_regression_data", "Do you already have long-term regression data?"),
    ("EVIDENCE", "has_witness_plan", "Do you already have a witness / lab plan?"),
]


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
        {"code": "MPR_REG", "title": "Long-term hydrostatic regression", "description": "PFR regression basis for nonmetallic reinforcement. Use ASTM D2992 Procedure B logic, exclude points below 10 h, calculate mean line, LCL, LPL, and LCL at RCRT.", "specimen_requirement": "18+ target", "clause_ref": "API 15S 5.3.2.3 / Annex E / Annex G", "applicability": "PFR"},
        {"code": "PV_1000H", "title": "PV 1000-hour constant pressure confirmation", "description": "Two-specimen 1000 h confirmation for product variants using the PFR relationship.", "specimen_requirement": "2", "clause_ref": "API 15S 5.3.4.2", "applicability": "PV"},
        {"code": "TEMP_ELEV", "title": "Elevated temperature test", "description": "Seal and polymer creep or relaxation confirmation above MAOT.", "specimen_requirement": "1", "clause_ref": "API 15S 5.3.5", "applicability": "ALL"},
        {"code": "TEMP_CYCLE", "title": "Temperature cycling", "description": "Thermal cycling confirmation for qualified size and rating combinations.", "specimen_requirement": "2", "clause_ref": "API 15S 5.3.6", "applicability": "ALL"},
        {"code": "RAPID_DECOMP", "title": "Rapid decompression", "description": "Required for gas or multiphase service.", "specimen_requirement": "1", "clause_ref": "API 15S 5.3.7 / Annex B", "applicability": "SERVICE_DEP"},
        {"code": "OPERATING_MBR", "title": "Operating MBR / respooling", "description": "Confirm operating and handling MBR and respooling performance.", "specimen_requirement": "2", "clause_ref": "API 15S 5.3.8", "applicability": "ALL"},
        {"code": "AXIAL_LOAD", "title": "Axial load capability", "description": "Max allowable axial load followed by additional confirmation.", "specimen_requirement": "2", "clause_ref": "API 15S 5.3.9", "applicability": "ALL"},
        {"code": "CRUSH", "title": "External load / crush", "description": "2-point radial crush confirmation.", "specimen_requirement": "3", "clause_ref": "API 15S 5.3.10", "applicability": "RANGE_DEP"},
        {"code": "LAOT", "title": "Lowest allowable operating temperature", "description": "Minimum operating temperature qualification.", "specimen_requirement": "2", "clause_ref": "API 15S 5.3.11", "applicability": "ALL"},
        {"code": "IMPACT", "title": "Impact resistance", "description": "Impact followed by additional test.", "specimen_requirement": "2", "clause_ref": "API 15S 5.3.12", "applicability": "ALL"},
        {"code": "TEC", "title": "Thermal expansion coefficient", "description": "Axial TEC measurement and hoop TEC where clearance is critical.", "specimen_requirement": "2", "clause_ref": "API 15S 5.3.13", "applicability": "ALL"},
        {"code": "GROWTH", "title": "Growth / shrinkage under pressure", "description": "Pressure elongation and dimensional response.", "specimen_requirement": "2", "clause_ref": "API 15S 5.3.14", "applicability": "ALL"},
        {"code": "CYCLIC_REG", "title": "Cyclic pressure regression", "description": "For cyclic service. Use cyclic regression and lower confidence basis.", "specimen_requirement": "18+ target", "clause_ref": "API 15S 5.3.16 / Annex D", "applicability": "SERVICE_DEP"},
    ]
    items = []
    for row in base:
        if row["applicability"] == "PFR" and pfr_or_pv != "PFR":
            continue
        if row["applicability"] == "PV" and pfr_or_pv != "PV":
            continue
        items.append(row)
    return items


WIZARD_DEFAULTS = {
    "claim_pressure_mpa": "10",
    "claim_temperature_c": "85",
    "launch_size_in": "4",
    "sister_size_in": "6",
    "family_same_construction": "yes",
    "service_medium": "water",
    "service_is_cyclic": "no",
    "service_requires_gas": "no",
    "liner_screened": "no",
    "reinforcement_screened": "no",
    "cover_screened": "no",
    "end_fitting_ready": "no",
    "has_burst_data": "no",
    "has_regression_data": "no",
    "has_witness_plan": "no",
}


def _seed_test_matrix(session: Session, program: RndQualificationProgram) -> None:
    existing = session.exec(select(RndQualificationTest).where(RndQualificationTest.program_id == program.id)).all()
    if not existing:
        for idx, item in enumerate(_default_test_matrix(program.pfr_or_pv), start=1):
            session.add(RndQualificationTest(program_id=program.id, sort_order=idx, clause_ref=item["clause_ref"], code=item["code"], title=item["title"], description=item["description"], specimen_requirement=item["specimen_requirement"], applicability=item["applicability"]))
        for component, material in [("LINER", program.liner_material), ("REINFORCEMENT", program.reinforcement_material), ("COVER", program.cover_material)]:
            session.add(RndMaterialQualification(program_id=program.id, component=component, material_name=material))
        session.commit()
    _seed_wizard_answers(session, program)
    _sync_checklist(session, program)


def _seed_wizard_answers(session: Session, program: RndQualificationProgram) -> None:
    existing = session.exec(select(RndWizardAnswer).where(RndWizardAnswer.program_id == program.id)).all()
    if existing:
        return
    defaults = dict(WIZARD_DEFAULTS)
    defaults["claim_pressure_mpa"] = f"{program.npr_mpa:g}"
    defaults["claim_temperature_c"] = f"{program.maot_c:g}"
    defaults["launch_size_in"] = f"{program.nominal_size_in:g}"
    defaults["service_medium"] = "gas" if any(k in (program.intended_service or "").lower() for k in ["gas", "multiphase"]) else "water"
    defaults["service_is_cyclic"] = "yes" if "cyclic" in (program.intended_service or "").lower() else "no"
    for section, key, label in WIZARD_FIELDS:
        session.add(RndWizardAnswer(program_id=program.id, section=section, key=key, label=label, value=str(defaults.get(key, ""))))
    session.commit()


def _wizard_answers_map(session: Session, program_id: int) -> dict[str, str]:
    rows = session.exec(select(RndWizardAnswer).where(RndWizardAnswer.program_id == program_id)).all()
    data = {row.key: row.value for row in rows}
    for key, value in WIZARD_DEFAULTS.items():
        data.setdefault(key, value)
    return data


def _wizard_sections(session: Session, program_id: int) -> list[dict]:
    answers = _wizard_answers_map(session, program_id)
    sections: dict[str, list[dict]] = {}
    for section, key, label in WIZARD_FIELDS:
        sections.setdefault(section, []).append({"key": key, "label": label, "value": answers.get(key, "")})
    order = ["BUSINESS", "FAMILY", "SERVICE", "MATERIALS", "EVIDENCE"]
    labels = {
        "BUSINESS": "Business target",
        "FAMILY": "Product family",
        "SERVICE": "Service envelope",
        "MATERIALS": "Material readiness",
        "EVIDENCE": "Existing evidence",
    }
    return [{"key": key, "title": labels.get(key, key.title()), "fields": sections.get(key, [])} for key in order]


def _upsert_wizard_answer(session: Session, program_id: int, section: str, key: str, label: str, value: str) -> None:
    row = session.exec(select(RndWizardAnswer).where(RndWizardAnswer.program_id == program_id, RndWizardAnswer.key == key)).first()
    if not row:
        row = RndWizardAnswer(program_id=program_id, section=section, key=key, label=label)
    row.section = section
    row.label = label
    row.value = value
    _touch_row(row)
    session.add(row)


def _to_bool(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _to_float(value: str | None, fallback: float = 0.0) -> float:
    try:
        return float(value or fallback)
    except Exception:
        return fallback


def _roadmap_summary(program: RndQualificationProgram, answers: dict[str, str], static_reg: dict | None = None) -> dict:
    claim_pressure = _to_float(answers.get("claim_pressure_mpa"), program.npr_mpa)
    claim_temp = _to_float(answers.get("claim_temperature_c"), program.maot_c)
    launch_size = _to_float(answers.get("launch_size_in"), program.nominal_size_in)
    sister_size = _to_float(answers.get("sister_size_in"), 0.0)
    same_family = _to_bool(answers.get("family_same_construction"))
    gas_service = _to_bool(answers.get("service_requires_gas")) or answers.get("service_medium", "").lower() in {"gas", "multiphase"}
    cyclic_service = _to_bool(answers.get("service_is_cyclic"))

    family_decision = "Use one size as the full PFR and the sister size as a PV within the same family." if sister_size and same_family else "Treat this as a standalone PFR until family similarity is proven."
    temp_decision = "A 90 C claim needs a 90 C qualification basis." if claim_temp >= 90 else f"Qualify at {claim_temp:g} C or above. That can support lower temperatures, but not a higher claim later."

    if static_reg and static_reg.get("count", 0) >= 2 and static_reg.get("mpr_mpa") is not None:
        rating_advice = f"Current regression-based MPR is {static_reg['mpr_mpa']:.2f} MPa versus claimed {claim_pressure:.2f} MPa."
    else:
        rating_advice = f"Run baseline burst plus long-term regression before deciding whether {claim_pressure:.2f} MPa is defensible."

    required_tests = ["Material screening", "Design freeze", "Burst baseline", "Static regression", "Evidence pack review"]
    if gas_service:
        required_tests.append("Rapid decompression")
    if cyclic_service:
        required_tests.append("Cyclic regression")
    if sister_size and same_family and program.pfr_or_pv == "PFR":
        required_tests.append("PV 1000 h confirmation for sister size")

    return {
        "claim_pressure": claim_pressure,
        "claim_temp": claim_temp,
        "launch_size": launch_size,
        "sister_size": sister_size,
        "gas_service": gas_service,
        "cyclic_service": cyclic_service,
        "family_decision": family_decision,
        "temp_decision": temp_decision,
        "rating_advice": rating_advice,
        "required_tests": required_tests,
        "next_action": "Complete the wizard, then start the first incomplete required checklist item.",
    }

def _burst_strength_summary(specimens: List[RndQualificationSpecimen]) -> dict:
    burst_types = {"BURST", "BURST_BASELINE", "MIN_BURST", "BURST_TEST"}
    values = [float(s.pressure_mpa) for s in specimens if (s.test_type or "").upper() in burst_types and s.pressure_mpa and s.pressure_mpa > 0]
    if not values:
        return {"count": 0, "min": None, "max": None, "mean": None}
    return {"count": len(values), "min": min(values), "max": max(values), "mean": sum(values) / len(values)}


def _pressure_recommendation(program: RndQualificationProgram, answers: dict[str, str], static_reg: dict, burst_summary: dict) -> dict:
    claim_pressure = _to_float(answers.get("claim_pressure_mpa"), program.npr_mpa)
    claim_temp = _to_float(answers.get("claim_temperature_c"), program.maot_c)
    if static_reg.get("count", 0) >= 6 and static_reg.get("mpr_mpa"):
        supported_npr = max(0.0, float(static_reg["mpr_mpa"]))
        status = "evidence_based"
        confidence = "HIGH" if static_reg.get("count", 0) >= static_reg.get("required_minimum", 18) else "MEDIUM"
        message = f"Current regression supports about {supported_npr:.2f} MPa at {claim_temp:.0f} C before any commercial rounding decision. Keep using regression as the release basis."
    elif burst_summary.get("count", 0) >= 3 and burst_summary.get("min"):
        supported_npr = max(0.0, float(burst_summary["min"]) * 0.50)
        status = "screening_only"
        confidence = "LOW"
        message = f"Only burst-style screening is available. A cautious planning target is around {supported_npr:.2f} MPa, but this is not an API 15S rating until regression matures."
    else:
        supported_npr = claim_pressure
        status = "wizard_only"
        confidence = "LOW"
        message = f"No evidence-based pressure recommendation yet. Keep {claim_pressure:.2f} MPa as a planning target only until burst and regression data are added."

    commercial_target = min(claim_pressure, supported_npr) if status == "evidence_based" else claim_pressure
    if commercial_target <= 0:
        commercial_target = claim_pressure or program.npr_mpa

    return {
        "status": status,
        "confidence": confidence,
        "supported_npr_mpa": round(supported_npr, 3),
        "recommended_commercial_target_mpa": round(commercial_target, 3),
        "message": message,
        "next_step": "Use burst to screen the design, then mature the static regression set until the lower confidence line at 175000 h is stable.",
    }


def _suggest_regression_matrix(program: RndQualificationProgram, answers: dict[str, str], specimens: List[RndQualificationSpecimen], static_reg: dict) -> dict:
    claim_pressure = _to_float(answers.get("claim_pressure_mpa"), program.npr_mpa)
    claim_temp = _to_float(answers.get("claim_temperature_c"), program.maot_c)
    existing = {}
    for s in specimens:
        if (s.test_type or "").upper() != "STATIC_REGRESSION" or not s.pressure_mpa:
            continue
        key = round(float(s.pressure_mpa), 1)
        existing[key] = existing.get(key, 0) + 1

    bands = [
        ("A", 1.55, "10 to 100 h"),
        ("B", 1.40, "100 to 300 h"),
        ("C", 1.28, "300 to 1000 h"),
        ("D", 1.18, "1000 to 3000 h"),
        ("E", 1.08, "3000 to 10000 h"),
        ("F", 1.00, "> 10000 h"),
    ]
    groups = []
    for code, factor, window in bands:
        pressure = round(max(claim_pressure * factor, claim_pressure * 0.95), 2)
        current = sum(v for k, v in existing.items() if abs(k - round(pressure, 1)) <= 0.2)
        groups.append({
            "band": code,
            "pressure_mpa": pressure,
            "target_window": window,
            "target_specimens": 3 if code in {"A", "B", "C", "D"} else 4,
            "existing_specimens": current,
            "status": "READY" if current >= (3 if code in {"A", "B", "C", "D"} else 4) else "ADD_POINTS",
        })

    return {
        "claim_pressure_mpa": claim_pressure,
        "claim_temperature_c": claim_temp,
        "recommended_points": 20,
        "current_points": static_reg.get("count", 0),
        "groups": groups,
        "notes": [
            "Use this as a planning matrix, not as a substitute for engineering review.",
            "Spread failures across short, medium, and long durations instead of clustering at one pressure.",
            "Keep the same temperature basis as the intended MAOT claim unless the program is explicitly a screening run.",
        ],
    }


def _test_guidance_rows(program: RndQualificationProgram, tests: List[RndQualificationTest], answers: dict[str, str]) -> list[dict]:
    gas_service = _to_bool(answers.get("service_requires_gas")) or answers.get("service_medium", "").lower() in {"gas", "multiphase"}
    cyclic_service = _to_bool(answers.get("service_is_cyclic"))
    hints = {
        "MPR_REG": ("Build enough pressure levels to cover short, mid, and long-term failures.", "A mature static regression with defensible exclusions and LCL at 175000 h."),
        "PV_1000H": ("Use only after a valid PFR relationship is defined.", "Two successful PV confirmation specimens at the required constant pressure duration."),
        "TEMP_ELEV": ("Run at or above the claimed MAOT.", "No unacceptable damage or leakage at the qualification temperature basis."),
        "TEMP_CYCLE": ("Use representative fittings and realistic cycle limits.", "No leakage, structural failure, or unacceptable degradation after the cycle sequence."),
        "RAPID_DECOMP": ("Required only for gas or multiphase routes.", "Acceptable performance after decompression exposure for the intended service."),
        "OPERATING_MBR": ("Test the handling route you will actually sell.", "Pipe and fitting system stays within acceptable damage limits at declared MBR / respooling conditions."),
        "AXIAL_LOAD": ("Confirm the axial envelope before release.", "Declared allowable axial load is validated and recorded."),
        "CRUSH": ("Use representative product and loading fixtures.", "No unacceptable structural damage under the required external load case."),
        "LOW_TEMP": ("Run only if the declared low temperature requires it.", "Product remains acceptable at the claimed low-temperature basis."),
        "IMPACT": ("Use conditioned specimens representative of the released construction.", "Impact acceptance limits are met without critical damage."),
        "TEC": ("Track dimensional response together with temperature and pressure.", "Thermal expansion coefficient and movement data are established for design use."),
        "GROWTH": ("Measure dimensional change under sustained pressure.", "Growth / shrinkage remains within accepted limits for the design basis."),
        "CYCLIC_REG": ("Only run for true cyclic duty.", "Cyclic regression basis supports the intended cycle severity and rating claim."),
    }
    rows = []
    for test in tests:
        apply = "Required"
        code = (test.code or "").upper()
        if code == "RAPID_DECOMP" and not gas_service:
            apply = "Not applicable"
        if code == "CYCLIC_REG" and not cyclic_service:
            apply = "Not applicable"
        if code == "PV_1000H" and program.pfr_or_pv == "PFR":
            apply = "Family-dependent"
        why, accept = hints.get(code, (test.description or "Follow the qualification route defined in the wizard.", "Record the evidence and reviewer decision."))
        rows.append({
            "test": test,
            "applicability_text": apply,
            "operator_tip": why,
            "acceptance_hint": accept,
        })
    return rows


def _dossier_sections(program: RndQualificationProgram, checklist: list[RndChecklistItem], tests: list[RndQualificationTest], materials: list[RndMaterialQualification], attachments: list[RndAttachmentRegister], static_reg: dict, roadmap: dict, pressure_plan: dict) -> list[dict]:
    required_checklist = [x for x in checklist if x.applicability == "REQUIRED"]
    done_checklist = [x for x in required_checklist if x.status in {"DONE", "WAIVED"}]
    sections = [
        {"title": "Program basis", "ready": bool(program.program_code and program.title and roadmap.get("claim_pressure")), "detail": f"Claim {roadmap.get('claim_pressure', 0):.2f} MPa at {roadmap.get('claim_temp', 0):.0f} C."},
        {"title": "Materials package", "ready": all((m.status or "").upper() == "APPROVED" for m in materials) and bool(materials), "detail": f"{sum(1 for m in materials if (m.status or '').upper() == 'APPROVED')} of {len(materials)} material rows approved."},
        {"title": "Test execution", "ready": all((t.status or '').upper() in {'PASSED', 'WAIVED'} or (t.applicability == 'SERVICE_DEP' and ((t.code == 'RAPID_DECOMP' and not roadmap.get('gas_service')) or (t.code == 'CYCLIC_REG' and not roadmap.get('cyclic_service')))) for t in tests) if tests else False, "detail": f"{sum(1 for t in tests if (t.status or '').upper() in {'PASSED','WAIVED'})} of {len(tests)} test rows closed."},
        {"title": "Regression basis", "ready": static_reg.get('count', 0) >= static_reg.get('required_minimum', 18) and static_reg.get('mpr_mpa') is not None, "detail": f"{static_reg.get('count', 0)} valid static points. Supported NPR now {pressure_plan.get('supported_npr_mpa', 0):.2f} MPa."},
        {"title": "Evidence register", "ready": len(attachments) >= 3, "detail": f"{len(attachments)} attachments logged in the dossier register."},
        {"title": "Checklist closeout", "ready": len(done_checklist) == len(required_checklist) and len(required_checklist) > 0, "detail": f"{len(done_checklist)} of {len(required_checklist)} required checklist items closed."},
    ]
    return sections



def _checklist_blueprint(program: RndQualificationProgram, answers: dict[str, str]) -> list[dict]:
    claim_pressure = _to_float(answers.get("claim_pressure_mpa"), program.npr_mpa)
    claim_temp = _to_float(answers.get("claim_temperature_c"), program.maot_c)
    same_family = _to_bool(answers.get("family_same_construction"))
    sister_size = _to_float(answers.get("sister_size_in"), 0.0)
    gas_service = _to_bool(answers.get("service_requires_gas")) or answers.get("service_medium", "").lower() in {"gas", "multiphase"}
    cyclic_service = _to_bool(answers.get("service_is_cyclic"))
    liner_done = _to_bool(answers.get("liner_screened"))
    reinforcement_done = _to_bool(answers.get("reinforcement_screened"))
    cover_done = _to_bool(answers.get("cover_screened"))
    fittings_done = _to_bool(answers.get("end_fitting_ready"))
    burst_done = _to_bool(answers.get("has_burst_data"))
    regression_started = _to_bool(answers.get("has_regression_data"))
    witness_done = _to_bool(answers.get("has_witness_plan"))

    items = [
        {"stage": "SCOPING", "code": "SCOPE-01", "title": "Freeze target commercial claim", "why": f"The team must agree whether the first launch target is {claim_pressure:g} MPa at {claim_temp:g} C before choosing the qualification route.", "acceptance": "Claim pressure, claim temperature, launch size, and intended service are approved.", "evidence": "Approved design basis record", "api": "Roadmap / program basis", "required": True, "status": "DONE" if claim_pressure > 0 and claim_temp > 0 else "PENDING"},
        {"stage": "MATERIALS", "code": "MAT-01", "title": "Screen liner material", "why": "The liner has to be shown suitable for pressure, temperature, and fluid compatibility before structural qualification starts.", "acceptance": "Approved material grade, supplier, certs, and screening notes recorded.", "evidence": "Material certs + screening memo", "api": "Material basis", "required": True, "status": "DONE" if liner_done else "PENDING"},
        {"stage": "MATERIALS", "code": "MAT-02", "title": "Screen reinforcement system", "why": "Polyester reinforcement must be checked for long-term durability, hydrolysis risk, and chemistry effects before regression is trusted.", "acceptance": "Reinforcement material selected with compatibility and durability rationale approved.", "evidence": "Reinforcement qualification memo", "api": "Nonmetallic reinforcement basis", "required": True, "status": "DONE" if reinforcement_done else "PENDING"},
        {"stage": "MATERIALS", "code": "MAT-03", "title": "Screen cover material", "why": "The cover protects the pipe system during handling and service exposure, so the material choice has to be locked early.", "acceptance": "Approved cover grade and traceability defined.", "evidence": "Cover material certs", "api": "Material basis", "required": True, "status": "DONE" if cover_done else "PENDING"},
        {"stage": "DESIGN", "code": "DSN-01", "title": "Freeze end fitting and build architecture", "why": "A changing fitting concept invalidates qualification logic and can break family similarity between sizes.", "acceptance": "Drawing revision, fitting concept, and build architecture frozen.", "evidence": "Approved drawings + BOM", "api": "Design freeze", "required": True, "status": "DONE" if fittings_done else "PENDING"},
        {"stage": "TESTING", "code": "TST-01", "title": "Run burst baseline", "why": f"You should not guess whether the product can support {claim_pressure:g} MPa. Burst baseline gives the first structural signal before long-term work begins.", "acceptance": "Burst results reviewed and used to choose sensible long-term pressure levels.", "evidence": "Burst test report", "api": "Section 5.3.13 reference path / design verification", "required": True, "status": "DONE" if burst_done else "PENDING"},
        {"stage": "TESTING", "code": "REG-01", "title": "Launch static regression matrix", "why": "Static long-term hydrostatic regression is the backbone for nonmetallic API 15S qualification and the basis for MPR.", "acceptance": "Matrix covers short, medium, and long failures with valid pressure spread and specimen traceability.", "evidence": "Regression matrix + live specimen log", "api": "API 15S 5.3.2.3 / Annex E / Annex G", "required": True, "status": "IN_PROGRESS" if regression_started else "PENDING"},
        {"stage": "TESTING", "code": "TMP-01", "title": "Confirm MAOT qualification basis", "why": f"The qualification temperature has to support the claimed MAOT of {claim_temp:g} C.", "acceptance": "Selected test temperature is at least as high as claimed MAOT and is reflected in the qualification plan.", "evidence": "Approved temperature basis note", "api": "Temperature qualification logic", "required": True, "status": "DONE" if claim_temp > 0 else "PENDING"},
        {"stage": "TESTING", "code": "MBR-01", "title": "Run handling / MBR confirmation", "why": "The product must prove it can be installed and handled within its operating bend radius and respooling limits.", "acceptance": "MBR and respooling limits are validated and recorded.", "evidence": "MBR test report", "api": "API 15S 5.3.8", "required": True, "status": "PENDING"},
        {"stage": "TESTING", "code": "AXL-01", "title": "Confirm axial load capability", "why": "Axial response is part of the structural envelope and must be known before release.", "acceptance": "Allowable axial load and confirmation test accepted.", "evidence": "Axial test report", "api": "API 15S 5.3.9", "required": True, "status": "PENDING"},
        {"stage": "TESTING", "code": "DOC-01", "title": "Prepare witness and lab plan", "why": "The team needs a controlled test plan, witness route, and lab responsibilities before the qualification program is considered certification-ready.", "acceptance": "Approved laboratory plan, witness route, and sample control list.", "evidence": "Lab / witness plan", "api": "Program control", "required": True, "status": "DONE" if witness_done else "PENDING"},
        {"stage": "REVIEW", "code": "RPT-01", "title": "Close final evidence pack", "why": "API monogram readiness depends on a clean evidence package, not only good raw numbers.", "acceptance": "All required reports, data logs, exclusions, approvals, and traceability records are attached.", "evidence": "Final dossier index", "api": "Certification readiness", "required": True, "status": "PENDING"},
    ]

    if program.pfr_or_pv == "PV" or (sister_size and same_family and program.pfr_or_pv == "PFR"):
        items.append({"stage": "TESTING", "code": "PV-01", "title": "Run PV 1000-hour confirmation", "why": "A sister size in the same family should normally be confirmed as a PV instead of starting a second full qualification from zero.", "acceptance": "PV constant pressure confirmation completed using the PFR relationship.", "evidence": "PV confirmation report", "api": "API 15S 5.3.4.2", "required": True, "status": "PENDING"})
    else:
        items.append({"stage": "TESTING", "code": "PV-01", "title": "PV 1000-hour confirmation", "why": "Only needed when a valid product variant is being claimed from the PFR family.", "acceptance": "N/A", "evidence": "-", "api": "API 15S 5.3.4.2", "required": False, "status": "NOT_APPLICABLE"})

    if gas_service:
        items.append({"stage": "TESTING", "code": "GAS-01", "title": "Run rapid decompression qualification", "why": "Gas or multiphase service brings decompression risk that cannot be waived.", "acceptance": "Rapid decompression results acceptable for intended gas or multiphase service.", "evidence": "RD report", "api": "API 15S 5.3.7 / Annex B", "required": True, "status": "PENDING"})
    else:
        items.append({"stage": "TESTING", "code": "GAS-01", "title": "Rapid decompression", "why": "Not normally needed for non-gas service.", "acceptance": "N/A", "evidence": "Service definition record", "api": "API 15S 5.3.7 / Annex B", "required": False, "status": "NOT_APPLICABLE"})

    if cyclic_service:
        items.append({"stage": "TESTING", "code": "CYC-01", "title": "Run cyclic regression", "why": "Cyclic service requires dedicated fatigue / cycle-based confirmation instead of relying only on static regression.", "acceptance": "Cyclic regression basis established for intended cycle severity.", "evidence": "Cyclic regression report", "api": "API 15S 5.3.16 / Annex D", "required": True, "status": "PENDING"})
    else:
        items.append({"stage": "TESTING", "code": "CYC-01", "title": "Cyclic regression", "why": "Only needed when the service envelope is cyclic enough to trigger it.", "acceptance": "N/A", "evidence": "Service duty record", "api": "API 15S 5.3.16 / Annex D", "required": False, "status": "NOT_APPLICABLE"})

    return items


def _sync_checklist(session: Session, program: RndQualificationProgram) -> None:
    answers = _wizard_answers_map(session, program.id)
    blueprint = _checklist_blueprint(program, answers)
    existing = session.exec(select(RndChecklistItem).where(RndChecklistItem.program_id == program.id)).all()
    by_code = {item.code: item for item in existing}
    seen_codes = set()
    for idx, row in enumerate(blueprint, start=1):
        item = by_code.get(row["code"]) or RndChecklistItem(program_id=program.id, code=row["code"])
        seen_codes.add(row["code"])
        preserve_status = item.status if item.id and item.status not in {"PENDING", "NOT_APPLICABLE"} else row["status"]
        item.sort_order = idx
        item.stage = row["stage"]
        item.title = row["title"]
        item.why_this_matters = row["why"]
        item.acceptance_rule = row["acceptance"]
        item.evidence_required = row["evidence"]
        item.api_reference = row["api"]
        item.applicability = "REQUIRED" if row["required"] else "NOT_APPLICABLE"
        item.status = preserve_status if row["required"] else row["status"]
        _touch_row(item)
        session.add(item)
    for old in existing:
        if old.code not in seen_codes:
            session.delete(old)
    session.commit()


def _checklist_counts(items: list[RndChecklistItem]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        counts[item.status] = counts.get(item.status, 0) + 1
    return counts


def _checklist_progress(items: list[RndChecklistItem]) -> int:
    applicable = [x for x in items if x.applicability == "REQUIRED"]
    if not applicable:
        return 0
    done = [x for x in applicable if x.status in {"DONE", "WAIVED"}]
    return int(round((len(done) / len(applicable)) * 100))


def _t_critical_975(df: int) -> float:
    table = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228, 11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131, 16: 2.120, 17: 2.110, 18: 2.101, 19: 2.093, 20: 2.086, 21: 2.080, 22: 2.074, 23: 2.069, 24: 2.064, 25: 2.060, 26: 2.056, 27: 2.052, 28: 2.048, 29: 2.045, 30: 2.042, 40: 2.021, 60: 2.000, 120: 1.980}
    if df <= 1:
        return table[1]
    keys = sorted(table.keys())
    if df in table:
        return table[df]
    if df > keys[-1]:
        return 1.960
    lower = max(k for k in keys if k < df)
    upper = min(k for k in keys if k > df)
    low_v = table[lower]
    up_v = table[upper]
    ratio = (df - lower) / (upper - lower)
    return low_v + (up_v - low_v) * ratio


def _regression_from_specimens(specimens: List[RndQualificationSpecimen], mode: str = "STATIC_REGRESSION", target_npr_mpa: float = 0.0) -> dict:
    filtered = []
    excluded = []
    for s in specimens:
        if s.test_type != mode:
            continue
        if not s.include_in_regression or not s.permissible_failure:
            excluded.append(s)
            continue
        x_raw = s.failure_hours if mode == "STATIC_REGRESSION" else s.failure_cycles
        y_raw = s.pressure_mpa
        if x_raw is None or y_raw is None or x_raw <= 0 or y_raw <= 0:
            continue
        if mode == "STATIC_REGRESSION" and x_raw < 10:
            excluded.append(s)
            continue
        filtered.append(s)

    n = len(filtered)
    required_minimum = 18 if mode in {"STATIC_REGRESSION", "CYCLIC_REGRESSION"} else 2
    result = {"count": n, "required_minimum": required_minimum, "points": [], "excluded_count": len(excluded), "excluded_ids": [s.specimen_id for s in excluded], "warning": ""}
    if n < 2:
        result["warning"] = "Need at least 2 valid points to calculate a regression line."
        return result

    pts, xs, ys = [], [], []
    for s in filtered:
        x_raw = s.failure_hours if mode == "STATIC_REGRESSION" else s.failure_cycles
        y_raw = s.pressure_mpa
        x = math.log10(float(x_raw))
        y = math.log10(float(y_raw))
        xs.append(x)
        ys.append(y)
        pts.append({"specimen_id": s.specimen_id, "x_raw": x_raw, "y_raw": y_raw, "x": x, "y": y, "temperature_c": s.temperature_c, "failure_mode": s.failure_mode})

    x_bar = sum(xs) / n
    y_bar = sum(ys) / n
    sxx = sum((x - x_bar) ** 2 for x in xs)
    if sxx == 0:
        result.update({"points": pts, "warning": "All time values are identical; regression cannot be calculated."})
        return result

    sxy = sum((xs[i] - x_bar) * (ys[i] - y_bar) for i in range(n))
    slope = sxy / sxx
    intercept = y_bar - slope * x_bar
    residuals = [ys[i] - (intercept + slope * xs[i]) for i in range(n)]
    df = max(1, n - 2)
    syx = math.sqrt(sum(r * r for r in residuals) / df)
    tcrit = _t_critical_975(df)

    def _predict(x_val: float) -> tuple[float, float, float]:
        mean_y = intercept + slope * x_val
        mean_se = syx * math.sqrt((1 / n) + ((x_val - x_bar) ** 2 / sxx))
        pred_se = syx * math.sqrt(1 + (1 / n) + ((x_val - x_bar) ** 2 / sxx))
        lcl_y = mean_y - tcrit * mean_se
        lpl_y = mean_y - tcrit * pred_se
        return mean_y, lcl_y, lpl_y

    basis_x = math.log10(RCRT_HOURS if mode == "STATIC_REGRESSION" else CYCLIC_BASIS_CYCLES)
    y_basis, lcl_basis, lpl_basis = _predict(basis_x)
    mean_basis_mpa = 10 ** y_basis
    lcl_basis_mpa = 10 ** lcl_basis
    lpl_basis_mpa = 10 ** lpl_basis
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
        mean_y, lcl_y, lpl_y = _predict(x_val)
        chart_points.append({"x": x_val, "time_or_cycles": round(10 ** x_val, 3), "mean_pressure": round(10 ** mean_y, 4), "lcl_pressure": round(10 ** lcl_y, 4), "lpl_pressure": round(10 ** lpl_y, 4)})

    result.update({
        "points": pts,
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
        "formula_text": "log10(P) = intercept + slope * log10(time)",
    })
    if n < required_minimum:
        result["warning"] = "Regression is calculated, but you are below the readiness target for a full qualification set."
    return result


def _matrix_counts(tests: List[RndQualificationTest]) -> dict:
    counts = {"PLANNED": 0, "IN_PROGRESS": 0, "PASSED": 0, "FAILED": 0, "WAIVED": 0}
    for t in tests:
        key = (t.status or "PLANNED").upper()
        counts[key] = counts.get(key, 0) + 1
    return counts


def _status_pct(counts: dict, total: int) -> int:
    if total <= 0:
        return 0
    done = counts.get("PASSED", 0) + counts.get("WAIVED", 0)
    return int(round((done / total) * 100))


def _qualification_guide(program: Optional[RndQualificationProgram] = None) -> dict:
    size = f"{program.nominal_size_in:g} in" if program else "4 in"
    npr = f"{program.npr_mpa:g} MPa" if program else "10 MPa"
    maot = f"{program.maot_c:g} °C" if program else "85 °C"
    intended_service = ((program.intended_service if program else "") or "Water service").lower()
    is_gas_service = any(k in intended_service for k in ["gas", "multiphase"])

    service_scope = "Gas or multiphase service is declared, so rapid decompression and gas-service discipline are required." if is_gas_service else "This looks like water or liquid service, so rapid decompression may be waived unless the product will also be sold for gas or multiphase duty."

    if program:
        current_size = float(program.nominal_size_in or 0)
        if current_size in (4.0, 6.0):
            family_logic = "For 4 in and 6 in at the same construction and pressure class, one size should normally be the fully qualified PFR and the other should be handled as a PV within the same product family, not as a second full qualification from zero."
        else:
            family_logic = "Choose the most demanding representative as the PFR, then qualify the rest of the family as PVs only where API 15S allows it."
    else:
        family_logic = "Choose the most demanding representative as the PFR, then qualify the rest of the family as PVs only where API 15S allows it."

    temperature_logic = "The qualification temperature for nonmetallic reinforced pipe has to be at least as high as the claimed MAOT. So a 90 °C rating needs a qualification basis at 90 °C or above; an 85 °C qualification can support 65 °C service, but it does not automatically justify 90 °C."

    return {
        "summary": f"API 15S qualification cockpit for nonmetallic LLRTP using PE-RT liner, polyester yarn reinforcement, and PE cover. The module is structured to guide the team from product definition to evidence pack, with regression monitoring kept as the central control point for {size} / {npr} / {maot}.",
        "product_story": [
            {"title": "Product family strategy", "text": family_logic},
            {"title": "Temperature ladder", "text": temperature_logic},
            {"title": "Regression priority", "text": "Because this is nonmetallic reinforced pipe, the long-term hydrostatic regression is the main qualification backbone. The module should therefore make specimen tracking, chart review, exclusion control, and LCL-based release decisions visible on every program."},
        ],
        "what_applies": [
            "PFR full long-term hydrostatic regression for the representative product.",
            "PV 1000-hour confirmation for product variants within the same family.",
            "Elevated temperature, temperature cycling, MBR and respooling, axial load, external load, LAOT, impact, TEC, and growth or shrinkage checks according to the size and rating range.",
            "Rapid decompression only when the product is intended for gas or multiphase service.",
            "Cyclic regression only when the service definition meets the cyclic threshold.",
        ],
        "what_not_needed": [
            "A second full regression program for every nearby size in the same qualified family.",
            "Rapid decompression for products not intended for gas or multiphase service.",
            "Cyclic regression for clearly non-cyclic service below the API 15S threshold.",
        ],
        "tips": [
            "Use the more demanding size or condition as the PFR where practical so the family coverage is stronger.",
            "Show the exact acceptance logic beside each test card so operators understand why the test exists.",
            "Give every test step a short plain-language reason, not only the clause reference.",
            "Make regression status visible on the dashboard with count of valid points, excluded points, current LCL, current MPR, and release margin versus NPR.",
        ],
        "avoid": [
            "Do not qualify every nearby size from zero if a valid PV route exists.",
            "Do not claim a higher temperature than the qualification basis.",
            "Do not over-read a provisional pass from a thin regression dataset.",
        ],
        "service_scope": service_scope,
    }


@router.get("")
def rnd_home() -> RedirectResponse:
    return RedirectResponse(url="/rnd/qualifications", status_code=303)


@router.get("/qualifications")
def rnd_dashboard(request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    programs = session.exec(select(RndQualificationProgram).order_by(RndQualificationProgram.updated_at.desc())).all()
    dashboard = []
    for program in programs:
        _seed_test_matrix(session, program)
        tests = session.exec(select(RndQualificationTest).where(RndQualificationTest.program_id == program.id)).all()
        specimens = session.exec(select(RndQualificationSpecimen).where(RndQualificationSpecimen.program_id == program.id)).all()
        static_reg = _regression_from_specimens(specimens, "STATIC_REGRESSION", program.npr_mpa)
        counts = _matrix_counts(tests)
        checklist = session.exec(select(RndChecklistItem).where(RndChecklistItem.program_id == program.id).order_by(RndChecklistItem.sort_order.asc())).all()
        dashboard.append({
            "program": program,
            "tests": tests,
            "specimens": specimens,
            "counts": counts,
            "progress_pct": _status_pct(counts, len(tests)),
            "static_reg": static_reg,
            "checklist_pct": _checklist_progress(checklist),
            "checklist_counts": _checklist_counts(checklist),
        })
    guide = _qualification_guide()
    return TEMPLATES.TemplateResponse("rnd_dashboard.html", {"request": request, "user": user, "dashboard": dashboard, "guide": guide, "design_factor_nonmetallic": DESIGN_FACTOR_NONMETALLIC, "rcrt_hours": RCRT_HOURS})


@router.get("/qualifications/new")
def rnd_new_program_form(request: Request, user: User = Depends(_require_user)):
    return TEMPLATES.TemplateResponse("rnd_program_form.html", {"request": request, "user": user})


@router.post("/qualifications/new")
def rnd_create_program(session: Session = Depends(get_session), user: User = Depends(_require_user), title: str = Form(...), program_code: str = Form(...), nominal_size_in: float = Form(...), npr_mpa: float = Form(...), maot_c: float = Form(...), laot_c: float = Form(0.0), pfr_or_pv: str = Form("PFR"), parent_program_id: Optional[int] = Form(None), intended_service: str = Form("Static water service"), notes: str = Form("")):
    program = RndQualificationProgram(program_code=(program_code or "").strip().upper(), title=(title or "").strip(), nominal_size_in=nominal_size_in, npr_mpa=npr_mpa, maot_c=maot_c, laot_c=laot_c, pfr_or_pv=(pfr_or_pv or "PFR").strip().upper(), parent_program_id=parent_program_id, intended_service=intended_service, notes=notes, created_by_name=(getattr(user, "display_name", "") or getattr(user, "username", "") or ""))
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
    return RedirectResponse(url=f"/rnd/qualifications/{program.id}/wizard", status_code=303)


@router.get("/qualifications/{program_id}/wizard")
def rnd_program_wizard(program_id: int, request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, "Program not found")
    _seed_test_matrix(session, program)
    answers = _wizard_answers_map(session, program_id)
    specimens = session.exec(select(RndQualificationSpecimen).where(RndQualificationSpecimen.program_id == program_id)).all()
    static_reg = _regression_from_specimens(specimens, "STATIC_REGRESSION", program.npr_mpa)
    roadmap = _roadmap_summary(program, answers, static_reg)
    checklist = session.exec(select(RndChecklistItem).where(RndChecklistItem.program_id == program_id).order_by(RndChecklistItem.sort_order.asc())).all()
    return TEMPLATES.TemplateResponse("rnd_wizard.html", {"request": request, "user": user, "program": program, "sections": _wizard_sections(session, program_id), "answers": answers, "checklist": checklist, "checklist_pct": _checklist_progress(checklist), "roadmap": roadmap, "guide": _qualification_guide(program)})


@router.post("/qualifications/{program_id}/wizard")
def rnd_save_program_wizard(
    program_id: int,
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),
    claim_pressure_mpa: str = Form("10"),
    claim_temperature_c: str = Form("85"),
    launch_size_in: str = Form("4"),
    sister_size_in: str = Form("6"),
    family_same_construction: str = Form("yes"),
    service_medium: str = Form("water"),
    service_is_cyclic: str = Form("no"),
    service_requires_gas: str = Form("no"),
    liner_screened: str = Form("no"),
    reinforcement_screened: str = Form("no"),
    cover_screened: str = Form("no"),
    end_fitting_ready: str = Form("no"),
    has_burst_data: str = Form("no"),
    has_regression_data: str = Form("no"),
    has_witness_plan: str = Form("no"),
):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, "Program not found")
    posted = {
        "claim_pressure_mpa": claim_pressure_mpa,
        "claim_temperature_c": claim_temperature_c,
        "launch_size_in": launch_size_in,
        "sister_size_in": sister_size_in,
        "family_same_construction": family_same_construction,
        "service_medium": service_medium,
        "service_is_cyclic": service_is_cyclic,
        "service_requires_gas": service_requires_gas,
        "liner_screened": liner_screened,
        "reinforcement_screened": reinforcement_screened,
        "cover_screened": cover_screened,
        "end_fitting_ready": end_fitting_ready,
        "has_burst_data": has_burst_data,
        "has_regression_data": has_regression_data,
        "has_witness_plan": has_witness_plan,
    }
    labels = {(key): (label, section) for section, key, label in WIZARD_FIELDS}
    for key, value in posted.items():
        label, section = labels[key]
        _upsert_wizard_answer(session, program_id, section, key, label, value)

    program.npr_mpa = _to_float(claim_pressure_mpa, program.npr_mpa)
    program.maot_c = _to_float(claim_temperature_c, program.maot_c)
    program.nominal_size_in = _to_float(launch_size_in, program.nominal_size_in)
    service_tokens = [service_medium.replace("_", " ")]
    if _to_bool(service_is_cyclic):
        service_tokens.append("cyclic")
    if _to_bool(service_requires_gas) and service_medium not in {"gas", "multiphase"}:
        service_tokens.append("gas qualified")
    program.intended_service = " / ".join([x for x in service_tokens if x])
    _touch_program(program)
    session.add(program)
    session.commit()
    _sync_checklist(session, program)
    return RedirectResponse(url=f"/rnd/qualifications/{program_id}", status_code=303)


@router.get("/qualifications/{program_id}")
def rnd_program_view(program_id: int, request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, "Program not found")
    _seed_test_matrix(session, program)
    tests = session.exec(select(RndQualificationTest).where(RndQualificationTest.program_id == program_id).order_by(RndQualificationTest.sort_order.asc(), RndQualificationTest.id.asc())).all()
    specimens = session.exec(select(RndQualificationSpecimen).where(RndQualificationSpecimen.program_id == program_id).order_by(RndQualificationSpecimen.created_at.desc())).all()
    materials = session.exec(select(RndMaterialQualification).where(RndMaterialQualification.program_id == program_id).order_by(RndMaterialQualification.id.asc())).all()
    attachments = session.exec(select(RndAttachmentRegister).where(RndAttachmentRegister.program_id == program_id).order_by(RndAttachmentRegister.created_at.desc())).all()
    checklist = session.exec(select(RndChecklistItem).where(RndChecklistItem.program_id == program_id).order_by(RndChecklistItem.sort_order.asc())).all()
    answers = _wizard_answers_map(session, program_id)
    static_reg = _regression_from_specimens(specimens, "STATIC_REGRESSION", program.npr_mpa)
    cyclic_reg = _regression_from_specimens(specimens, "CYCLIC_REGRESSION", program.npr_mpa)
    counts = _matrix_counts(tests)
    guide = _qualification_guide(program)
    roadmap = _roadmap_summary(program, answers, static_reg)
    pressure_plan = _pressure_recommendation(program, answers, static_reg, _burst_strength_summary(specimens))
    matrix_plan = _suggest_regression_matrix(program, answers, specimens, static_reg)
    test_guidance = _test_guidance_rows(program, tests, answers)
    dossier_sections = _dossier_sections(program, checklist, tests, materials, attachments, static_reg, roadmap, pressure_plan)
    return TEMPLATES.TemplateResponse("rnd_program_view.html", {"request": request, "user": user, "program": program, "tests": tests, "specimens": specimens, "materials": materials, "attachments": attachments, "static_reg": static_reg, "cyclic_reg": cyclic_reg, "counts": counts, "progress_pct": _status_pct(counts, len(tests)), "guide": guide, "design_factor_nonmetallic": DESIGN_FACTOR_NONMETALLIC, "rcrt_hours": RCRT_HOURS, "checklist": checklist, "checklist_pct": _checklist_progress(checklist), "checklist_counts": _checklist_counts(checklist), "answers": answers, "roadmap": roadmap, "pressure_plan": pressure_plan, "matrix_plan": matrix_plan, "test_guidance": test_guidance, "dossier_sections": dossier_sections})


@router.post("/qualifications/{program_id}/checklist/{item_id}")
def rnd_update_checklist_item(program_id: int, item_id: int, status: str = Form(...), notes: str = Form(""), session: Session = Depends(get_session), user: User = Depends(_require_user)):
    row = session.get(RndChecklistItem, item_id)
    if not row or row.program_id != program_id:
        raise HTTPException(404, "Checklist item not found")
    row.status = (status or "PENDING").strip().upper()
    row.notes = notes or ""
    _touch_row(row)
    session.add(row)
    program = session.get(RndQualificationProgram, program_id)
    if program:
        _touch_program(program)
        session.add(program)
    session.commit()
    return RedirectResponse(url=f"/rnd/qualifications/{program_id}", status_code=303)


@router.post("/qualifications/{program_id}/status")
def rnd_update_program_status(program_id: int, status: str = Form(...), session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, "Program not found")
    program.status = (status or "DRAFT").strip().upper()
    _touch_program(program)
    session.add(program)
    session.commit()
    return RedirectResponse(url=f"/rnd/qualifications/{program_id}", status_code=303)


@router.post("/qualifications/{program_id}/tests/{test_id}")
def rnd_update_test(program_id: int, test_id: int, status: str = Form(...), result_summary: str = Form(""), session: Session = Depends(get_session), user: User = Depends(_require_user)):
    test = session.get(RndQualificationTest, test_id)
    if not test or test.program_id != program_id:
        raise HTTPException(404, "Test not found")
    test.status = (status or "PLANNED").strip().upper()
    test.result_summary = result_summary or ""
    _touch_row(test)
    session.add(test)
    program = session.get(RndQualificationProgram, program_id)
    if program:
        _touch_program(program)
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


@router.post("/qualifications/{program_id}/attachments/new")
def rnd_add_attachment_register(program_id: int, category: str = Form("REPORT"), title: str = Form(...), reference_no: str = Form(""), file_note: str = Form(""), session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, "Program not found")
    session.add(RndAttachmentRegister(program_id=program_id, category=(category or "REPORT").strip().upper(), title=title.strip(), reference_no=reference_no.strip(), file_note=file_note.strip()))
    _touch_program(program)
    session.add(program)
    session.commit()
    return RedirectResponse(url=f"/rnd/qualifications/{program_id}", status_code=303)


@router.post("/qualifications/{program_id}/specimens/new")
def rnd_add_specimen(program_id: int, session: Session = Depends(get_session), user: User = Depends(_require_user), specimen_id: str = Form(...), test_type: str = Form(...), test_id: Optional[int] = Form(None), sample_date: date = Form(...), nominal_size_in: float = Form(0.0), pressure_mpa: float = Form(0.0), temperature_c: float = Form(0.0), failure_hours: Optional[float] = Form(None), failure_cycles: Optional[float] = Form(None), failure_mode: str = Form(""), permissible_failure: Optional[str] = Form(None), is_runout: Optional[str] = Form(None), include_in_regression: Optional[str] = Form(None), fitting_type: str = Form("Field fitting"), lab_name: str = Form(""), witness_name: str = Form(""), notes: str = Form("")):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, "Program not found")
    specimen = RndQualificationSpecimen(program_id=program_id, test_id=test_id, specimen_id=(specimen_id or "").strip().upper(), test_type=(test_type or "STATIC_REGRESSION").strip().upper(), sample_date=sample_date, nominal_size_in=nominal_size_in or program.nominal_size_in, pressure_mpa=pressure_mpa, temperature_c=temperature_c, failure_hours=failure_hours, failure_cycles=failure_cycles, failure_mode=failure_mode, permissible_failure=bool(permissible_failure), is_runout=bool(is_runout), include_in_regression=bool(include_in_regression), fitting_type=fitting_type, lab_name=lab_name, witness_name=witness_name, notes=notes)
    session.add(specimen)
    _touch_program(program)
    session.add(program)
    session.commit()
    return RedirectResponse(url=f"/rnd/qualifications/{program_id}", status_code=303)


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
    return RedirectResponse(url=f"/rnd/qualifications/{program_id}", status_code=303)


@router.get("/qualifications/{program_id}/regression")
def rnd_regression_view(program_id: int, request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, "Program not found")
    specimens = session.exec(select(RndQualificationSpecimen).where(RndQualificationSpecimen.program_id == program_id).order_by(RndQualificationSpecimen.created_at.asc())).all()
    static_reg = _regression_from_specimens(specimens, "STATIC_REGRESSION", program.npr_mpa)
    cyclic_reg = _regression_from_specimens(specimens, "CYCLIC_REGRESSION", program.npr_mpa)
    pv_formula = None
    if program.pfr_or_pv == "PV" and program.parent_program_id:
        parent = session.get(RndQualificationProgram, program.parent_program_id)
        if parent:
            ratio = (program.npr_mpa / parent.npr_mpa) if parent.npr_mpa else None
            pv_formula = {"pfr_code": parent.program_code, "npr_pv": program.npr_mpa, "npr_pfr": parent.npr_mpa, "formula": "PPV1000 = PPFR1000 x (NPR_PV / NPR_PFR)", "ratio": ratio}
    guide = _qualification_guide(program)
    return TEMPLATES.TemplateResponse("rnd_regression_view.html", {"request": request, "user": user, "program": program, "specimens": specimens, "static_reg": static_reg, "cyclic_reg": cyclic_reg, "pv_formula": pv_formula, "guide": guide, "design_factor_nonmetallic": DESIGN_FACTOR_NONMETALLIC, "rcrt_hours": RCRT_HOURS})


@router.get("/qualifications/{program_id}/report")
def rnd_final_report(program_id: int, request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, "Program not found")
    _seed_test_matrix(session, program)
    answers = _wizard_answers_map(session, program_id)
    checklist = session.exec(select(RndChecklistItem).where(RndChecklistItem.program_id == program_id).order_by(RndChecklistItem.sort_order.asc())).all()
    tests = session.exec(select(RndQualificationTest).where(RndQualificationTest.program_id == program_id).order_by(RndQualificationTest.sort_order.asc())).all()
    materials = session.exec(select(RndMaterialQualification).where(RndMaterialQualification.program_id == program_id).order_by(RndMaterialQualification.id.asc())).all()
    attachments = session.exec(select(RndAttachmentRegister).where(RndAttachmentRegister.program_id == program_id).order_by(RndAttachmentRegister.created_at.desc())).all()
    specimens = session.exec(select(RndQualificationSpecimen).where(RndQualificationSpecimen.program_id == program_id).order_by(RndQualificationSpecimen.created_at.asc())).all()
    static_reg = _regression_from_specimens(specimens, "STATIC_REGRESSION", program.npr_mpa)
    roadmap = _roadmap_summary(program, answers, static_reg)
    pressure_plan = _pressure_recommendation(program, answers, static_reg, _burst_strength_summary(specimens))
    dossier_sections = _dossier_sections(program, checklist, tests, materials, attachments, static_reg, roadmap, pressure_plan)
    return TEMPLATES.TemplateResponse("rnd_report.html", {"request": request, "user": user, "program": program, "roadmap": roadmap, "checklist": checklist, "tests": tests, "materials": materials, "attachments": attachments, "static_reg": static_reg, "pressure_plan": pressure_plan, "dossier_sections": dossier_sections, "checklist_pct": _checklist_progress(checklist)})


@router.get("/qualifications/{program_id}/regression/export.csv")
def rnd_regression_export_csv(program_id: int, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, "Program not found")
    specimens = session.exec(select(RndQualificationSpecimen).where(RndQualificationSpecimen.program_id == program_id).order_by(RndQualificationSpecimen.created_at.asc())).all()
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["specimen_id", "test_type", "pressure_mpa", "temperature_c", "failure_hours", "failure_cycles", "failure_mode", "included"])
    for s in specimens:
        writer.writerow([s.specimen_id, s.test_type, s.pressure_mpa, s.temperature_c, s.failure_hours or "", s.failure_cycles or "", s.failure_mode or "", "yes" if s.include_in_regression and s.permissible_failure else "no"])
    mem = StringIO(output.getvalue())
    headers = {"Content-Disposition": f'attachment; filename="{program.program_code.lower()}-regression.csv"'}
    return StreamingResponse(iter([mem.getvalue()]), media_type="text/csv", headers=headers)
