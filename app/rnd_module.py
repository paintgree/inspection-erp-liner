from __future__ import annotations

import json
import math
import os
from datetime import datetime, date
from typing import Optional, List

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
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
        if row['applicability'] == 'PFR' and pfr_or_pv != 'PFR':
            continue
        if row['applicability'] == 'PV' and pfr_or_pv != 'PV':
            continue
        items.append(row)
    return items


def _seed_test_matrix(session: Session, program: RndQualificationProgram) -> None:
    existing = session.exec(select(RndQualificationTest).where(RndQualificationTest.program_id == program.id)).all()
    if existing:
        return
    for idx, item in enumerate(_default_test_matrix(program.pfr_or_pv), start=1):
        session.add(RndQualificationTest(program_id=program.id, sort_order=idx, clause_ref=item['clause_ref'], code=item['code'], title=item['title'], description=item['description'], specimen_requirement=item['specimen_requirement'], applicability=item['applicability']))
    for component, material in [('LINER', program.liner_material), ('REINFORCEMENT', program.reinforcement_material), ('COVER', program.cover_material)]:
        session.add(RndMaterialQualification(program_id=program.id, component=component, material_name=material))
    session.commit()


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


def _regression_from_specimens(specimens: List[RndQualificationSpecimen], mode: str = 'STATIC_REGRESSION', target_npr_mpa: float = 0.0) -> dict:
    filtered = []
    excluded = []
    for s in specimens:
        if s.test_type != mode:
            continue
        if not s.include_in_regression or not s.permissible_failure:
            excluded.append(s)
            continue
        x_raw = s.failure_hours if mode == 'STATIC_REGRESSION' else s.failure_cycles
        y_raw = s.pressure_mpa
        if x_raw is None or y_raw is None or x_raw <= 0 or y_raw <= 0:
            continue
        if mode == 'STATIC_REGRESSION' and x_raw < 10:
            excluded.append(s)
            continue
        filtered.append(s)

    n = len(filtered)
    required_minimum = 18 if mode in {'STATIC_REGRESSION', 'CYCLIC_REGRESSION'} else 2
    result = {'count': n, 'required_minimum': required_minimum, 'points': [], 'excluded_count': len(excluded), 'excluded_ids': [s.specimen_id for s in excluded], 'warning': ''}
    if n < 2:
        result['warning'] = 'Need at least 2 valid points to calculate a regression line.'
        return result

    pts, xs, ys = [], [], []
    for s in filtered:
        x_raw = s.failure_hours if mode == 'STATIC_REGRESSION' else s.failure_cycles
        y_raw = s.pressure_mpa
        x = math.log10(float(x_raw))
        y = math.log10(float(y_raw))
        xs.append(x); ys.append(y)
        pts.append({'specimen_id': s.specimen_id, 'x_raw': x_raw, 'y_raw': y_raw, 'x': x, 'y': y, 'temperature_c': s.temperature_c, 'failure_mode': s.failure_mode})

    x_bar = sum(xs) / n
    y_bar = sum(ys) / n
    sxx = sum((x - x_bar) ** 2 for x in xs)
    if sxx == 0:
        result.update({'points': pts, 'warning': 'All time values are identical; regression cannot be calculated.'})
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

    basis_x = math.log10(RCRT_HOURS if mode == 'STATIC_REGRESSION' else CYCLIC_BASIS_CYCLES)
    y_basis, lcl_basis, lpl_basis = _predict(basis_x)
    mean_basis_mpa = 10 ** y_basis
    lcl_basis_mpa = 10 ** lcl_basis
    lpl_basis_mpa = 10 ** lpl_basis
    design_factor = DESIGN_FACTOR_NONMETALLIC if mode == 'STATIC_REGRESSION' else 1.0
    mpr_mpa = lcl_basis_mpa * design_factor if mode == 'STATIC_REGRESSION' else lcl_basis_mpa
    margin_mpa = mpr_mpa - target_npr_mpa if target_npr_mpa else None
    pass_status = None if not target_npr_mpa else ('PASS' if mpr_mpa >= target_npr_mpa else 'FAIL')

    chart_points = []
    x_min = min(xs)
    x_max = max(max(xs), basis_x)
    steps = 24
    for i in range(steps + 1):
        x_val = x_min + (x_max - x_min) * i / steps
        mean_y, lcl_y, lpl_y = _predict(x_val)
        chart_points.append({'x': x_val, 'time_or_cycles': round(10 ** x_val, 3), 'mean_pressure': round(10 ** mean_y, 4), 'lcl_pressure': round(10 ** lcl_y, 4), 'lpl_pressure': round(10 ** lpl_y, 4)})

    result.update({
        'points': pts, 'slope': slope, 'intercept': intercept, 'syx': syx, 'tcrit': tcrit, 'x_bar': x_bar, 'y_bar': y_bar,
        'x_basis': basis_x, 'rcrt_hours': RCRT_HOURS, 'cyclic_basis_cycles': CYCLIC_BASIS_CYCLES,
        'mean_rcrt_mpa': mean_basis_mpa, 'lcl_rcrt_mpa': lcl_basis_mpa, 'lpl_rcrt_mpa': lpl_basis_mpa,
        'chart_points': chart_points, 'design_factor': design_factor, 'mpr_mpa': mpr_mpa,
        'target_npr_mpa': target_npr_mpa, 'margin_mpa': margin_mpa, 'pass_status': pass_status,
        'formula_text': 'log10(P) = intercept + slope * log10(time)',
    })
    if n < required_minimum:
        result['warning'] = 'Regression is calculated, but you are below the readiness target for a full qualification set.'
    return result


def _matrix_counts(tests: List[RndQualificationTest]) -> dict:
    counts = {'PLANNED': 0, 'IN_PROGRESS': 0, 'PASSED': 0, 'FAILED': 0, 'WAIVED': 0}
    for t in tests:
        key = (t.status or 'PLANNED').upper()
        counts[key] = counts.get(key, 0) + 1
    return counts


def _status_pct(counts: dict, total: int) -> int:
    if total <= 0:
        return 0
    done = counts.get('PASSED', 0) + counts.get('WAIVED', 0)
    return int(round((done / total) * 100))


def _qualification_guide(program: Optional[RndQualificationProgram] = None) -> dict:
    size = f"{program.nominal_size_in:g} in" if program else '4 in'
    npr = f"{program.npr_mpa:g} MPa" if program else '10 MPa'
    maot = f"{program.maot_c:g} °C" if program else '65 °C'
    return {
        'summary': f'This workspace organizes API 15S qualification for LLRTP with PE-RT liner, polyester fiber reinforcement, and PE100 cover. It guides the user through product definition, test matrix, specimen tracking, and regression review for {size} / {npr} / {maot}.',
        'steps': [
            {'title': '1. Define the qualification basis', 'text': 'Create the program as PFR or PV, set size, NPR, MAOT, service, and material stack. Use the most demanding representative as the PFR when possible.'},
            {'title': '2. Lock the material system', 'text': 'Record liner, reinforcement, and cover grade, supplier, batch, and certificate references before test execution.'},
            {'title': '3. Build the specimen plan', 'text': 'Prepare static regression specimens, cyclic specimens if cyclic service applies, and the rest of the API 15S matrix such as temperature cycling, MBR, impact, axial load, and decompression when applicable.'},
            {'title': '4. Run regression correctly', 'text': 'For static regression, record pressure, temperature, time to failure, and failure mode. Exclude invalid failures and any point below 10 h from the regression dataset.'},
            {'title': '5. Review the lower confidence basis', 'text': 'Use the lower confidence result at 175,000 h for nonmetallic reinforcement, then apply the design factor to compare against the target NPR.'},
            {'title': '6. Close the program only with full evidence', 'text': 'A program is ready to close only when the matrix is complete, materials are traceable, exclusions are justified, and the final qualification package is signed off.'},
        ],
        'observe': [
            'Use the correct end fittings so fitting failures do not corrupt the pipe qualification dataset.',
            'Keep test temperature stable and recorded for every specimen.',
            'Keep a clear reason whenever a point is excluded from regression.',
            'Do not use average pressure alone for acceptance; review LCL and MPR basis.',
        ],
        'avoid': [
            'Do not mix different designs or reinforcement constructions in one regression set.',
            'Do not include points below 10 h in static regression.',
            'Do not treat the software as a substitute for engineering review or third-party witness requirements.',
            'Do not close a qualification with missing raw records, certificates, or failure descriptions.',
        ],
        'formula_examples': [
            {'label': 'Regression line', 'expr': 'log10(P) = intercept + slope * log10(time)'},
            {'label': 'Lower confidence at design life', 'expr': 'LCL_175000h = lower confidence pressure at 175,000 h'},
            {'label': 'Nonmetallic MPR', 'expr': 'MPR = LCL_175000h x 0.67'},
            {'label': 'PV helper', 'expr': 'PPV1000 = PPFR1000 x (NPR_PV / NPR_PFR)'},
        ],
    }


def _program_answers(program: RndQualificationProgram) -> dict:
    raw = (program.notes or '').strip()
    if raw.startswith('__RNDJSON__'):
        try:
            return json.loads(raw[len('__RNDJSON__'):])
        except Exception:
            return {}
    return {}


def _save_program_answers(program: RndQualificationProgram, answers: dict) -> None:
    program.notes = '__RNDJSON__' + json.dumps(answers, ensure_ascii=False)
    _touch_program(program)


def _wizard_state(program: RndQualificationProgram) -> dict:
    answers = _program_answers(program)
    launch_size = answers.get('launch_size_in') or f"{program.nominal_size_in:g}"
    sister_size = answers.get('sister_size_in') or '6'
    service_route = answers.get('service_route') or ('gas_multiphase' if 'gas' in (program.intended_service or '').lower() else 'static_liquid')
    cyclic_service = answers.get('cyclic_service', 'no')
    decision = {
        'launch_size': launch_size,
        'sister_size': sister_size,
        'service_route': service_route,
        'cyclic_service': cyclic_service,
        'family_decision': f'Use {launch_size} in as the main qualified size and handle {sister_size} in as a PV only if materials, pressure class, and construction remain matched.',
        'service_decision': 'Rapid decompression is required.' if service_route == 'gas_multiphase' else 'Rapid decompression is not required for static liquid service.',
        'cyclic_decision': 'Cyclic regression route is required.' if cyclic_service == 'yes' else 'Cyclic route is not required unless the field duty exceeds the API cyclic trigger.',
        'temperature_decision': f'Qualification temperature should be at least the claimed MAOT of {program.maot_c:g} °C. Higher claims need their own basis.',
        'wizard_complete': True,
    }
    return {'answers': answers, 'decision': decision}


def _material_screening_state(materials: List[RndMaterialQualification]) -> dict:
    rows = []
    all_ready = True
    for m in materials:
        missing = []
        if not (m.material_name or '').strip():
            missing.append('material name')
        if not (m.supplier_name or '').strip():
            missing.append('supplier')
        if not (m.grade_name or '').strip():
            missing.append('grade')
        if not (m.certificate_ref or '').strip():
            missing.append('certificate ref')
        if not (m.batch_ref or '').strip():
            missing.append('batch ref')
        if not (m.notes or '').strip():
            missing.append('screening note')
        ready = len(missing) == 0
        all_ready = all_ready and ready
        rows.append({'row': m, 'missing': missing, 'ready': ready})
    return {
        'rows': rows,
        'complete': all_ready and len(rows) >= 3,
        'status_label': 'Accepted' if all_ready and len(rows) >= 3 else 'More data required',
        'headline': 'Record traceable grade, supplier, certificate, batch, and a short screening note for each material before structural testing starts.',
    }


def _burst_threshold(program: RndQualificationProgram) -> float:
    if program.npr_mpa <= 0:
        return 0.0
    return round(program.npr_mpa / DESIGN_FACTOR_NONMETALLIC, 3)


def _burst_state(program: RndQualificationProgram, specimens: List[RndQualificationSpecimen]) -> dict:
    burst_rows = [s for s in specimens if (s.test_type or '').upper() == 'BURST_QUALIFICATION']
    threshold = _burst_threshold(program)
    evaluated = []
    accepted = 0
    review_needed = 0
    for s in burst_rows:
        flags = []
        if s.pressure_mpa <= 0:
            flags.append('Burst pressure missing.')
        if s.temperature_c and abs(float(s.temperature_c) - float(program.maot_c)) > 5.0:
            flags.append(f'Test temperature {s.temperature_c:g} °C is outside the ±5 °C window around the qualification basis.')
        mode = (s.failure_mode or '').strip().lower()
        if mode and mode not in {'burst', 'rupture'}:
            flags.append('Failure mode is not a clear burst/rupture and needs engineering review.')
        if s.pressure_mpa and s.pressure_mpa < threshold:
            flags.append(f'Burst pressure is below the minimum screen threshold of {threshold:.3f} MPa.')
        status = 'ACCEPTED' if not flags else 'REVIEW'
        if status == 'ACCEPTED':
            accepted += 1
        else:
            review_needed += 1
        evaluated.append({'specimen': s, 'flags': flags, 'status': status})
    require_count = 5
    complete = accepted >= require_count and review_needed == 0
    return {
        'threshold_mpa': threshold,
        'required_count': require_count,
        'accepted_count': accepted,
        'review_count': review_needed,
        'rows': evaluated,
        'complete': complete,
        'headline': 'Run burst testing first as a design screen. The system will only unlock the next step once five acceptable specimens are recorded against the minimum burst threshold.',
    }


def _active_stage(program: RndQualificationProgram, materials: List[RndMaterialQualification], specimens: List[RndQualificationSpecimen]) -> dict:
    wizard = _wizard_state(program)
    material_state = _material_screening_state(materials)
    burst_state = _burst_state(program, specimens)
    static_reg = _regression_from_specimens(specimens, 'STATIC_REGRESSION', program.npr_mpa)
    if not material_state['complete']:
        current = 'materials'
        percent = 33
    elif not burst_state['complete']:
        current = 'burst'
        percent = 58
    elif static_reg['count'] < static_reg['required_minimum']:
        current = 'regression'
        percent = 78
    else:
        current = 'review'
        percent = 100
    return {
        'wizard': wizard,
        'materials': material_state,
        'burst': burst_state,
        'static_reg': static_reg,
        'current': current,
        'progress_pct': percent,
    }




def _test_stage_label(code: str) -> str:
    code = (code or '').upper()
    if code in {'MPR_REG', 'PV_1000H', 'CYCLIC_REG'}:
        return 'Pressure Qualification'
    if code in {'TEMP_ELEV', 'TEMP_CYCLE', 'LAOT', 'IMPACT', 'TEC', 'GROWTH'}:
        return 'Environmental and Thermal'
    if code in {'RAPID_DECOMP'}:
        return 'Service Specific'
    if code in {'OPERATING_MBR', 'AXIAL_LOAD', 'CRUSH'}:
        return 'Installation and Handling'
    return 'Qualification Matrix'


def _test_instruction_pack(test: RndQualificationTest, program: RndQualificationProgram) -> dict:
    code = (test.code or '').upper()
    threshold = _burst_threshold(program)
    packs = {
        'MPR_REG': {
            'why': 'This is the main long-term pressure qualification backbone for nonmetallic reinforced PFR products.',
            'how': [
                'Create a static hydrostatic specimen matrix with pressure bands that generate both shorter and longer time-to-failure points.',
                'Record pressure, temperature, time to failure, and clear failure mode for every specimen.',
                'Exclude points below 10 h from the static regression basis and document why any point is excluded.',
                f'Project the lower confidence basis to {RCRT_HOURS:,.0f} h and compare the resulting MPR with the claimed NPR.'
            ],
            'acceptance': [
                'Target at least 18 valid long-term points for the PFR qualification set.',
                'Use only permissible failures in the regression set.',
                'Do not rely on a provisional pass from only a few points.'
            ],
            'basis': f'API 15S uses the lower confidence basis at {RCRT_HOURS:,.0f} h for nonmetallic reinforced pipe, then applies the design factor {DESIGN_FACTOR_NONMETALLIC:.2f} to determine MPR.'
        },
        'PV_1000H': {
            'why': 'This confirms that a product variant remains inside the already-qualified family envelope.',
            'how': ['Prepare two PV specimens using the qualified construction relationship.', 'Run the 1000-hour constant pressure confirmation according to the PV route.', 'Compare the PV confirmation pressure against the family relationship and record survival or failure.'],
            'acceptance': ['Both specimens should satisfy the PV confirmation basis.', 'Use this route only when the sister product truly remains within the same qualified family.'],
            'basis': 'API 15S allows a PV route instead of a second full qualification from zero when family rules are respected.'
        },
        'TEMP_ELEV': {
            'why': 'This checks performance above the claimed operating temperature and confirms thermal stability of the assembly.',
            'how': ['Condition specimens at the selected elevated temperature basis.', 'Run the required hold or follow-up confirmation according to the procedure.', 'Record whether leakage, delamination, creep, or fitting instability is observed.'],
            'acceptance': ['Specimens should survive the required thermal exposure without disqualifying failure.', 'Any change beyond the intended claim should be justified by the higher temperature basis.'],
            'basis': 'The qualification temperature must support the claimed MAOT.'
        },
        'TEMP_CYCLE': {
            'why': 'This verifies repeated thermal expansion and contraction of the pipe structure and fittings.',
            'how': ['Cycle between the required temperature limits.', 'Keep pressure and end-fittings consistent with the selected route.', 'Inspect for leaks, cracks, and loss of integrity after cycling.'],
            'acceptance': ['No disqualifying structural or sealing failure should remain after the cycle exposure.'],
            'basis': 'Thermal cycling is part of the API 15S qualification matrix for nonmetallic reinforced pipe.'
        },
        'RAPID_DECOMP': {
            'why': 'Gas and multiphase service can damage nonmetallic structures if decompression behavior is poor.',
            'how': ['Use the gas or multiphase route only when that service is intended.', 'Run the decompression sequence and inspect the structure afterward.', 'Record blistering, cracking, separation, or seal damage.'],
            'acceptance': ['Required only when the intended service route includes gas or multiphase exposure.'],
            'basis': 'API 15S makes this test service-dependent rather than universal.'
        },
        'OPERATING_MBR': {
            'why': 'This confirms the pipe can bend, spool, or respool without losing performance.',
            'how': ['Condition or bend the specimen to the relevant MBR.', 'Perform the required follow-up pressure or confirmation test after bending.', 'Capture bend radius, conditioning route, and final result.'],
            'acceptance': [f'Where a burst follow-up is used as the screening note, each burst should remain at or above {threshold:.3f} MPa for the current 10 MPa planning basis.', 'For nonmetallic PV follow-up, use the 1000-hour route where applicable.'],
            'basis': 'The post-bending follow-up test demonstrates that handling did not damage qualification performance.'
        },
        'AXIAL_LOAD': {
            'why': 'This checks whether installation axial loads damage the pipe or fitting system.',
            'how': ['Apply the stated axial load route.', 'Run the required follow-up confirmation test.', 'Record whether the specimen still meets structural requirements.'],
            'acceptance': ['No disqualifying failure after the load and follow-up confirmation.'],
            'basis': 'API 15S includes axial load capability in the qualification matrix.'
        },
        'CRUSH': {
            'why': 'This demonstrates resistance to external radial loading.',
            'how': ['Perform the external load procedure on the required number of specimens.', 'Measure deformation and any loss of performance.', 'Record whether the pipe still meets the follow-up acceptance route.'],
            'acceptance': ['Use only when applicable to the selected range or service route.'],
            'basis': 'External load performance is part of the nonmetallic qualification table.'
        },
        'LAOT': {
            'why': 'This confirms the minimum temperature claim can be supported without brittle or sealing problems.',
            'how': ['Condition the specimens at the selected low temperature basis.', 'Run the required follow-up confirmation.', 'Record crack, leak, or rupture behavior.'],
            'acceptance': ['The assembly should survive the low-temperature route without disqualifying failure.'],
            'basis': 'LAOT is a separate qualification check from the high-temperature claim.'
        },
        'IMPACT': {
            'why': 'This checks whether incidental impact damages the product beyond acceptable limits.',
            'how': ['Apply the stated impact condition.', 'Run the required follow-up test.', 'Record impact damage and whether it remains acceptable.'],
            'acceptance': ['No disqualifying failure after the impact route and follow-up confirmation.'],
            'basis': 'Impact resistance is explicitly listed in the qualification matrix.'
        },
        'TEC': {
            'why': 'Thermal expansion data helps design and installation control, especially where clearance or movement is critical.',
            'how': ['Measure axial thermal expansion coefficient, and hoop coefficient where relevant.', 'Record the temperature range and specimen construction.', 'Store the results for design and installation reports.'],
            'acceptance': ['Results should be traceable and representative of the qualified product construction.'],
            'basis': 'This is a property report that supports engineering use of the qualified product.'
        },
        'GROWTH': {
            'why': 'Pressure can change pipe length or dimensions and this must be known for design and operation.',
            'how': ['Apply pressure according to the route.', 'Measure growth and shrinkage response.', 'Record the permanent and recoverable dimensional change.'],
            'acceptance': ['Results should remain within the design assumptions used by engineering.'],
            'basis': 'Growth and shrinkage on pressure application is part of the API 15S qualification matrix.'
        },
        'CYCLIC_REG': {
            'why': 'Products intended for cyclic service need a cyclic pressure basis, not only static regression.',
            'how': ['Build a cyclic specimen set at relevant cycle counts and pressure ranges.', 'Record cycles to failure and permissible failure modes.', 'Use a lower confidence basis for cyclic duty.'],
            'acceptance': ['Use this route only when service truly falls into the cyclic qualification case.', 'Do not replace the static route with cyclic unless the standard permits it for the application.'],
            'basis': 'API 15S includes cyclic pressure fluctuation testing when the service case requires it.'
        }
    }
    pack = packs.get(code, {
        'why': test.description or 'Qualification activity from the API 15S matrix.',
        'how': ['Follow the applicable procedure, record the raw data, and keep the evidence traceable.'],
        'acceptance': ['Acceptance should be judged against the selected API 15S route and the program basis.'],
        'basis': test.clause_ref or 'API 15S qualification matrix'
    })
    pack['stage'] = _test_stage_label(code)
    return pack


def _matrix_report_rows(program: RndQualificationProgram, tests: List[RndQualificationTest]) -> list[dict]:
    flow = _active_stage(program, [], [])
    rows = []
    for test in tests:
        pack = _test_instruction_pack(test, program)
        rows.append({'test': test, 'pack': pack})
    return rows


def _report_context(program: RndQualificationProgram, tests: List[RndQualificationTest], materials: List[RndMaterialQualification], specimens: List[RndQualificationSpecimen]) -> dict:
    static_reg = _regression_from_specimens(specimens, 'STATIC_REGRESSION', program.npr_mpa)
    burst = _burst_state(program, specimens)
    material_state = _material_screening_state(materials)
    counts = _matrix_counts(tests)
    return {
        'program': program,
        'tests': tests,
        'materials': materials,
        'material_state': material_state,
        'specimens': specimens,
        'burst': burst,
        'static_reg': static_reg,
        'counts': counts,
        'completion_pct': _status_pct(counts, len(tests)),
    }

@router.get('')
def rnd_home() -> RedirectResponse:
    return RedirectResponse(url='/rnd/qualifications', status_code=303)


@router.get('/qualifications')
def rnd_dashboard(request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    programs = session.exec(select(RndQualificationProgram).order_by(RndQualificationProgram.updated_at.desc())).all()
    dashboard = []
    for program in programs:
        materials = session.exec(select(RndMaterialQualification).where(RndMaterialQualification.program_id == program.id).order_by(RndMaterialQualification.id.asc())).all()
        specimens = session.exec(select(RndQualificationSpecimen).where(RndQualificationSpecimen.program_id == program.id).order_by(RndQualificationSpecimen.created_at.desc())).all()
        flow = _active_stage(program, materials, specimens)
        dashboard.append({'program': program, 'flow': flow})
    guide = _qualification_guide()
    return TEMPLATES.TemplateResponse(request=request, name='rnd_dashboard.html', context={'request': request, 'user': user, 'dashboard': dashboard, 'guide': guide})


@router.get('/qualifications/new')
def rnd_new_program_form(request: Request, user: User = Depends(_require_user)):
    return TEMPLATES.TemplateResponse(request=request, name='rnd_program_form.html', context={'request': request, 'user': user})


@router.post('/qualifications/new')
def rnd_create_program(session: Session = Depends(get_session), user: User = Depends(_require_user), title: str = Form(...), program_code: str = Form(...), nominal_size_in: float = Form(...), npr_mpa: float = Form(...), maot_c: float = Form(...), laot_c: float = Form(0.0), pfr_or_pv: str = Form('PFR'), parent_program_id: Optional[int] = Form(None), intended_service: str = Form('Static water service'), notes: str = Form('')):
    program = RndQualificationProgram(program_code=(program_code or '').strip().upper(), title=(title or '').strip(), nominal_size_in=nominal_size_in, npr_mpa=npr_mpa, maot_c=maot_c, laot_c=laot_c, pfr_or_pv=(pfr_or_pv or 'PFR').strip().upper(), parent_program_id=parent_program_id, intended_service=intended_service, notes='', created_by_name=(getattr(user, 'display_name', '') or getattr(user, 'username', '') or ''))
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
    _save_program_answers(program, {'free_notes': notes})
    session.add(program)
    session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program.id}', status_code=303)


@router.get('/qualifications/{program_id}')
def rnd_program_view(program_id: int, request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')

    _seed_test_matrix(session, program)

    materials = session.exec(
        select(RndMaterialQualification)
        .where(RndMaterialQualification.program_id == program_id)
        .order_by(RndMaterialQualification.id.asc())
    ).all()

    specimens = session.exec(
        select(RndQualificationSpecimen)
        .where(RndQualificationSpecimen.program_id == program_id)
        .order_by(RndQualificationSpecimen.created_at.desc())
    ).all()

    tests = session.exec(
        select(RndQualificationTest)
        .where(RndQualificationTest.program_id == program_id)
        .order_by(RndQualificationTest.sort_order.asc(), RndQualificationTest.id.asc())
    ).all()

    flow = _active_stage(program, materials, specimens)
    guide = _qualification_guide(program)

    return TEMPLATES.TemplateResponse(
        request=request,
        name='rnd_program_view.html',
        context={
            'request': request,
            'user': user,
            'program': program,
            'materials': materials,
            'specimens': specimens,
            'tests': tests,
            'guide': guide,
            'flow': flow,
            'wizard': flow['wizard'],
            'material_state': flow['materials'],
            'burst_state': flow['burst'],
            'static_reg': flow['static_reg'],
            'threshold_mpa': _burst_threshold(program),
            'rcrt_hours': RCRT_HOURS,
            'design_factor_nonmetallic': DESIGN_FACTOR_NONMETALLIC,
        }
    )

@router.post('/qualifications/{program_id}/wizard/save')
def rnd_save_wizard(program_id: int, session: Session = Depends(get_session), user: User = Depends(_require_user), launch_size_in: str = Form(...), sister_size_in: str = Form('6'), service_route: str = Form('static_liquid'), cyclic_service: str = Form('no'), intended_service: str = Form('Static water service'), npr_mpa: float = Form(...), maot_c: float = Form(...), pfr_or_pv: str = Form('PFR')):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')
    program.nominal_size_in = float(launch_size_in)
    program.npr_mpa = npr_mpa
    program.maot_c = maot_c
    program.pfr_or_pv = (pfr_or_pv or 'PFR').upper()
    program.intended_service = intended_service
    answers = _program_answers(program)
    answers.update({'launch_size_in': launch_size_in, 'sister_size_in': sister_size_in, 'service_route': service_route, 'cyclic_service': cyclic_service})
    _save_program_answers(program, answers)
    session.add(program)
    session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}', status_code=303)


@router.post('/qualifications/{program_id}/materials/{material_id}')
def rnd_update_material(program_id: int, material_id: int, material_name: str = Form(''), supplier_name: str = Form(''), grade_name: str = Form(''), certificate_ref: str = Form(''), batch_ref: str = Form(''), status: str = Form('PLANNED'), notes: str = Form(''), session: Session = Depends(get_session), user: User = Depends(_require_user)):
    row = session.get(RndMaterialQualification, material_id)
    if not row or row.program_id != program_id:
        raise HTTPException(404, 'Material row not found')
    row.material_name = material_name or row.material_name
    row.supplier_name = supplier_name or ''
    row.grade_name = grade_name or ''
    row.certificate_ref = certificate_ref or ''
    row.batch_ref = batch_ref or ''
    row.status = (status or 'PLANNED').strip().upper()
    row.notes = notes or ''
    _touch_row(row)
    session.add(row)
    program = session.get(RndQualificationProgram, program_id)
    if program:
        _touch_program(program)
        session.add(program)
    session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}', status_code=303)


@router.post('/qualifications/{program_id}/burst/add')
def rnd_add_burst_result(program_id: int, specimen_id: str = Form(...), pressure_mpa: float = Form(...), temperature_c: float = Form(...), failure_mode: str = Form(...), notes: str = Form(''), session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')
    specimen = RndQualificationSpecimen(program_id=program_id, specimen_id=(specimen_id or '').strip().upper(), test_type='BURST_QUALIFICATION', sample_date=date.today(), nominal_size_in=program.nominal_size_in, pressure_mpa=pressure_mpa, temperature_c=temperature_c, failure_mode=failure_mode, permissible_failure=(failure_mode or '').strip().lower() in {'burst', 'rupture'}, include_in_regression=False, notes=notes)
    session.add(specimen)
    _touch_program(program)
    session.add(program)
    session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}', status_code=303)


@router.post('/qualifications/{program_id}/specimens/new')
def rnd_add_specimen(program_id: int, session: Session = Depends(get_session), user: User = Depends(_require_user), specimen_id: str = Form(...), test_type: str = Form(...), test_id: Optional[int] = Form(None), sample_date: date = Form(...), nominal_size_in: float = Form(0.0), pressure_mpa: float = Form(0.0), temperature_c: float = Form(0.0), failure_hours: Optional[float] = Form(None), failure_cycles: Optional[float] = Form(None), failure_mode: str = Form(''), permissible_failure: Optional[str] = Form(None), is_runout: Optional[str] = Form(None), include_in_regression: Optional[str] = Form(None), fitting_type: str = Form('Field fitting'), lab_name: str = Form(''), witness_name: str = Form(''), notes: str = Form('')):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')
    specimen = RndQualificationSpecimen(program_id=program_id, test_id=test_id, specimen_id=(specimen_id or '').strip().upper(), test_type=(test_type or 'STATIC_REGRESSION').strip().upper(), sample_date=sample_date, nominal_size_in=nominal_size_in or program.nominal_size_in, pressure_mpa=pressure_mpa, temperature_c=temperature_c, failure_hours=failure_hours, failure_cycles=failure_cycles, failure_mode=failure_mode, permissible_failure=bool(permissible_failure), is_runout=bool(is_runout), include_in_regression=bool(include_in_regression), fitting_type=fitting_type, lab_name=lab_name, witness_name=witness_name, notes=notes)
    session.add(specimen)
    _touch_program(program)
    session.add(program)
    session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}/regression', status_code=303)


@router.post('/qualifications/{program_id}/specimens/{specimen_id}/delete')
def rnd_delete_specimen(program_id: int, specimen_id: int, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    specimen = session.get(RndQualificationSpecimen, specimen_id)
    if not specimen or specimen.program_id != program_id:
        raise HTTPException(404, 'Specimen not found')
    session.delete(specimen)
    program = session.get(RndQualificationProgram, program_id)
    if program:
        _touch_program(program)
        session.add(program)
    session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}', status_code=303)




@router.get('/qualifications/{program_id}/tests/{test_id}')
def rnd_test_view(program_id: int, test_id: int, request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(status_code=404, detail='Program not found.')
    _seed_test_matrix(session, program)
    test = session.get(RndQualificationTest, test_id)
    if not test or test.program_id != program_id:
        raise HTTPException(status_code=404, detail='Test not found.')
    materials = session.exec(select(RndMaterialQualification).where(RndMaterialQualification.program_id == program_id).order_by(RndMaterialQualification.component.asc())).all()
    specimens = session.exec(select(RndQualificationSpecimen).where(RndQualificationSpecimen.program_id == program_id).order_by(RndQualificationSpecimen.created_at.asc())).all()
    related = [s for s in specimens if (s.test_id == test.id) or ((test.code == 'MPR_REG' and s.test_type == 'STATIC_REGRESSION') or (test.code == 'CYCLIC_REG' and s.test_type == 'CYCLIC_REGRESSION') or (test.code in {'OPERATING_MBR','AXIAL_LOAD','IMPACT'} and s.test_type == 'BURST_QUALIFICATION'))]
    pack = _test_instruction_pack(test, program)
    return TEMPLATES.TemplateResponse(request=request, name='rnd_test_detail.html', context={'request': request, 'user': user, 'program': program, 'test': test, 'pack': pack, 'materials': materials, 'specimens': related, 'threshold_mpa': _burst_threshold(program), 'rcrt_hours': RCRT_HOURS, 'design_factor_nonmetallic': DESIGN_FACTOR_NONMETALLIC})


@router.get('/qualifications/{program_id}/reports/{report_type}')
def rnd_report_view(program_id: int, report_type: str, request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(status_code=404, detail='Program not found.')
    _seed_test_matrix(session, program)
    tests = session.exec(select(RndQualificationTest).where(RndQualificationTest.program_id == program_id).order_by(RndQualificationTest.sort_order.asc(), RndQualificationTest.id.asc())).all()
    materials = session.exec(select(RndMaterialQualification).where(RndMaterialQualification.program_id == program_id).order_by(RndMaterialQualification.component.asc())).all()
    specimens = session.exec(select(RndQualificationSpecimen).where(RndQualificationSpecimen.program_id == program_id).order_by(RndQualificationSpecimen.created_at.asc())).all()
    report = _report_context(program, tests, materials, specimens)
    report_type = (report_type or 'final').lower()
    if report_type not in {'materials','burst','regression','final'}:
        raise HTTPException(status_code=404, detail='Unknown report type.')
    return TEMPLATES.TemplateResponse(request=request, name='rnd_report.html', context={'request': request, 'user': user, 'report_type': report_type, **report, 'rcrt_hours': RCRT_HOURS, 'design_factor_nonmetallic': DESIGN_FACTOR_NONMETALLIC})

@router.get('/qualifications/{program_id}/regression')
def rnd_regression_view(program_id: int, request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')
    specimens = session.exec(select(RndQualificationSpecimen).where(RndQualificationSpecimen.program_id == program_id).order_by(RndQualificationSpecimen.created_at.asc())).all()
    static_reg = _regression_from_specimens(specimens, 'STATIC_REGRESSION', program.npr_mpa)
    cyclic_reg = _regression_from_specimens(specimens, 'CYCLIC_REGRESSION', program.npr_mpa)
    pv_formula = None
    if program.pfr_or_pv == 'PV' and program.parent_program_id:
        parent = session.get(RndQualificationProgram, program.parent_program_id)
        if parent:
            ratio = (program.npr_mpa / parent.npr_mpa) if parent.npr_mpa else None
            pv_formula = {'pfr_code': parent.program_code, 'npr_pv': program.npr_mpa, 'npr_pfr': parent.npr_mpa, 'formula': 'PPV1000 = PPFR1000 x (NPR_PV / NPR_PFR)', 'ratio': ratio}
    guide = _qualification_guide(program)
    return TEMPLATES.TemplateResponse(request=request, name='rnd_regression_view.html', context={'request': request, 'user': user, 'program': program, 'specimens': specimens, 'static_reg': static_reg, 'cyclic_reg': cyclic_reg, 'pv_formula': pv_formula, 'guide': guide, 'design_factor_nonmetallic': DESIGN_FACTOR_NONMETALLIC, 'rcrt_hours': RCRT_HOURS})
