from __future__ import annotations

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
    maot = f"{program.maot_c:g} °C" if program else '85 °C'
    intended_service = ((program.intended_service if program else '') or 'Water service').lower()
    is_gas_service = any(k in intended_service for k in ['gas', 'multiphase'])
    is_cyclic = 'cyclic' in intended_service or 'fatigue' in intended_service or 'pump' in intended_service

    service_scope = 'Gas or multiphase service is declared, so rapid decompression and gas-service discipline are required.' if is_gas_service else 'This looks like water or liquid service, so rapid decompression may be waived unless the product will also be sold for gas or multiphase duty.'
    cyclic_scope = 'Cyclic pressure qualification is required if the service exceeds 7000 cycles with ΔP/NPR ≥ 6 %.'

    if program:
        current_size = float(program.nominal_size_in or 0)
        if current_size in (4.0, 6.0):
            family_logic = 'For 4 in and 6 in at the same construction and pressure class, one size should normally be the fully qualified PFR and the other should be handled as a PV within the same product family, not as a second full qualification from zero.'
        else:
            family_logic = 'Choose the most demanding representative as the PFR, then qualify the rest of the family as PVs only where API 15S allows it.'
    else:
        family_logic = 'Choose the most demanding representative as the PFR, then qualify the rest of the family as PVs only where API 15S allows it.'

    temperature_logic = 'The qualification temperature for nonmetallic reinforced pipe has to be at least as high as the claimed MAOT. So a 90 °C rating needs a qualification basis at 90 °C or above; an 85 °C qualification can support 65 °C service, but it does not automatically justify 90 °C.'

    return {
        'summary': f'API 15S qualification cockpit for nonmetallic LLRTP using PE-RT liner, polyester yarn reinforcement, and PE cover. The module is structured to guide the team from product definition to evidence pack, with regression monitoring kept as the central control point for {size} / {npr} / {maot}.',
        'steps': [
            {'title': '1. Define the qualification family', 'text': family_logic},
            {'title': '2. Freeze the materials and process window', 'text': 'Record liner, reinforcement yarn, cover, matrix or adhesive system, supplier, grade, certificates, and any change-control notes before logging any qualification data.'},
            {'title': '3. Build the mandatory API 15S matrix', 'text': 'Set the program up around long-term hydrostatic regression for the PFR, PV 1000-hour confirmation where applicable, and the additional qualification tests required for temperature, MBR, impact, axial load, external load, and dimensional behavior.'},
            {'title': '4. Run the regression program correctly', 'text': 'For nonmetallic reinforcement, keep valid long-term points, exclude sub-10-hour points from static regression, capture failure mode, and protect the dataset from non-permissible failures.'},
            {'title': '5. Decide what is in scope for service', 'text': service_scope + ' ' + cyclic_scope},
            {'title': '6. Close the qualification only with a complete evidence pack', 'text': 'A program is only ready for release when test records, calculations, traceability, exclusions, witness records, and final review notes are complete.'},
        ],
        'observe': [
            'Use field fittings for PV confirmation and for tests where the standard specifically requires them.',
            'Keep the test temperature stable and linked to each specimen and chart.',
            'Protect the regression dataset by documenting every exclusion and every non-permissible failure.',
            'Use the lower confidence basis, not only the average line, when making release decisions.',
        ],
        'avoid': [
            'Do not qualify 4 in and 6 in as two separate full programs unless the construction or family limits force you to.',
            'Do not claim 90 °C capability from an 85 °C qualification basis.',
            'Do not mix design changes, different yarn systems, or process changes into one regression set.',
            'Do not treat the software as a substitute for engineering review, witness requirements, or laboratory accreditation.',
        ],
        'formula_examples': [
            {'label': 'Static regression line', 'expr': 'log10(P) = intercept + slope × log10(time)'},
            {'label': 'Nonmetallic MPR', 'expr': 'MPR = LCL at 175,000 h × 0.67'},
            {'label': 'PV confirmation pressure', 'expr': 'PPV1000 = PPFR1000 × (NPR_PV / NPR_PFR)'},
            {'label': 'Gas-service operating limit', 'expr': 'MOP = NPR × service factors'},
        ],
        'product_story': [
            {'title': 'Product family strategy', 'text': family_logic},
            {'title': 'Temperature ladder', 'text': temperature_logic},
            {'title': 'Regression priority', 'text': 'Because this is nonmetallic reinforced pipe, the long-term hydrostatic regression is the main qualification backbone. The module should therefore make specimen tracking, chart review, exclusion control, and LCL-based release decisions visible on every program.'},
        ],
        'what_applies': [
            'PFR full long-term hydrostatic regression for the representative product.',
            'PV 1000-hour confirmation for product variants within the same family.',
            'Elevated temperature, temperature cycling, MBR and respooling, axial load, external load, LAOT, impact, TEC, and growth or shrinkage checks according to the size and rating range.',
            'Rapid decompression only when the product is intended for gas or multiphase service.',
            'Cyclic regression only when the service definition meets the cyclic threshold.',
        ],
        'what_not_needed': [
            'A second full regression program for every nearby size in the same qualified family.',
            'Rapid decompression for products not intended for gas or multiphase service.',
            'Cyclic regression for clearly non-cyclic service below the API 15S threshold.',
        ],
        'tips': [
            'Use the more demanding size or condition as the PFR where practical so the family coverage is stronger.',
            'Show the exact acceptance logic beside each test card so operators understand why the test exists.',
            'Give every test step a short plain-language reason, not only the clause reference.',
            'Make regression status visible on the dashboard with count of valid points, excluded points, current LCL, current MPR, and release margin versus NPR.',
        ],
    }


@router.get('')
def rnd_home() -> RedirectResponse:
    return RedirectResponse(url='/rnd/qualifications', status_code=303)


@router.get('/qualifications')
def rnd_dashboard(request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    programs = session.exec(select(RndQualificationProgram).order_by(RndQualificationProgram.updated_at.desc())).all()
    dashboard = []
    for program in programs:
        tests = session.exec(select(RndQualificationTest).where(RndQualificationTest.program_id == program.id)).all()
        specimens = session.exec(select(RndQualificationSpecimen).where(RndQualificationSpecimen.program_id == program.id)).all()
        static_reg = _regression_from_specimens(specimens, 'STATIC_REGRESSION', program.npr_mpa)
        cyclic_reg = _regression_from_specimens(specimens, 'CYCLIC_REGRESSION', program.npr_mpa)
        counts = _matrix_counts(tests)
        dashboard.append({'program': program, 'tests': tests, 'specimens': specimens, 'counts': counts, 'progress_pct': _status_pct(counts, len(tests)), 'static_reg': static_reg, 'cyclic_reg': cyclic_reg})
    guide = _qualification_guide()
    return TEMPLATES.TemplateResponse('rnd_dashboard.html', {'request': request, 'user': user, 'dashboard': dashboard, 'guide': guide, 'design_factor_nonmetallic': DESIGN_FACTOR_NONMETALLIC, 'rcrt_hours': RCRT_HOURS})


@router.get('/qualifications/new')
def rnd_new_program_form(request: Request, user: User = Depends(_require_user)):
    return TEMPLATES.TemplateResponse('rnd_program_form.html', {'request': request, 'user': user})


@router.post('/qualifications/new')
def rnd_create_program(session: Session = Depends(get_session), user: User = Depends(_require_user), title: str = Form(...), program_code: str = Form(...), nominal_size_in: float = Form(...), npr_mpa: float = Form(...), maot_c: float = Form(...), laot_c: float = Form(0.0), pfr_or_pv: str = Form('PFR'), parent_program_id: Optional[int] = Form(None), intended_service: str = Form('Static water service'), notes: str = Form('')):
    program = RndQualificationProgram(program_code=(program_code or '').strip().upper(), title=(title or '').strip(), nominal_size_in=nominal_size_in, npr_mpa=npr_mpa, maot_c=maot_c, laot_c=laot_c, pfr_or_pv=(pfr_or_pv or 'PFR').strip().upper(), parent_program_id=parent_program_id, intended_service=intended_service, notes=notes, created_by_name=(getattr(user, 'display_name', '') or getattr(user, 'username', '') or ''))
    session.add(program); session.commit(); session.refresh(program)
    if program.parent_program_id:
        parent = session.get(RndQualificationProgram, program.parent_program_id)
        if parent:
            program.pfr_reference_code = parent.program_code
            _touch_program(program)
            session.add(program)
            session.commit()
    _seed_test_matrix(session, program)
    return RedirectResponse(url=f'/rnd/qualifications/{program.id}', status_code=303)


@router.get('/qualifications/{program_id}')
def rnd_program_view(program_id: int, request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')
    _seed_test_matrix(session, program)
    tests = session.exec(select(RndQualificationTest).where(RndQualificationTest.program_id == program_id).order_by(RndQualificationTest.sort_order.asc(), RndQualificationTest.id.asc())).all()
    specimens = session.exec(select(RndQualificationSpecimen).where(RndQualificationSpecimen.program_id == program_id).order_by(RndQualificationSpecimen.created_at.desc())).all()
    materials = session.exec(select(RndMaterialQualification).where(RndMaterialQualification.program_id == program_id).order_by(RndMaterialQualification.id.asc())).all()
    attachments = session.exec(select(RndAttachmentRegister).where(RndAttachmentRegister.program_id == program_id).order_by(RndAttachmentRegister.created_at.desc())).all()
    static_reg = _regression_from_specimens(specimens, 'STATIC_REGRESSION', program.npr_mpa)
    cyclic_reg = _regression_from_specimens(specimens, 'CYCLIC_REGRESSION', program.npr_mpa)
    counts = _matrix_counts(tests)
    guide = _qualification_guide(program)
    return TEMPLATES.TemplateResponse('rnd_program_view.html', {'request': request, 'user': user, 'program': program, 'tests': tests, 'specimens': specimens, 'materials': materials, 'attachments': attachments, 'static_reg': static_reg, 'cyclic_reg': cyclic_reg, 'counts': counts, 'progress_pct': _status_pct(counts, len(tests)), 'guide': guide, 'design_factor_nonmetallic': DESIGN_FACTOR_NONMETALLIC, 'rcrt_hours': RCRT_HOURS})


@router.post('/qualifications/{program_id}/status')
def rnd_update_program_status(program_id: int, status: str = Form(...), session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')
    program.status = (status or 'DRAFT').strip().upper(); _touch_program(program); session.add(program); session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}', status_code=303)


@router.post('/qualifications/{program_id}/tests/{test_id}')
def rnd_update_test(program_id: int, test_id: int, status: str = Form(...), result_summary: str = Form(''), session: Session = Depends(get_session), user: User = Depends(_require_user)):
    test = session.get(RndQualificationTest, test_id)
    if not test or test.program_id != program_id:
        raise HTTPException(404, 'Test not found')
    test.status = (status or 'PLANNED').strip().upper(); test.result_summary = result_summary or ''; _touch_row(test); session.add(test)
    program = session.get(RndQualificationProgram, program_id)
    if program:
        _touch_program(program); session.add(program)
    session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}', status_code=303)


@router.post('/qualifications/{program_id}/materials/{material_id}')
def rnd_update_material(program_id: int, material_id: int, material_name: str = Form(''), supplier_name: str = Form(''), grade_name: str = Form(''), certificate_ref: str = Form(''), batch_ref: str = Form(''), status: str = Form('PLANNED'), notes: str = Form(''), session: Session = Depends(get_session), user: User = Depends(_require_user)):
    row = session.get(RndMaterialQualification, material_id)
    if not row or row.program_id != program_id:
        raise HTTPException(404, 'Material row not found')
    row.material_name = material_name or row.material_name; row.supplier_name = supplier_name or ''; row.grade_name = grade_name or ''; row.certificate_ref = certificate_ref or ''; row.batch_ref = batch_ref or ''; row.status = (status or 'PLANNED').strip().upper(); row.notes = notes or ''; _touch_row(row); session.add(row)
    program = session.get(RndQualificationProgram, program_id)
    if program:
        _touch_program(program); session.add(program)
    session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}', status_code=303)


@router.post('/qualifications/{program_id}/attachments/new')
def rnd_add_attachment_register(program_id: int, category: str = Form('REPORT'), title: str = Form(...), reference_no: str = Form(''), file_note: str = Form(''), session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')
    session.add(RndAttachmentRegister(program_id=program_id, category=(category or 'REPORT').strip().upper(), title=title.strip(), reference_no=reference_no.strip(), file_note=file_note.strip()))
    _touch_program(program); session.add(program); session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}', status_code=303)


@router.post('/qualifications/{program_id}/specimens/new')
def rnd_add_specimen(program_id: int, session: Session = Depends(get_session), user: User = Depends(_require_user), specimen_id: str = Form(...), test_type: str = Form(...), test_id: Optional[int] = Form(None), sample_date: date = Form(...), nominal_size_in: float = Form(0.0), pressure_mpa: float = Form(0.0), temperature_c: float = Form(0.0), failure_hours: Optional[float] = Form(None), failure_cycles: Optional[float] = Form(None), failure_mode: str = Form(''), permissible_failure: Optional[str] = Form(None), is_runout: Optional[str] = Form(None), include_in_regression: Optional[str] = Form(None), fitting_type: str = Form('Field fitting'), lab_name: str = Form(''), witness_name: str = Form(''), notes: str = Form('')):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')
    specimen = RndQualificationSpecimen(program_id=program_id, test_id=test_id, specimen_id=(specimen_id or '').strip().upper(), test_type=(test_type or 'STATIC_REGRESSION').strip().upper(), sample_date=sample_date, nominal_size_in=nominal_size_in or program.nominal_size_in, pressure_mpa=pressure_mpa, temperature_c=temperature_c, failure_hours=failure_hours, failure_cycles=failure_cycles, failure_mode=failure_mode, permissible_failure=bool(permissible_failure), is_runout=bool(is_runout), include_in_regression=bool(include_in_regression), fitting_type=fitting_type, lab_name=lab_name, witness_name=witness_name, notes=notes)
    session.add(specimen); _touch_program(program); session.add(program); session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}', status_code=303)


@router.post('/qualifications/{program_id}/specimens/{specimen_id}/delete')
def rnd_delete_specimen(program_id: int, specimen_id: int, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    specimen = session.get(RndQualificationSpecimen, specimen_id)
    if not specimen or specimen.program_id != program_id:
        raise HTTPException(404, 'Specimen not found')
    session.delete(specimen)
    program = session.get(RndQualificationProgram, program_id)
    if program:
        _touch_program(program); session.add(program)
    session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}', status_code=303)


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
    return TEMPLATES.TemplateResponse('rnd_regression_view.html', {'request': request, 'user': user, 'program': program, 'specimens': specimens, 'static_reg': static_reg, 'cyclic_reg': cyclic_reg, 'pv_formula': pv_formula, 'guide': guide, 'design_factor_nonmetallic': DESIGN_FACTOR_NONMETALLIC, 'rcrt_hours': RCRT_HOURS})
