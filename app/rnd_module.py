from __future__ import annotations

import math
import os
import json
from datetime import datetime, date
from typing import Optional, List

from fastapi import APIRouter, Depends, Form, HTTPException, Request, UploadFile, File
from fastapi.responses import RedirectResponse, FileResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import SQLModel, Field, Session, select
from pathlib import Path
import shutil
import uuid
import mimetypes

from .db import get_session
from .models import User

router = APIRouter(prefix="/rnd", tags=["R&D Qualification"])
TEMPLATES = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))
RCRT_HOURS = 175000.0
CYCLIC_BASIS_CYCLES = 1_000_000.0
DESIGN_FACTOR_NONMETALLIC = 0.67

BASE_DIR = Path(__file__).resolve().parent
RND_UPLOAD_DIR = BASE_DIR / "uploaded_rnd_files"
RND_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED_RND_FILE_EXTENSIONS = {
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv",
    ".jpg", ".jpeg", ".png", ".webp",
    ".txt", ".zip"
}

def _safe_filename(filename: str) -> str:
    raw = (filename or "").strip()
    if not raw:
        raw = "file"
    safe = "".join(c if c.isalnum() or c in {".", "-", "_"} else "_" for c in raw)
    return safe[:200]

def _program_upload_dir(program_id: int) -> Path:
    folder = RND_UPLOAD_DIR / f"program_{program_id}"
    folder.mkdir(parents=True, exist_ok=True)
    return folder

def _save_rnd_upload(program_id: int, uploaded_file: UploadFile) -> dict:
    if uploaded_file is None:
        raise HTTPException(status_code=400, detail="No file uploaded.")

    original_name = _safe_filename(uploaded_file.filename or "file")
    suffix = Path(original_name).suffix.lower()

    if suffix not in ALLOWED_RND_FILE_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"File type not allowed: {suffix or 'unknown'}"
        )

    target_dir = _program_upload_dir(program_id)
    stored_name = f"{uuid.uuid4().hex}{suffix}"
    target_path = target_dir / stored_name

    size_bytes = 0
    with target_path.open("wb") as buffer:
        while True:
            chunk = uploaded_file.file.read(1024 * 1024)
            if not chunk:
                break
            size_bytes += len(chunk)
            buffer.write(chunk)

    content_type = uploaded_file.content_type or mimetypes.guess_type(original_name)[0] or "application/octet-stream"

    return {
        "original_filename": original_name,
        "stored_filename": stored_name,
        "file_path": str(target_path),
        "content_type": content_type,
        "file_size_bytes": size_bytes,
    }

DEFAULT_SPECIMEN_RULE = {
    "min_specimens": "As defined by the applicable qualification test requirement.",
    "minimum_cut_length": "Specimens shall be cut on the basis of total cut length and shall not be cut to active or effective test length only.",
    "effective_length": "The effective test length shall be the active section defined by the applicable test setup, fixture arrangement, support span, or pressurized free length.",
    "od_basis_notice": "Before any diameter-based preparation rule is applied, the actual qualified pipe outside diameter (OD) shall be confirmed from the controlled product definition, dimensional record, or approved size specification. Diameter-based rules shall not be calculated from nominal size designation alone unless the nominal size has already been mapped to a controlled OD value in the qualification basis.",
    "calculation_rule_notice": "Any preparation rule expressed as D, OD, 1D, 3D, 5D, or 1.5 x OD shall be calculated using the confirmed pipe outside diameter and then converted into millimetres for specimen release and cutting control.",
    "recorded_dimensions_notice": "Before cutting, the specimen register shall record the confirmed pipe OD, the rule basis used, the calculated effective length, the end allowances, the trimming margin, and the total cut length in millimetres.",
    "end_allowance_each_side": "Unless a stricter test-specific requirement applies, each specimen shall include an end allowance on both sides sufficient for gripping, sealing, end fittings, support, trimming, and handling. The baseline preparation allowance shall be not less than 1.0 x outside diameter on each side.",
    "total_length_formula": "Total cut length = effective test length + left end allowance + right end allowance + trimming margin.",
    "marking_requirements": [
        "Each specimen shall be assigned a unique identification number before cutting.",
        "Each specimen shall be marked with pipe size, batch or lot reference, and intended test code.",
        "Where applicable, the centerline or active test section shall be marked prior to preparation.",
        "Identification markings should be placed outside the critical observation or expected failure zone whenever practical."
    ],
    "visual_acceptance": [
        "Specimens shall be free from visible cuts, gouges, cracks, crushed areas, and other preparation damage.",
        "Specimens shall be free from obvious ovality or deformation caused by handling.",
        "Cut ends shall be square and suitable for the required end preparation.",
        "Traceability markings shall remain legible after preparation."
    ],
    "preconditioning": {
        "required": "depends",
        "when_required": "Preconditioning shall be applied whenever required by the qualification basis, applicable procedure, test condition, or environmental exposure requirement.",
        "medium": "Ambient air unless otherwise required by the applicable test method or internal procedure.",
        "target_temperature": "As required by the selected test condition.",
        "minimum_process": [
            "Prepare, cut, and identify the specimen.",
            "Verify dimensions, cut quality, and end preparation before conditioning.",
            "Place the specimen in the required conditioning environment at the specified target temperature.",
            "Allow the specimen to stabilize before commencement of testing.",
            "Record conditioning start time, target temperature, observed temperature, and responsible operator.",
            "Testing shall not begin until specimen stabilization has been confirmed."
        ],
        "records_required": [
            "Conditioning start time",
            "Conditioning end time or release time",
            "Target temperature",
            "Observed temperature",
            "Conditioning medium or environment",
            "Operator or approver"
        ],
    },
    "technician_tips": [
        "Do not cut specimens to effective test length only.",
        "Record both total cut length and effective test length before release for test.",
        "Where fixture or fitting engagement length is uncertain, confirm the requirement before cutting.",
        "Reject any specimen damaged during cutting, machining, or handling."
    ],
    "release_checks": [
        "Specimen identification is assigned and traceable to production batch.",
        "Total cut length is measured and recorded.",
        "Effective test length is identified.",
        "End preparation is complete.",
        "No visible damage is present after preparation.",
        "Preconditioning is complete where required.",
        "Specimen is released for testing by the responsible person."
    ],
}

SPECIMEN_PREP_RULES = {
    "mpr_reg": {
        "min_specimens": 18,
        "minimum_cut_length": "Specimens for long-term hydrostatic regression shall be cut to provide a consistent free pressurized section for the regression setup together with sufficient additional length for end terminations, sealing, and trimming.",
        "effective_length": "The effective length shall be the free pressurized section between end terminations used for long-term hydrostatic exposure.",
        "end_allowance_each_side": "A preparation allowance of not less than 1.5 x outside diameter shall be provided on each side as a baseline for long-term end termination and sealing requirements. Greater allowance shall be used where the termination system requires additional engagement length.",
        "total_length_formula": "Total cut length = effective regression section + 2 x termination allowance + trimming margin.",
        "preconditioning": {
            "required": True,
            "when_required": "Required before pressurization at the selected regression test temperature.",
            "medium": "Controlled temperature environment matching the regression condition",
            "target_temperature": "Selected regression temperature",
        },
    },
    "pv_1000h": {
        "min_specimens": 2,
        "minimum_cut_length": "Specimens for the 1000-hour pressure confirmation test shall be cut to provide the required active pressurized section together with full end-sealing allowance.",
        "effective_length": "The effective length shall be the active pressurized section between end fittings for the PV confirmation specimen.",
        "end_allowance_each_side": "A baseline sealing allowance of not less than 1.0 x outside diameter shall be provided on each side.",
        "preconditioning": {
            "required": True,
            "when_required": "Condition before the 1000-hour hold at the selected test temperature.",
            "medium": "Controlled environment",
            "target_temperature": "Selected PV confirmation temperature"
        },
    },
    "temp_elev": {
        "min_specimens": 1,
        "minimum_cut_length": "Specimens shall be cut to provide the active elevated-temperature test section together with sufficient end engagement and trimming allowance.",
        "effective_length": "The effective length shall be the section exposed to the elevated temperature and pressure condition.",
        "preconditioning": {
            "required": True,
            "when_required": "Elevated-temperature stabilization is required before test start.",
            "medium": "Controlled temperature environment",
            "target_temperature": "Selected elevated test temperature"
        },
    },
    "temp_cycle": {
        "min_specimens": 2,
        "minimum_cut_length": "Specimens shall be cut to provide the active thermal cycling section together with secure end engagement for the cycling setup.",
        "effective_length": "The effective length shall be the section subjected to thermal cycling.",
        "preconditioning": {
            "required": True,
            "when_required": "Stabilization at the starting temperature is required before thermal cycling begins.",
            "medium": "Controlled temperature environment",
            "target_temperature": "Cycle start temperature"
        },
    },
    "rapid_decomp": {
        "min_specimens": 1,
        "minimum_cut_length": "Specimens shall be cut to provide the decompression exposure section together with safe end engagement and sealing allowance.",
        "effective_length": "The effective length shall be the pressurized section exposed to gas decompression.",
        "preconditioning": {
            "required": True,
            "when_required": "Conditioning shall be applied as required by the gas charging and temperature setup.",
            "medium": "Gas exposure environment",
            "target_temperature": "Selected rapid decompression temperature"
        },
    },
    "operating_mbr": {
        "min_specimens": 2,
        "minimum_cut_length": "Specimens shall be prepared using the full length required to achieve the specified bending radius and safe handling arrangement.",
        "effective_length": "The effective length shall be the full section involved in bending or respooling exposure.",
        "end_allowance_each_side": "Additional length shall be included as required for gripping, handling, and fixture engagement.",
    },
    "axial_load": {
        "min_specimens": 2,
        "minimum_cut_length": "Specimens shall be cut to provide the required gauge section together with sufficient end gripping allowance.",
        "effective_length": "The effective length shall be the section over which axial response is evaluated.",
    },
    "crush": {
        "min_specimens": 3,
        "minimum_cut_length": "Specimens shall be cut to a length sufficient to suit the crush test arrangement with proper support and alignment.",
        "effective_length": "The effective length shall be the section subjected to radial external loading.",
    },
    "laot": {
        "min_specimens": 2,
        "minimum_cut_length": "Specimens shall be cut to provide the low-temperature qualification section together with the required end engagement.",
        "effective_length": "The effective length shall be the section exposed to the lowest allowable operating temperature qualification condition.",
        "preconditioning": {
            "required": True,
            "when_required": "Condition at the selected low temperature before test start.",
            "medium": "Low-temperature environment",
            "target_temperature": "Selected LAOT"
        },
    },
    "impact": {
        "min_specimens": 2,
        "minimum_cut_length": "Specimens shall be cut to provide the required support span or impact location together with trimming margin.",
        "effective_length": "The effective length shall be the supported test section containing the impact location.",
        "end_allowance_each_side": "Preparation allowance shall be based on the support arrangement rather than pressure sealing requirements.",
        "preconditioning": {
            "required": True,
            "when_required": "Required when impact testing is performed at a controlled test temperature.",
            "medium": "Conditioning environment at test temperature",
            "target_temperature": "Selected impact test temperature"
        },
    },
    "tec": {
        "min_specimens": 2,
        "minimum_cut_length": "Specimens shall be cut to provide sufficient length for axial response measurement over the defined gauge or effective section.",
        "effective_length": "The effective length shall be the section used for thermal expansion coefficient measurement.",
        "preconditioning": {
            "required": True,
            "when_required": "Thermal stabilization is required before thermal expansion measurement.",
            "medium": "Controlled temperature environment",
            "target_temperature": "Selected TEC temperature"
        },
    },
    "growth": {
        "min_specimens": 2,
        "minimum_cut_length": "Specimens shall be cut to provide sufficient length for measurement of growth or shrinkage over the intended gauge section.",
        "effective_length": "The effective length shall be the section over which dimensional response is recorded.",
    },
    "cyclic_reg": {
        "min_specimens": 18,
        "minimum_cut_length": "Specimens for cyclic pressure regression shall be cut to provide the active cycling section together with full termination or fitting allowance.",
        "effective_length": "The effective length shall be the active section subjected to repeated pressure cycling.",
        "end_allowance_each_side": "Sufficient length shall be provided for termination and sealing without compromising the active cycling section.",
        "preconditioning": {
            "required": True,
            "when_required": "Required when the cyclic test is conducted at controlled temperature or conditioned service state.",
            "medium": "Controlled environment as required by the test condition",
            "target_temperature": "Selected cyclic qualification temperature"
        },
    },
}


TEST_GUIDANCE = {
    "MPR_REG": {
        "when_required": "Always for the PFR qualification route.",
        "specimen_count": "18 specimens minimum target",
        "api_clause": "API 15S 5.3.2",
        "external_standard": "ASTM D1598 or ISO 1167-1; ASTM D2992-18 Procedure B",
        "conditioning_required": "Conditional",
        "conditioning_steps": [
            "Either pass the applicable 5.3.8 bend / handling preconditioning route, or precondition the PFR specimens to the required handling MBR / respooling cycle condition before regression testing.",
            "Use field fittings for PV testing where applicable.",
            "Run the long-term hydrostatic test with unrestricted ends.",
        ],
        "core_process": [
            "Run long-term hydrostatic test with unrestricted ends.",
            "Use specimen length in accordance with the approved setup and end termination requirements.",
            "Develop regression per ASTM D2992 Procedure B and determine LCL at RCRT.",
            "Do not use points below 10 hours in regression calculations.",
        ],
        "acceptance": [
            "MPR equals LCL RCT x selected service factor.",
            "All failures shall be reported in the qualification report.",
            "Tensile rupture of reinforcement is permissible where allowed by the basis for non-metallic reinforcement.",
        ],
        "retest_logic": "Retesting per 5.5 is triggered if required preconditioning steps are not satisfied.",
        "practical_notes": [
            "Laboratory test fittings are allowed for MPR determination if their stated limitations are understood.",
            "This is the core PV membership test for non-metallic reinforcement.",
        ],
    },
    "PV_1000H": {
        "when_required": "Every PV in the qualified family.",
        "specimen_count": "2 specimens for each PV",
        "api_clause": "API 15S 5.3.4.1 / 5.3.4.2",
        "external_standard": "ASTM D1598 or ISO 1167-1",
        "conditioning_required": "No standard bend preconditioning unless stated in the selected route",
        "conditioning_steps": [
            "Use field fittings for PV testing.",
            "Set pressure using the selected PV basis.",
            "Test 2 specimens at the qualification temperature for 1000 hours.",
        ],
        "core_process": [
            "Run the constant-pressure exposure for 1000 hours at the qualified condition.",
            "Record any time-to-failure and compare to the 1000-hour requirement.",
        ],
        "acceptance": [
            "Both original or retest specimens shall survive to 1000 hours, otherwise full qualification is required.",
        ],
        "retest_logic": "If any specimen fails before 1000 hours, apply 5.5 retest logic.",
        "practical_notes": [
            "This is the core PV membership test for non-metallic reinforcement.",
        ],
    },
    "TEMP_ELEV": {
        "when_required": "Always for listed PFR / PV boundaries where elevated temperature confirmation applies.",
        "specimen_count": "1 specimen per pressure / temperature condition",
        "api_clause": "API 15S 5.3.5",
        "external_standard": "API 15S Eq. (4) / API 15S procedure",
        "conditioning_required": "No bend preconditioning specified",
        "conditioning_steps": [
            "Install unrestrained field end-fittings or couplings.",
            "Choose test temperature above MAOT.",
            "Set minimum test pressure to 1.5 x NDR.",
        ],
        "core_process": [
            "Maintain internal medium and outer wall within the required test temperature tolerance throughout the test.",
            "After the main test, depressurize and store at ambient for at least 24 hours.",
            "Then leak test at 150 psi +/- 50 psi for 24 hours at ambient and check for visible leakage.",
        ],
        "acceptance": [
            "No leakage through the full main test period.",
            "No visible leakage during the 24-hour ambient leak check.",
            "Brittle failure is not allowed.",
        ],
        "retest_logic": "Retest per 5.5 after resolving the brittle-failure cause if applicable.",
        "practical_notes": [
            "For polyethylene use alpha = 0.112 decades/C, otherwise determine alpha or assume 0.05 decades/C.",
        ],
    },
    "TEMP_CYCLE": {
        "when_required": "Always for listed PFR / PV boundaries.",
        "specimen_count": "2 specimens",
        "api_clause": "API 15S 5.3.6",
        "external_standard": "API 15S procedure",
        "conditioning_required": "Temperature conditioning is built into the test",
        "conditioning_steps": [
            "Condition the specimen to the fitting lowest installation temperature for at least 2.5 hours.",
            "Install fittings per written instructions at that temperature.",
        ],
        "core_process": [
            "Condition to MAOT for at least 2.5 hours, then to the lower cycle temperature for at least 2.5 hours.",
            "Repeat the MAOT / lower-temperature sequence for a total of 3 cycles.",
            "Return to ambient for at least 2.5 hours.",
            "Leak test at 1.5 x NPR for at least 2 minutes.",
        ],
        "acceptance": [
            "No leakage during the final leak test.",
        ],
        "retest_logic": "Retest per 5.5.",
        "practical_notes": [
            "Installation temperature limits may differ from operating temperature limits.",
        ],
    },
    "RAPID_DECOMP": {
        "when_required": "Only for gas or multiphase service where the selected PV is susceptible.",
        "specimen_count": "1 selected PV specimen",
        "api_clause": "API 15S 5.3.7 and Annex B",
        "external_standard": "API 15S Annex B",
        "conditioning_required": "No separate bend preconditioning specified",
        "conditioning_steps": [
            "Select the PV with the highest susceptibility to rapid decompression damage.",
            "Run the decompression test at maximum design temperature using Annex B laboratory method.",
        ],
        "core_process": [
            "Inspect for collapse, blistering, cover blow-off, and disbondment after decompression.",
        ],
        "acceptance": [
            "No collapse, blistering, or cover blow-off.",
            "No disbondment beyond manufacturer acceptance criteria.",
        ],
        "retest_logic": "Not intended for gas or multiphase service if unsuccessful; check class or service applicability.",
        "practical_notes": [
            "Pipe not intended for gas or multiphase service must be clearly marked accordingly.",
        ],
    },
    "OPERATING_MBR": {
        "when_required": "Always where operating MBR confirmation is required.",
        "specimen_count": "2 specimens",
        "api_clause": "API 15S 5.3.8.1",
        "external_standard": "Follow-on test: 5.3.4.2",
        "conditioning_required": "Yes",
        "conditioning_steps": [
            "Place the specimen in a fixture at the operating MBR.",
            "No prior bend cycles are required unless triggered elsewhere.",
        ],
        "core_process": [
            "Hold the specimen at operating MBR during the 1000-hour follow-on proof test.",
        ],
        "acceptance": [
            "Pass the follow-on 1000-hour proof test or an allowed alternative on PVs if justified.",
        ],
        "retest_logic": "Retest per 5.5.",
        "practical_notes": [
            "This is the actual confirmation of installed / pressurized bend capability.",
        ],
    },
    "HANDLING_MBR": {
        "when_required": "Only if handling MBR is smaller than operating MBR.",
        "specimen_count": "2 qualification specimens",
        "api_clause": "API 15S 5.3.8.2",
        "external_standard": "Uses 5.3.8.1 follow-on test",
        "conditioning_required": "Yes",
        "conditioning_steps": [
            "Pre-bend the specimen to handling MBR.",
            "Then hold at operating MBR during the 1000-hour proof test.",
        ],
        "core_process": [
            "Perform the operating MBR confirmation of 5.3.8.1 after handling-MBR preconditioning.",
        ],
        "acceptance": [
            "Pass the follow-on proof test.",
        ],
        "retest_logic": "If unsuccessful, all PFR MPR specimens must be preconditioned to handling MBR before full qualification.",
        "practical_notes": [
            "If handling MBR is greater than or equal to operating MBR, no extra preconditioning step beyond 5.3.8.1 is needed.",
        ],
    },
    "HANDLING_AND_SPOOLING": {
        "when_required": "Always for PFR where handling/spooling damage screening is required.",
        "specimen_count": "2 PFR specimens",
        "api_clause": "API 15S 5.3.8.3",
        "external_standard": "Follow-on test: 5.3.4.2",
        "conditioning_required": "Yes",
        "conditioning_steps": [
            "Precondition 2 PFR specimens with 10 bending cycles to operating MBR or smaller.",
            "If handling MBR is smaller than operating MBR, 1 of the 10 cycles shall be at handling MBR.",
        ],
        "core_process": [
            "After preconditioning, run the 1000-hour follow-on proof test.",
        ],
        "acceptance": [
            "Pass the follow-on 1000-hour proof test.",
        ],
        "retest_logic": "If unsuccessful, or if preferred, full PFR qualification specimens shall be conditioned with 1 cycle at qualification MBR and full PFR qualification repeated.",
        "practical_notes": [
            "This is a key durability / damage-screening step before full PFR qualification.",
        ],
    },
    "RESPOOLING": {
        "when_required": "Only if respooling is allowed / claimed.",
        "specimen_count": "2 PFR specimens",
        "api_clause": "API 15S 5.3.8.4",
        "external_standard": "Follow-on test: 5.3.4.2",
        "conditioning_required": "Yes",
        "conditioning_steps": [
            "Precondition 2 PFR specimens to the stated allowable number of cycles at the applicable respooling MBR.",
        ],
        "core_process": [
            "After respooling preconditioning, run the 1000-hour follow-on proof test.",
        ],
        "acceptance": [
            "Pass the follow-on 1000-hour proof test.",
        ],
        "retest_logic": "If unsuccessful, all PFR MPR specimens must be preconditioned to respooling MBR before full qualification per 5.3.2.",
        "practical_notes": [
            "If respooling cycles are 10 or more and respooling MBR is less than or equal to operating MBR, this can also satisfy 5.3.8.3.",
        ],
    },
    "AXIAL_LOAD": {
        "when_required": "Always where installation load conditioning applies.",
        "specimen_count": "2 specimens",
        "api_clause": "API 15S 5.3.9",
        "external_standard": "Follow-on test: 5.3.4.2 or 5.3.4.2 burst alternative if justified",
        "conditioning_required": "Yes (axial load conditioning)",
        "conditioning_steps": [
            "If installation uses pulling pipe with attached fittings / couplings, test the assembled pipe body.",
            "Apply allowable axial tension load at less than or equal to 50% of the lowest measured or calculated failure tension.",
            "Reach target load in 1 to 20 minutes and hold for at least 1 hour with no internal pressure.",
        ],
        "core_process": [
            "Run the 1000-hour follow-on proof test after axial load conditioning.",
        ],
        "acceptance": [
            "Pass the follow-on 1000-hour proof test.",
        ],
        "retest_logic": "Retest per 5.5.",
        "practical_notes": [
            "This is conditioning before proof testing, not bend preconditioning.",
        ],
    },
    "CRUSH": {
        "when_required": "Mandatory only for selected PV characterization cases.",
        "specimen_count": "3 specimens",
        "api_clause": "API 15S 5.3.10",
        "external_standard": "ASTM D2412",
        "conditioning_required": "No specific preconditioning stated",
        "conditioning_steps": [
            "No specific conditioning step defined beyond approved setup preparation.",
        ],
        "core_process": [
            "Characterize external load performance using ASTM D2412 with 3 specimens that may be cut from the same pipe.",
        ],
        "acceptance": [
            "Characterization data shall be obtained per ASTM D2412.",
        ],
        "retest_logic": "No special retest language beyond general engineering disposition.",
        "practical_notes": [
            "This frames the characterization rather than a detailed pass/fail formula in the clause.",
        ],
    },
    "LAOT": {
        "when_required": "Mandatory for listed PV boundaries.",
        "specimen_count": "2 specimens",
        "api_clause": "API 15S 5.3.11",
        "external_standard": "API 15S procedure",
        "conditioning_required": "Yes (temperature conditioning)",
        "conditioning_steps": [
            "Condition specimen to steady state at LAOT and hold for at least 2.5 hours.",
        ],
        "core_process": [
            "Use only field-fittings qualified by the pipe manufacturer.",
            "Then perform leak test at MPR for at least 60 minutes.",
            "Then perform leak test at 150 psi +/- 50 psi for at least 10 minutes at LAOT.",
        ],
        "acceptance": [
            "All specimens shall survive without leakage for the full test period.",
        ],
        "retest_logic": "Retest per 5.5.",
        "practical_notes": [
            "Use only qualified field-fittings.",
        ],
    },
    "IMPACT": {
        "when_required": "Always for listed PFR / PV boundaries.",
        "specimen_count": "2 specimens",
        "api_clause": "API 15S 5.3.12",
        "external_standard": "ASTM D2444 Tup B or equivalent; follow-on 5.3.4.2",
        "conditioning_required": "Yes (test at lowest allowable installation temperature)",
        "conditioning_steps": [
            "Condition the specimens to the lowest allowable installation temperature before impact.",
        ],
        "core_process": [
            "Impact 2 specimens using applicable sections of ASTM D2444 Tup B or equivalent.",
            "After impact, run the 1000-hour follow-on proof test.",
            "Inspect the cover for breach.",
        ],
        "acceptance": [
            "Follow-on proof test shall pass.",
            "The cover shall not be breached.",
        ],
        "retest_logic": "Retest per 5.5.",
        "practical_notes": [
            "Impact energy / report can help establish deployment-condition impact resistance.",
        ],
    },
    "TEC": {
        "when_required": "Always for listed PFR / PV boundaries.",
        "specimen_count": "2 specimens",
        "api_clause": "API 15S 5.3.13",
        "external_standard": "API 15S procedure",
        "conditioning_required": "Optional",
        "conditioning_steps": [
            "Specimens may be preconditioned if desired or needed by the manufacturer; document the method if used.",
        ],
        "core_process": [
            "Use specimen length greater than or equal to 6 x nominal diameter.",
            "Measure axial thermal expansion coefficient over at least 50 F (28 C) range.",
            "Measurements shall be conducted unpressurized and at NPR as required by the design basis.",
            "Where OD clearance is critical, determine hoop TEC.",
        ],
        "acceptance": [
            "Measured and reported TEC values shall be established.",
        ],
        "retest_logic": "No explicit pass/fail formula in the clause; use results in design and installation basis.",
        "practical_notes": [
            "API notes that preconditioning may affect TEC for some designs.",
        ],
    },
    "GROWTH": {
        "when_required": "Always for listed PFR / PV boundaries.",
        "specimen_count": "2 specimens",
        "api_clause": "API 15S 5.3.14",
        "external_standard": "API 15S procedure",
        "conditioning_required": "No specific preconditioning stated",
        "conditioning_steps": [
            "No separate conditioning is specified beyond approved preparation and setup.",
        ],
        "core_process": [
            "Use specimen length greater than or equal to 6 x nominal diameter.",
            "Measure change in length and diameter from ambient to expected field hydrotest pressure or included pressure condition.",
            "Keep specimen unconstrained so values represent likely field behavior.",
        ],
        "acceptance": [
            "Measured and reported growth / shrinkage values shall be established.",
        ],
        "retest_logic": "No explicit pass/fail formula in the clause; use results for installation and restraint design.",
        "practical_notes": [
            "Useful for anchor spacing, tie-in movement, and field hydrotest planning.",
        ],
    },
    "CYCLIC_REG": {
        "when_required": "Only if the product is intended for cyclic service.",
        "specimen_count": "Per Annex D / at least 18 pipe regression points plus 2 preconditioned burst specimens where required",
        "api_clause": "API 15S 5.3.16 and Annex D",
        "external_standard": "Annex D; ASTM D2992-18 Procedure A; ASTM E466 / ASTM D3479 route for reinforcement materials",
        "conditioning_required": "Conditional",
        "conditioning_steps": [
            "Confirm the product is intended for cyclic service (>7000 cycles and dP/NPR >= 6%).",
            "Choose cyclic route for pipe, reinforcement, or material validation as applicable.",
            "Precondition at least 2 pipe specimens with field-fittings to the allowable number of cycles at MAOT where required for the service-factor check.",
        ],
        "core_process": [
            "For pipe route, regression requires at least 18 points at MAOT with Annex D exceptions.",
            "Apply service factor <= 0.1 to cycles from LCL where required.",
            "Run the preconditioned burst check after establishing the cyclic regression basis where applicable.",
        ],
        "acceptance": [
            "Final preconditioned burst result is reported but is not itself a direct pass/fail criterion.",
        ],
        "retest_logic": "If retest is failed in the screening step, full cyclic regression testing is required.",
        "practical_notes": [
            "Only include this row in qualification planning when cyclic service is claimed or expected.",
        ],
    },
}

def get_specimen_prep(test_code: str):
    key = (test_code or '').strip().lower()
    base = {
        **DEFAULT_SPECIMEN_RULE,
        "marking_requirements": list(DEFAULT_SPECIMEN_RULE["marking_requirements"]),
        "visual_acceptance": list(DEFAULT_SPECIMEN_RULE["visual_acceptance"]),
        "technician_tips": list(DEFAULT_SPECIMEN_RULE["technician_tips"]),
        "release_checks": list(DEFAULT_SPECIMEN_RULE["release_checks"]),
        "preconditioning": {
            **DEFAULT_SPECIMEN_RULE["preconditioning"],
            "minimum_process": list(DEFAULT_SPECIMEN_RULE["preconditioning"]["minimum_process"]),
            "records_required": list(DEFAULT_SPECIMEN_RULE["preconditioning"]["records_required"]),
        },
    }
    override = SPECIMEN_PREP_RULES.get(key, {})
    for k, v in override.items():
        if isinstance(v, dict) and k in base and isinstance(base[k], dict):
            merged = dict(base[k]); merged.update(v); base[k] = merged
        else:
            base[k] = v
    return base


def get_test_guidance(test_code: str) -> dict:
    code = (test_code or "").strip().upper()
    base = TEST_GUIDANCE.get(code, {
        "when_required": "Refer to the approved qualification basis.",
        "specimen_count": "As required by the approved route.",
        "api_clause": "",
        "external_standard": "",
        "conditioning_required": "Refer to applicable procedure",
        "conditioning_steps": ["Refer to the approved procedure and setup instructions."],
        "core_process": ["Perform the test in accordance with the approved method."],
        "acceptance": ["Apply the approved acceptance basis."],
        "retest_logic": "Follow the approved retest logic.",
        "practical_notes": [],
    })

    test_procedure = []
    for item in base.get("conditioning_steps", []):
        test_procedure.append(item)
    for item in base.get("core_process", []):
        test_procedure.append(item)

    operator_checks = [
        "Verify specimen identity, batch traceability, and test assignment before setup.",
        "Confirm the applicable fittings, fixtures, and calibrated instruments are available.",
        "Confirm conditioning has been completed where required.",
        "Record setup condition before applying load, pressure, temperature, or cycling.",
        "Capture any abnormal observation immediately and do not rely on memory after the test.",
    ]

    records_to_capture = [
        "Specimen ID",
        "Operator / witness",
        "Date and time",
        "Applied setup / fixture basis",
        "Pressure / temperature / time / cycles as applicable",
        "Observed failure mode or survival condition",
        "Acceptance decision",
    ]

    if code == "MPR_REG":
        records_to_capture.extend([
            "Failure hours",
            "Pressure at failure",
            "Failure location",
            "Permissible / excluded failure decision",
        ])
    elif code == "PV_1000H":
        records_to_capture.extend([
            "Hold pressure",
            "Elapsed hours",
            "Leak / survival result",
        ])
    elif code == "TEMP_CYCLE":
        records_to_capture.extend([
            "Cycle range",
            "Cycle count",
            "Leak / post-cycle condition",
        ])
    elif code == "RAPID_DECOMP":
        records_to_capture.extend([
            "Soak pressure",
            "Soak duration",
            "Decompression result",
            "Damage / blister / disbondment observation",
        ])
    elif code == "IMPACT":
        records_to_capture.extend([
            "Impact setup / energy",
            "Post-impact condition",
            "Follow-up proof result",
        ])
    elif code in {"AXIAL_LOAD", "AXIAL"}:
        records_to_capture.extend([
            "Applied axial load",
            "Hold duration",
            "Post-load proof result",
        ])

    enriched = dict(base)
    enriched["test_procedure"] = test_procedure
    enriched["operator_checks"] = operator_checks
    enriched["records_to_capture"] = records_to_capture
    return enriched

class RndQualificationProgram(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    program_code: str = Field(default="", index=True)
    title: str = Field(default="", index=True)

    # NEW: generic qualification handling
    program_type: str = Field(default="API_15S", index=True)  # API_15S / OTHER
    service_medium: str = Field(default="WATER", index=True)  # WATER / GAS / HYDROCARBON / LIQUIDS
    service_factor: float = Field(default=1.0)

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

    # NEW: for custom qualification programs
    custom_requirements: str = Field(default="")
    custom_acceptance_criteria: str = Field(default="")


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
    scope_tag: str = Field(default="BOTH", index=True)  # PFR / PV / BOTH / CUSTOM
    source_standard: str = Field(default="API_15S", index=True)  # API_15S / CUSTOM / CLIENT / INTERNAL
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

    scope_tag: str = Field(default="BOTH", index=True)  # PFR / PV / BOTH / CUSTOM

    material_ref: str = Field(default="", index=True)
    conditioning_required: Optional[bool] = Field(default=None)
    nominal_size_in: float = Field(default=0.0)
    confirmed_od_mm: Optional[float] = Field(default=None)
    preparation_rule_basis: str = Field(default="")

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

    batch_ref: str = Field(default="", index=True)
    source_pipe_ref: str = Field(default="", index=True)
    cut_by: str = Field(default="")
    total_cut_length_mm: Optional[float] = Field(default=None)
    effective_length_mm: Optional[float] = Field(default=None)
    end_allowance_each_side_mm: Optional[float] = Field(default=None)
    trimming_margin_mm: Optional[float] = Field(default=None)

    conditioning_complete: bool = Field(default=False)
    pretest_visual_ok: bool = Field(default=False)
    released_for_test: bool = Field(default=False)

    planned_pressure_mpa: Optional[float] = Field(default=None)
    actual_pressure_at_failure_mpa: Optional[float] = Field(default=None)
    pressure_at_hold_mpa: Optional[float] = Field(default=None)
    failure_time_sec: Optional[float] = Field(default=None)
    pre_failure_condition: str = Field(default="")
    pre_failure_visual: str = Field(default="")
    post_failure_visual: str = Field(default="")
    failure_location: str = Field(default="")
    failure_description: str = Field(default="")
    leak_observation: str = Field(default="")
    result_status: str = Field(default="PENDING", index=True)
    qa_review_status: str = Field(default="PENDING", index=True)

class RndMaterialQualification(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    program_id: int = Field(index=True)

    component: str = Field(default="LINER", index=True)
    material_name: str = Field(default="")
    supplier_name: str = Field(default="")
    manufacturer_name: str = Field(default="")
    grade_name: str = Field(default="")
    trade_name: str = Field(default="")
    certificate_ref: str = Field(default="")
    batch_ref: str = Field(default="")
    lot_ref: str = Field(default="")
    status: str = Field(default="PLANNED", index=True)
    notes: str = Field(default="")

    standard_ref: str = Field(default="")
    material_family: str = Field(default="")
    service_fluid_basis: str = Field(default="")
    service_notes: str = Field(default="")
    max_service_temp_c: Optional[float] = Field(default=None)
    min_service_temp_c: Optional[float] = Field(default=None)

    classification_basis: str = Field(default="")
    pe_cell_class: str = Field(default="")
    hdb_basis: str = Field(default="")
    uv_class: str = Field(default="")

    reinforcement_type: str = Field(default="", index=True)
    reinforcement_form: str = Field(default="")
    reinforcement_layer_count: Optional[int] = Field(default=None)
    reinforcement_layout_notes: str = Field(default="")
    reinforcement_matrix_material: str = Field(default="")
    steel_processing_history: str = Field(default="")
    fiber_sizing_notes: str = Field(default="")

    matrix_material_name: str = Field(default="")
    matrix_resin_type: str = Field(default="")
    cure_method: str = Field(default="")
    tg_min_c: Optional[float] = Field(default=None)

    compatibility_status: str = Field(default="UNKNOWN", index=True)
    evidence_status: str = Field(default="MISSING", index=True)
    review_outcome: str = Field(default="MORE_DATA_REQUIRED", index=True)
    review_summary: str = Field(default="")
    clarification_required: str = Field(default="")
    additional_tests_required: str = Field(default="")
    change_requalification_flag: bool = Field(default=False)


class RndMaterialTestRecord(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    program_id: int = Field(index=True)
    material_id: int = Field(index=True)

    test_type: str = Field(default="", index=True)
    standard_ref: str = Field(default="")
    specimen_ref: str = Field(default="")
    report_ref: str = Field(default="")
    lab_name: str = Field(default="")
    test_date: Optional[date] = Field(default=None)

    result_value: str = Field(default="")
    result_unit: str = Field(default="")
    acceptance_basis: str = Field(default="")
    decision: str = Field(default="PENDING", index=True)
    notes: str = Field(default="")

class RndAttachmentRegister(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    program_id: int = Field(index=True)
    test_id: Optional[int] = Field(default=None, index=True)
    specimen_id: Optional[int] = Field(default=None, index=True)

    scope_tag: str = Field(default="BOTH", index=True)  # PFR / PV / BOTH / CUSTOM

    category: str = Field(default="REPORT", index=True)
    title: str = Field(default="")
    reference_no: str = Field(default="")
    file_note: str = Field(default="")
    document_type: str = Field(default="", index=True)
    is_mandatory: bool = Field(default=False)
    uploaded_by_name: str = Field(default="")
    approval_status: str = Field(default="PENDING", index=True)

    original_filename: str = Field(default="")
    stored_filename: str = Field(default="")
    file_path: str = Field(default="")
    content_type: str = Field(default="")
    file_size_bytes: Optional[int] = Field(default=None)
    source_mode: str = Field(default="UPLOAD", index=True)  # UPLOAD / GENERATED
    is_signed_copy: bool = Field(default=False, index=True)

def _specimen_lifecycle_summary(specimens: list[RndQualificationSpecimen]) -> dict:
    total = len(specimens)
    prepared = sum(1 for s in specimens if s.pretest_visual_ok)
    released = sum(1 for s in specimens if s.released_for_test)
    executed = sum(1 for s in specimens if s.result_status and s.result_status != 'PENDING')
    qa_ready = sum(1 for s in specimens if (s.qa_review_status or 'PENDING') in {'ACCEPTED', 'APPROVED'})
    failed_logged = sum(1 for s in specimens if s.actual_pressure_at_failure_mpa is not None or s.failure_mode or s.failure_location)
    return {
        'total': total,
        'prepared': prepared,
        'released': released,
        'executed': executed,
        'qa_ready': qa_ready,
        'failed_logged': failed_logged,
    }


def _test_progress_snapshot(test: RndQualificationTest, specimens: list[RndQualificationSpecimen], attachments: list[RndAttachmentRegister]) -> dict:
    lifecycle = _specimen_lifecycle_summary(specimens)
    evidence = _evidence_status(test.code, attachments)
    has_result = bool((test.result_summary or '').strip())
    ready_for_close = lifecycle['executed'] > 0 and evidence['complete'] and has_result
    return {
        'lifecycle': lifecycle,
        'evidence': evidence,
        'has_result': has_result,
        'ready_for_close': ready_for_close,
    }


def _phase_cards(program: RndQualificationProgram, tests: list[RndQualificationTest], materials: list[RndMaterialQualification], specimens: list[RndQualificationSpecimen], attachments: list[RndAttachmentRegister]) -> list[dict]:
    materials_ok = all((m.status or '').upper() in {'APPROVED', 'ACCEPTED', 'COMPLETE'} for m in materials) if materials else False
    tests_with_plan = sum(1 for t in tests if (t.specimen_requirement or '').strip())
    executed_tests = 0
    evidence_complete_tests = 0
    closed_tests = 0
    for t in tests:
        ts = [s for s in specimens if s.test_id == t.id]
        ta = [a for a in attachments if a.test_id == t.id]
        snap = _test_progress_snapshot(t, ts, ta)
        if snap['lifecycle']['executed']:
            executed_tests += 1
        if snap['evidence']['complete']:
            evidence_complete_tests += 1
        if (t.status or '').upper() in {'COMPLETE', 'CLOSED', 'ACCEPTED'}:
            closed_tests += 1
    total_tests = max(len(tests), 1)
    return [
        {'name': 'Definition', 'status': 'Complete' if (program.title and program.program_code) else 'Open', 'detail': f"{program.qualification_standard or 'Qualification basis'} / {program.pfr_or_pv}"},
        {'name': 'Material Control', 'status': 'Complete' if materials_ok else 'In Progress', 'detail': f"{sum(1 for m in materials if (m.status or '').upper() in {'APPROVED', 'ACCEPTED', 'COMPLETE'})}/{len(materials)} approved" if materials else 'No materials confirmed yet'},
        {'name': 'Test Planning', 'status': 'Complete' if tests_with_plan == len(tests) and tests else 'In Progress', 'detail': f"{tests_with_plan}/{len(tests)} tests with defined specimen requirement" if tests else 'No tests listed yet'},
        {'name': 'Execution', 'status': 'Complete' if executed_tests == len(tests) and tests else 'In Progress', 'detail': f"{executed_tests}/{len(tests)} tests with execution records" if tests else 'Waiting for specimens'},
        {'name': 'Evidence Review', 'status': 'Complete' if evidence_complete_tests == len(tests) and tests else 'In Progress', 'detail': f"{evidence_complete_tests}/{len(tests)} tests with complete evidence checklist" if tests else 'Waiting for evidence'},
        {'name': 'Closure / Monogram', 'status': 'Ready' if closed_tests == len(tests) and len(tests) > 0 else 'Pending', 'detail': f"{closed_tests}/{len(tests)} tests closed with result summary" if tests else 'No closeout yet'},
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




def _material_required_test_types(material: 'RndMaterialQualification', program: Optional[RndQualificationProgram] = None) -> list[str]:
    component = (material.component or "").strip().upper()
    reinforcement_type = (material.reinforcement_type or "").strip().upper()
    material_family = (material.material_family or "").strip().upper()
    service_text = " ".join([
        (program.intended_service if program else "") or "",
        material.service_fluid_basis or "",
        material.service_notes or "",
    ]).lower()

    required = []

    if component == "LINER":
        required.extend(["FLUID_COMPATIBILITY", "AGING"])
        if "gas" in service_text or "multiphase" in service_text:
            required.append("BLISTER_RESISTANCE")
        required.append("PERMEABILITY")

        if material_family in {"PE", "PE100", "PE-RT", "POLYETHYLENE"}:
            required.append("PE_CLASSIFICATION_BASIS")

    elif component == "REINFORCEMENT":
        required.extend(["LOAD_CAPABILITY", "FLUID_COMPATIBILITY", "AGING"])

        if reinforcement_type == "STEEL":
            required.append("CORROSION_REVIEW")
            if "cathod" in service_text or "seawater" in service_text or "subsea" in service_text:
                required.append("CATHODIC_CHARGING")
            if "sour" in service_text or "h2s" in service_text:
                required.extend(["SSC", "HIC"])

        elif reinforcement_type in {"GLASS", "ARAMID", "POLYESTER", "NONMETALLIC"}:
            required.append("FIBER_MECHANICAL_BASIS")
            required.append("HYDROLYSIS_PH_REVIEW")

    elif component == "MATRIX":
        required.extend(["FLUID_COMPATIBILITY", "AGING"])
        if (material.matrix_resin_type or "").strip().upper() in {"THERMOSET", "EPOXY"}:
            required.append("TG_OR_CURE_BASIS")

    elif component == "COVER":
        required.append("WEATHERING_UV")
        required.append("LOW_TEMP_DUCTILITY")

    elif component in {"FITTING", "COUPLING"}:
        required.append("MATERIAL_SPEC_BASIS")
        if "sour" in service_text or "h2s" in service_text:
            required.append("SOUR_SERVICE_REVIEW")

    # unique, keep order
    seen = set()
    ordered = []
    for item in required:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def _material_test_map(material_tests: List['RndMaterialTestRecord']) -> dict:
    out = {}
    for row in material_tests:
        key = (row.test_type or "").strip().upper()
        if not key:
            continue
        out.setdefault(key, []).append(row)
    return out


def _material_review(material: 'RndMaterialQualification', material_tests: List['RndMaterialTestRecord'], program: Optional[RndQualificationProgram] = None) -> dict:
    required = _material_required_test_types(material, program)
    test_map = _material_test_map(material_tests)

    missing = []
    failed = []
    pending = []

    for test_type in required:
        rows = test_map.get(test_type, [])
        if not rows:
            missing.append(test_type)
            continue

        decisions = {(r.decision or "PENDING").strip().upper() for r in rows}
        if "FAIL" in decisions or "REJECTED" in decisions or "NOT_ACCEPTABLE" in decisions:
            failed.append(test_type)
        elif decisions <= {"PENDING", "UNDER_REVIEW"}:
            pending.append(test_type)

    component = (material.component or "").strip().upper()
    reinforcement_type = (material.reinforcement_type or "").strip().upper()

    clarifications = []
    if component == "REINFORCEMENT":
        if not reinforcement_type:
            clarifications.append("Reinforcement type is not defined.")
        if material.reinforcement_layer_count is None:
            clarifications.append("Reinforcement layer count is not defined.")
        if not (material.reinforcement_form or "").strip():
            clarifications.append("Reinforcement form is not defined.")
    if component == "LINER":
        if not (material.standard_ref or "").strip():
            clarifications.append("Liner classification/standard basis is not defined.")
    if component == "COVER":
        if not (material.material_family or "").strip():
            clarifications.append("Cover material family is not defined.")

    if failed:
        outcome = "NOT_ACCEPTABLE"
    elif missing:
        outcome = "ADDITIONAL_TESTING_REQUIRED" if component in {"LINER", "REINFORCEMENT", "MATRIX", "COVER"} else "MORE_DATA_REQUIRED"
    elif pending or clarifications:
        outcome = "MATCH_WITH_CLARIFICATION"
    else:
        outcome = "MATCH"

    compatibility_status = "SUPPORTED"
    if failed:
        compatibility_status = "NOT_SUPPORTED"
    elif missing or pending:
        compatibility_status = "PARTIAL"

    evidence_status = "COMPLETE"
    if failed:
        evidence_status = "FAILED"
    elif missing:
        evidence_status = "MISSING"
    elif pending:
        evidence_status = "PENDING_REVIEW"

    summary_parts = []
    if outcome == "MATCH":
        summary_parts.append("Material basis and available evidence align with the current qualification input.")
    if missing:
        summary_parts.append("Missing required test/evidence: " + ", ".join(missing))
    if failed:
        summary_parts.append("Failed or unacceptable evidence in: " + ", ".join(failed))
    if pending:
        summary_parts.append("Pending review for: " + ", ".join(pending))
    if clarifications:
        summary_parts.append("Clarification needed: " + " ".join(clarifications))

    additional_tests = ", ".join(missing) if missing else ""
    clarification_text = " ".join(clarifications).strip()

    return {
        "required_tests": required,
        "missing_tests": missing,
        "failed_tests": failed,
        "pending_tests": pending,
        "compatibility_status": compatibility_status,
        "evidence_status": evidence_status,
        "review_outcome": outcome,
        "review_summary": " ".join(summary_parts).strip(),
        "clarification_required": clarification_text,
        "additional_tests_required": additional_tests,
        "change_requalification_flag": bool(
            component in {"LINER", "REINFORCEMENT", "MATRIX", "COVER"} and (
                missing or failed or clarification_text
            )
        ),
    }


def _refresh_material_review(session: Session, material: 'RndMaterialQualification', program: Optional[RndQualificationProgram] = None) -> 'RndMaterialQualification':
    tests = session.exec(
        select(RndMaterialTestRecord)
        .where(RndMaterialTestRecord.material_id == material.id)
        .order_by(RndMaterialTestRecord.test_date.desc(), RndMaterialTestRecord.id.desc())
    ).all()

    review = _material_review(material, tests, program)

    material.compatibility_status = review["compatibility_status"]
    material.evidence_status = review["evidence_status"]
    material.review_outcome = review["review_outcome"]
    material.review_summary = review["review_summary"]
    material.clarification_required = review["clarification_required"]
    material.additional_tests_required = review["additional_tests_required"]
    material.change_requalification_flag = review["change_requalification_flag"]
    _touch_row(material)
    session.add(material)
    return material

def _material_reference_options(materials: List['RndMaterialQualification']) -> list[dict]:
    options: list[dict] = []
    for row in materials:
        label_parts = []

        component = (row.component or '').strip().title() if (row.component or '').strip() else 'Material'
        label_parts.append(component)

        if (row.material_family or '').strip():
            label_parts.append((row.material_family or '').strip())

        if (row.material_name or '').strip():
            label_parts.append((row.material_name or '').strip())

        if (row.grade_name or '').strip():
            label_parts.append((row.grade_name or '').strip())

        if (row.reinforcement_type or '').strip():
            label_parts.append((row.reinforcement_type or '').strip())

        if row.reinforcement_layer_count is not None:
            label_parts.append(f"{row.reinforcement_layer_count}L")

        if (row.batch_ref or '').strip():
            label_parts.append(f"Batch {row.batch_ref.strip()}")

        value = " | ".join(part for part in label_parts if part)
        if value:
            options.append({
                'value': value,
                'label': value,
                'component': row.component or '',
                'review_outcome': row.review_outcome or 'MORE_DATA_REQUIRED',
            })
    return options

def _coalesce_material_ref(material_ref: str, batch_ref: str, source_pipe_ref: str, materials: List['RndMaterialQualification']) -> str:
    raw = (material_ref or '').strip()
    if raw:
        return raw
    options = _material_reference_options(materials)
    if len(options) == 1:
        return options[0]['value']
    batch_ref = (batch_ref or '').strip()
    source_pipe_ref = (source_pipe_ref or '').strip()
    if batch_ref and source_pipe_ref:
        return f"Batch {batch_ref} | Pipe {source_pipe_ref}"
    if batch_ref:
        return f"Batch {batch_ref}"
    if source_pipe_ref:
        return f"Pipe {source_pipe_ref}"
    return 'UNASSIGNED'




def _conditioning_required_flag(value: str | None) -> bool:
    text = (value or '').strip().lower()
    if not text:
        return False
    no_tokens = ['no ', 'not required', 'no specific', 'optional', 'conditional', 'refer to applicable procedure']
    if any(token in text for token in no_tokens):
        return False
    return 'yes' in text or 'conditioning' in text or 'temperature' in text

def _material_test_rows_by_material(material_tests: List['RndMaterialTestRecord']) -> dict[int, list['RndMaterialTestRecord']]:
    grouped: dict[int, list[RndMaterialTestRecord]] = {}
    for row in material_tests:
        grouped.setdefault(row.material_id, []).append(row)
    return grouped


def _material_dashboard_rows(
    materials: List['RndMaterialQualification'],
    material_tests: List['RndMaterialTestRecord'],
    program: Optional[RndQualificationProgram] = None,
) -> list[dict]:
    grouped = _material_test_rows_by_material(material_tests)
    rows = []

    for material in materials:
        tests = grouped.get(material.id or 0, [])
        required = _material_required_test_types(material, program)
        review = {
            "required_tests": required,
            "test_count": len(tests),
            "review_outcome": material.review_outcome or "MORE_DATA_REQUIRED",
            "review_summary": material.review_summary or "",
            "compatibility_status": material.compatibility_status or "UNKNOWN",
            "evidence_status": material.evidence_status or "MISSING",
            "clarification_required": material.clarification_required or "",
            "additional_tests_required": material.additional_tests_required or "",
            "change_requalification_flag": bool(material.change_requalification_flag),
        }
        rows.append({
            "material": material,
            "tests": tests,
            "review": review,
        })

    return rows


def _required_attachment_types(test_code: str) -> list[str]:
    common = [
        "TEST_PROCEDURE",
        "SPECIMEN_PHOTO",
        "DIMENSION_RECORD",
        "CALIBRATION_CERTIFICATE",
        "RESULT_SHEET",
    ]
    specific = {
        "MPR_REG": ["PRESSURE_LOG", "TEMPERATURE_LOG", "REGRESSION_REPORT"],
        "PV_1000H": ["PRESSURE_LOG", "TEMPERATURE_LOG"],
        "TEMP_ELEV": ["TEMPERATURE_LOG", "SETUP_PHOTO"],
        "TEMP_CYCLE": ["TEMPERATURE_LOG", "CYCLE_LOG", "SETUP_PHOTO"],
        "RAPID_DECOMP": ["GAS_CHARGE_RECORD", "TEMPERATURE_LOG", "SETUP_PHOTO"],
        "OPERATING_MBR": ["SETUP_PHOTO", "BEND_LAYOUT", "RESULT_SHEET"],
        "AXIAL_LOAD": ["SETUP_PHOTO", "LOAD_RECORD"],
        "CRUSH": ["SETUP_PHOTO", "LOAD_RECORD"],
        "LAOT": ["TEMPERATURE_LOG", "SETUP_PHOTO"],
        "IMPACT": ["SETUP_PHOTO", "IMPACT_RECORD"],
        "TEC": ["TEMPERATURE_LOG", "MEASUREMENT_RECORD"],
        "GROWTH": ["PRESSURE_LOG", "MEASUREMENT_RECORD"],
        "CYCLIC_REG": ["PRESSURE_LOG", "CYCLE_LOG", "REGRESSION_REPORT"],
    }
    return common + specific.get((test_code or "").upper(), [])


def _execution_requirements(test_code: str) -> dict:
    code = (test_code or "").upper()
    base = {
        "equipment": [
            "Calibrated test rig suitable for the test type",
            "Calibrated pressure, temperature, and dimensional measuring devices as applicable",
            "Controlled fixtures, grips, supports, or end terminations suited to the specimen",
        ],
        "records": [
            "Operator name",
            "Test date and time",
            "Specimen identification",
            "Applied test conditions",
            "Observed result",
            "Acceptance decision",
        ],
        "hold_points": [
            "Specimen preparation completed",
            "Pre-test visual verification completed",
            "Conditioning completed where required",
            "Calibration validity verified before test",
        ],
    }

    specific = {
        "MPR_REG": {
            "equipment": [
                "Long-term hydrostatic pressure system",
                "Controlled temperature environment",
                "Qualified end terminations for long-duration exposure",
            ],
            "records": [
                "Pressure level",
                "Temperature",
                "Failure time in hours",
                "Failure mode",
                "Runout status",
            ],
        },
        "PV_1000H": {
            "equipment": [
                "Constant pressure hold system",
                "Controlled temperature environment",
            ],
            "records": [
                "Pressure hold value",
                "Temperature",
                "Exposure duration",
                "Result after 1000 hours",
            ],
        },
        "TEMP_CYCLE": {
            "records": [
                "Cycle start temperature",
                "Cycle end temperature",
                "Cycle count",
                "Leakage or damage observation",
            ],
        },
        "IMPACT": {
            "records": [
                "Impact location",
                "Impact energy or setup basis",
                "Post-impact condition",
                "Follow-up acceptance result",
            ],
        },
    }

    merged = dict(base)
    extra = specific.get(code, {})
    if "equipment" in extra:
        merged["equipment"] = extra["equipment"]
    if "records" in extra:
        merged["records"] = extra["records"]
    return merged


def _acceptance_criteria(test_code: str) -> list[str]:
    code = (test_code or "").upper()
    criteria = {
        "MPR_REG": [
            "Only valid and permissible failures shall be included in the regression data set.",
            "Excluded points shall be documented with technical justification.",
            "Regression output shall satisfy the qualification basis and required confidence treatment.",
        ],
        "PV_1000H": [
            "Required specimens shall complete the 1000-hour confirmation without disqualifying failure.",
        ],
        "TEMP_ELEV": [
            "Specimen shall maintain integrity under the elevated-temperature qualification condition.",
        ],
        "TEMP_CYCLE": [
            "Specimen shall complete the required thermal cycling without disqualifying damage or leakage.",
        ],
        "RAPID_DECOMP": [
            "Specimen shall meet the acceptance basis defined for decompression resistance.",
        ],
        "OPERATING_MBR": [
            "Pipe shall demonstrate acceptable performance at the qualified operating bending radius.",
        ],
        "AXIAL_LOAD": [
            "Specimen shall satisfy the required axial load capability without disqualifying failure.",
        ],
        "CRUSH": [
            "Specimen shall satisfy the external load or crush acceptance basis.",
        ],
        "LAOT": [
            "Specimen shall satisfy qualification at the lowest allowable operating temperature.",
        ],
        "IMPACT": [
            "Specimen shall satisfy impact acceptance and any required follow-up confirmation.",
        ],
        "TEC": [
            "Measured thermal expansion values shall be recorded and accepted against the qualification basis.",
        ],
        "GROWTH": [
            "Measured dimensional growth or shrinkage shall remain within the acceptance basis.",
        ],
        "CYCLIC_REG": [
            "Cyclic regression data shall satisfy the qualification basis and required confidence treatment.",
        ],
    }
    return criteria.get(code, ["Test shall comply with the approved qualification basis and internal acceptance procedure."])


def _evidence_status(test_code: str, attachments: list) -> dict:
    required = _required_attachment_types(test_code)
    uploaded = {(a.document_type or "").upper() for a in attachments}
    rows = []
    missing = []
    for item in required:
        ok = item in uploaded
        rows.append({"document_type": item, "present": ok})
        if not ok:
            missing.append(item)
    return {
        "required": required,
        "rows": rows,
        "missing": missing,
        "complete": len(missing) == 0,
    }


def _specimen_readiness(specimens: list, prep: dict | None = None) -> dict:
    total = len(specimens)
    released = 0
    conditioning_pending = 0
    visual_pending = 0

    preconditioning_required = None
    if prep:
        preconditioning_required = prep.get("preconditioning", {}).get("required")

    for s in specimens:
        if s.released_for_test:
            released += 1
        if preconditioning_required is True and not s.conditioning_complete:
            conditioning_pending += 1
        if not s.pretest_visual_ok:
            visual_pending += 1

    return {
        "total": total,
        "released": released,
        "conditioning_pending": conditioning_pending,
        "visual_pending": visual_pending,
        "complete": total > 0 and conditioning_pending == 0 and visual_pending == 0,
    }


def _default_test_matrix(pfr_or_pv: str) -> list[dict]:
    route = (pfr_or_pv or '').strip().upper()

    base = [
        {"code": "MPR_REG", "title": "Long-term hydrostatic regression", "description": "Primary regression basis for qualification. Use ASTM D2992 Procedure B logic, exclude points below 10 h, calculate mean line, LCL, LPL, and LCL at RCRT.", "specimen_requirement": "18+ target", "clause_ref": "API 15S 5.3.2.3 / Annex E / Annex G", "applicability": "CORE", "scope_tag": "PFR", "source_standard": "API_15S"},
        {"code": "PV_1000H", "title": "1000-hour constant pressure confirmation", "description": "1000-hour proof / confirmation exposure used as a core verification step and for PV relationship confirmation.", "specimen_requirement": "2", "clause_ref": "API 15S 5.3.4.2", "applicability": "CORE", "scope_tag": "PV", "source_standard": "API_15S"},
        {"code": "TEMP_ELEV", "title": "Elevated temperature test", "description": "Seal and polymer creep or relaxation confirmation above MAOT.", "specimen_requirement": "1", "clause_ref": "API 15S 5.3.5", "applicability": "CORE", "scope_tag": "BOTH", "source_standard": "API_15S"},
        {"code": "TEMP_CYCLE", "title": "Temperature cycling", "description": "Thermal cycling confirmation for qualified size and rating combinations.", "specimen_requirement": "2", "clause_ref": "API 15S 5.3.6", "applicability": "CORE", "scope_tag": "BOTH", "source_standard": "API_15S"},
        {"code": "RAPID_DECOMP", "title": "Rapid decompression", "description": "Required for gas or multiphase service.", "specimen_requirement": "1", "clause_ref": "API 15S 5.3.7 / Annex B", "applicability": "SERVICE_DEP", "scope_tag": "BOTH", "source_standard": "API_15S"},
        {"code": "OPERATING_MBR", "title": "Operating MBR", "description": "Confirm operating bending performance.", "specimen_requirement": "2", "clause_ref": "API 15S 5.3.8.1", "applicability": "CORE", "scope_tag": "BOTH", "source_standard": "API_15S"},
        {"code": "HANDLING_MBR", "title": "Handling MBR preconditioning", "description": "Applies when handling MBR is smaller than operating MBR.", "specimen_requirement": "2", "clause_ref": "API 15S 5.3.8.2", "applicability": "RANGE_DEP", "scope_tag": "BOTH", "source_standard": "API_15S"},
        {"code": "HANDLING_AND_SPOOLING", "title": "Handling and spooling durability", "description": "Handling / spooling preconditioning followed by proof confirmation.", "specimen_requirement": "2", "clause_ref": "API 15S 5.3.8.3", "applicability": "RANGE_DEP", "scope_tag": "PFR", "source_standard": "API_15S"},
        {"code": "RESPOOLING", "title": "Respooling qualification", "description": "Used where respooling is claimed or allowed.", "specimen_requirement": "2", "clause_ref": "API 15S 5.3.8.4", "applicability": "RANGE_DEP", "scope_tag": "PFR", "source_standard": "API_15S"},
        {"code": "AXIAL_LOAD", "title": "Axial load capability", "description": "Maximum allowable axial load followed by proof confirmation.", "specimen_requirement": "2", "clause_ref": "API 15S 5.3.9", "applicability": "CORE", "scope_tag": "BOTH", "source_standard": "API_15S"},
        {"code": "CRUSH", "title": "External load / crush", "description": "Radial crush / external load characterization.", "specimen_requirement": "3", "clause_ref": "API 15S 5.3.10", "applicability": "RANGE_DEP", "scope_tag": "BOTH", "source_standard": "API_15S"},
        {"code": "LAOT", "title": "Lowest allowable operating temperature", "description": "Minimum operating temperature qualification.", "specimen_requirement": "2", "clause_ref": "API 15S 5.3.11", "applicability": "CORE", "scope_tag": "BOTH", "source_standard": "API_15S"},
        {"code": "IMPACT", "title": "Impact resistance", "description": "Impact followed by proof confirmation.", "specimen_requirement": "2", "clause_ref": "API 15S 5.3.12", "applicability": "CORE", "scope_tag": "BOTH", "source_standard": "API_15S"},
        {"code": "TEC", "title": "Thermal expansion coefficient", "description": "Axial TEC measurement and hoop TEC where clearance is critical.", "specimen_requirement": "2", "clause_ref": "API 15S 5.3.13", "applicability": "CORE", "scope_tag": "BOTH", "source_standard": "API_15S"},
        {"code": "GROWTH", "title": "Growth / shrinkage under pressure", "description": "Pressure elongation and dimensional response.", "specimen_requirement": "2", "clause_ref": "API 15S 5.3.14", "applicability": "CORE", "scope_tag": "BOTH", "source_standard": "API_15S"},
        {"code": "CYCLIC_REG", "title": "Cyclic pressure regression", "description": "For cyclic service. Use cyclic regression and lower confidence basis.", "specimen_requirement": "18+ target", "clause_ref": "API 15S 5.3.16 / Annex D", "applicability": "SERVICE_DEP", "scope_tag": "BOTH", "source_standard": "API_15S"},
    ]

    items = []
    for row in base:
        entry = dict(row)

        if route == 'PFR':
            if row["code"] == "MPR_REG":
                entry["route_note"] = "Primary PFR qualification regression"
            elif row["code"] == "PV_1000H":
                entry["route_note"] = "PV-only requirement kept visible for family planning"
            else:
                entry["route_note"] = "PFR qualification matrix item"
        elif route == 'PV':
            if row["code"] == "PV_1000H":
                entry["route_note"] = "Primary PV confirmation test"
            elif row["code"] == "MPR_REG":
                entry["route_note"] = "Reference PFR regression basis from the same qualification family"
            else:
                entry["route_note"] = "PV verification / inherited qualification matrix item"
        else:
            entry["route_note"] = "Qualification matrix item"

        items.append(entry)

    return items

def _seed_test_matrix(session: Session, program: RndQualificationProgram) -> None:
    existing = session.exec(
        select(RndQualificationTest).where(RndQualificationTest.program_id == program.id)
    ).all()
    if existing:
        return

    if not session.exec(
        select(RndMaterialQualification).where(RndMaterialQualification.program_id == program.id)
    ).first():
        for component, material in [
            ('LINER', program.liner_material),
            ('REINFORCEMENT', program.reinforcement_material),
            ('COVER', program.cover_material),
        ]:
            row = RndMaterialQualification(
                program_id=program.id,
                component=component,
                material_name=material,
                material_family=(
                    'POLYMER' if component in {'LINER', 'COVER'}
                    else 'REINFORCEMENT'
                ),
                reinforcement_type=(
                    'NONMETALLIC'
                    if component == 'REINFORCEMENT' and 'steel' not in (material or '').lower()
                    else ('STEEL' if component == 'REINFORCEMENT' else '')
                ),
                reinforcement_layer_count=(2 if component == 'REINFORCEMENT' else None),
                status='PLANNED',
                review_outcome='MORE_DATA_REQUIRED',
                evidence_status='MISSING',
                compatibility_status='UNKNOWN',
            )
            session.add(row)

    for idx, item in enumerate(_default_test_matrix(program.pfr_or_pv), start=1):
        session.add(
            RndQualificationTest(
                program_id=program.id,
                sort_order=idx,
                clause_ref=item['clause_ref'],
                code=item['code'],
                title=item['title'],
                description=item['description'],
                specimen_requirement=item['specimen_requirement'],
                applicability=item['applicability'],
                scope_tag=item.get('scope_tag', 'BOTH'),
                source_standard=item.get('source_standard', 'API_15S'),
            )
        )

    session.commit()

def _ensure_complete_test_matrix(session: Session, program: RndQualificationProgram) -> None:
    existing = session.exec(
        select(RndQualificationTest)
        .where(RndQualificationTest.program_id == program.id)
        .order_by(RndQualificationTest.sort_order.asc(), RndQualificationTest.id.asc())
    ).all()

    existing_by_code = {(row.code or '').strip().upper(): row for row in existing}
    matrix = _default_test_matrix(program.pfr_or_pv)

    next_order = max([row.sort_order for row in existing], default=0)

    changed = False
    for item in matrix:
        code = (item['code'] or '').strip().upper()
        row = existing_by_code.get(code)

        if row is None:
            next_order += 1
            session.add(
                RndQualificationTest(
                    program_id=program.id,
                    sort_order=next_order,
                    clause_ref=item['clause_ref'],
                    code=code,
                    title=item['title'],
                    description=item['description'],
                    specimen_requirement=item['specimen_requirement'],
                    applicability=item['applicability'],
                    scope_tag=item.get('scope_tag', 'BOTH'),
                    source_standard=item.get('source_standard', 'API_15S'),
                    status='PLANNED',
                )
            )
            changed = True
        else:
            row.clause_ref = item['clause_ref']
            row.title = item['title']
            row.description = item['description']
            row.specimen_requirement = item['specimen_requirement']
            row.applicability = item['applicability']
            row.scope_tag = item.get('scope_tag', row.scope_tag or 'BOTH')
            row.source_standard = item.get('source_standard', row.source_standard or 'API_15S')
            _touch_row(row)
            session.add(row)
            changed = True

    if changed:
        _touch_program(program)
        session.add(program)
        session.commit()


def _pv_extension_matrix() -> list[dict]:
    return [
        {
            "code": "PV_1000H",
            "title": "1000-hour constant pressure confirmation",
            "description": "Primary PV confirmation test for the added PV scope.",
            "specimen_requirement": "2",
            "clause_ref": "API 15S 5.3.4.2",
            "applicability": "CORE",
            "scope_tag": "PV",
            "source_standard": "API_15S",
        },
        {
            "code": "TEMP_ELEV",
            "title": "Elevated temperature test",
            "description": "Seal and polymer creep or relaxation confirmation above MAOT for the PV scope.",
            "specimen_requirement": "1",
            "clause_ref": "API 15S 5.3.5",
            "applicability": "CORE",
            "scope_tag": "BOTH",
            "source_standard": "API_15S",
        },
        {
            "code": "TEMP_CYCLE",
            "title": "Temperature cycling",
            "description": "Thermal cycling confirmation for the PV scope.",
            "specimen_requirement": "2",
            "clause_ref": "API 15S 5.3.6",
            "applicability": "CORE",
            "scope_tag": "BOTH",
            "source_standard": "API_15S",
        },
        {
            "code": "RAPID_DECOMP",
            "title": "Rapid decompression",
            "description": "Required for gas or multiphase service where the PV scope is applicable.",
            "specimen_requirement": "1",
            "clause_ref": "API 15S 5.3.7 / Annex B",
            "applicability": "SERVICE_DEP",
            "scope_tag": "BOTH",
            "source_standard": "API_15S",
        },
        {
            "code": "OPERATING_MBR",
            "title": "Operating MBR",
            "description": "Confirm operating bending performance for the PV scope.",
            "specimen_requirement": "2",
            "clause_ref": "API 15S 5.3.8.1",
            "applicability": "CORE",
            "scope_tag": "BOTH",
            "source_standard": "API_15S",
        },
        {
            "code": "AXIAL_LOAD",
            "title": "Axial load capability",
            "description": "Maximum allowable axial load followed by proof confirmation for the PV scope.",
            "specimen_requirement": "2",
            "clause_ref": "API 15S 5.3.9",
            "applicability": "CORE",
            "scope_tag": "BOTH",
            "source_standard": "API_15S",
        },
        {
            "code": "LAOT",
            "title": "Lowest allowable operating temperature",
            "description": "Minimum operating temperature qualification for the PV scope.",
            "specimen_requirement": "2",
            "clause_ref": "API 15S 5.3.11",
            "applicability": "CORE",
            "scope_tag": "BOTH",
            "source_standard": "API_15S",
        },
        {
            "code": "IMPACT",
            "title": "Impact resistance",
            "description": "Impact followed by proof confirmation for the PV scope.",
            "specimen_requirement": "2",
            "clause_ref": "API 15S 5.3.12",
            "applicability": "CORE",
            "scope_tag": "BOTH",
            "source_standard": "API_15S",
        },
        {
            "code": "TEC",
            "title": "Thermal expansion coefficient",
            "description": "Axial TEC measurement and hoop TEC where clearance is critical for the PV scope.",
            "specimen_requirement": "2",
            "clause_ref": "API 15S 5.3.13",
            "applicability": "CORE",
            "scope_tag": "BOTH",
            "source_standard": "API_15S",
        },
        {
            "code": "GROWTH",
            "title": "Growth / shrinkage under pressure",
            "description": "Pressure elongation and dimensional response for the PV scope.",
            "specimen_requirement": "2",
            "clause_ref": "API 15S 5.3.14",
            "applicability": "CORE",
            "scope_tag": "BOTH",
            "source_standard": "API_15S",
        },
        {
            "code": "CYCLIC_REG",
            "title": "Cyclic pressure regression",
            "description": "For cyclic service when the PV scope is intended for cyclic duty.",
            "specimen_requirement": "18+ target",
            "clause_ref": "API 15S 5.3.16 / Annex D",
            "applicability": "SERVICE_DEP",
            "scope_tag": "BOTH",
            "source_standard": "API_15S",
        },
    ]


def _add_pv_extension_to_program(session: Session, program: RndQualificationProgram) -> dict:
    existing_tests = session.exec(
        select(RndQualificationTest)
        .where(RndQualificationTest.program_id == program.id)
        .order_by(RndQualificationTest.sort_order.asc(), RndQualificationTest.id.asc())
    ).all()

    existing_by_code = {(row.code or '').strip().upper(): row for row in existing_tests}
    next_order = max([row.sort_order for row in existing_tests], default=0)

    created = []
    updated = []

    for item in _pv_extension_matrix():
        code = (item["code"] or "").strip().upper()
        row = existing_by_code.get(code)

        if row is None:
            next_order += 1
            row = RndQualificationTest(
                program_id=program.id,
                sort_order=next_order,
                clause_ref=item["clause_ref"],
                code=code,
                title=item["title"],
                description=item["description"],
                specimen_requirement=item["specimen_requirement"],
                applicability=item["applicability"],
                scope_tag=item.get("scope_tag", "PV"),
                source_standard=item.get("source_standard", "API_15S"),
                status="PLANNED",
            )
            session.add(row)
            created.append(code)
        else:
            # Do not destroy existing row content; only widen scope where needed
            current_scope = (row.scope_tag or "BOTH").strip().upper()
            wanted_scope = (item.get("scope_tag") or "PV").strip().upper()

            if current_scope != wanted_scope:
                if {current_scope, wanted_scope} & {"PFR", "PV"}:
                    row.scope_tag = "BOTH"
                elif current_scope == "CUSTOM":
                    row.scope_tag = "BOTH"
                else:
                    row.scope_tag = wanted_scope

            if not (row.clause_ref or "").strip():
                row.clause_ref = item["clause_ref"]
            if not (row.description or "").strip():
                row.description = item["description"]
            if not (row.specimen_requirement or "").strip():
                row.specimen_requirement = item["specimen_requirement"]
            if not (row.applicability or "").strip():
                row.applicability = item["applicability"]
            if not (row.source_standard or "").strip():
                row.source_standard = item.get("source_standard", "API_15S")

            _touch_row(row)
            session.add(row)
            updated.append(code)

    # mark program as now supporting PV in the same qualification family
    if (program.pfr_or_pv or "").strip().upper() == "PFR":
        program.pfr_or_pv = "PFR"
    _touch_program(program)
    session.add(program)

    session.commit()

    return {
        "created_codes": created,
        "updated_codes": updated,
    }
    

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


def _regression_from_specimens(specimens: List[RndQualificationSpecimen], mode: str = 'STATIC_REGRESSION', target_npr_mpa: float = 0.0, design_factor: float | None = None) -> dict:
    filtered = []
    excluded = []

    for s in specimens:
        specimen_type = (s.test_type or '').strip().upper()
        mode_key = (mode or '').strip().upper()

        static_aliases = {'STATIC_REGRESSION', 'MPR_REG'}
        cyclic_aliases = {'CYCLIC_REGRESSION', 'CYCLIC_REG'}

        if mode_key in static_aliases:
            if specimen_type not in static_aliases:
                continue
        elif mode_key in cyclic_aliases:
            if specimen_type not in cyclic_aliases:
                continue
        else:
            if specimen_type != mode_key:
                continue

        if not s.include_in_regression or not s.permissible_failure:
            excluded.append(s)
            continue

        x_raw = s.failure_hours if mode_key in static_aliases else s.failure_cycles
        y_raw = s.pressure_mpa

        if x_raw is None or y_raw is None or x_raw <= 0 or y_raw <= 0:
            continue

        if mode_key in static_aliases and x_raw < 10:
            excluded.append(s)
            continue

        filtered.append(s)

    n = len(filtered)
    required_minimum = 18 if mode.upper() in {'STATIC_REGRESSION', 'CYCLIC_REGRESSION', 'MPR_REG', 'CYCLIC_REG'} else 2

    result = {
        'count': n,
        'required_minimum': required_minimum,
        'points': [],
        'excluded_count': len(excluded),
        'excluded_ids': [s.specimen_id for s in excluded],
        'warning': '',
    }

    if n < 2:
        result['warning'] = 'Need at least 2 valid points to calculate a regression line.'
        return result

    pts, xs, ys = [], [], []
    for s in filtered:
        x_raw = s.failure_hours if mode_key in static_aliases else s.failure_cycles
        y_raw = s.pressure_mpa
        x = math.log10(float(x_raw))
        y = math.log10(float(y_raw))
        xs.append(x)
        ys.append(y)
        pts.append({
            'specimen_id': s.specimen_id,
            'x_raw': x_raw,
            'y_raw': y_raw,
            'x': x,
            'y': y,
            'temperature_c': s.temperature_c,
            'failure_mode': s.failure_mode
        })

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

    def _predict_components(x_val: float) -> dict:
        mean_y = intercept + slope * x_val
        mean_se = syx * math.sqrt((1 / n) + ((x_val - x_bar) ** 2 / sxx))
        pred_se = syx * math.sqrt(1 + (1 / n) + ((x_val - x_bar) ** 2 / sxx))
        lcl_y = mean_y - tcrit * mean_se
        lpl_y = mean_y - tcrit * pred_se
        return {
            'x_val': x_val,
            'mean_y': mean_y,
            'mean_se': mean_se,
            'pred_se': pred_se,
            'lcl_y': lcl_y,
            'lpl_y': lpl_y,
            'mean_pressure': 10 ** mean_y,
            'lcl_pressure': 10 ** lcl_y,
            'lpl_pressure': 10 ** lpl_y,
        }

    basis_x = math.log10(RCRT_HOURS if mode_key in static_aliases else CYCLIC_BASIS_CYCLES)
    basis_calc = _predict_components(basis_x)

    mean_basis_mpa = basis_calc['mean_pressure']
    lcl_basis_mpa = basis_calc['lcl_pressure']
    lpl_basis_mpa = basis_calc['lpl_pressure']

    effective_design_factor = (
        float(design_factor) if (design_factor is not None and mode_key in static_aliases)
        else (DESIGN_FACTOR_NONMETALLIC if mode_key in static_aliases else 1.0)
    )
    mpr_mpa = lcl_basis_mpa * effective_design_factor if mode_key in static_aliases else lcl_basis_mpa
    margin_mpa = mpr_mpa - target_npr_mpa if target_npr_mpa else None
    pass_status = None if not target_npr_mpa else ('PASS' if mpr_mpa >= target_npr_mpa else 'FAIL')

    chart_points = []
    x_min = min(xs)
    x_max = max(max(xs), basis_x)
    steps = 24
    for i in range(steps + 1):
        x_val = x_min + (x_max - x_min) * i / steps
        comp = _predict_components(x_val)
        chart_points.append({
            'x': x_val,
            'time_or_cycles': round(10 ** x_val, 3),
            'mean_pressure': round(comp['mean_pressure'], 4),
            'lcl_pressure': round(comp['lcl_pressure'], 4),
            'lpl_pressure': round(comp['lpl_pressure'], 4)
        })

    result.update({
        'points': pts,
        'slope': slope,
        'intercept': intercept,
        'syx': syx,
        'tcrit': tcrit,
        'x_bar': x_bar,
        'y_bar': y_bar,
        'sxx': sxx,
        'n': n,
        'df': df,
        'x_basis': basis_x,
        'rcrt_hours': RCRT_HOURS,
        'cyclic_basis_cycles': CYCLIC_BASIS_CYCLES,
        'mean_rcrt_mpa': mean_basis_mpa,
        'lcl_rcrt_mpa': lcl_basis_mpa,
        'lpl_rcrt_mpa': lpl_basis_mpa,
        'chart_points': chart_points,
        'design_factor': effective_design_factor,
        'mpr_mpa': mpr_mpa,
        'target_npr_mpa': target_npr_mpa,
        'margin_mpa': margin_mpa,
        'pass_status': pass_status,
        'formula_text': 'log10(P) = intercept + slope * log10(time)',
        'basis_calc': basis_calc,
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
        'summary': (f'This workspace organizes API 15S qualification for LLRTP with PE-RT liner, polyester fiber reinforcement, and PE100 cover. It guides the user through product definition, test matrix, specimen tracking, and regression review for {size} / {npr} / {maot}.' if (program and not (program.qualification_standard or '').strip().upper().startswith('OTHER')) else f'This workspace organizes a custom qualification program with checklist tracking, evidence collection, test guidance, and result review for {size} / {npr} / {maot}.'),
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
    service_route = answers.get('service_route') or (
        'gas_multiphase' if 'gas' in (program.intended_service or '').lower() else 'static_liquid'
    )
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

        if not (m.component or '').strip():
            missing.append('component')
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
        if not (m.standard_ref or '').strip():
            missing.append('standard basis')

        if (m.component or '').strip().upper() == 'REINFORCEMENT':
            if not (m.reinforcement_type or '').strip():
                missing.append('reinforcement type')
            if m.reinforcement_layer_count is None:
                missing.append('reinforcement layer count')
            if not (m.reinforcement_form or '').strip():
                missing.append('reinforcement form')

        ready = len(missing) == 0 and (m.review_outcome or '') in {'MATCH', 'MATCH_WITH_CLARIFICATION'}
        all_ready = all_ready and ready

        rows.append({
            'row': m,
            'missing': missing,
            'ready': ready,
            'review_outcome': m.review_outcome or 'MORE_DATA_REQUIRED',
            'review_summary': m.review_summary or '',
        })

    return {
        'rows': rows,
        'complete': all_ready and len(rows) >= 3,
        'status_label': 'Accepted' if all_ready and len(rows) >= 3 else 'More data required',
        'headline': 'Record traceable material identity, standards basis, qualification evidence, and reinforcement construction before structural testing starts.',
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
    static_reg = _regression_from_specimens(specimens, 'STATIC_REGRESSION', program.npr_mpa, program.service_factor)

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


@router.get('')
def rnd_home() -> RedirectResponse:
    return RedirectResponse(url='/rnd/qualifications', status_code=303)

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

@router.get('/qualifications')
def rnd_dashboard(request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    programs = session.exec(
        select(RndQualificationProgram).order_by(RndQualificationProgram.updated_at.desc())
    ).all()

    dashboard = []
    for program in programs:
        _ensure_complete_test_matrix(session, program)
        materials = session.exec(
            select(RndMaterialQualification)
            .where(RndMaterialQualification.program_id == program.id)
            .order_by(RndMaterialQualification.id.asc())
        ).all()

        specimens = session.exec(
            select(RndQualificationSpecimen)
            .where(RndQualificationSpecimen.program_id == program.id)
            .order_by(RndQualificationSpecimen.created_at.desc())
        ).all()

        flow = _active_stage(program, materials, specimens)

        dashboard.append({
            "program": program,
            "flow": flow,
        })

    guide = _qualification_guide()

    return TEMPLATES.TemplateResponse(
        request=request,
        name='rnd_dashboard.html',
        context={
            'request': request,
            'user': user,
            'dashboard': dashboard,
            'guide': guide,
            'design_factor_nonmetallic': DESIGN_FACTOR_NONMETALLIC,
            'rcrt_hours': RCRT_HOURS,
        },
    )

@router.get('/qualifications/new')
def rnd_new_program_form(request: Request, user: User = Depends(_require_user)):
    return TEMPLATES.TemplateResponse(request,'rnd_program_form.html', {'request': request, 'user': user})


@router.post('/qualifications/new')
def rnd_create_program(
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),

    title: str = Form(...),
    program_code: str = Form(...),

    program_type: str = Form('API_15S'),
    qualification_standard: str = Form('API 15S R3'),

    nominal_size_in: float = Form(4.0),
    npr_mpa: float = Form(10.0),
    maot_c: float = Form(65.0),
    laot_c: float = Form(0.0),

    pfr_or_pv: str = Form('PFR'),
    parent_program_id: Optional[int] = Form(None),

    service_medium: str = Form('WATER'),
    service_factor: float = Form(1.0),
    intended_service: str = Form('Static water service'),

    product_family: str = Form('LLRTP-PE-RT-PET-PE100'),
    reinforcement_type: str = Form('NONMETALLIC'),
    liner_material: str = Form('PE-RT'),
    reinforcement_material: str = Form('Polyester Fiber'),
    cover_material: str = Form('PE100'),

    custom_requirements: str = Form(''),
    custom_acceptance_criteria: str = Form(''),
    custom_tests: str = Form(''),

    notes: str = Form(''),
):
    safe_program_type = (program_type or 'API_15S').strip().upper()
    if safe_program_type not in {'API_15S', 'OTHER'}:
        safe_program_type = 'API_15S'

    safe_service_medium = (service_medium or 'WATER').strip().upper()
    if safe_service_medium not in {'WATER', 'GAS', 'HYDROCARBON', 'LIQUIDS'}:
        safe_service_medium = 'WATER'

    safe_pfr_or_pv = (pfr_or_pv or 'PFR').strip().upper()
    if safe_pfr_or_pv not in {'PFR', 'PV'}:
        safe_pfr_or_pv = 'PFR'

    # OTHER programs should not pretend to be API 15S / PFR
    if safe_program_type == 'OTHER':
        safe_standard = (qualification_standard or 'OTHER QUALIFICATION').strip()
        safe_pfr_or_pv = 'PFR'
        parent_program_id = None
    else:
        safe_standard = (qualification_standard or 'API 15S R3').strip()

    program = RndQualificationProgram(
        program_code=(program_code or '').strip().upper(),
        title=(title or '').strip(),

        program_type=safe_program_type,
        qualification_standard=safe_standard,

        nominal_size_in=nominal_size_in,
        npr_mpa=npr_mpa,
        maot_c=maot_c,
        laot_c=laot_c,

        pfr_or_pv=safe_pfr_or_pv,
        parent_program_id=parent_program_id,

        service_medium=safe_service_medium,
        service_factor=service_factor,
        intended_service=(intended_service or '').strip(),

        product_family=(product_family or '').strip(),
        reinforcement_type=(reinforcement_type or 'NONMETALLIC').strip().upper(),
        liner_material=(liner_material or '').strip(),
        reinforcement_material=(reinforcement_material or '').strip(),
        cover_material=(cover_material or '').strip(),

        custom_requirements=(custom_requirements or '').strip(),
        custom_acceptance_criteria=(custom_acceptance_criteria or '').strip(),

        notes=(notes or '').strip(),
        created_by_name=(getattr(user, 'display_name', '') or getattr(user, 'username', '') or ''),
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

    # Seed default API matrix only for API_15S programs
    if program.program_type == 'API_15S':
        _seed_test_matrix(session, program)
    else:
        # Seed default material rows for custom programs too
        if not session.exec(
            select(RndMaterialQualification).where(RndMaterialQualification.program_id == program.id)
        ).first():
            for component, material in [
                ('LINER', program.liner_material or 'CUSTOM'),
                ('REINFORCEMENT', program.reinforcement_material or 'CUSTOM'),
                ('COVER', program.cover_material or 'CUSTOM'),
            ]:
                row = RndMaterialQualification(
                    program_id=program.id,
                    component=component,
                    material_name=material,
                    material_family=('POLYMER' if component in {'LINER', 'COVER'} else 'REINFORCEMENT'),
                    reinforcement_type=(
                        'NONMETALLIC'
                        if component == 'REINFORCEMENT' and 'steel' not in (material or '').lower()
                        else ('STEEL' if component == 'REINFORCEMENT' else '')
                    ),
                    reinforcement_layer_count=(2 if component == 'REINFORCEMENT' else None),
                    status='PLANNED',
                    review_outcome='MORE_DATA_REQUIRED',
                    evidence_status='MISSING',
                    compatibility_status='UNKNOWN',
                )
                session.add(row)
            session.commit()

        # Build custom tests from textarea lines
        # Expected one line per test, example:
        # Hydrostatic proof | 2 specimens | Client Spec 4.2 | Hold at pressure for 24 h
        custom_rows = []
        for raw_line in (custom_tests or '').splitlines():
            line = (raw_line or '').strip()
            if not line:
                continue

            parts = [p.strip() for p in line.split('|')]
            title_part = parts[0] if len(parts) > 0 else 'Custom Test'
            specimens_part = parts[1] if len(parts) > 1 else 'As required'
            clause_part = parts[2] if len(parts) > 2 else 'CUSTOM'
            desc_part = parts[3] if len(parts) > 3 else 'Custom qualification requirement.'

            custom_rows.append({
                'title': title_part,
                'specimen_requirement': specimens_part,
                'clause_ref': clause_part,
                'description': desc_part,
            })

        for idx, item in enumerate(custom_rows, start=1):
            code = f'OTHER_{idx}'
            session.add(
                RndQualificationTest(
                    program_id=program.id,
                    sort_order=idx,
                    clause_ref=item['clause_ref'],
                    code=code,
                    title=item['title'],
                    description=item['description'],
                    specimen_requirement=item['specimen_requirement'],
                    applicability='CUSTOM',
                    status='PLANNED',
                )
            )

        session.commit()

    return RedirectResponse(url=f'/rnd/qualifications/{program.id}', status_code=303)

@router.get('/qualifications/{program_id}')
def rnd_program_view(program_id: int, request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')

    _seed_test_matrix(session, program)
    _ensure_complete_test_matrix(session, program)

    tests = session.exec(
        select(RndQualificationTest)
        .where(RndQualificationTest.program_id == program_id)
        .order_by(RndQualificationTest.sort_order.asc(), RndQualificationTest.id.asc())
    ).all()

    specimens = session.exec(
        select(RndQualificationSpecimen)
        .where(RndQualificationSpecimen.program_id == program_id)
        .order_by(RndQualificationSpecimen.created_at.desc())
    ).all()

    materials = session.exec(
        select(RndMaterialQualification)
        .where(RndMaterialQualification.program_id == program_id)
        .order_by(RndMaterialQualification.id.asc())
    ).all()

    material_tests = session.exec(
        select(RndMaterialTestRecord)
        .where(RndMaterialTestRecord.program_id == program_id)
        .order_by(RndMaterialTestRecord.test_date.desc(), RndMaterialTestRecord.id.desc())
    ).all()

    attachments = session.exec(
        select(RndAttachmentRegister)
        .where(RndAttachmentRegister.program_id == program_id)
        .order_by(RndAttachmentRegister.created_at.desc())
    ).all()

    # Refresh all material review states on page load so the UI always gets current judgment
    for material in materials:
        _refresh_material_review(session, material, program)
    session.commit()

    # Reload materials after refresh to ensure current values are used
    materials = session.exec(
        select(RndMaterialQualification)
        .where(RndMaterialQualification.program_id == program_id)
        .order_by(RndMaterialQualification.id.asc())
    ).all()

    material_dashboard = _material_dashboard_rows(materials, material_tests, program)

    static_reg = _regression_from_specimens(specimens, 'STATIC_REGRESSION', program.npr_mpa, program.service_factor)
    cyclic_reg = _regression_from_specimens(specimens, 'CYCLIC_REGRESSION', program.npr_mpa)
    counts = _matrix_counts(tests)
    guide = _qualification_guide(program)
    phase_cards = _phase_cards(program, tests, materials, specimens, attachments)

    return TEMPLATES.TemplateResponse(
        request,
        'rnd_program_view.html',
        {
            'request': request,
            'user': user,
            'program': program,
            'tests': tests,
            'specimens': specimens,
            'materials': materials,
            'material_tests': material_tests,
            'material_dashboard': material_dashboard,
            'attachments': attachments,
            'static_reg': static_reg,
            'cyclic_reg': cyclic_reg,
            'counts': counts,
            'progress_pct': _status_pct(counts, len(tests)),
            'guide': guide,
            'phase_cards': phase_cards,
            'design_factor_nonmetallic': DESIGN_FACTOR_NONMETALLIC,
            'rcrt_hours': RCRT_HOURS,
        }
    )

@router.post('/qualifications/{program_id}/status')
def rnd_update_program_status(program_id: int, status: str = Form(...), session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')
    program.status = (status or 'DRAFT').strip().upper(); _touch_program(program); session.add(program); session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}', status_code=303)


@router.get('/qualifications/{program_id}/edit')
def rnd_edit_program_form(
    program_id: int,
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),
):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')

    return TEMPLATES.TemplateResponse(
        request,
        'rnd_program_edit.html',
        {
            'request': request,
            'user': user,
            'program': program,
        }
    )


@router.post('/qualifications/{program_id}/edit')
def rnd_edit_program(
    program_id: int,
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),

    title: str = Form(...),
    program_code: str = Form(...),

    program_type: str = Form('API_15S'),
    qualification_standard: str = Form('API 15S R3'),

    nominal_size_in: float = Form(4.0),
    npr_mpa: float = Form(10.0),
    maot_c: float = Form(65.0),
    laot_c: float = Form(0.0),

    pfr_or_pv: str = Form('PFR'),
    parent_program_id: Optional[int] = Form(None),

    service_medium: str = Form('WATER'),
    service_factor: float = Form(1.0),
    intended_service: str = Form(''),

    product_family: str = Form(''),
    reinforcement_type: str = Form('NONMETALLIC'),
    liner_material: str = Form(''),
    reinforcement_material: str = Form(''),
    cover_material: str = Form(''),

    custom_requirements: str = Form(''),
    custom_acceptance_criteria: str = Form(''),
    notes: str = Form(''),
):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')

    old_program_type = (program.program_type or 'API_15S').strip().upper()

    safe_program_type = (program_type or 'API_15S').strip().upper()
    if safe_program_type not in {'API_15S', 'OTHER'}:
        safe_program_type = 'API_15S'

    safe_service_medium = (service_medium or 'WATER').strip().upper()
    if safe_service_medium not in {'WATER', 'GAS', 'HYDROCARBON', 'LIQUIDS'}:
        safe_service_medium = 'WATER'

    safe_pfr_or_pv = (pfr_or_pv or 'PFR').strip().upper()
    if safe_pfr_or_pv not in {'PFR', 'PV'}:
        safe_pfr_or_pv = 'PFR'

    program.title = (title or '').strip()
    program.program_code = (program_code or '').strip().upper()

    program.program_type = safe_program_type
    program.service_medium = safe_service_medium
    program.service_factor = service_factor

    program.nominal_size_in = nominal_size_in
    program.npr_mpa = npr_mpa
    program.maot_c = maot_c
    program.laot_c = laot_c

    program.intended_service = (intended_service or '').strip()
    program.product_family = (product_family or '').strip()
    program.reinforcement_type = (reinforcement_type or 'NONMETALLIC').strip().upper()
    program.liner_material = (liner_material or '').strip()
    program.reinforcement_material = (reinforcement_material or '').strip()
    program.cover_material = (cover_material or '').strip()

    program.custom_requirements = (custom_requirements or '').strip()
    program.custom_acceptance_criteria = (custom_acceptance_criteria or '').strip()
    program.notes = (notes or '').strip()

    if safe_program_type == 'OTHER':
        program.qualification_standard = (qualification_standard or 'OTHER QUALIFICATION').strip()
        program.pfr_or_pv = 'PFR'
        program.parent_program_id = None
        program.pfr_reference_code = ''
    else:
        program.qualification_standard = (qualification_standard or 'API 15S R3').strip()
        program.pfr_or_pv = safe_pfr_or_pv
        program.parent_program_id = parent_program_id

        if parent_program_id:
            parent = session.get(RndQualificationProgram, parent_program_id)
            if parent:
                program.pfr_reference_code = parent.program_code
            else:
                program.pfr_reference_code = ''
        else:
            program.pfr_reference_code = ''

    _touch_program(program)
    session.add(program)
    session.commit()

    # If switching into API_15S, ensure the default API matrix exists
    if old_program_type != 'API_15S' and program.program_type == 'API_15S':
        _seed_test_matrix(session, program)
        _ensure_complete_test_matrix(session, program)

    return RedirectResponse(url=f'/rnd/qualifications/{program.id}', status_code=303)
    

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
def rnd_update_material(
    program_id: int,
    material_id: int,
    material_name: str = Form(''),
    supplier_name: str = Form(''),
    manufacturer_name: str = Form(''),
    grade_name: str = Form(''),
    trade_name: str = Form(''),
    certificate_ref: str = Form(''),
    batch_ref: str = Form(''),
    lot_ref: str = Form(''),
    status: str = Form('PLANNED'),
    notes: str = Form(''),
    standard_ref: str = Form(''),
    material_family: str = Form(''),
    service_fluid_basis: str = Form(''),
    service_notes: str = Form(''),
    max_service_temp_c: Optional[float] = Form(None),
    min_service_temp_c: Optional[float] = Form(None),
    classification_basis: str = Form(''),
    pe_cell_class: str = Form(''),
    hdb_basis: str = Form(''),
    uv_class: str = Form(''),
    reinforcement_type: str = Form(''),
    reinforcement_form: str = Form(''),
    reinforcement_layer_count: Optional[int] = Form(None),
    reinforcement_layout_notes: str = Form(''),
    reinforcement_matrix_material: str = Form(''),
    steel_processing_history: str = Form(''),
    fiber_sizing_notes: str = Form(''),
    matrix_material_name: str = Form(''),
    matrix_resin_type: str = Form(''),
    cure_method: str = Form(''),
    tg_min_c: Optional[float] = Form(None),
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),
):
    row = session.get(RndMaterialQualification, material_id)
    if not row or row.program_id != program_id:
        raise HTTPException(404, 'Material row not found')

    row.material_name = (material_name or '').strip()
    row.supplier_name = (supplier_name or '').strip()
    row.manufacturer_name = (manufacturer_name or '').strip()
    row.grade_name = (grade_name or '').strip()
    row.trade_name = (trade_name or '').strip()
    row.certificate_ref = (certificate_ref or '').strip()
    row.batch_ref = (batch_ref or '').strip()
    row.lot_ref = (lot_ref or '').strip()
    row.status = (status or 'PLANNED').strip().upper()
    row.notes = (notes or '').strip()

    row.standard_ref = (standard_ref or '').strip()
    row.material_family = (material_family or '').strip().upper()
    row.service_fluid_basis = (service_fluid_basis or '').strip()
    row.service_notes = (service_notes or '').strip()
    row.max_service_temp_c = max_service_temp_c
    row.min_service_temp_c = min_service_temp_c

    row.classification_basis = (classification_basis or '').strip()
    row.pe_cell_class = (pe_cell_class or '').strip().upper()
    row.hdb_basis = (hdb_basis or '').strip()
    row.uv_class = (uv_class or '').strip().upper()

    row.reinforcement_type = (reinforcement_type or '').strip().upper()
    row.reinforcement_form = (reinforcement_form or '').strip()
    row.reinforcement_layer_count = reinforcement_layer_count
    row.reinforcement_layout_notes = (reinforcement_layout_notes or '').strip()
    row.reinforcement_matrix_material = (reinforcement_matrix_material or '').strip()
    row.steel_processing_history = (steel_processing_history or '').strip()
    row.fiber_sizing_notes = (fiber_sizing_notes or '').strip()

    row.matrix_material_name = (matrix_material_name or '').strip()
    row.matrix_resin_type = (matrix_resin_type or '').strip().upper()
    row.cure_method = (cure_method or '').strip()
    row.tg_min_c = tg_min_c

    _touch_row(row)
    session.add(row)

    program = session.get(RndQualificationProgram, program_id)
    _refresh_material_review(session, row, program)

    if program:
        _touch_program(program)
        session.add(program)

    session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}?tab=materials', status_code=303)



@router.post('/qualifications/{program_id}/materials/{material_id}/tests/new')
def rnd_add_material_test_record(
    program_id: int,
    material_id: int,
    test_type: str = Form(...),
    standard_ref: str = Form(''),
    specimen_ref: str = Form(''),
    report_ref: str = Form(''),
    lab_name: str = Form(''),
    test_date: Optional[date] = Form(None),
    result_value: str = Form(''),
    result_unit: str = Form(''),
    acceptance_basis: str = Form(''),
    decision: str = Form('PENDING'),
    notes: str = Form(''),
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),
):
    material = session.get(RndMaterialQualification, material_id)
    if not material or material.program_id != program_id:
        raise HTTPException(404, 'Material row not found')

    row = RndMaterialTestRecord(
        program_id=program_id,
        material_id=material_id,
        test_type=(test_type or '').strip().upper(),
        standard_ref=(standard_ref or '').strip(),
        specimen_ref=(specimen_ref or '').strip(),
        report_ref=(report_ref or '').strip(),
        lab_name=(lab_name or '').strip(),
        test_date=test_date,
        result_value=(result_value or '').strip(),
        result_unit=(result_unit or '').strip(),
        acceptance_basis=(acceptance_basis or '').strip(),
        decision=(decision or 'PENDING').strip().upper(),
        notes=(notes or '').strip(),
    )
    session.add(row)

    program = session.get(RndQualificationProgram, program_id)
    _refresh_material_review(session, material, program)

    if program:
        _touch_program(program)
        session.add(program)

    session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}?tab=materials', status_code=303)


@router.post('/qualifications/{program_id}/materials/tests/{test_row_id}/delete')
def rnd_delete_material_test_record(
    program_id: int,
    test_row_id: int,
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),
):
    row = session.get(RndMaterialTestRecord, test_row_id)
    if not row or row.program_id != program_id:
        raise HTTPException(404, 'Material test record not found')

    material = session.get(RndMaterialQualification, row.material_id)
    session.delete(row)

    program = session.get(RndQualificationProgram, program_id)
    if material:
        _refresh_material_review(session, material, program)

    if program:
        _touch_program(program)
        session.add(program)

    session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}?tab=materials', status_code=303)


@router.post('/qualifications/{program_id}/attachments/new')
def rnd_add_attachment_register(program_id: int, category: str = Form('REPORT'), title: str = Form(...), reference_no: str = Form(''), file_note: str = Form(''), session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')
    session.add(RndAttachmentRegister(program_id=program_id, category=(category or 'REPORT').strip().upper(), title=title.strip(), reference_no=reference_no.strip(), file_note=file_note.strip()))
    _touch_program(program); session.add(program); session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}', status_code=303)


@router.post('/qualifications/{program_id}/specimens/new')
def rnd_add_specimen(program_id: int, session: Session = Depends(get_session), user: User = Depends(_require_user), specimen_id: str = Form(...), test_type: str = Form(...), test_id: Optional[int] = Form(None), sample_date: date = Form(...), material_ref: str = Form(''), nominal_size_in: float = Form(0.0), pressure_mpa: float = Form(0.0), temperature_c: float = Form(0.0), failure_hours: Optional[float] = Form(None), failure_cycles: Optional[float] = Form(None), failure_mode: str = Form(''), permissible_failure: Optional[str] = Form(None), is_runout: Optional[str] = Form(None), include_in_regression: Optional[str] = Form(None), fitting_type: str = Form('Field fitting'), lab_name: str = Form(''), witness_name: str = Form(''), notes: str = Form('')):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')
    materials = session.exec(select(RndMaterialQualification).where(RndMaterialQualification.program_id == program_id).order_by(RndMaterialQualification.component.asc(), RndMaterialQualification.id.asc())).all()
    specimen = RndQualificationSpecimen(program_id=program_id, test_id=test_id, specimen_id=(specimen_id or '').strip().upper(), test_type=(test_type or 'STATIC_REGRESSION').strip().upper(), sample_date=sample_date, material_ref=_coalesce_material_ref(material_ref, '', '', materials), nominal_size_in=nominal_size_in or program.nominal_size_in, pressure_mpa=pressure_mpa, temperature_c=temperature_c, failure_hours=failure_hours, failure_cycles=failure_cycles, failure_mode=failure_mode, permissible_failure=bool(permissible_failure), is_runout=bool(is_runout), include_in_regression=bool(include_in_regression), fitting_type=fitting_type, lab_name=lab_name, witness_name=witness_name, notes=notes)
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



@router.post('/qualifications/{program_id}/tests/custom')
def rnd_add_custom_test(program_id: int, title: str = Form(...), code: str = Form('OTHER'), description: str = Form(''), specimen_requirement: str = Form('As required'), clause_ref: str = Form('CUSTOM'), session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')
    existing = session.exec(select(RndQualificationTest).where(RndQualificationTest.program_id == program_id).order_by(RndQualificationTest.sort_order.desc(), RndQualificationTest.id.desc())).first()
    next_order = (existing.sort_order if existing else 0) + 1
    norm_code = (code or 'OTHER').strip().upper()
    if norm_code == 'OTHER':
        norm_code = f"OTHER_{next_order}"
    row = RndQualificationTest(program_id=program_id, sort_order=next_order, clause_ref=(clause_ref or 'CUSTOM').strip(), code=norm_code, title=(title or '').strip(), description=(description or '').strip(), specimen_requirement=(specimen_requirement or 'As required').strip(), applicability='CUSTOM', status='PLANNED')
    session.add(row)
    _touch_program(program)
    session.add(program)
    session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}', status_code=303)


@router.post('/qualifications/{program_id}/add-pv-extension')
def rnd_add_pv_extension(
    program_id: int,
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),
):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')

    if (program.program_type or 'API_15S').strip().upper() != 'API_15S':
        raise HTTPException(400, 'PV extension is only available for API 15S programs.')

    result = _add_pv_extension_to_program(session, program)
    return RedirectResponse(
        url=f"/rnd/qualifications/{program_id}?pv_extension_added=1&created={len(result['created_codes'])}&updated={len(result['updated_codes'])}",
        status_code=303,
    )
    

@router.get('/qualifications/{program_id}/regression')
def rnd_regression_view(program_id: int, request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')
    specimens = session.exec(select(RndQualificationSpecimen).where(RndQualificationSpecimen.program_id == program_id).order_by(RndQualificationSpecimen.created_at.asc())).all()
    static_reg = _regression_from_specimens(specimens, 'STATIC_REGRESSION', program.npr_mpa, program.service_factor)
    cyclic_reg = _regression_from_specimens(specimens, 'CYCLIC_REGRESSION', program.npr_mpa)
    pv_formula = None
    if program.pfr_or_pv == 'PV' and program.parent_program_id:
        parent = session.get(RndQualificationProgram, program.parent_program_id)
        if parent:
            ratio = (program.npr_mpa / parent.npr_mpa) if parent.npr_mpa else None
            pv_formula = {'pfr_code': parent.program_code, 'npr_pv': program.npr_mpa, 'npr_pfr': parent.npr_mpa, 'formula': 'PPV1000 = PPFR1000 x (NPR_PV / NPR_PFR)', 'ratio': ratio}
    guide = _qualification_guide(program)
    return TEMPLATES.TemplateResponse(request,'rnd_regression_view.html', {'request': request, 'user': user, 'program': program, 'specimens': specimens, 'static_reg': static_reg, 'cyclic_reg': cyclic_reg, 'pv_formula': pv_formula, 'guide': guide, 'design_factor_nonmetallic': DESIGN_FACTOR_NONMETALLIC, 'rcrt_hours': RCRT_HOURS})


@router.get('/qualifications/{program_id}/tests/{test_id}')
def rnd_test_detail(program_id: int, test_id: int, request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    test = session.get(RndQualificationTest, test_id)
    if not test:
        raise HTTPException(404, 'Test not found')
    
    program = session.get(RndQualificationProgram, test.program_id)
    mpr_test = session.exec(
        select(RndQualificationTest)
        .where(RndQualificationTest.program_id == program_id)
        .where(RndQualificationTest.code == 'MPR_REG')
    ).first()
    if not program:
        raise HTTPException(404, 'Program not found')
    
    if int(program_id) != int(test.program_id):
        raise HTTPException(400, 'Test does not belong to this program')




    prep = get_specimen_prep(test.code)
    guidance = get_test_guidance(test.code)

    specimens = session.exec(
        select(RndQualificationSpecimen)
        .where(RndQualificationSpecimen.program_id == program_id)
        .where(RndQualificationSpecimen.test_id == test_id)
        .order_by(RndQualificationSpecimen.created_at.desc())
    ).all()

    attachments = session.exec(
        select(RndAttachmentRegister)
        .where(RndAttachmentRegister.program_id == program_id)
        .where(RndAttachmentRegister.test_id == test_id)
        .order_by(RndAttachmentRegister.created_at.desc())
    ).all()

    materials = session.exec(
        select(RndMaterialQualification)
        .where(RndMaterialQualification.program_id == program_id)
        .order_by(RndMaterialQualification.component.asc(), RndMaterialQualification.id.asc())
    ).all()
    material_options = _material_reference_options(materials)

    execution = _execution_requirements(test.code)
    acceptance = _acceptance_criteria(test.code)
    evidence = _evidence_status(test.code, attachments)
    specimen_state = _specimen_readiness(specimens, prep)
    specimen_lifecycle = _specimen_lifecycle_summary(specimens)
    progress = _test_progress_snapshot(test, specimens, attachments)

    return TEMPLATES.TemplateResponse(
        request,
        'rnd_test_detail.html',
        {
            'request': request,
            'user': user,
            'program': program,
            'test': test,
            'prep': prep,
            'specimens': specimens,
            'attachments': attachments,
            'execution': execution,
            'acceptance': acceptance,
            'evidence': evidence,
            'specimen_state': specimen_state,
            'specimen_lifecycle': specimen_lifecycle,
            'progress': progress,
            'guidance': guidance,
            'materials': materials,
            'material_options': material_options,
            'mpr_test': mpr_test,
        }
    )

@router.post('/qualifications/{program_id}/tests/{test_id}/specimens/new')
def rnd_add_test_specimen(
    request: Request,
    program_id: int,
    test_id: int,
    specimen_id: str = Form(...),
    test_type: str = Form(''),
    sample_date: str = Form(''),
    nominal_size_in: str = Form(''),
    confirmed_od_mm: str = Form(''),
    preparation_rule_basis: str = Form(''),
    pressure_mpa: str = Form(''),
    temperature_c: str = Form(''),
    failure_hours: str = Form(''),
    failure_cycles: str = Form(''),
    failure_mode: str = Form(''),
    permissible_failure: str = Form(''),
    is_runout: Optional[str] = Form(None),
    include_in_regression: Optional[str] = Form(None),
    fitting_type: str = Form(''),
    lab_name: str = Form(''),
    witness_name: str = Form(''),
    notes: str = Form(''),
    batch_ref: str = Form(''),
    source_pipe_ref: str = Form(''),
    cut_by: str = Form(''),
    total_cut_length_mm: str = Form(''),
    effective_length_mm: str = Form(''),
    end_allowance_each_side_mm: str = Form(''),
    trimming_margin_mm: str = Form(''),
    conditioning_complete: Optional[str] = Form(None),
    pretest_visual_ok: Optional[str] = Form(None),
    released_for_test: Optional[str] = Form(None),
    planned_pressure_mpa: str = Form(''),
    actual_pressure_at_failure_mpa: str = Form(''),
    pressure_at_hold_mpa: str = Form(''),
    failure_time_sec: str = Form(''),
    pre_failure_condition: str = Form(''),
    pre_failure_visual: str = Form(''),
    post_failure_visual: str = Form(''),
    failure_location: str = Form(''),
    failure_description: str = Form(''),
    leak_observation: str = Form(''),
    result_status: str = Form('PENDING'),
    qa_review_status: str = Form('PENDING'),
    session: Session = Depends(get_session),
):
    def as_float(value):
        if value is None:
            return None
        value = str(value).strip()
        if value == '':
            return None
        try:
            return float(value)
        except Exception:
            return None

    def as_bool(value):
        return value in (True, 'true', 'True', 'on', '1', 1)

    def as_date(value):
        value = (value or '').strip()
        if not value:
            return date.today()
        try:
            return datetime.strptime(value, '%Y-%m-%d').date()
        except Exception:
            return date.today()

    # First try to recover the test, even if program_id is stale
    test = session.get(RndQualificationTest, test_id)

    if not test:
        desired_code = (test_type or '').strip().upper()
        if desired_code:
            test = session.exec(
                select(RndQualificationTest).where(
                    RndQualificationTest.program_id == program_id,
                    RndQualificationTest.code == desired_code,
                )
            ).first()

    if not test:
        raise HTTPException(404, 'Test not found')

    # Use the real program from the recovered test, not blindly from URL
    program = session.get(RndQualificationProgram, test.program_id)
    if not program:
        raise HTTPException(404, 'Program not found')

    existing_specimen = session.exec(
        select(RndQualificationSpecimen)
        .where(RndQualificationSpecimen.program_id == program.id)
        .where(RndQualificationSpecimen.test_id == test.id)
        .where(RndQualificationSpecimen.specimen_id == (specimen_id or '').strip())
    ).first()
    
    if existing_specimen:
        return RedirectResponse(
            url=f'/rnd/qualifications/{program.id}/tests/{test.id}?tab=specimens&error=duplicate_specimen_id',
            status_code=303,
        )

    safe_batch_ref = (batch_ref or '').strip()
    safe_source_pipe_ref = (source_pipe_ref or '').strip()

    material_ref_parts = []
    if safe_batch_ref:
        material_ref_parts.append(f'Batch {safe_batch_ref}')
    if safe_source_pipe_ref:
        material_ref_parts.append(f'Pipe {safe_source_pipe_ref}')
    safe_material_ref = ' | '.join(material_ref_parts) or 'FINAL_PRODUCT'

    specimen = RndQualificationSpecimen(
        program_id=program.id,
        test_id=test.id,
        specimen_id=(specimen_id or '').strip(),
        test_type=(test.code or test_type or 'MPR_REG').strip().upper(),
        sample_date=as_date(sample_date),
        material_ref=safe_material_ref,
        conditioning_required=_conditioning_required_flag(
            get_test_guidance(test.code).get('conditioning_required')
        ),
        nominal_size_in=as_float(nominal_size_in) or program.nominal_size_in,
        confirmed_od_mm=as_float(confirmed_od_mm),
        preparation_rule_basis=(preparation_rule_basis or '').strip(),
        pressure_mpa=(
            as_float(actual_pressure_at_failure_mpa)
            if as_float(actual_pressure_at_failure_mpa) is not None and (test.code or '').upper() in {'MPR_REG', 'CYCLIC_REG'}
            else (as_float(pressure_mpa) or 0.0)
        ),
        temperature_c=as_float(temperature_c) or program.maot_c,
        failure_hours=as_float(failure_hours),
        failure_cycles=as_float(failure_cycles),
        failure_mode=(failure_mode or '').strip(),
        permissible_failure=as_bool(permissible_failure),
        is_runout=as_bool(is_runout),
        include_in_regression=True if include_in_regression is None else as_bool(include_in_regression),
        fitting_type=(fitting_type or 'Field fitting').strip(),
        lab_name=(lab_name or '').strip(),
        witness_name=(witness_name or '').strip(),
        notes=(notes or '').strip(),
        batch_ref=safe_batch_ref,
        source_pipe_ref=safe_source_pipe_ref,
        cut_by=(cut_by or '').strip(),
        total_cut_length_mm=as_float(total_cut_length_mm),
        effective_length_mm=as_float(effective_length_mm),
        end_allowance_each_side_mm=as_float(end_allowance_each_side_mm),
        trimming_margin_mm=as_float(trimming_margin_mm),
        conditioning_complete=as_bool(conditioning_complete),
        pretest_visual_ok=as_bool(pretest_visual_ok),
        released_for_test=as_bool(released_for_test),
        planned_pressure_mpa=as_float(planned_pressure_mpa),
        actual_pressure_at_failure_mpa=as_float(actual_pressure_at_failure_mpa),
        pressure_at_hold_mpa=as_float(pressure_at_hold_mpa),
        failure_time_sec=as_float(failure_time_sec),
        pre_failure_condition=(pre_failure_condition or '').strip(),
        pre_failure_visual=(pre_failure_visual or '').strip(),
        post_failure_visual=(post_failure_visual or '').strip(),
        failure_location=(failure_location or '').strip(),
        failure_description=(failure_description or '').strip(),
        leak_observation=(leak_observation or '').strip(),
        result_status=(result_status or 'PENDING').strip().upper(),
        qa_review_status=(qa_review_status or 'PENDING').strip().upper(),
    )

    session.add(specimen)

    test.status = 'IN_PROGRESS'
    _touch_row(test)
    session.add(test)

    _touch_program(program)
    session.add(program)

    session.commit()

    return RedirectResponse(
        url=f'/rnd/qualifications/{program.id}/tests/{test.id}?tab=specimens&specimen_id={specimen.id}',
        status_code=303,
    )

@router.post('/qualifications/{program_id}/tests/{test_id}/specimens/{specimen_row_id}/update')
def rnd_update_test_specimen(
    program_id: int,
    test_id: int,
    specimen_row_id: int,
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),
    material_ref: str = Form(''),
    planned_pressure_mpa: Optional[float] = Form(None),
    actual_pressure_at_failure_mpa: Optional[float] = Form(None),
    pressure_at_hold_mpa: Optional[float] = Form(None),
    failure_time_sec: Optional[float] = Form(None),
    failure_hours: Optional[float] = Form(None),
    failure_cycles: Optional[float] = Form(None),
    failure_mode: str = Form(''),
    failure_location: str = Form(''),
    failure_description: str = Form(''),
    leak_observation: str = Form(''),
    pre_failure_condition: str = Form(''),
    pre_failure_visual: str = Form(''),
    post_failure_visual: str = Form(''),
    result_status: str = Form('PENDING'),
    qa_review_status: str = Form('PENDING'),
    permissible_failure: Optional[str] = Form(None),
    is_runout: Optional[str] = Form(None),
    include_in_regression: Optional[str] = Form(None),
    notes: str = Form(''),
):
    specimen = session.get(RndQualificationSpecimen, specimen_row_id)
    if not specimen or specimen.program_id != program_id or specimen.test_id != test_id:
        raise HTTPException(404, 'Specimen not found')

    test = session.get(RndQualificationTest, test_id)
    if not test or test.program_id != program_id:
        raise HTTPException(404, 'Test not found')

    test_code = (test.code or '').strip().upper()

    specimen.material_ref = (material_ref or specimen.material_ref or 'FINAL_PRODUCT').strip()

    if specimen.conditioning_required is None:
        specimen.conditioning_required = _conditioning_required_flag(
            get_test_guidance(test.code).get('conditioning_required')
        )

    specimen.result_status = (result_status or 'PENDING').strip().upper()
    specimen.qa_review_status = (qa_review_status or 'PENDING').strip().upper()
    specimen.permissible_failure = bool(permissible_failure)
    specimen.is_runout = bool(is_runout)
    specimen.include_in_regression = bool(include_in_regression)

    # Shared fields kept for all test types
    specimen.planned_pressure_mpa = planned_pressure_mpa
    specimen.pressure_at_hold_mpa = pressure_at_hold_mpa
    specimen.failure_time_sec = failure_time_sec
    specimen.failure_hours = failure_hours
    specimen.failure_cycles = failure_cycles
    specimen.failure_mode = (failure_mode or '').strip()
    specimen.failure_location = (failure_location or '').strip()
    specimen.failure_description = (failure_description or '').strip()
    specimen.leak_observation = (leak_observation or '').strip()
    specimen.pre_failure_condition = (pre_failure_condition or '').strip()
    specimen.pre_failure_visual = (pre_failure_visual or '').strip()
    specimen.post_failure_visual = (post_failure_visual or '').strip()

    # Test-specific handling
    if test_code in {'MPR_REG', 'STATIC_REGRESSION'}:
        specimen.actual_pressure_at_failure_mpa = actual_pressure_at_failure_mpa

        # Keep regression pressure aligned with actual failure pressure
        if actual_pressure_at_failure_mpa is not None:
            specimen.pressure_mpa = actual_pressure_at_failure_mpa

        specimen.notes = (notes or '').strip()

    elif test_code == 'PV_1000H':
        # PV confirmation is about hold pressure + survival duration
        specimen.actual_pressure_at_failure_mpa = actual_pressure_at_failure_mpa
        specimen.pressure_mpa = planned_pressure_mpa or specimen.pressure_mpa

        pv_lines = []
        if pre_failure_condition:
            pv_lines.append(f"Setup: {pre_failure_condition.strip()}")
        if leak_observation:
            pv_lines.append(f"End observation: {leak_observation.strip()}")
        if notes:
            pv_lines.append(f"Notes: {notes.strip()}")
        specimen.notes = "\n".join(pv_lines).strip()

    elif test_code in {'TEMP_CYCLE', 'THERMAL'}:
        specimen.actual_pressure_at_failure_mpa = actual_pressure_at_failure_mpa

        cycle_lines = [
            "TEMP_CYCLE RECORD",
            f"High temp placeholder: {planned_pressure_mpa if planned_pressure_mpa is not None else '-'}",
            f"Low temp placeholder: {pressure_at_hold_mpa if pressure_at_hold_mpa is not None else '-'}",
            f"Cycle count: {failure_cycles if failure_cycles is not None else '-'}",
            f"Leak/proof pressure: {actual_pressure_at_failure_mpa if actual_pressure_at_failure_mpa is not None else '-'}",
            f"Mode: {(failure_mode or '').strip() or '-'}",
            f"Location: {(failure_location or '').strip() or '-'}",
            f"Pre-cycle setup: {(pre_failure_condition or '').strip() or '-'}",
            f"Post-cycle observation: {(leak_observation or '').strip() or '-'}",
        ]
        if notes:
            cycle_lines.append(f"Notes: {notes.strip()}")
        specimen.notes = "\n".join(cycle_lines)

    elif test_code == 'RAPID_DECOMP':
        specimen.actual_pressure_at_failure_mpa = actual_pressure_at_failure_mpa

        decomp_lines = [
            "RAPID_DECOMP RECORD",
            f"Soak pressure: {planned_pressure_mpa if planned_pressure_mpa is not None else '-'}",
            f"Decompression stop pressure: {pressure_at_hold_mpa if pressure_at_hold_mpa is not None else '-'}",
            f"Soak duration hours: {failure_hours if failure_hours is not None else '-'}",
            f"Cycle/rate placeholder: {failure_cycles if failure_cycles is not None else '-'}",
            f"Observation mode: {(failure_mode or '').strip() or '-'}",
            f"Location: {(failure_location or '').strip() or '-'}",
            f"Gas/setup: {(pre_failure_condition or '').strip() or '-'}",
            f"Post-test visual: {(post_failure_visual or '').strip() or '-'}",
            f"Leak/decompression observation: {(leak_observation or '').strip() or '-'}",
            f"Failure description: {(failure_description or '').strip() or '-'}",
        ]
        if notes:
            decomp_lines.append(f"Notes: {notes.strip()}")
        specimen.notes = "\n".join(decomp_lines)

    elif test_code == 'IMPACT':
        specimen.actual_pressure_at_failure_mpa = actual_pressure_at_failure_mpa

        impact_lines = [
            "IMPACT RECORD",
            f"Impact setup: {(pre_failure_condition or '').strip() or '-'}",
            f"Follow-up pressure: {planned_pressure_mpa if planned_pressure_mpa is not None else '-'}",
            f"Result mode: {(failure_mode or '').strip() or '-'}",
            f"Location: {(failure_location or '').strip() or '-'}",
            f"Post-impact visual: {(post_failure_visual or '').strip() or '-'}",
        ]
        if notes:
            impact_lines.append(f"Notes: {notes.strip()}")
        specimen.notes = "\n".join(impact_lines)

    elif test_code in {'AXIAL_LOAD', 'AXIAL'}:
        specimen.actual_pressure_at_failure_mpa = actual_pressure_at_failure_mpa

        axial_lines = [
            "AXIAL LOAD RECORD",
            f"Applied axial load: {(pre_failure_condition or '').strip() or '-'}",
            f"Hold duration sec: {failure_time_sec if failure_time_sec is not None else '-'}",
            f"Follow-up pressure: {planned_pressure_mpa if planned_pressure_mpa is not None else '-'}",
            f"Result mode: {(failure_mode or '').strip() or '-'}",
            f"Location: {(failure_location or '').strip() or '-'}",
            f"Post-load observation: {(leak_observation or '').strip() or '-'}",
        ]
        if notes:
            axial_lines.append(f"Notes: {notes.strip()}")
        specimen.notes = "\n".join(axial_lines)

    else:
        specimen.actual_pressure_at_failure_mpa = actual_pressure_at_failure_mpa
        specimen.notes = (notes or '').strip()

    _touch_row(specimen)
    session.add(specimen)

    all_specimens = session.exec(
        select(RndQualificationSpecimen)
        .where(RndQualificationSpecimen.program_id == program_id)
        .where(RndQualificationSpecimen.test_id == test_id)
        .order_by(RndQualificationSpecimen.id.asc())
    ).all()

    executed = sum(1 for s in all_specimens if (s.result_status or 'PENDING') != 'PENDING')
    accepted = sum(1 for s in all_specimens if (s.qa_review_status or 'PENDING') in {'ACCEPTED', 'APPROVED'})

    test.result_summary = f"{executed} specimen(s) executed, {accepted} QA accepted"
    if all_specimens and accepted == len(all_specimens):
        test.status = 'COMPLETE'
    elif executed:
        test.status = 'IN_PROGRESS'

    _touch_row(test)
    session.add(test)

    program = session.get(RndQualificationProgram, program_id)
    if program:
        _touch_program(program)
        session.add(program)

    session.commit()
    return RedirectResponse(
        url=f'/rnd/qualifications/{program_id}/tests/{test_id}?tab=specimens&specimen_id={specimen.id}',
        status_code=303,
    )
@router.post('/qualifications/{program_id}/tests/{test_id}/attachments/new')
def rnd_add_test_attachment(
    program_id: int,
    test_id: int,
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),
    category: str = Form('REPORT'),
    document_type: str = Form(...),
    title: str = Form(...),
    reference_no: str = Form(''),
    file_note: str = Form(''),
    is_mandatory: Optional[str] = Form(None),
    approval_status: str = Form('PENDING'),
    source_mode: str = Form('UPLOAD'),
    is_signed_copy: Optional[str] = Form(None),
    uploaded_file: UploadFile = File(...),
):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')

    test = session.get(RndQualificationTest, test_id)
    if not test or test.program_id != program_id:
        raise HTTPException(404, 'Test not found')

    saved = _save_rnd_upload(program_id, uploaded_file)

    row = RndAttachmentRegister(
        program_id=program_id,
        test_id=test_id,
        category=(category or 'REPORT').strip().upper(),
        document_type=(document_type or '').strip().upper(),
        title=(title or '').strip(),
        reference_no=(reference_no or '').strip(),
        file_note=(file_note or '').strip(),
        is_mandatory=bool(is_mandatory),
        uploaded_by_name=(getattr(user, 'display_name', '') or getattr(user, 'username', '') or ''),
        approval_status=(approval_status or 'PENDING').strip().upper(),
        original_filename=saved["original_filename"],
        stored_filename=saved["stored_filename"],
        file_path=saved["file_path"],
        content_type=saved["content_type"],
        file_size_bytes=saved["file_size_bytes"],
        source_mode=(source_mode or 'UPLOAD').strip().upper(),
        is_signed_copy=bool(is_signed_copy),
    )
    session.add(row)

    test.status = 'IN_PROGRESS' if (test.status or '').upper() == 'PLANNED' else test.status
    _touch_row(test)
    session.add(test)

    _touch_program(program)
    session.add(program)

    session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}/tests/{test_id}', status_code=303)

@router.get('/qualifications/{program_id}/attachments/{attachment_id}/download')
def rnd_download_attachment(
    program_id: int,
    attachment_id: int,
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),
):
    row = session.get(RndAttachmentRegister, attachment_id)
    if not row or row.program_id != program_id:
        raise HTTPException(404, 'Attachment not found')

    if not row.file_path or not os.path.exists(row.file_path):
        raise HTTPException(404, 'Stored file not found')

    filename = row.original_filename or row.title or 'attachment'
    return FileResponse(
        path=row.file_path,
        media_type=row.content_type or 'application/octet-stream',
        filename=filename,
    )


@router.get('/qualifications/{program_id}/attachments/{attachment_id}/view')
def rnd_view_attachment(
    program_id: int,
    attachment_id: int,
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),
):
    row = session.get(RndAttachmentRegister, attachment_id)
    if not row or row.program_id != program_id:
        raise HTTPException(404, 'Attachment not found')

    if not row.file_path or not os.path.exists(row.file_path):
        raise HTTPException(404, 'Stored file not found')

    media_type = row.content_type or 'application/octet-stream'

    return FileResponse(
        path=row.file_path,
        media_type=media_type,
        filename=row.original_filename or row.title or 'attachment',
    )
