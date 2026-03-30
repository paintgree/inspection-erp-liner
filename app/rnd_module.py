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
            "MPR equals LCL RCT x 0.67.",
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
    return TEST_GUIDANCE.get(code, {
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
    grade_name: str = Field(default="")
    certificate_ref: str = Field(default="")
    batch_ref: str = Field(default="")
    status: str = Field(default="PLANNED", index=True)
    notes: str = Field(default="")


class RndAttachmentRegister(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    program_id: int = Field(index=True)
    test_id: Optional[int] = Field(default=None, index=True)
    specimen_id: Optional[int] = Field(default=None, index=True)

    category: str = Field(default="REPORT", index=True)
    title: str = Field(default="")
    reference_no: str = Field(default="")
    file_note: str = Field(default="")
    document_type: str = Field(default="", index=True)
    is_mandatory: bool = Field(default=False)
    uploaded_by_name: str = Field(default="")
    approval_status: str = Field(default="PENDING", index=True)


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
    if (program.qualification_standard or '').strip().upper().startswith('OTHER'):
        for component, material in [('LINER', program.liner_material), ('REINFORCEMENT', program.reinforcement_material), ('COVER', program.cover_material)]:
            session.add(RndMaterialQualification(program_id=program.id, component=component, material_name=material))
        session.commit()
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
def rnd_create_program(session: Session = Depends(get_session), user: User = Depends(_require_user), title: str = Form(...), program_code: str = Form(...), nominal_size_in: float = Form(...), npr_mpa: float = Form(...), maot_c: float = Form(...), laot_c: float = Form(0.0), pfr_or_pv: str = Form('PFR'), parent_program_id: Optional[int] = Form(None), intended_service: str = Form('Static water service'), qualification_standard: str = Form('API 15S R3'), notes: str = Form('')):
    program = RndQualificationProgram(program_code=(program_code or '').strip().upper(), title=(title or '').strip(), nominal_size_in=nominal_size_in, npr_mpa=npr_mpa, maot_c=maot_c, laot_c=laot_c, pfr_or_pv=(pfr_or_pv or 'PFR').strip().upper(), parent_program_id=parent_program_id, intended_service=intended_service, qualification_standard=(qualification_standard or 'API 15S R3').strip(), notes=notes, created_by_name=(getattr(user, 'display_name', '') or getattr(user, 'username', '') or ''))
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
    phase_cards = _phase_cards(program, tests, materials, specimens, attachments)
    return TEMPLATES.TemplateResponse(request,'rnd_program_view.html', {'request': request, 'user': user, 'program': program, 'tests': tests, 'specimens': specimens, 'materials': materials, 'attachments': attachments, 'static_reg': static_reg, 'cyclic_reg': cyclic_reg, 'counts': counts, 'progress_pct': _status_pct(counts, len(tests)), 'guide': guide, 'phase_cards': phase_cards, 'design_factor_nonmetallic': DESIGN_FACTOR_NONMETALLIC, 'rcrt_hours': RCRT_HOURS})


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
    return TEMPLATES.TemplateResponse(request,'rnd_regression_view.html', {'request': request, 'user': user, 'program': program, 'specimens': specimens, 'static_reg': static_reg, 'cyclic_reg': cyclic_reg, 'pv_formula': pv_formula, 'guide': guide, 'design_factor_nonmetallic': DESIGN_FACTOR_NONMETALLIC, 'rcrt_hours': RCRT_HOURS})


@router.get('/qualifications/{program_id}/tests/{test_id}')
def rnd_test_detail(program_id: int, test_id: int, request: Request, session: Session = Depends(get_session), user: User = Depends(_require_user)):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')

    test = session.get(RndQualificationTest, test_id)
    if not test or test.program_id != program_id:
        raise HTTPException(404, 'Test not found')

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
        }
    )

@router.post('/qualifications/{program_id}/tests/{test_id}/specimens/new')
def rnd_add_test_specimen(
    program_id: int,
    test_id: int,
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),
    specimen_id: str = Form(...),
    sample_date: date = Form(...),
    nominal_size_in: float = Form(0.0),
    confirmed_od_mm: Optional[float] = Form(None),
    preparation_rule_basis: str = Form(''),
    batch_ref: str = Form(''),
    source_pipe_ref: str = Form(''),
    cut_by: str = Form(''),
    total_cut_length_mm: Optional[float] = Form(None),
    effective_length_mm: Optional[float] = Form(None),
    end_allowance_each_side_mm: Optional[float] = Form(None),
    trimming_margin_mm: Optional[float] = Form(None),
    planned_pressure_mpa: Optional[float] = Form(None),
    temperature_c: float = Form(0.0),
    conditioning_complete: Optional[str] = Form(None),
    pretest_visual_ok: Optional[str] = Form(None),
    released_for_test: Optional[str] = Form(None),
    pre_failure_condition: str = Form(''),
    pre_failure_visual: str = Form(''),
    notes: str = Form(''),
):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')

    test = session.get(RndQualificationTest, test_id)
    if not test or test.program_id != program_id:
        raise HTTPException(404, 'Test not found')

    specimen = RndQualificationSpecimen(
        program_id=program_id,
        test_id=test_id,
        specimen_id=(specimen_id or '').strip().upper(),
        test_type=(test.code or '').strip().upper(),
        sample_date=sample_date,
        nominal_size_in=nominal_size_in or program.nominal_size_in,
        confirmed_od_mm=confirmed_od_mm,
        preparation_rule_basis=preparation_rule_basis,
        batch_ref=batch_ref,
        source_pipe_ref=source_pipe_ref,
        cut_by=cut_by,
        total_cut_length_mm=total_cut_length_mm,
        effective_length_mm=effective_length_mm,
        end_allowance_each_side_mm=end_allowance_each_side_mm,
        trimming_margin_mm=trimming_margin_mm,
        planned_pressure_mpa=planned_pressure_mpa,
        temperature_c=temperature_c,
        conditioning_complete=bool(conditioning_complete),
        pretest_visual_ok=bool(pretest_visual_ok),
        released_for_test=bool(released_for_test),
        pre_failure_condition=pre_failure_condition,
        pre_failure_visual=pre_failure_visual,
        notes=notes,
    )
    session.add(specimen)
    _touch_program(program)
    session.add(program)
    session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}/tests/{test_id}', status_code=303)


@router.post('/qualifications/{program_id}/tests/{test_id}/specimens/{specimen_row_id}/update')
def rnd_update_test_specimen(
    program_id: int,
    test_id: int,
    specimen_row_id: int,
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),
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

    specimen.planned_pressure_mpa = planned_pressure_mpa
    specimen.actual_pressure_at_failure_mpa = actual_pressure_at_failure_mpa
    specimen.pressure_at_hold_mpa = pressure_at_hold_mpa
    specimen.failure_time_sec = failure_time_sec
    specimen.failure_hours = failure_hours
    specimen.failure_cycles = failure_cycles
    specimen.failure_mode = failure_mode
    specimen.failure_location = failure_location
    specimen.failure_description = failure_description
    specimen.leak_observation = leak_observation
    specimen.pre_failure_condition = pre_failure_condition
    specimen.pre_failure_visual = pre_failure_visual
    specimen.post_failure_visual = post_failure_visual
    specimen.result_status = (result_status or 'PENDING').strip().upper()
    specimen.qa_review_status = (qa_review_status or 'PENDING').strip().upper()
    specimen.permissible_failure = bool(permissible_failure)
    specimen.is_runout = bool(is_runout)
    specimen.include_in_regression = bool(include_in_regression)
    specimen.notes = notes
    _touch_row(specimen)
    session.add(specimen)

    test = session.get(RndQualificationTest, test_id)
    if test:
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
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}/tests/{test_id}', status_code=303)


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
):
    program = session.get(RndQualificationProgram, program_id)
    if not program:
        raise HTTPException(404, 'Program not found')

    test = session.get(RndQualificationTest, test_id)
    if not test or test.program_id != program_id:
        raise HTTPException(404, 'Test not found')

    row = RndAttachmentRegister(
        program_id=program_id,
        test_id=test_id,
        category=(category or 'REPORT').strip().upper(),
        document_type=(document_type or '').strip().upper(),
        title=title,
        reference_no=reference_no,
        file_note=file_note,
        is_mandatory=bool(is_mandatory),
        uploaded_by_name=(getattr(user, 'display_name', '') or getattr(user, 'username', '') or ''),
        approval_status=(approval_status or 'PENDING').strip().upper(),
    )
    session.add(row)
    _touch_program(program)
    session.add(program)
    session.commit()
    return RedirectResponse(url=f'/rnd/qualifications/{program_id}/tests/{test_id}', status_code=303)
