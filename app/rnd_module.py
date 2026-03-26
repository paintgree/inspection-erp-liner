from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

# ==============================
# DEFAULT (ALL TESTS)
# ==============================
DEFAULT_SPECIMEN_RULE = {
    "min_specimens": "Defined by test requirement",
    "minimum_cut_length": "Use the total cut-length formula below. Do not cut to effective length only.",
    "effective_length": "Active section defined by the test setup or fixture arrangement.",
    "end_allowance_each_side": "Use enough extra length for gripping, sealing, end fittings, and trimming. Baseline internal guide: not less than 1.0 × OD each side unless test-specific rule is stricter.",
    "total_length_formula": "Total cut length = effective test length + left end allowance + right end allowance + trimming margin",
    "marking_requirements": [
        "Assign a unique specimen ID before cutting",
        "Mark pipe size, batch/lot, and intended test code",
        "Mark the active test section centerline where applicable",
        "Keep markings outside the critical failure observation area when possible"
    ],
    "visual_acceptance": [
        "No visible cuts, gouges, cracks, or crushed areas",
        "No obvious ovality or damage from handling",
        "Ends are square and suitable for end preparation",
        "Traceability marking remains readable after preparation"
    ],
    "preconditioning": {
        "required": "depends",
        "when_required": "Apply when the test method or qualification basis requires stabilization at a specific temperature or environment before testing.",
        "medium": "Ambient air unless the test setup or internal method specifies otherwise",
        "target_temperature": "As required by the selected test condition",
        "minimum_process": [
            "Prepare and identify the specimen",
            "Verify dimensions and end preparation before conditioning",
            "Place the specimen in the conditioning environment at the target temperature",
            "Allow the specimen to stabilize before test start",
            "Record start time, target temperature, actual temperature, and operator",
            "Do not begin the test until the specimen is confirmed stabilized"
        ],
        "records_required": [
            "Conditioning start time",
            "Conditioning end time / release time",
            "Target temperature",
            "Observed temperature",
            "Medium/environment",
            "Operator / approver"
        ]
    },
    "technician_tips": [
        "Never cut the specimen to the active test length only; include end allowances and trimming margin",
        "Record both the total cut length and the intended effective test length",
        "If there is any uncertainty in the fixture engagement length, confirm it before cutting",
        "Reject specimens with damage introduced during cutting or handling"
    ],
    "release_checks": [
        "Specimen ID assigned and traceable to batch",
        "Total cut length measured and recorded",
        "Effective test length identified",
        "End preparation completed",
        "No visible damage after preparation",
        "Preconditioning completed where required",
        "Specimen released for test by responsible person"
    ]
}

# ==============================
# TEST-SPECIFIC OVERRIDES
# ==============================
SPECIMEN_PREP_RULES = {
    "burst": {
        "min_specimens": 5,
        "minimum_cut_length": "Cut enough length to provide the required active burst section plus full engagement into both end fittings/seals plus trimming margin.",
        "effective_length": "The active pressurized section between end fittings. This is the section expected to represent the burst location.",
        "end_allowance_each_side": "Not less than 1.0 × OD each side as a baseline for sealing/grip allowance; increase if your fitting system requires more engagement length.",
        "total_length_formula": "Total cut length = active burst section + 2 × end-fitting allowance + trimming margin",
        "preconditioning": {
            "required": False,
            "when_required": "Normally not required for ambient burst screening. Required when the burst test is conducted at an elevated qualification temperature.",
            "medium": "Ambient air unless elevated-temperature method is used",
            "target_temperature": "Ambient or selected elevated test temperature",
            "minimum_process": [
                "Identify and prepare the specimen",
                "Verify burst fixture/end-fitting engagement requirement",
                "If elevated-temperature burst applies, stabilize the specimen at test temperature before pressurization",
                "Record release to test"
            ]
        },
        "technician_tips": [
            "Make sure the burst is intended to occur in the active test section, not at the fitting interface",
            "Do not undercut the specimen; most burst preparation problems come from insufficient end allowance",
            "Confirm the fixture engagement length before cutting the first sample"
        ]
    },
    "regression": {
        "min_specimens": 18,
        "minimum_cut_length": "Cut enough length to maintain a consistent active free length for the regression setup and enough extra length for both end terminations, sealing, and trimming.",
        "effective_length": "The free pressurized section between end terminations used for long-term hydrostatic exposure.",
        "end_allowance_each_side": "Use not less than 1.5 × OD each side as a baseline when long-term end termination and sealing space are needed; increase if the termination system requires more.",
        "total_length_formula": "Total cut length = effective regression section + 2 × termination allowance + trimming margin",
        "preconditioning": {
            "required": True,
            "when_required": "Required before pressurization at the selected regression test temperature.",
            "medium": "Controlled temperature environment matching the regression condition",
            "target_temperature": "Selected regression temperature (for example the qualification temperature basis)",
            "minimum_process": [
                "Prepare and identify the specimen",
                "Verify effective free length and end terminations",
                "Place the specimen in the controlled environment at the target temperature",
                "Allow stabilization before pressurization",
                "Record start time, actual temperature, pressure group, and release for test",
                "Do not start the long-term test until stabilization is confirmed"
            ]
        },
        "technician_tips": [
            "Use consistent preparation across all pressure groups",
            "Record the same geometry fields for every regression specimen",
            "Any leak or preparation issue before official start should be corrected before the specimen enters the dataset"
        ]
    },
    "cyclic": {
        "min_specimens": "Defined by cyclic qualification program",
        "minimum_cut_length": "Cut enough length to achieve the active cycling section and full termination/fitting allowance.",
        "effective_length": "The active section subjected to repeated pressure cycling.",
        "end_allowance_each_side": "Use enough length for termination and sealing; do not compromise the active cycling section.",
        "preconditioning": {
            "required": True,
            "when_required": "Required when cyclic test is run at controlled temperature or conditioned service state.",
            "medium": "Controlled environment as required by the test condition",
            "target_temperature": "Selected cyclic qualification temperature"
        }
    },
    "impact": {
        "min_specimens": "Defined by impact test program",
        "minimum_cut_length": "Cut enough length to provide the required support span / impact location plus trimming margin.",
        "effective_length": "The supported test section containing the impact location.",
        "end_allowance_each_side": "Based on support arrangement rather than pressure sealing needs.",
        "preconditioning": {
            "required": True,
            "when_required": "Required when impact is performed at a controlled test temperature.",
            "medium": "Conditioning environment at test temperature",
            "target_temperature": "Selected impact test temperature"
        }
    },
    "mbr": {
        "min_specimens": "Defined by bending/spooling program",
        "minimum_cut_length": "Use full specimen length required to achieve the bending radius and handling arrangement safely.",
        "effective_length": "The full section involved in bending/spooling exposure.",
        "end_allowance_each_side": "Include enough extra length for gripping, handling, and fixture engagement."
    }
}

def get_specimen_prep(test_code: str):
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
    override = SPECIMEN_PREP_RULES.get(test_code, {})
    for key, value in override.items():
        if isinstance(value, dict) and key in base and isinstance(base[key], dict):
            merged = dict(base[key])
            merged.update(value)
            base[key] = merged
        else:
            base[key] = value
    return base

@router.get("/rnd/tests/{test_code}", response_class=HTMLResponse)
async def test_detail(request: Request, test_code: str):
    prep = get_specimen_prep(test_code)
    return templates.TemplateResponse(
        request=request,
        name="rnd_test_detail.html",
        context={
            "request": request,
            "test_code": test_code,
            "prep": prep
        }
    )
