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
    "effective_length": "Active section defined by test setup",
    "end_allowance_each_side": "≥ 1–1.5 × OD (grip / sealing)",
    "total_length_formula": "Effective length + 2 × allowance + margin",
    "conditioning": {
        "required": "depends",
        "notes": "Check temperature / fluid / aging requirement"
    },
    "release_checks": [
        "Specimen ID assigned",
        "Pipe size verified",
        "Total length recorded",
        "Ends prepared",
        "No visible damage",
        "Traceability confirmed"
    ]
}

# ==============================
# TEST-SPECIFIC OVERRIDES
# ==============================
SPECIMEN_PREP_RULES = {
    "burst": {
        "min_specimens": 5,
        "effective_length": "Between end fittings",
        "end_allowance_each_side": "≥ 1 × OD",
        "conditioning": {
            "required": False,
            "notes": "Only if elevated temperature testing"
        }
    },
    "regression": {
        "min_specimens": 18,
        "effective_length": "Free pressurized section",
        "end_allowance_each_side": "≥ 1.5 × OD",
        "conditioning": {
            "required": True,
            "notes": "Condition at test temperature"
        }
    },
    "cyclic": {
        "effective_length": "Active cycling section"
    },
    "impact": {
        "conditioning": {
            "required": True,
            "notes": "Condition before test"
        }
    }
}

def get_specimen_prep(test_code):
    base = DEFAULT_SPECIMEN_RULE.copy()
    override = SPECIMEN_PREP_RULES.get(test_code, {})

    for key, value in override.items():
        if isinstance(value, dict) and key in base:
            base[key].update(value)
        else:
            base[key] = value

    return base

# ==============================
# TEST DETAIL ROUTE
# ==============================
@router.get("/rnd/tests/{test_code}", response_class=HTMLResponse)
async def test_detail(request: Request, test_code: str):
    prep = get_specimen_prep(test_code)

    return templates.TemplateResponse(
        "rnd_test_detail.html",
        {
            "request": request,
            "test_code": test_code,
            "prep": prep
        }
    )
