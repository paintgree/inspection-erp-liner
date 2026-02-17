from __future__ import annotations

import os
import mimetypes
import traceback

from datetime import datetime, date, time as dtime, timedelta
from io import BytesIO
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from reportlab.lib.units import inch
from sqlalchemy import func


from typing import Dict, List, Optional, Tuple
from pathlib import Path
import subprocess
import tempfile
import json
import os
from datetime import datetime

import openpyxl
from openpyxl.drawing.image import Image as XLImage
from pypdf import PdfWriter, PdfReader, Transformation


from fastapi import (
    FastAPI,
    Depends,
    Request,
    Form,
    HTTPException,
    UploadFile,
    File,
)
from fastapi.responses import (
    HTMLResponse,
    RedirectResponse,
    StreamingResponse,
    Response,
    FileResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from sqlmodel import Session, select




from .auth import hash_password, verify_password
from .db import create_db_and_tables, get_session
from .models import (
    User,
    ProductionRun,
    RunMachine,
    RunParameter,
    InspectionEntry,
    InspectionValue,
    InspectionValueAudit,
    MaterialLot,
    MaterialUseEvent,
    MrrDocument,
    MrrReceiving,
    MrrReceivingInspection,  # ✅ required because your code uses this name
    MrrInspection,           # ✅ now works (alias in models.py)
    MrrInspectionPhoto,      # ✅ NEW
)
SLOTS = [
    "00:00", "02:00", "04:00", "06:00",
    "08:00", "10:00", "12:00", "14:00",
    "16:00", "18:00", "20:00", "22:00"
]

# =========================
# MRR helpers (units + report no)
# =========================

UNIT_MULTIPLIER = {
    "KG": 1.0,
    "T": 1000.0,   # 1 Ton = 1000 KG
}
def safe_json_loads(val):
    if not val:
        return {}
    if isinstance(val, dict):
        return val
    try:
        return json.loads(val)
    except Exception:
        return {}

def normalize_qty_to_kg(qty: float, unit: str) -> float:
    u = (unit or "KG").upper().strip()
    if u in ["T", "TON", "TONNE"]:
        return float(qty) * 1000.0
    if u in ["KG", "KGS"]:
        return float(qty)
    # For PCS we cannot convert to KG; keep as-is (handled by units_compatible)
    return float(qty)

def dn_doc_exists(session: Session, lot_id: int, dn_number: str) -> bool:
    dn_number = (dn_number or "").strip()
    if not dn_number:
        return False
    doc = session.exec(
        select(MrrDocument).where(
            (MrrDocument.ticket_id == lot_id) &
            (MrrDocument.doc_type == "DELIVERY_NOTE") &
            (MrrDocument.doc_number == dn_number)
        )
    ).first()
    return bool(doc)
    
def quality_doc_exists(session: Session, lot_id: int) -> bool:
    """
    True if at least ONE quality doc exists:
    COA OR MTC OR INSPECTION_REPORT
    """
    q = select(MrrDocument).where(
        (MrrDocument.ticket_id == lot_id) &
        (MrrDocument.doc_type.in_(["COA", "MTC", "INSPECTION_REPORT"]))
    )
    return session.exec(q).first() is not None


def get_submitted_shipment_by_dn(session: Session, lot_id: int, dn_number: str):
    dn_number = (dn_number or "").strip()
    if not dn_number:
        return None
    # DN is "consumed" ONLY if shipment was submitted (inspector_confirmed True) or approved
    return session.exec(
        select(MrrReceivingInspection).where(
            (MrrReceivingInspection.ticket_id == lot_id) &
            (MrrReceivingInspection.delivery_note_no == dn_number) &
            (
                (MrrReceivingInspection.inspector_confirmed == True) |
                (MrrReceivingInspection.manager_approved == True)
            )
        )
    ).first()

def get_draft_shipment_by_dn(session: Session, lot_id: int, dn_number: str):
    dn_number = (dn_number or "").strip()
    if not dn_number:
        return None
    # Draft = created but NOT submitted yet
    return session.exec(
        select(MrrReceivingInspection).where(
            (MrrReceivingInspection.ticket_id == lot_id) &
            (MrrReceivingInspection.delivery_note_no == dn_number) &
            (MrrReceivingInspection.inspector_confirmed == False) &
            (MrrReceivingInspection.manager_approved == False)
        )
    ).first()

def get_latest_draft_shipment(session: Session, lot_id: int):
    return session.exec(
        select(MrrReceivingInspection).where(
            (MrrReceivingInspection.ticket_id == lot_id) &
            (MrrReceivingInspection.inspector_confirmed == False) &
            (MrrReceivingInspection.manager_approved == False)
        ).order_by(MrrReceivingInspection.created_at.desc())
    ).first()


def units_compatible(po_unit: str, arrived_unit: str) -> bool:
    pu = (po_unit or "").upper().strip()
    au = (arrived_unit or "").upper().strip()
    # KG and T compatible, PCS only with PCS
    if pu == "PCS" or au == "PCS":
        return pu == au
    return True

def generate_report_no(ticket_id: int, seq: int) -> str:
    # MRR-YYYY-MM-<ticket>-<shipment>
    now = datetime.utcnow()
    return f"MRR-{now.year}-{now.month:02d}-{ticket_id:04d}-{seq:02d}"

# =========================
# MRR REPORT TEMPLATES PATHS
# =========================
import os
import io
import re
import json
import zipfile
import subprocess
from datetime import datetime, date

import openpyxl
from fastapi import HTTPException
from fastapi.responses import Response

import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MRR_TEMPLATE_DIR = os.path.join(BASE_DIR, "templates", "templates_xlsx")

MRR_TEMPLATE_XLSX_MAP = {
    "RAW": os.path.join(MRR_TEMPLATE_DIR, "QAP0600-F01.xlsx"),
    # later we can add other xlsx templates if needed
}

MRR_TEMPLATE_DOCX_MAP = {
    "OUTSOURCED": os.path.join(MRR_TEMPLATE_DIR, "QAP0600-F02.docx"),
}


def _safe_upper(x: str | None) -> str:
    return (x or "").strip().upper()

def _to_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default

def _as_date_str(d) -> str:
    if isinstance(d, (datetime, date)):
        return d.strftime("%Y-%m-%d")
    if isinstance(d, str) and d.strip():
        return d.strip()
    return ""

def _normalize_key(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()

def _xlsx_bytes_from_wb(wb) -> bytes:
    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()

from openpyxl.utils.cell import coordinate_to_tuple

def _ws_set_value_safe(ws, addr: str, value):
    """
    Safely set a value even if the address points inside a merged cell.
    Writes to the top-left cell of the merged range.
    """
    r, c = coordinate_to_tuple(addr)

    # If addr is inside a merged cell, redirect to the merged range start
    for mrange in ws.merged_cells.ranges:
        if mrange.min_row <= r <= mrange.max_row and mrange.min_col <= c <= mrange.max_col:
            ws.cell(mrange.min_row, mrange.min_col).value = value
            return

    ws[addr].value = value


def fill_mrr_f01_xlsx_bytes(
    *,
    lot,
    receiving,
    inspection,
    docs: list,
    photos_by_group: dict | None = None,
) -> bytes:
    """
    Fills QAP0600-F01.xlsx using:
    - ticket (lot)
    - receiving (docs info / PO checks)
    - inspection (shipment inspection values + inspection_json tables)
    - docs (uploaded docs list)
    """
    template_path = MRR_TEMPLATE_XLSX_MAP.get("RAW")
    if not template_path or not os.path.exists(template_path):
        raise HTTPException(
            500,
            f"RAW template missing. Put QAP0600-F01.xlsx in {MRR_TEMPLATE_DIR}",
        )

    wb = openpyxl.load_workbook(template_path)
    ws = wb.active

    # ---- HEADER MAP (F01) ----
    _ws_set_value_safe(ws, "E1", inspection.report_no or "")
    _ws_set_value_safe(ws, "E2", _as_date_str(getattr(inspection, "created_at", None) or datetime.utcnow()))

    _ws_set_value_safe(ws, "B6", getattr(lot, "supplier_name", "") or "")
    _ws_set_value_safe(ws, "F6", getattr(lot, "po_number", "") or "")

    _ws_set_value_safe(ws, "B7", getattr(lot, "material_name", "") or "")
    _ws_set_value_safe(ws, "B9", getattr(lot, "batch_no", "") or "")

    data = {}
    try:
        data = json.loads(getattr(inspection, "inspection_json", None) or "{}")
    except Exception:
        data = {}

    grade = (data.get("material_grade") or data.get("grade") or "").strip()
    fam = (data.get("material_fam") or data.get("material_type") or "").strip()
    mat_name = (getattr(lot, "material_name", "") or "").strip()

    _ws_set_value_safe(ws, "B8", grade)
    _ws_set_value_safe(ws, "F8", getattr(inspection, "delivery_note_no", "") or "")

    # --- COMMENTS / REMARKS into the report ---
    # IMPORTANT: Change "B49" to the real cell address of your "Comments:" box in QAP0600-F01.xlsx
    _ws_set_value_safe(ws, "A46", (data.get("remarks") or "").strip())
    

    # ---- Dynamic Template Titles ----
    # Show only what user selected, remove hardcoded template names
    title = ""
    if fam and grade:
        title = f"{fam} ({grade})"
    elif fam:
        title = fam
    elif mat_name:
        title = mat_name

    # These cells in your template contain the hardcoded material headers
    _ws_set_value_safe(ws, "A11", title)
    _ws_set_value_safe(ws, "E11", "")
    _ws_set_value_safe(ws, "A24", "")
    _ws_set_value_safe(ws, "D24", "")

    bn = data.get("batch_numbers")
    if isinstance(bn, list):
        _ws_set_value_safe(ws, "B9", ", ".join([str(x).strip() for x in bn if str(x).strip()]))
    elif isinstance(bn, str):
        _ws_set_value_safe(ws, "B9", bn.strip())
    else:
        _ws_set_value_safe(ws, "B9", "")

    qty_arrived = getattr(inspection, "qty_arrived", None)
    qty_unit = getattr(inspection, "qty_unit", None) or ""
    _ws_set_value_safe(ws, "F9", f"{_to_float(qty_arrived, 0)} {qty_unit}".strip())

    # ---- PROPERTIES TABLE FILL (generic matcher) ----
    def _build_prop_map(items):
        m = {}
        if not isinstance(items, list):
            return m
        for it in items:
            if not isinstance(it, dict):
                continue
            name = _normalize_key(str(it.get("name") or it.get("property") or ""))
            if not name:
                continue
            m[name] = it
        return m

    prop_items = (
        data.get("properties")
        or data.get("pe_properties")
        or data.get("raw_properties")
        or []
    )
    
    # If old-style form keys exist (pe_* / fb_*), convert them to the "properties" list
    if not prop_items:
        converted = []
    
        # Must match the "Property" text that appears in your Excel column A
        pe_rows = [
            ("density", "Density"),
            ("mfr", "Melt Flow Rate (MFR) -190°C / 5kg"),
            ("flexural", "Flexural Modulus"),
            ("tensile", "Tensile Strength at Yield"),
            ("elong", "Elongation at Break"),
            ("escr", "ESCR (Environmental Stress Crack Resistance)"),
            ("oits", "Oxidation Induction Time (OIT)"),
            ("cb", "Carbon Black Content"),
            ("cbd", "Carbon Black Dispersion"),
            ("mvd", "Volatile Matter"),
            ("ash", "Ash Content"),
            ("moist", "Moisture"),
        ]
    
        fb_rows = [
            ("denier", "Linear Density"),          # Excel says Linear Density
            ("tenacity", "Tenacity"),              # matches
            ("elong", "Elongation at Break"),      # Excel says Elongation at Break
            ("melt", "Melting Point"),             # Excel says Melting Point (needs a form field)
            ("break", "Breaking Strength"),        # Excel says Breaking Strength (needs a form field)
        ]

    
        for k, label in pe_rows:
            r = (data.get(f"pe_{k}_result") or "").strip()
            rm = (data.get(f"pe_{k}_remarks") or "").strip()
            if r or rm:
                converted.append({"name": label, "result": r, "remarks": rm})
    
        for k, label in fb_rows:
            r = (data.get(f"fb_{k}_result") or "").strip()
            rm = (data.get(f"fb_{k}_remarks") or "").strip()
            if r or rm:
                converted.append({"name": label, "result": r, "remarks": rm})
    
        prop_items = converted
    
    prop_map = _build_prop_map(prop_items)


        # ---- SAFE WRITE into merged cells (fix: 'MergedCell' value is read-only) ----
    def _write_cell_safe(sheet, row, col, value):
        cell = sheet.cell(row, col)

        # If it's part of a merged range, write into the top-left of that merged range
        if isinstance(cell, openpyxl.cell.cell.MergedCell):
            for mr in sheet.merged_cells.ranges:
                if mr.min_row <= row <= mr.max_row and mr.min_col <= col <= mr.max_col:
                    sheet.cell(mr.min_row, mr.min_col).value = value
                    return
            return  # merged but no range found (rare)

        # normal cell
        cell.value = value

    # Decide which section to fill (prevents Fiber values appearing in PE table)
    fam_ui = (data.get("material_family") or data.get("material_fam") or data.get("material_type") or "").strip().upper()

    # These row ranges must match your Excel template (based on your screenshot):
    # PE table rows are around 12-23
    # Fiber table rows are around 26-30
    if fam_ui == "FIBER":
        allowed_row_min, allowed_row_max = 26, 30
    elif fam_ui == "PE":
        allowed_row_min, allowed_row_max = 12, 23
    else:
        # fallback: allow all (old behavior)
        allowed_row_min, allowed_row_max = 1, ws.max_row

    # Fill by matching property names in column A
    for r in range(1, ws.max_row + 1):
        if not (allowed_row_min <= r <= allowed_row_max):
            continue

        cell_val = ws.cell(r, 1).value  # column A: property names
        if not isinstance(cell_val, str):
            continue

        key = _normalize_key(cell_val)
        if key in prop_map:
            it = prop_map[key]

            # Your Excel screenshot shows:
            # H = PDS/COA Results, I = Remarks
            _write_cell_safe(ws, r, 8, it.get("result") or it.get("value") or "")  # column H
            _write_cell_safe(ws, r, 9, it.get("remarks") or "")                   # column I




    # ---- VISUAL CHECKS (optional) ----
    vc = data.get("visual_checks")
    if isinstance(vc, dict):
        for r in range(34, 38):
            label = ws.cell(r, 1).value
            if isinstance(label, str) and label in vc:
                v = vc.get(label)
                if isinstance(v, bool):
                    ws.cell(r, 7).value = "YES" if v else "NO"


    status = (data.get("approval_status") or "").strip().upper()

    # IMPORTANT: replace these cell addresses with the real ones in your Excel template
    # Put a "✓" in the right option cell
    _ws_set_value_safe(ws, "A44", "✓" if status == "VERIFIED" else "")
    _ws_set_value_safe(ws, "D44", "✓" if status == "HOLD" else "")
    _ws_set_value_safe(ws, "G44", "✓" if status == "NONCONFORM" else "")
    
    _ws_set_value_safe(ws, "A46", (data.get("on_hold_reason") or "").strip())

    # ---- SIGNATURES ----
    _ws_set_value_safe(ws, "B51", getattr(inspection, "inspector_name", "") or "")
    _ws_set_value_safe(ws, "B52", _as_date_str(datetime.utcnow()))

    if bool(getattr(inspection, "manager_approved", False)):
        _ws_set_value_safe(ws, "D51", "MANAGER")
        _ws_set_value_safe(ws, "D52", _as_date_str(datetime.utcnow()))

    # -------------------------
    # LOGO
    # -------------------------
    # Do NOT insert logo into worksheet cells.
    # We stamp the logo into the PDF after conversion (true header behavior).


    # -------------------------
    # PDF EXPORT PAGE SETUP
    # -------------------------
    # IMPORTANT: make print area end at last real content row, otherwise it shrinks everything.
    try:
        ws.page_setup.scale = None
        ws.page_setup.orientation = ws.ORIENTATION_PORTRAIT
        ws.page_setup.paperSize = ws.PAPERSIZE_A4
    
        # Fit to 1 page
        ws.page_setup.fitToWidth = 1
        ws.page_setup.fitToHeight = 1
        ws.sheet_properties.pageSetUpPr.fitToPage = True
    
        # ---- AUTO PRINT AREA (A1 to L<last_used_row>) ----
        # Scan for last row that has any value in columns A..L
        last_used_row = 1
        for r in range(ws.max_row, 0, -1):
            row_has_data = False
            for c in range(1, 13):  # A..L
                v = ws.cell(r, c).value
                if v is not None and str(v).strip() != "":
                    row_has_data = True
                    break
            if row_has_data:
                last_used_row = r
                break
    
        # Add a small safety pad (in case borders/text are near the end)
        last_used_row = min(last_used_row + 2, ws.max_row)
    
        ws.print_area = f"A1:L{last_used_row}"
    
        # Remove manual page breaks embedded in template
        try:
            ws.row_breaks.brk = []
            ws.col_breaks.brk = []
        except Exception:
            pass
    
        # Margins (small = bigger content)
        ws.page_margins.left = 0.10
        ws.page_margins.right = 0.10
        ws.page_margins.top = 0.10
        ws.page_margins.bottom = 0.15
    except Exception:
        pass


    return _xlsx_bytes_from_wb(wb)


from io import BytesIO
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from pypdf import PdfReader, PdfWriter, Transformation


def fit_pdf_pages_to_a4(
    pdf_bytes: bytes,
    margin_left_right: float = 3.0,
    margin_bottom: float = 3.0,
    header_reserved: float = 78.0,
    zoom: float = 1.18,        # <-- makes content bigger (try 1.15 to 1.25)
    shift_x: float =  40.0,    # <-- move content LEFT  (fixes big right gap)
    shift_y: float = -80.0,      # <-- move content UP/DOWN (small tweak)
) -> bytes:
    """
    Force pages onto A4, reserve header space, then zoom and shift
    to remove big empty gaps from LibreOffice output.
    """
    reader = PdfReader(BytesIO(pdf_bytes))
    writer = PdfWriter()

    a4_w, a4_h = A4

    usable_w = a4_w - 2 * margin_left_right
    usable_h = a4_h - margin_bottom - header_reserved

    for page in reader.pages:
        src_w = float(page.mediabox.width)
        src_h = float(page.mediabox.height)

        # Base scale to fit inside the usable area
        base_scale = min(usable_w / src_w, usable_h / src_h)

        # Apply extra zoom to fill page more
        scale = base_scale * zoom

        new_page = writer.add_blank_page(width=a4_w, height=a4_h)

        content_w = src_w * scale
        content_h = src_h * scale

        # Center inside usable area, then apply shift
        tx = (a4_w - content_w) / 2.0 + shift_x
        ty = margin_bottom + (usable_h - content_h) / 2.0 + shift_y

        new_page.merge_transformed_page(
            page,
            Transformation().scale(scale).translate(tx, ty)
        )

    out = BytesIO()
    writer.write(out)
    out.seek(0)
    return out.read()


def make_logo_stamp_pdf(page_w: float, page_h: float, logo_path: str) -> bytes:
    """
    Create a transparent 1-page PDF with a centered logo at top.
    """
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=(page_w, page_h))

    img = ImageReader(logo_path)
    iw, ih = img.getSize()

    # Logo size: ~24% of page width (adjust if you want bigger/smaller)
    target_w = page_w * 0.32
    scale = target_w / float(iw)
    target_h = float(ih) * scale

    top_margin = 16
    x = (page_w - target_w) / 2.0
    y = page_h - top_margin - target_h

    c.drawImage(img, x, y, width=target_w, height=target_h, mask="auto")
    c.showPage()
    c.save()

    buf.seek(0)
    return buf.getvalue()


def stamp_logo_on_pdf(pdf_bytes: bytes, logo_path: str) -> bytes:
    """
    Overlay the logo stamp onto every page (top-center).
    """
    if not logo_path or not os.path.exists(logo_path):
        return pdf_bytes

    reader = PdfReader(BytesIO(pdf_bytes))
    writer = PdfWriter()

    for page in reader.pages:
        w = float(page.mediabox.width)
        h = float(page.mediabox.height)

        stamp_pdf = make_logo_stamp_pdf(w, h, logo_path)
        stamp_reader = PdfReader(BytesIO(stamp_pdf))

        page.merge_page(stamp_reader.pages[0])
        writer.add_page(page)

    out = BytesIO()
    writer.write(out)
    out.seek(0)
    return out.getvalue()





app = FastAPI()

BASE_DIR = os.path.dirname(__file__)

# =========================
# Upload storage (local FS)
# =========================
DATA_DIR = os.environ.get("DATA_DIR", "/tmp/inspection_erp_data")
MRR_UPLOAD_DIR = os.path.join(DATA_DIR, "mrr_uploads")
MRR_PHOTO_DIR = os.path.join(DATA_DIR, "mrr_photos")

os.makedirs(MRR_UPLOAD_DIR, exist_ok=True)
os.makedirs(MRR_PHOTO_DIR, exist_ok=True)



templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
# =========================
# File upload directories
# =========================
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")



IMAGE_MAP = {
    "LINER": "/static/images/liner.png",
    "REINFORCEMENT": "/static/images/reinforcement.png",
    "COVER": "/static/images/cover.png",
}

PAPER_BG_MAP = {
    "LINER": os.path.join(BASE_DIR, "static", "papers", "liner_bg.pdf"),
    "REINFORCEMENT": os.path.join(BASE_DIR, "static", "papers", "reinforcement_bg.pdf"),
    "COVER": os.path.join(BASE_DIR, "static", "papers", "cover_bg.pdf"),
}

TEMPLATE_XLSX_MAP = {
    "LINER": os.path.join(BASE_DIR, "templates", "templates_xlsx", "liner.xlsx"),
    "REINFORCEMENT": os.path.join(BASE_DIR, "templates", "templates_xlsx", "reinforcement.xlsx"),
    "COVER": os.path.join(BASE_DIR, "templates", "templates_xlsx", "cover.xlsx"),
}
def resolve_mrr_doc_path(p: str) -> str:
    """Resolve stored MRR document path to an existing file on disk.

    Backward compatible with rows that stored absolute paths, relative paths, or just filenames.
    """
    if not p:
        return ""

    p_norm = p.replace("\\", "/").lstrip("/")

    # absolute
    try:
        if os.path.isabs(p) and os.path.exists(p):
            return p
    except Exception:
        pass

    candidates = [
        p_norm,
        os.path.join(BASE_DIR, p_norm) if 'BASE_DIR' in globals() else p_norm,
        os.path.join(DATA_DIR, p_norm) if 'DATA_DIR' in globals() else p_norm,
        os.path.join(MRR_UPLOAD_DIR, p_norm),
        os.path.join(MRR_UPLOAD_DIR, os.path.basename(p_norm)),
    ]

    for c in candidates:
        if c and os.path.exists(c):
            return c

    return ""

# ==========================================
# EXPORT PER-INSPECTION (SEPARATE REPORTS)
# ==========================================
@app.get("/mrr/{lot_id}/inspection/id/{inspection_id}/export/xlsx")
def mrr_export_inspection_xlsx(
    lot_id: int,
    inspection_id: int,
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    insp = session.get(MrrReceivingInspection, inspection_id)
    if not insp or insp.ticket_id != lot_id:
        raise HTTPException(404, "MRR Inspection not found")

    # Docs are ticket-level in your current DB
    docs = session.exec(
        select(MrrDocument)
        .where(MrrDocument.ticket_id == lot_id)
        .order_by(MrrDocument.created_at.asc())
    ).all()

    # Draft inspection (not submitted yet) - allow inspector to resume (currently unused)
    draft_inspection = session.exec(
        select(MrrReceivingInspection)
        .where(
            (MrrReceivingInspection.ticket_id == lot_id)
            & (MrrReceivingInspection.inspector_confirmed == False)
        )
        .order_by(MrrReceivingInspection.created_at.desc())
    ).first()

    receiving = session.exec(
        select(MrrReceiving).where(MrrReceiving.ticket_id == lot_id)
    ).first()

    # Only allow export if submitted (optional rule)
    if not insp.inspector_confirmed:
        raise HTTPException(400, "Inspection must be submitted before export")

    # Decide template based on inspection.template_type
    tpl = _safe_upper(getattr(insp, "template_type", "RAW"))
    if tpl != "RAW":
        raise HTTPException(
            400, f"Template type {tpl} export not wired yet (we will do F02 next)"
        )

    xlsx_bytes = fill_mrr_f01_xlsx_bytes(
        lot=lot,
        receiving=receiving,
        inspection=insp,
        docs=docs,
        photos_by_group=None,
    )

    filename = f"{insp.report_no or f'MRR-{lot_id}-{inspection_id}'}.xlsx"
    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _try_convert_xlsx_to_pdf_bytes(xlsx_bytes: bytes) -> bytes:
    """
    Best-effort conversion using LibreOffice.
    If LO is not installed in your environment, we raise a clear error.
    """
    tmp_dir = "/tmp/mrr_export"
    os.makedirs(tmp_dir, exist_ok=True)

    xlsx_path = os.path.join(tmp_dir, "report.xlsx")
    with open(xlsx_path, "wb") as f:
        f.write(xlsx_bytes)

    # Try LibreOffice headless conversion
    cmd = [
        "soffice",
        "--headless",
        "--nologo",
        "--nolockcheck",
        "--nodefault",
        "--norestore",
        "--convert-to",
        "pdf",
        "--outdir",
        tmp_dir,
        xlsx_path,
    ]
    try:
        subprocess.check_call(cmd)
    except FileNotFoundError:
        raise HTTPException(
            500,
            "PDF export needs LibreOffice (soffice) installed on the server. "
            "For now use Export XLSX, or tell me and I’ll add a pure-ReportLab PDF layout."
        )
    except Exception:
        raise HTTPException(500, "Failed to convert XLSX to PDF (LibreOffice error).")

    pdf_path = os.path.join(tmp_dir, "report.pdf")
    if not os.path.exists(pdf_path):
        raise HTTPException(500, "Conversion did not produce PDF output.")

    with open(pdf_path, "rb") as f:
        pdf = f.read()
    
    # 1) Scale to A4 (makes it bigger / readable)
    pdf = fit_pdf_pages_to_a4(pdf, margin_left_right=0.3, margin_bottom=0.3, header_reserved=55.0)



    
    # 2) Stamp logo in header area (top-center)
    base_dir = os.path.dirname(__file__)
    logo_path = os.path.join(base_dir, "static", "images", "logo.png")
    pdf = stamp_logo_on_pdf(pdf, logo_path)
    
    return pdf




@app.get("/mrr/{lot_id}/inspection/id/{inspection_id}/export/pdf")
def mrr_export_inspection_pdf(
    lot_id: int,
    inspection_id: int,
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    insp = session.get(MrrReceivingInspection, inspection_id)
    if not insp or insp.ticket_id != lot_id:
        raise HTTPException(404, "MRR Inspection not found")

    docs = session.exec(select(MrrDocument).where(MrrDocument.ticket_id == lot_id).order_by(MrrDocument.created_at.asc())).all()
    receiving = session.exec(select(MrrReceiving).where(MrrReceiving.ticket_id == lot_id)).first()

    if not insp.inspector_confirmed:
        raise HTTPException(400, "Inspection must be submitted before export")

    tpl = _safe_upper(getattr(insp, "template_type", "RAW"))
    if tpl != "RAW":
        raise HTTPException(400, f"Template type {tpl} export not wired yet (we will do F02 next)")

    xlsx_bytes = fill_mrr_f01_xlsx_bytes(
        lot=lot,
        receiving=receiving,
        inspection=insp,
        docs=docs,
        photos_by_group=None,
    )

    pdf_bytes = _try_convert_xlsx_to_pdf_bytes(xlsx_bytes)

    filename = f"{insp.report_no or f'MRR-{lot_id}-{inspection_id}'}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

@app.get("/health")
def health():
    return {"ok": True}


@app.on_event("startup")
def on_startup():
    create_db_and_tables()
    ensure_default_users()


def ensure_default_users():
    from .db import engine
    with Session(engine) as session:
        u = session.exec(select(User).where(User.username == "manager")).first()
        if not u:
            session.add(User(
                username="manager",
                display_name="Manager",
                role="MANAGER",
                password_hash=hash_password("manager123"),
            ))

        i = session.exec(select(User).where(User.username == "inspector")).first()
        if not i:
            session.add(User(
                username="inspector",
                display_name="Inspector",
                role="INSPECTOR",
                password_hash=hash_password("inspector"),
            ))

        b = session.exec(select(User).where(User.username == "boss")).first()
        if not b:
            session.add(User(
                username="boss",
                display_name="Boss",
                role="BOSS",
                password_hash=hash_password("boss123"),
            ))

        session.commit()


OMAN_TZ = ZoneInfo("Asia/Muscat")

def format_oman_dt(dt_utc: datetime | None) -> str:
    if not dt_utc:
        return ""
    # dt_utc is stored as naive UTC in DB
    dt_local = dt_utc.replace(tzinfo=ZoneInfo("UTC")).astimezone(OMAN_TZ)
    return dt_local.strftime("%Y-%m-%d %H:%M")

def make_approval_stamp_pdf(page_w: float, page_h: float, text_lines: list[str]) -> bytes:
    """
    Create a 1-page transparent PDF with approval text in bottom-right corner.
    """
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=(page_w, page_h))

    # position (bottom-right)
    x = page_w - 40
    y = 60

    c.setFont("Helvetica-Bold", 10)
    for line in text_lines:
        c.drawRightString(x, y, line)
        y -= 12

    c.showPage()
    c.save()
    buf.seek(0)
    return buf.getvalue()

from io import BytesIO
from datetime import datetime
try:
    from pypdf import PdfReader, PdfWriter
except Exception:
    from PyPDF2 import PdfReader, PdfWriter


def stamp_approval_on_pdf(
    pdf_bytes: bytes,
    approved_by: str,
    approved_at_utc: datetime | None
) -> bytes:
    """
    Pure helper: stamps APPROVED info on every page.
    Does NOT query DB, does NOT need session.
    """
    if not approved_by or not approved_at_utc:
        return pdf_bytes

    approved_local = format_oman_dt(approved_at_utc)

    reader = PdfReader(BytesIO(pdf_bytes))
    writer = PdfWriter()

    for page in reader.pages:
        w = float(page.mediabox.width)
        h = float(page.mediabox.height)

        stamp_pdf = make_approval_stamp_pdf(
            w, h,
            [
                "APPROVED",
                f"By: {approved_by}",
                f"At: {approved_local} (Oman)",
            ],
        )

        stamp_reader = PdfReader(BytesIO(stamp_pdf))
        page.merge_page(stamp_reader.pages[0])
        writer.add_page(page)

    out = BytesIO()
    writer.write(out)
    out.seek(0)
    return out.getvalue()


def get_current_user(request: Request, session: Session) -> User:
    username = request.cookies.get("user")
    if not username:
        raise HTTPException(401, "Not logged in")
    user = session.exec(select(User).where(User.username == username)).first()
    if not user:
        raise HTTPException(401, "Invalid user")
    return user

def require_manager(user: User):
    if user.role != "MANAGER":
        raise HTTPException(403, "Manager only")

def forbid_boss(user: User):
    if user.role == "BOSS":
        raise HTTPException(403, "Read-only user")


@app.post("/users/{username}/update")
def users_update(
    username: str,
    request: Request,
    session: Session = Depends(get_session),
    display_name: str = Form(""),
    role: str = Form(""),
    password: str = Form(""),
):
    user = get_current_user(request, session)
    require_manager(user)

    target = session.exec(select(User).where(User.username == username)).first()
    if not target:
        raise HTTPException(404, "User not found")

    if display_name.strip():
        target.display_name = display_name.strip()

    if role.strip():
        r = role.strip().upper()
        if r not in ["INSPECTOR", "MANAGER", "BOSS", "RUN_CREATOR"]:
            raise HTTPException(400, "Invalid role")
        target.role = r

    if password.strip():
        if len(password.strip()) < 4:
            raise HTTPException(400, "Password too short")
        target.password_hash = hash_password(password.strip())

    session.add(target)
    session.commit()
    return RedirectResponse("/users", status_code=302)


@app.post("/users/{username}/delete")
def users_delete(
    username: str,
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    require_manager(user)

    # safety: do not delete yourself
    if user.username == username:
        raise HTTPException(400, "You cannot delete your own account")

    target = session.exec(select(User).where(User.username == username)).first()
    if not target:
        raise HTTPException(404, "User not found")

    session.delete(target)
    session.commit()
    return RedirectResponse("/users", status_code=302)


@app.get("/", response_class=HTMLResponse)
def home(request: Request, session: Session = Depends(get_session)):
    # If logged in -> dashboard, else -> login (so root stays "healthy")
    username = request.cookies.get("user")
    if username:
        u = session.exec(select(User).where(User.username == username)).first()
        if u:
            return RedirectResponse("/dashboard", status_code=302)
    return RedirectResponse("/login", status_code=302)



@app.get("/login", response_class=HTMLResponse)
def login_get(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": ""})


@app.post("/login")
def login_post(
    request: Request,
    session: Session = Depends(get_session),
    username: str = Form(...),
    password: str = Form(...),
):
    user = session.exec(select(User).where(User.username == username)).first()
    if not user or not verify_password(password, user.password_hash):
        return templates.TemplateResponse("login.html", {"request": request, "error": "Invalid credentials"})
    resp = RedirectResponse("/dashboard", status_code=302)
    resp.set_cookie("user", user.username, httponly=True)
    return resp


@app.get("/logout")
def logout():
    resp = RedirectResponse("/login", status_code=302)
    resp.delete_cookie("user")
    return resp


def get_days_for_run(session: Session, run_id: int) -> List[date]:
    days = session.exec(
        select(InspectionEntry.actual_date)
        .where(InspectionEntry.run_id == run_id)
        .distinct()
        .order_by(InspectionEntry.actual_date)
    ).all()
    return list(days)



@app.get("/users", response_class=HTMLResponse)
def users_get(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)

    users = session.exec(select(User).order_by(User.username)).all()
    return templates.TemplateResponse(
        "users.html",
        {"request": request, "user": user, "users": users, "error": ""},
    )


@app.post("/users")
def users_post(
    request: Request,
    session: Session = Depends(get_session),
    username: str = Form(...),
    display_name: str = Form(...),
    role: str = Form(...),
    password: str = Form(...),
):
    user = get_current_user(request, session)
    require_manager(user)

    username = username.strip().lower()
    display_name = display_name.strip()
    role = role.strip().upper()

    if role not in ["INSPECTOR", "MANAGER", "BOSS"]:
        users = session.exec(select(User).order_by(User.username)).all()
        return templates.TemplateResponse(
            "users.html",
            {"request": request, "user": user, "users": users, "error": "Invalid role"},
        )

    existing = session.exec(select(User).where(User.username == username)).first()
    if existing:
        users = session.exec(select(User).order_by(User.username)).all()
        return templates.TemplateResponse(
            "users.html",
            {"request": request, "user": user, "users": users, "error": "Username already exists"},
        )

    if len(password.strip()) < 4:
        users = session.exec(select(User).order_by(User.username)).all()
        return templates.TemplateResponse(
            "users.html",
            {"request": request, "user": user, "users": users, "error": "Password too short (min 4)"},
        )

    session.add(User(
        username=username,
        display_name=display_name,
        role=role,
        password_hash=hash_password(password),
    ))
    session.commit()

    return RedirectResponse("/users", status_code=302)


def slot_from_time_str(t: str) -> tuple[str, int]:
    """
    HARD RULE (2-hour slots):
    - HH:00 .. HH+1:30  -> HH:00
    - HH+1:31 .. HH+2:00 -> HH+2:00

    Special rule for end of day:
    - If the calculated next slot becomes 24:00, return ("00:00", 1) meaning next day.
    """
    parts = t.split(":")
    hh = int(parts[0])
    mm = int(parts[1]) if len(parts) > 1 else 0
    total_min = hh * 60 + mm

    # base even hour (00,02,04,...,22)
    base_h = (hh // 2) * 2
    base_min = base_h * 60
    delta = total_min - base_min

    # exact even hour always stays in its slot
    if delta == 0:
        slot_min = base_min
    # 0..90 minutes => same slot
    elif 0 < delta <= 90:
        slot_min = base_min
    # 91..120 => next slot
    else:
        slot_min = base_min + 120

    # If we rolled past midnight (24:00), move to next day 00:00
    if slot_min >= 24 * 60:
        return "00:00", 1

    # Normal clamp low end (shouldn't happen, but safe)
    if slot_min < 0:
        slot_min = 0

    return f"{slot_min // 60:02d}:00", 0



def get_progress_percent(session: Session, run: ProductionRun) -> int:
    if run.total_length_m <= 0:
        return 0
    max_len = 0.0
    entries = session.exec(select(InspectionEntry).where(InspectionEntry.run_id == run.id)).all()
    for e in entries:
        vals = session.exec(
            select(InspectionValue).where(
                InspectionValue.entry_id == e.id,
                InspectionValue.param_key == "length_m"
            )
        ).all()
        for v in vals:
            if v.value is not None and v.value > max_len:
                max_len = v.value
    return int(min(100.0, (max_len / run.total_length_m) * 100.0))


def get_day_latest_trace(session: Session, run_id: int, day: date) -> dict:
    entries = session.exec(
        select(InspectionEntry)
        .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date == day)
        .order_by(InspectionEntry.created_at)
    ).all()

    raw_batches = get_day_material_batches(session, run_id, day)

    tool1 = (None, None, None)
    tool2 = (None, None, None)
    for e in entries:
        if (e.tool1_name or e.tool1_serial or e.tool1_calib_due):
            tool1 = (e.tool1_name, e.tool1_serial, e.tool1_calib_due)
        if (e.tool2_name or e.tool2_serial or e.tool2_calib_due):
            tool2 = (e.tool2_name, e.tool2_serial, e.tool2_calib_due)

    tools = []
    if any(tool1):
        tools.append(tool1)
    if any(tool2):
        tools.append(tool2)

    return {"entries": entries, "raw_batches": raw_batches, "tools": tools}


def get_last_known_trace_before_day(session: Session, run_id: int, day: date) -> dict:
    # ✅ Get last known RAW batch from MaterialUseEvent (not from entries)
    raw = ""

    last_ev = session.exec(
        select(MaterialUseEvent)
        .where(
            MaterialUseEvent.run_id == run_id,
            MaterialUseEvent.day < day,
        )
        .order_by(
            MaterialUseEvent.day.desc(),
            MaterialUseEvent.slot_time.desc(),
            MaterialUseEvent.created_at.desc(),
        )
    ).first()

    if last_ev:
        lot = session.get(MaterialLot, last_ev.lot_id)
        if lot and lot.batch_no:
            raw = lot.batch_no

    # Keep existing tool carry-forward logic
    entries = session.exec(
        select(InspectionEntry)
        .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date < day)
        .order_by(InspectionEntry.actual_date, InspectionEntry.created_at)
    ).all()

    tool1 = (None, None, None)
    tool2 = (None, None, None)

    for e in entries:
        if (e.tool1_name or e.tool1_serial or e.tool1_calib_due):
            tool1 = (e.tool1_name, e.tool1_serial, e.tool1_calib_due)
        if (e.tool2_name or e.tool2_serial or e.tool2_calib_due):
            tool2 = (e.tool2_name, e.tool2_serial, e.tool2_calib_due)

    tools = []
    if any(tool1):
        tools.append(tool1)
    if any(tool2):
        tools.append(tool2)

    return {"raw": raw, "tools": tools}


def get_current_material_lot_for_slot(session: Session, run_id: int, day: date, slot_time: str):
    """
    Returns MaterialLot or None.
    Rule:
      - Find latest MaterialUseEvent for this run/day with slot_time <= current slot_time
      - If none: fallback to latest event before this day
    """
    # events for same day up to this slot
    ev = session.exec(
        select(MaterialUseEvent)
        .where(
            MaterialUseEvent.run_id == run_id,
            MaterialUseEvent.day == day,
            MaterialUseEvent.slot_time <= slot_time,
        )
        .order_by(MaterialUseEvent.slot_time.desc(), MaterialUseEvent.created_at.desc())
    ).first()

    if not ev:
        # fallback: last event before this day
        ev = session.exec(
            select(MaterialUseEvent)
            .where(
                MaterialUseEvent.run_id == run_id,
                MaterialUseEvent.day < day,
            )
            .order_by(MaterialUseEvent.day.desc(), MaterialUseEvent.slot_time.desc(), MaterialUseEvent.created_at.desc())
        ).first()

    if not ev:
        return None

    return session.get(MaterialLot, ev.lot_id)


def get_day_material_batches(session: Session, run_id: int, day: date) -> list[str]:
    """
    Returns unique batch_no used in THIS day.
    First tries MaterialUseEvent; if none, falls back to InspectionEntry.raw_material_batch_no.
    """

    # 1) Events for this day
    events = session.exec(
        select(MaterialUseEvent)
        .where(MaterialUseEvent.run_id == run_id, MaterialUseEvent.day == day)
        .order_by(MaterialUseEvent.slot_time, MaterialUseEvent.created_at)
    ).all()

    batch_nos: list[str] = []
    seen = set()

    for ev in events:
        lot = session.get(MaterialLot, ev.lot_id)
        bn = (lot.batch_no or "").strip() if lot else ""
        if bn and bn not in seen:
            seen.add(bn)
            batch_nos.append(bn)

    # 2) Fallback: if no events found, read from entries
    if not batch_nos:
        entries = session.exec(
            select(InspectionEntry)
            .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date == day)
            .order_by(InspectionEntry.created_at)
        ).all()

        for e in entries:
            bn = (e.raw_material_batch_no or "").strip()
            if bn and bn not in seen:
                seen.add(bn)
                batch_nos.append(bn)

    return batch_nos


def format_spec_for_export(rule: str, mn: float | None, mx: float | None):
    """
    Returns (set_value, tolerance_text)
    """
    rule = (rule or "").upper()

    if rule == "RANGE" and mn is not None and mx is not None:
        set_value = (mn + mx) / 2.0
        tol = abs(mx - mn) / 2.0
        return set_value, f"±{tol:g}"

    if rule == "MAX_ONLY" and mx is not None:
        return mx, "max"   # you requested: set=35, tol=max (like temperature max 35C)

    if rule == "MIN_ONLY" and mn is not None:
        return mn, "min"

    # fallback (no spec)
    return None, ""


def _safe_float(x: Optional[str]) -> Optional[float]:
    if x is None:
        return None
    x = str(x).strip()
    if x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None
# =========================
# Excel row maps (template coordinates)
# =========================
# Map param_key (from RunParameter.param_key / edit page "key: ...")
# -> Excel ROW number where that parameter's value belongs in the template.
#
# Liner + Cover template:
#   Spec column = C, Tol column = D (your apply_specs_to_template already uses this)
#
# Reinforcement template:
#   Spec column = D, Tol column = E

ROW_MAP_LINER_COVER = {
    # From your liner screenshot (sheet: In-process (Liner)):
    "length_m": 22,
    "od_mm": 23,
    "wall_thickness_mm": 24,
    "cooling_water_c": 25,
    "line_speed_m_min": 26,
    "tractor_pressure_mpa": 27,

    # If your edit page keys match these (very likely):
    "body_temp_1": 28,
    "body_temp_2": 29,
    "body_temp_3": 30,
    "body_temp_4": 31,
    "body_temp_5": 32,

    "noising_temp_1": 33,
    "noising_temp_2": 34,
    "noising_temp_3": 35,
    "noising_temp_4": 36,
    "noising_temp_5": 37,
}

ROW_MAP_REINF = {
    # From your reinforcement screenshot (sheet: In-process (Reinforcement)):
    "length_m": 21,

    # IMPORTANT:
    # These keys MUST match what your edit page shows as: key: ....
    # If your keys are different, keep the row numbers but rename the keys.
    "annular_od_1_mm": 22,
    "annular_od_2_mm": 23,
    "internal_tensile_od_mm": 24,
    "external_tensile_od_mm": 25,
    "core_mould_dia_mm": 26,
    "annular_width_1_mm": 27,
    "annular_width_2_mm": 28,

    # Next visible row in your screenshot looks like it continues at 29:
    "screw_yarn_width_mm": 29,
}



def apply_specs_to_template(ws, run: ProductionRun, session: Session):
    params = session.exec(select(RunParameter).where(RunParameter.run_id == run.id)).all()

    # pick row map per process
    row_map = ROW_MAP_REINF if run.process == "REINFORCEMENT" else ROW_MAP_LINER_COVER

    # columns per template
    if run.process in ["LINER", "COVER"]:
        SPEC_COL = "C"
        TOL_COL = "D"
    else:  # REINFORCEMENT
        SPEC_COL = "D"
        TOL_COL = "E"

    if (run.status or "").upper() == "APPROVED":
        approved_name = run.approved_by_user_name or ""
        approved_at = run.approved_at_utc

        if approved_at:
            oman_time = approved_at.astimezone(ZoneInfo("Asia/Muscat"))
            approved_at_str = oman_time.strftime("%d-%m-%Y %H:%M")
        else:
            approved_at_str = ""

        # --- Approval stamp cell differs by template/process ---
        proc = (run.process or "").strip().upper()

        # ✅ IMPORTANT FIX: proc is uppercase, so compare uppercase
        if "REINFORCEMENT" in proc:
            # Reinforcement template: merged stamp area starts at M42 (covers M42:M44)
            ws["M42"] = f"Approved by: {approved_name}"
            if approved_at_str:
                ws["M42"] = f"Approved by: {approved_name}\nApproved at: {approved_at_str}"
        else:
            # Liner + Cover templates (your original behavior)
            ws["M43"] = f"Approved by: {approved_name}"
            # If you have a separate cell for date/time in liner/cover, keep it:
            # ws["M44"] = f"Approved at: {approved_at_str}"

    for p in params:
        r = row_map.get(p.param_key)
        if not r:
            continue

        set_val, tol_txt = format_spec_for_export(p.rule, p.min_value, p.max_value)

        _set_cell_safe(ws, f"{SPEC_COL}{r}", set_val if set_val is not None else "")
        _set_cell_safe(ws, f"{TOL_COL}{r}", tol_txt)


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)

    # Optional filter (?process=LINER)
    q_process = (request.query_params.get("process") or "").strip().upper()
    if q_process and q_process not in ["LINER", "REINFORCEMENT", "COVER"]:
        q_process = ""

    # Production runs (optionally filtered)
    q = select(ProductionRun).order_by(ProductionRun.created_at.desc())
    if q_process:
        q = q.where(ProductionRun.process == q_process)

    runs = session.exec(q).all()

    grouped: Dict[str, List[ProductionRun]] = {}
    for r in runs:
        grouped.setdefault(r.dhtp_batch_no, []).append(r)

    progress_map = {r.id: get_progress_percent(session, r) for r in runs}

    # Build "batch cards" for the dashboard UI
    batch_cards = []
    for batch_no, batch_runs in grouped.items():
        if not batch_no:
            continue

        batch_runs_sorted = sorted(
            batch_runs,
            key=lambda r: (r.created_at or datetime.min),
            reverse=True,
        )
        rep = batch_runs_sorted[0]

        pcts = [progress_map.get(r.id, 0) for r in batch_runs if r.id is not None]
        avg_progress = int(sum(pcts) / max(1, len(pcts)))

        any_open = any((r.status or "").upper() == "OPEN" for r in batch_runs)
        priority = "HIGH PRIORITY" if any_open else ""

        processes = sorted({(r.process or "").strip().upper() for r in batch_runs if r.process})

        batch_cards.append(
            {
                "batch_no": batch_no,
                "client_name": rep.client_name or "",
                "po_number": rep.po_number or "",
                "itp_number": rep.itp_number or "",
                "created_at": rep.created_at,
                "avg_progress": avg_progress,
                "priority": priority,
                "processes": processes,
                "run_count": len(batch_runs),
            }
        )

    batch_cards.sort(key=lambda x: x.get("created_at") or datetime.min, reverse=True)

    # ---------- Process tiles stats ----------
    all_runs = session.exec(select(ProductionRun).order_by(ProductionRun.created_at.desc())).all()

    process_stats = {}
    for proc in ["LINER", "REINFORCEMENT", "COVER"]:
        proc_runs = [r for r in all_runs if (r.process or "").upper() == proc]
        if not proc_runs:
            continue

        open_cnt = sum(1 for r in proc_runs if (r.status or "").upper() == "OPEN")
        closed_cnt = sum(1 for r in proc_runs if (r.status or "").upper() == "CLOSED")
        approved_cnt = sum(1 for r in proc_runs if (r.status or "").upper() == "APPROVED")

        avg_progress = int(
            sum(get_progress_percent(session, r) for r in proc_runs) / max(1, len(proc_runs))
        )

        process_stats[proc] = {
            "open": open_cnt,
            "closed": closed_cnt,
            "approved": approved_cnt,
            "avg_progress": avg_progress,
            "icon": IMAGE_MAP.get(proc, ""),
        }

    # ---------- MRR status summary ----------
    lots = session.exec(
        select(MaterialLot)
        .where(MaterialLot.status != MRR_CANCELED_STATUS)
        .order_by(MaterialLot.created_at.desc())
    ).all()

    lot_ids = [l.id for l in lots if l and l.id is not None]

    receiving_map = {}
    inspection_map = {}

    if lot_ids:
        receivings = session.exec(
            select(MrrReceiving).where(MrrReceiving.ticket_id.in_(lot_ids))
        ).all()
        receiving_map = {r.ticket_id: r for r in receivings}

        inspections = session.exec(
            select(MrrReceivingInspection).where(MrrReceivingInspection.ticket_id.in_(lot_ids))
        ).all()
        inspection_map = {i.ticket_id: i for i in inspections}

    mrr_pending_docs = 0
    mrr_docs_cleared = 0
    mrr_insp_submitted = 0
    mrr_final_approved = 0

    for lot in lots:
        rec = receiving_map.get(lot.id)
        insp = inspection_map.get(lot.id)

        docs_ok = bool(rec and (rec.inspector_confirmed_po or rec.manager_confirmed_po))
        insp_submitted = bool(insp and insp.inspector_confirmed)
        insp_ok = bool(insp and insp.manager_approved)

        if (lot.status or "").upper() == "APPROVED":
            mrr_final_approved += 1
        else:
            if not docs_ok:
                mrr_pending_docs += 1
            else:
                mrr_docs_cleared += 1

            if insp_submitted and not insp_ok:
                mrr_insp_submitted += 1

    mrr_stats = {
        "pending_docs": mrr_pending_docs,
        "docs_cleared": mrr_docs_cleared,
        "insp_submitted": mrr_insp_submitted,
        "final_approved": mrr_final_approved,
    }

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "user": user,
            "grouped": grouped,
            "progress_map": progress_map,
            "mrr_stats": mrr_stats,
            "process_stats": process_stats,
            "selected_process": q_process,
            "batch_cards": batch_cards,
        },
    )
@app.get("/batches/{batch_no}", response_class=HTMLResponse)
def batch_detail(batch_no: str, request: Request, session: Session = Depends(get_session)):
    """Batch detail page: shows all production runs (processes) under the same batch."""
    user = get_current_user(request, session)

    batch_no = (batch_no or "").strip()
    if not batch_no:
        return RedirectResponse("/dashboard", status_code=302)

    runs = session.exec(
        select(ProductionRun)
        .where(ProductionRun.dhtp_batch_no == batch_no)
        .order_by(ProductionRun.created_at.desc())
    ).all()

    if not runs:
        return RedirectResponse("/dashboard", status_code=302)

    progress_map = {r.id: get_progress_percent(session, r) for r in runs}

    rep = runs[0]
    pcts = [progress_map.get(r.id, 0) for r in runs if r.id is not None]
    avg_progress = int(sum(pcts) / max(1, len(pcts)))

    return templates.TemplateResponse(
        "batch_detail.html",
        {
            "request": request,
            "user": user,
            "batch_no": batch_no,
            "runs": runs,
            "progress_map": progress_map,
            "avg_progress": avg_progress,
            "client_name": rep.client_name or "",
            "po_number": rep.po_number or "",
            "itp_number": rep.itp_number or "",
        },
    )


from sqlalchemy import or_, cast
from sqlalchemy.types import String as SqlString

@app.get("/mrr", response_class=HTMLResponse)
def mrr_list(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)

    q = (request.query_params.get("q") or "").strip()
    like = f"%{q}%"

    base_stmt = select(MaterialLot).where(MaterialLot.lot_type.in_(["RAW", "OUTSOURCED"]))

    # ---- SEARCH FILTERING ----
    if not q:
        lots = session.exec(base_stmt.order_by(MaterialLot.id.desc())).all()
    else:
        # Search on lots directly
        lot_stmt = base_stmt.where(
            or_(
                cast(MaterialLot.id, SqlString).ilike(like),
                (MaterialLot.material_name or "").ilike(like),
                (MaterialLot.supplier_name or "").ilike(like),
                (MaterialLot.po_number or "").ilike(like),
                (MaterialLot.batch_no or "").ilike(like),
                (MaterialLot.status or "").ilike(like),
            )
        )
        lots_direct = session.exec(lot_stmt.order_by(MaterialLot.id.desc())).all()
        direct_ids = {l.id for l in lots_direct}

        # Search on inspection side (DN, report no, JSON: batches/grade/etc)
        insp_hits = session.exec(
            select(MrrReceivingInspection.ticket_id).where(
                or_(
                    (MrrReceivingInspection.delivery_note_no or "").ilike(like),
                    (MrrReceivingInspection.report_no or "").ilike(like),
                    (MrrReceivingInspection.inspection_json or "").ilike(like),
                )
            )
        ).all()
        insp_ticket_ids = {x for x in insp_hits if x is not None}

        all_ids = list(direct_ids.union(insp_ticket_ids))

        if not all_ids:
            lots = []
        else:
            lots = session.exec(
                select(MaterialLot).where(MaterialLot.id.in_(all_ids)).order_by(MaterialLot.id.desc())
            ).all()

    # ---- BUILD MAPS REQUIRED BY TEMPLATE ----
    lot_ids = [l.id for l in lots]

    receiving_map = {}
    inspection_map = {}
    docs_status_map = {}

    if lot_ids:
        receivings = session.exec(
            select(MrrReceiving).where(MrrReceiving.ticket_id.in_(lot_ids))
        ).all()
        receiving_map = {r.ticket_id: r for r in receivings}

        inspections = session.exec(
            select(MrrReceivingInspection)
            .where(MrrReceivingInspection.ticket_id.in_(lot_ids))
            .order_by(MrrReceivingInspection.created_at.desc())
        ).all()

        # latest inspection per ticket
        for ins in inspections:
            if ins.ticket_id not in inspection_map:
                inspection_map[ins.ticket_id] = ins

        docs = session.exec(
            select(MrrDocument).where(MrrDocument.ticket_id.in_(lot_ids))
        ).all()

        # very lightweight docs status: Pending/Done
        # (Your UI probably expects this)
        docs_by_ticket = {}
        for d in docs:
            docs_by_ticket.setdefault(d.ticket_id, []).append(d)

        for tid in lot_ids:
            docs_list = docs_by_ticket.get(tid, [])
            # Consider docs done if at least one PO exists (since you require PO)
            has_po = any((x.doc_type or "").upper() == "PO" for x in docs_list)
            docs_status_map[tid] = "Done" if has_po else "Pending"

    return templates.TemplateResponse(
        "mrr_list.html",
        {
            "request": request,
            "user": user,
            "lots": lots,
            "q": q,
            "receiving_map": receiving_map,
            "inspection_map": inspection_map,
            "docs_status_map": docs_status_map,
        },
    )




@app.get("/mrr/canceled", response_class=HTMLResponse)
def mrr_list_canceled(request: Request, session: Session = Depends(get_session)):
    """Show canceled (soft-deleted) MRR tickets for audit/review."""
    user = get_current_user(request, session)
    require_manager(user)


    lots = session.exec(
        select(MaterialLot)
        .where(MaterialLot.status == MRR_CANCELED_STATUS)
        .order_by(MaterialLot.id.desc())
    ).all()

    lot_ids = [l.id for l in lots if l and l.id is not None]

    receiving_map = {}
    inspection_map = {}

    if lot_ids:
        receivings = session.exec(
            select(MrrReceiving).where(MrrReceiving.ticket_id.in_(lot_ids))
        ).all()
        receiving_map = {r.ticket_id: r for r in receivings}

        inspections = session.exec(
            select(MrrReceivingInspection).where(MrrReceivingInspection.ticket_id.in_(lot_ids))
        ).all()
        inspection_map = {i.ticket_id: i for i in inspections}

    return templates.TemplateResponse(
        "mrr_list.html",
        {
            "request": request,
            "user": user,
            "lots": lots,
            "receiving_map": receiving_map,
            "inspection_map": inspection_map,
            "error": "",
            "showing_canceled": True,
        },
    )

@app.get("/mrr/new", response_class=HTMLResponse)
def mrr_new_get(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)

    return templates.TemplateResponse(
        "mrr_new.html",
        {
            "request": request,
            "user": user,
            "error": "",
        },
    )

@app.post("/mrr/new")
async def mrr_new(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)

    form = await request.form()

    material_name = str(form.get("material_name", "")).strip()
    supplier_name = str(form.get("supplier_name", "")).strip()

    lot_type = str(form.get("lot_type", "RAW")).strip().upper()
    if lot_type not in ["RAW", "OUTSOURCED"]:
        lot_type = "RAW"

    po_number = str(form.get("po_number", "")).strip()

    qty_raw = str(form.get("quantity", "")).strip()
    if qty_raw == "":
        raise HTTPException(400, "PO Quantity is required")
    try:
        quantity = float(qty_raw)
    except Exception:
        raise HTTPException(400, "Invalid PO Quantity")

    quantity_unit = str(form.get("quantity_unit", "KG")).strip().upper()
    if quantity_unit not in ["KG", "T", "PCS"]:
        quantity_unit = "KG"

    lot = MaterialLot(
        lot_type=lot_type,
        batch_no="",  # ✅ DO NOT auto-generate batch here
        material_name=material_name,
        supplier_name=supplier_name,
        po_number=po_number,
        quantity=quantity,
        quantity_unit=quantity_unit,
        received_total=0.0,
        status="PENDING",
    )

    session.add(lot)
    session.commit()

    return RedirectResponse("/mrr", status_code=303)


@app.post("/mrr/{lot_id}/approve")
def mrr_approve(lot_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    # ✅ Do NOT approve the lot for production here anymore.
    # This button now only confirms the ticket is valid to proceed (still not production-approved).
    # We'll mark it as PENDING and rely on Receiving Inspection approval to set APPROVED.
    lot.status = "PENDING"
    session.add(lot)
    session.commit()

    return RedirectResponse(f"/mrr/{lot_id}", status_code=303)



@app.post("/mrr/{lot_id}/reject")
def mrr_reject(lot_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    lot.status = "REJECTED"
    session.add(lot)
    session.commit()

    return RedirectResponse(f"/mrr/{lot_id}", status_code=303)



@app.post("/mrr/{lot_id}/cancel")
def mrr_cancel(lot_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    lot.status = MRR_CANCELED_STATUS
    session.add(lot)
    session.commit()

    return RedirectResponse("/mrr", status_code=303)





    
@app.get("/mrr/{lot_id}", response_class=HTMLResponse)
def mrr_view(lot_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    readonly = is_mrr_canceled(lot)

    docs = session.exec(
        select(MrrDocument)
        .where(MrrDocument.ticket_id == lot_id)
        .order_by(MrrDocument.created_at.desc())
    ).all()

    receiving = session.exec(
        select(MrrReceiving).where(MrrReceiving.ticket_id == lot_id)
    ).first()

    # Latest inspection (could be draft OR submitted)
    inspection = session.exec(
        select(MrrReceivingInspection)
        .where(MrrReceivingInspection.ticket_id == lot_id)
        .order_by(MrrReceivingInspection.created_at.desc())
    ).first()

    # Submitted shipments (for showing DN list + photos)
    submitted_shipments = session.exec(
        select(MrrReceivingInspection)
        .where(
            (MrrReceivingInspection.ticket_id == lot_id) &
            (MrrReceivingInspection.inspector_confirmed == True)
        )
        .order_by(MrrReceivingInspection.created_at.asc())
    ).all()

    # Draft inspection (saved but not submitted) - allow resume
    draft_inspection = session.exec(
        select(MrrReceivingInspection)
        .where(
            (MrrReceivingInspection.ticket_id == lot_id) &
            (MrrReceivingInspection.inspector_confirmed == False)
        )
        .order_by(MrrReceivingInspection.created_at.desc())
    ).first()

    # ✅ Build a map: inspection_id -> batch_numbers list (safe for SQLModel)
    batch_numbers_map = {}
    for s in submitted_shipments:
        data = safe_json_loads(getattr(s, "inspection_json", None))
        batch_numbers_map[int(s.id)] = data.get("batch_numbers", []) or []

    

    used_dns = [
        (s.delivery_note_no or "").strip()
        for s in submitted_shipments
        if (s.delivery_note_no or "").strip()
    ]

    # ✅ Needs-approval inspection (latest submitted not approved)
    inspection_to_approve = session.exec(
        select(MrrReceivingInspection)
        .where(
            (MrrReceivingInspection.ticket_id == lot_id) &
            (MrrReceivingInspection.inspector_confirmed == True) &
            (MrrReceivingInspection.manager_approved == False)
        )
        .order_by(MrrReceivingInspection.created_at.desc())
    ).first()

    # ✅ Latest approved inspection (for Unapprove button)
    latest_approved_inspection = session.exec(
        select(MrrReceivingInspection)
        .where(
            (MrrReceivingInspection.ticket_id == lot_id) &
            (MrrReceivingInspection.inspector_confirmed == True) &
            (MrrReceivingInspection.manager_approved == True)
        )
        .order_by(MrrReceivingInspection.created_at.desc())
    ).first()

    # Photos grouped by inspection
    all_photos = session.exec(
        select(MrrInspectionPhoto)
        .where(MrrInspectionPhoto.ticket_id == lot_id)
        .order_by(MrrInspectionPhoto.created_at.asc())
    ).all()

    photos_by_inspection: Dict[int, Dict[str, List[MrrInspectionPhoto]]] = {}
    for p in all_photos:
        photos_by_inspection.setdefault(int(p.inspection_id), {})
        g = (p.group_name or "General").strip() or "General"
        photos_by_inspection[int(p.inspection_id)].setdefault(g, []).append(p)

    docs_ok = bool(
        receiving and (
            getattr(receiving, "inspector_confirmed_po", False) or
            getattr(receiving, "manager_confirmed_po", False)
        )
    )
    insp_submitted = bool(inspection and getattr(inspection, "inspector_confirmed", False))
    insp_ok = bool(inspection and getattr(inspection, "manager_approved", False))

    return templates.TemplateResponse(
        "mrr_view.html",
        {
            "request": request,
            "user": user,
            "lot": lot,
            "readonly": readonly,
            "error": request.query_params.get("error", ""),
            "docs": docs,
            "receiving": receiving,
            "docs_ok": docs_ok,
            "inspection": inspection,
            "insp_submitted": insp_submitted,
            "insp_ok": insp_ok,
            "submitted_shipments": submitted_shipments,
            "used_dns": used_dns,
            "photos_by_inspection": photos_by_inspection,
            "inspection_to_approve": inspection_to_approve,
            "latest_approved_inspection": latest_approved_inspection,
            "batch_numbers_map": batch_numbers_map,
            "draft_inspection": draft_inspection,

        },
    )

@app.get("/mrr/{lot_id}/docs")
def mrr_docs_page(lot_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    receiving = session.exec(select(MrrReceiving).where(MrrReceiving.ticket_id == lot_id)).first()
    docs = session.exec(select(MrrDocument).where(MrrDocument.ticket_id == lot_id).order_by(MrrDocument.created_at.desc())).all()

    readonly = is_mrr_canceled(lot)

    return render_template(
        request,
        "mrr_doc_upload.html",
        {
            "user": user,
            "lot": lot,
            "receiving": receiving,
            "docs": docs,
            "readonly": readonly,
        },
    )


@app.post("/mrr/{lot_id}/docs/upload")
async def mrr_doc_upload(
    lot_id: int,
    request: Request,
    session: Session = Depends(get_session),
    doc_type: str = Form(...),
    doc_title: str = Form(""),
    doc_number: str = Form(""),
    file: UploadFile = File(...),
):
    user = get_current_user(request, session)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")
    block_if_mrr_canceled(lot)

    

    safe_original = os.path.basename(file.filename or "upload.bin")
    filename = f"{lot_id}_{int(datetime.utcnow().timestamp())}_{safe_original}"
    abs_path = os.path.join(MRR_UPLOAD_DIR, filename)

    # write file
    with open(abs_path, "wb") as f:
        f.write(await file.read())

    dt = (doc_type or "").strip().upper()
        # ✅ For PO docs, doc number is the ticket PO number (auto)
    if dt == "PO":
        doc_number = (lot.po_number or "").strip()


    # Auto doc name unless RELATED
    title = (doc_title or "").strip()
    if dt != "RELATED":
        title = {
            "PO": "PO Copy",
            "DELIVERY_NOTE": "Delivery Note",
            "COA": "COA / Lab Test",
        }.get(dt, dt)

    if dt == "RELATED" and not title:
        raise HTTPException(400, "Document Name is required when type is RELATED")

    # ✅ store RELATIVE path (portable)
    rel_path = os.path.relpath(abs_path, BASE_DIR)

    if dt != "PO" and not (doc_number or "").strip():
        raise HTTPException(400, "Document Number is required")


    doc = MrrDocument(
        ticket_id=lot_id,
        doc_type=dt,
        doc_name=title,
        doc_number=(doc_number or "").strip(),
        file_path=rel_path,
        uploaded_by_user_id=user.id,
        uploaded_by_user_name=user.display_name,
    )

    session.add(doc)
    session.commit()

    return RedirectResponse(f"/mrr/{lot_id}", status_code=303)





@app.get("/mrr/docs/{doc_id}/download")
def mrr_doc_download(doc_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)

    doc = session.get(MrrDocument, doc_id)
    if not doc:
        raise HTTPException(404, "File not found")

    real_path = resolve_mrr_doc_path(doc.file_path)
    if not real_path:
        raise HTTPException(404, "File not found")

    return FileResponse(
        real_path,
        filename=os.path.basename(real_path),
    )

@app.get("/mrr/docs/{doc_id}/inline")
def mrr_doc_inline(doc_id: int, request: Request, session: Session = Depends(get_session)):
    """
    Open document in browser (inline). Used by the "View" button in templates.

    Important:
    - Always resolve the stored file_path via resolve_mrr_doc_path(), because old records might store
      absolute paths from a different machine, or relative paths.
    """
    user = get_current_user(request, session)
    forbid_none(user)

    doc = session.get(MrrDocument, doc_id)
    if not doc:
        raise HTTPException(404, "Document not found")

    resolved = resolve_mrr_doc_path(doc.file_path)
    if not resolved or not os.path.exists(resolved):
        raise HTTPException(404, "File not found")

    media_type, _ = mimetypes.guess_type(resolved)
    media_type = media_type or "application/octet-stream"
    return FileResponse(
        resolved,
        media_type=media_type,
        filename=os.path.basename(resolved),
        headers={"Content-Disposition": f'inline; filename="{os.path.basename(resolved)}"'},
    )


@app.get("/mrr/docs/{doc_id}/view")
def mrr_doc_view(doc_id: int, request: Request, session: Session = Depends(get_session)):
    """
    View an uploaded MRR document in the browser (PDF/images inline).
    Falls back to download for other file types.

    NOTE: We intentionally do NOT rely on the `filename=` parameter here because
    some Starlette versions behave differently on mobile Safari when combined with
    inline Content-Disposition.
    """
    user = get_current_user(request, session)
    

    doc = session.get(MrrDocument, doc_id)
    if not doc:
        raise HTTPException(404, "Document not found")

    resolved = resolve_mrr_doc_path(doc.file_path or "")
    if not resolved or not os.path.exists(resolved):
        # Keep same behavior as download: explicit 404
        raise HTTPException(404, "File not found")

    # Guess content-type
    ctype, _ = mimetypes.guess_type(resolved)
    ctype = ctype or "application/octet-stream"

    # Inline only for types browsers can render nicely
    is_inline = (
        ctype.startswith("image/") or
        ctype in ("application/pdf",)
    )

    safe_name = os.path.basename(resolved)

    headers = {}
    if is_inline:
        headers["Content-Disposition"] = f'inline; filename="{safe_name}"'
    else:
        headers["Content-Disposition"] = f'attachment; filename="{safe_name}"'

    try:
        return FileResponse(resolved, media_type=ctype, headers=headers)
    except Exception as e:
        # If FileResponse fails for any environment-specific reason, stream it.
        def file_iter():
            with open(resolved, "rb") as f:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    yield chunk

        return StreamingResponse(file_iter(), media_type=ctype, headers=headers)

@app.post("/mrr/{lot_id}/docs/submit")
def mrr_docs_submit(
    lot_id: int,
    request: Request,
    session: Session = Depends(get_session),
    inspector_po_number: str = Form(...),
):
    user = get_current_user(request, session)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")
    block_if_mrr_canceled(lot)

    # create or load receiving record (this is ONLY for PO verification now)
    receiving = session.exec(
        select(MrrReceiving).where(MrrReceiving.ticket_id == lot_id)
    ).first()

    if not receiving:
        receiving = MrrReceiving(ticket_id=lot_id)

    receiving.inspector_po_number = inspector_po_number.strip()

    # PO match check
    receiving.po_match = (receiving.inspector_po_number == (lot.po_number or "").strip())

    session.add(receiving)
    session.commit()

    return RedirectResponse(f"/mrr/{lot_id}", status_code=303)


@app.post("/mrr/{lot_id}/docs/confirm")
def mrr_docs_confirm(lot_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    forbid_boss(user)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")
    block_if_mrr_canceled(lot)

    rec = session.exec(select(MrrReceiving).where(MrrReceiving.ticket_id == lot_id)).first()
    docs = session.exec(select(MrrDocument).where(MrrDocument.ticket_id == lot_id)).all()

    if not rec:
        raise HTTPException(400, "Documentation not saved")

    has_po_doc = any(((d.doc_type or "").strip().upper() == "PO") for d in docs)
    po_match = rec.inspector_po_number.strip() == (lot.po_number or "").strip()

    rec.po_match = po_match

    if has_po_doc and po_match:
        rec.inspector_confirmed_po = True
    else:
        rec.inspector_confirmed_po = False
        rec.manager_confirmed_po = False

    session.add(rec)
    session.commit()

    return RedirectResponse(f"/mrr/{lot_id}", status_code=303)

@app.post("/mrr/{lot_id}/docs/submit_and_confirm")
def mrr_docs_submit_and_confirm(
    lot_id: int,
    request: Request,
    session: Session = Depends(get_session),
    inspector_po_number: str = Form(...),
):
    user = get_current_user(request, session)
    forbid_boss(user)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")
    block_if_mrr_canceled(lot)

    # Load or create receiving record
    rec = session.exec(select(MrrReceiving).where(MrrReceiving.ticket_id == lot_id)).first()
    if not rec:
        rec = MrrReceiving(ticket_id=lot_id)

    # Save inspector PO
    rec.inspector_po_number = (inspector_po_number or "").strip()

    # PO match
    ticket_po = (lot.po_number or "").strip()
    po_match = bool(rec.inspector_po_number and ticket_po and rec.inspector_po_number == ticket_po)
    rec.po_match = po_match

    # Check if PO document uploaded
    docs = session.exec(select(MrrDocument).where(MrrDocument.ticket_id == lot_id)).all()
    has_po_doc = any(((d.doc_type or "").strip().upper() == "PO") for d in docs)

    # Confirm rules
    if has_po_doc and po_match:
        rec.inspector_confirmed_po = True
    else:
        rec.inspector_confirmed_po = False
        rec.manager_confirmed_po = False

    session.add(rec)
    session.commit()

    return RedirectResponse(f"/mrr/{lot_id}", status_code=303)

@app.post("/mrr/{lot_id}/docs/approve")
def mrr_docs_manager_approve(lot_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)

    rec = session.exec(select(MrrReceiving).where(MrrReceiving.ticket_id == lot_id)).first()
    if not rec:
        raise HTTPException(404, "Receiving record not found")

    rec.manager_confirmed_po = True
    rec.inspector_confirmed_po = True

    session.add(rec)
    session.commit()

    return RedirectResponse(f"/mrr/{lot_id}", status_code=303)

def mrr_docs_are_cleared(session: Session, lot_id: int) -> bool:
    rec = session.exec(
        select(MrrReceiving).where(MrrReceiving.ticket_id == lot_id)
    ).first()

    if not rec:
        return False


# =========================
# MRR Cancel helpers (soft delete)
# =========================
MRR_CANCELED_STATUS = "CANCELED"

def is_mrr_canceled(lot):
    return bool(lot and (getattr(lot, "status", "") or "").upper() == MRR_CANCELED_STATUS)

def block_if_mrr_canceled(lot):
    """Guard helper: prevent actions on canceled MRR tickets."""
    if is_mrr_canceled(lot):
        raise HTTPException(status_code=403, detail="This MRR Ticket is canceled")




def mrr_inspection_approve(lot_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)

    insp = session.exec(
        select(MrrReceivingInspection)
        .where(MrrReceivingInspection.ticket_id == lot_id)
    ).first()

    if not insp:
        raise HTTPException(404, "Receiving Inspection not found")

    # ✅ Manager approves the receiving inspection
    insp.manager_approved = True
    session.add(insp)

    # ✅ THIS is the ONLY place the batch becomes usable in production
    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")
    block_if_mrr_canceled(lot)

    lot.status = "APPROVED"   # <-- makes it appear in production dropdown
    session.add(lot)

    session.commit()
    return RedirectResponse(f"/mrr/{lot_id}", status_code=303)

@app.get("/mrr-pending-approvals", response_class=HTMLResponse)
def mrr_pending(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)

    # Find inspections submitted by inspector but not yet manager-approved
    pending = session.exec(
        select(MrrReceivingInspection)
        .where(MrrReceivingInspection.inspector_confirmed == True)
        .where(MrrReceivingInspection.manager_approved == False)
        .order_by(MrrReceivingInspection.created_at.desc())
    ).all()

    # Load lots to show ticket info
    lot_ids = [p.ticket_id for p in pending]
    lots = []
    lot_map = {}
    if lot_ids:
        lots = session.exec(select(MaterialLot).where(MaterialLot.id.in_(lot_ids))).all()
        lot_map = {l.id: l for l in lots}

    return templates.TemplateResponse(
        "mrr_pending.html",
        {
            "request": request,
            "user": user,
            "pending": pending,
            "lot_map": lot_map,
        },
    )

@app.get("/mrr/{lot_id}/inspection/new", response_class=HTMLResponse)
@app.get("/mrr/{lot_id}/inspection/new", response_class=HTMLResponse)
def new_shipment_inspection_page(
    lot_id: int,
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    forbid_boss(user)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    if (lot.status or "").upper() == "CANCELED":
        return RedirectResponse(f"/mrr/{lot_id}?error=Ticket%20is%20canceled", status_code=303)

    # ✅ LOCK: if ticket is approved (fully received) no new shipment inspection allowed
    if (lot.status or "").upper() == "APPROVED":
        return RedirectResponse(
            f"/mrr/{lot_id}?error=Ticket%20is%20APPROVED%20(receiving%20closed).%20Manager%20must%20unapprove%20to%20reopen.",
            status_code=303,
        )

    # Documentation prerequisites
    receiving = session.exec(select(MrrReceiving).where(MrrReceiving.ticket_id == lot_id)).first()

    has_po_doc = session.exec(
        select(MrrDocument).where((MrrDocument.ticket_id == lot_id) & (MrrDocument.doc_type == "PO"))
    ).first() is not None

    if not has_po_doc:
        return RedirectResponse(
            f"/mrr/{lot_id}?error=Upload%20PO%20document%20(Type:%20PO)%20before%20starting%20Receiving%20Inspection",
            status_code=303,
        )

            # ✅ Require at least ONE quality document before starting inspection
        if not quality_doc_exists(session, lot_id):
            return RedirectResponse(
                f"/mrr/{lot_id}?error=Upload%20one%20Quality%20Document%20(COA%20or%20MTC%20or%20Inspection%20Report)%20before%20starting%20Receiving%20Inspection",
                status_code=303,
            )


    if not receiving:
        return RedirectResponse(
            f"/mrr/{lot_id}?error=Documentation%20is%20not%20saved.%20Please%20fill%20Documentation%20and%20click%20Save%20first",
            status_code=303,
        )

    docs_ok = bool(getattr(receiving, "inspector_confirmed_po", False) or getattr(receiving, "manager_confirmed_po", False))
    if not docs_ok:
        return RedirectResponse(
            f"/mrr/{lot_id}?error=Documentation%20is%20not%20cleared.%20Click%20Confirm%20Documentation%20Complete%20first",
            status_code=303,
        )

    # If everything ok -> show your existing "new shipment" page
    return templates.TemplateResponse(
        "mrr_new_shipment.html",
        {
            "request": request,
            "user": user,
            "lot": lot,
            "error": request.query_params.get("error", ""),
        },
    )

@app.post("/mrr/{lot_id}/inspection/{inspection_id}/submit")
def mrr_inspection_submit(
    lot_id: int,
    inspection_id: int,
    request: Request,
    session: Session = Depends(get_session),

    # NEW: action decides draft vs submit
    action: str = Form("submit"),

    # header fields (if you already had them in your function, keep them here)
    delivery_note_no: str = Form(""),
    qty_arrived: float = Form(0.0),
    qty_unit: str = Form(""),

    # your form fields
    batch_numbers: List[str] = Form([]),
    mismatch_reason: str = Form(""),
    material_family: str = Form(""),
    material_model: str = Form(""),
    material_grade: str = Form(""),

    # PE table fields (keep as text)
    pe_density_result: str = Form(""),
    pe_density_remarks: str = Form(""),
    pe_mfr_result: str = Form(""),
    pe_mfr_remarks: str = Form(""),
    pe_flexural_result: str = Form(""),
    pe_flexural_remarks: str = Form(""),
    pe_tensile_result: str = Form(""),
    pe_tensile_remarks: str = Form(""),
    pe_elong_result: str = Form(""),
    pe_elong_remarks: str = Form(""),
    pe_escr_result: str = Form(""),
    pe_escr_remarks: str = Form(""),
    pe_oits_result: str = Form(""),
    pe_oits_remarks: str = Form(""),
    pe_cb_result: str = Form(""),
    pe_cb_remarks: str = Form(""),
    pe_cbd_result: str = Form(""),
    pe_cbd_remarks: str = Form(""),
    pe_mvd_result: str = Form(""),
    pe_mvd_remarks: str = Form(""),
    pe_ash_result: str = Form(""),
    pe_ash_remarks: str = Form(""),
    pe_moist_result: str = Form(""),
    pe_moist_remarks: str = Form(""),

    # Fiber table fields
    fb_denier_result: str = Form(""),
    fb_denier_remarks: str = Form(""),
    fb_tenacity_result: str = Form(""),
    fb_tenacity_remarks: str = Form(""),
    fb_elong_result: str = Form(""),
    fb_elong_remarks: str = Form(""),
    fb_moist_result: str = Form(""),
    fb_moist_remarks: str = Form(""),
    fb_finish_result: str = Form(""),
    fb_finish_remarks: str = Form(""),

    # remarks
    remarks: str = Form(""),
    inspected_by: str = Form(""),
):
    user = get_current_user(request, session)
    forbid_boss(user)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    insp = session.get(MrrReceivingInspection, inspection_id)
    if not insp or insp.ticket_id != lot_id:
        raise HTTPException(404, "MRR Inspection not found")

    # Normalize action
    action = (action or "submit").strip().lower()
    # Final submit requires at least 1 batch number (draft saves can be empty)
    if action == "submit":
        if not any((b or "").strip() for b in (batch_numbers or [])):
            raise HTTPException(status_code=400, detail="Batch number is required to submit.")

    # Build JSON exactly like your template expects
    inspection_data = {
        "report_no": getattr(insp, "report_no", "") or "",
        "batch_numbers": [x.strip() for x in (batch_numbers or []) if str(x).strip()],
        "mismatch_reason": (mismatch_reason or "").strip(),

        "material_family": (material_family or "").strip(),
        "material_model": (material_model or "").strip(),
        "material_grade": (material_grade or "").strip(),

        # PE table
        "pe_density_result": (pe_density_result or "").strip(),
        "pe_density_remarks": (pe_density_remarks or "").strip(),
        "pe_mfr_result": (pe_mfr_result or "").strip(),
        "pe_mfr_remarks": (pe_mfr_remarks or "").strip(),
        "pe_flexural_result": (pe_flexural_result or "").strip(),
        "pe_flexural_remarks": (pe_flexural_remarks or "").strip(),
        "pe_tensile_result": (pe_tensile_result or "").strip(),
        "pe_tensile_remarks": (pe_tensile_remarks or "").strip(),
        "pe_elong_result": (pe_elong_result or "").strip(),
        "pe_elong_remarks": (pe_elong_remarks or "").strip(),
        "pe_escr_result": (pe_escr_result or "").strip(),
        "pe_escr_remarks": (pe_escr_remarks or "").strip(),
        "pe_oits_result": (pe_oits_result or "").strip(),
        "pe_oits_remarks": (pe_oits_remarks or "").strip(),
        "pe_cb_result": (pe_cb_result or "").strip(),
        "pe_cb_remarks": (pe_cb_remarks or "").strip(),
        "pe_cbd_result": (pe_cbd_result or "").strip(),
        "pe_cbd_remarks": (pe_cbd_remarks or "").strip(),
        "pe_mvd_result": (pe_mvd_result or "").strip(),
        "pe_mvd_remarks": (pe_mvd_remarks or "").strip(),
        "pe_ash_result": (pe_ash_result or "").strip(),
        "pe_ash_remarks": (pe_ash_remarks or "").strip(),
        "pe_moist_result": (pe_moist_result or "").strip(),
        "pe_moist_remarks": (pe_moist_remarks or "").strip(),

        # Fiber table
        "fb_denier_result": (fb_denier_result or "").strip(),
        "fb_denier_remarks": (fb_denier_remarks or "").strip(),
        "fb_tenacity_result": (fb_tenacity_result or "").strip(),
        "fb_tenacity_remarks": (fb_tenacity_remarks or "").strip(),
        "fb_elong_result": (fb_elong_result or "").strip(),
        "fb_elong_remarks": (fb_elong_remarks or "").strip(),
        "fb_moist_result": (fb_moist_result or "").strip(),
        "fb_moist_remarks": (fb_moist_remarks or "").strip(),
        "fb_finish_result": (fb_finish_result or "").strip(),
        "fb_finish_remarks": (fb_finish_remarks or "").strip(),

        # remarks
        "remarks": (remarks or "").strip(),
        "inspected_by": (inspected_by or "").strip() or getattr(user, "display_name", "") or "",
    }

    # Always save data (draft or submit)
    insp.delivery_note_no = (delivery_note_no or "").strip() or (getattr(insp, "delivery_note_no", "") or "")
    insp.qty_arrived = qty_arrived
    insp.qty_unit = (qty_unit or "").strip() or (getattr(insp, "qty_unit", "") or "")
    insp.inspection_json = json.dumps(inspection_data, ensure_ascii=False)

    if action == "draft":
        # Draft save only
        insp.inspector_confirmed = False
        session.add(insp)
        session.commit()
        return RedirectResponse(f"/mrr/{lot_id}/inspection/id/{inspection_id}?saved=draft", status_code=302)

    # Final submit
    insp.inspector_confirmed = True
    session.add(insp)
    session.commit()
    return RedirectResponse(f"/mrr/{lot_id}", status_code=302)



@app.post("/mrr/{lot_id}/inspection/new")
async def create_shipment_inspection(
    lot_id: int,
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    forbid_boss(user)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    if (lot.status or "").upper() == "CANCELED":
        return RedirectResponse(f"/mrr/{lot_id}?error=Ticket%20is%20canceled", status_code=303)

    # ✅ LOCK
    if (lot.status or "").upper() == "APPROVED":
        return RedirectResponse(
            f"/mrr/{lot_id}?error=Ticket%20is%20APPROVED%20(receiving%20closed).%20Manager%20must%20unapprove%20to%20reopen.",
            status_code=303,
        )
        
    # ✅ NEW: Require at least one quality doc (COA/MTC/INSPECTION_REPORT) before inspection
    has_quality_doc = session.exec(
        select(MrrDocument).where(
            (MrrDocument.ticket_id == lot_id)
            & (MrrDocument.doc_type.in_(["COA", "MTC", "INSPECTION_REPORT"]))
        )
    ).first() is not None
    
    if not has_quality_doc:
        return RedirectResponse(
            f"/mrr/{lot_id}?error=Upload%20Quality%20Document%20(COA%20or%20MTC%20or%20INSPECTION%20REPORT)%20before%20starting%20Receiving%20Inspection",
            status_code=303,
        )

    # Documentation prerequisites
    receiving = session.exec(select(MrrReceiving).where(MrrReceiving.ticket_id == lot_id)).first()

    has_po_doc = session.exec(
        select(MrrDocument).where((MrrDocument.ticket_id == lot_id) & (MrrDocument.doc_type == "PO"))
    ).first() is not None

    if not has_po_doc:
        return RedirectResponse(
            f"/mrr/{lot_id}?error=Upload%20PO%20document%20(Type:%20PO)%20before%20starting%20Receiving%20Inspection",
            status_code=303,
        )
            # ✅ Require at least ONE quality document before starting inspection
        if not quality_doc_exists(session, lot_id):
            return RedirectResponse(
                f"/mrr/{lot_id}?error=Upload%20one%20Quality%20Document%20(COA%20or%20MTC%20or%20Inspection%20Report)%20before%20starting%20Receiving%20Inspection",
                status_code=303,
            )


    if not receiving:
        return RedirectResponse(
            f"/mrr/{lot_id}?error=Documentation%20is%20not%20saved.%20Please%20fill%20Documentation%20and%20click%20Save%20first",
            status_code=303,
        )

    docs_ok = bool(getattr(receiving, "inspector_confirmed_po", False) or getattr(receiving, "manager_confirmed_po", False))
    if not docs_ok:
        return RedirectResponse(
            f"/mrr/{lot_id}?error=Documentation%20is%20not%20cleared.%20Click%20Confirm%20Documentation%20Complete%20first",
            status_code=303,
        )

    form = await request.form()

    dn = (form.get("delivery_note_no") or "").strip()
    qty_arrived = form.get("qty_arrived")
    qty_unit = (form.get("qty_unit") or "KG").strip().upper()

    if not dn:
        return RedirectResponse(f"/mrr/{lot_id}/inspection/new?error=Delivery%20Note%20is%20required", status_code=303)

    try:
        qty_arrived_val = float(qty_arrived or 0.0)
    except Exception:
        return RedirectResponse(f"/mrr/{lot_id}/inspection/new?error=Invalid%20quantity", status_code=303)

    if qty_arrived_val <= 0:
        return RedirectResponse(f"/mrr/{lot_id}/inspection/new?error=Quantity%20must%20be%20greater%20than%200", status_code=303)

    # DN must not be reused in another SUBMITTED/APPROVED shipment
    dn_used = session.exec(
        select(MrrReceivingInspection).where(
            (MrrReceivingInspection.ticket_id == lot_id)
            & (MrrReceivingInspection.delivery_note_no == dn)
            & (
                (MrrReceivingInspection.inspector_confirmed == True)
                | (MrrReceivingInspection.manager_approved == True)
            )
        )
    ).first()

    if dn_used:
        return RedirectResponse(
            f"/mrr/{lot_id}/inspection/new?error=This%20Delivery%20Note%20was%20already%20used%20in%20a%20submitted%20shipment",
            status_code=303,
        )

    # Create inspection record
    report_no = generate_report_no(lot_id, 1)

    insp = MrrReceivingInspection(
        ticket_id=lot_id,
        inspector_id=user.id,
        inspector_name=user.display_name,
        delivery_note_no=dn,
        qty_arrived=qty_arrived_val,
        qty_unit=qty_unit,
        report_no=report_no,
        template_type="RAW",
        inspection_json="{}",
        inspector_confirmed=False,
        manager_approved=False,
    )

    session.add(insp)
    session.commit()
    session.refresh(insp)

    return RedirectResponse(f"/mrr/{lot_id}/inspection/id/{insp.id}", status_code=303)


from sqlalchemy import or_  # ✅ add this import near your imports if not already

from collections import defaultdict
import hashlib
from sqlalchemy import or_
from sqlmodel import select

@app.get("/runs", response_class=HTMLResponse)
def runs_list(
    request: Request,
    q: str = "",
    view: str = "open",   # open | approved
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)

    # ✅ Only 2 views:
    # - open: everything except APPROVED (includes CLOSED)
    # - approved: only APPROVED
    view = (view or "open").lower().strip()
    if view not in ["open", "approved"]:
        view = "open"

    stmt = select(ProductionRun)

    # ✅ If user is searching, show ALL (open + approved) regardless of tab
    # ✅ If no search, keep the split logic
    q_clean = (q or "").strip()
    if not q_clean:
        if view == "approved":
            stmt = stmt.where(ProductionRun.status == "APPROVED")
        else:
            stmt = stmt.where(ProductionRun.status != "APPROVED")


    # ✅ Search across multiple fields
    q_clean = (q or "").strip()
    if q_clean:
        like = f"%{q_clean}%"
        stmt = stmt.where(
            or_(
                ProductionRun.dhtp_batch_no.ilike(like),
                ProductionRun.process.ilike(like),
                ProductionRun.client_name.ilike(like),
                ProductionRun.po_number.ilike(like),
                ProductionRun.itp_number.ilike(like),
                ProductionRun.pipe_specification.ilike(like),
                ProductionRun.raw_material_spec.ilike(like),
            )
        )

    stmt = stmt.order_by(ProductionRun.created_at.desc())
    runs = session.exec(stmt).all()

    # ✅ Group by batch number
    grouped = defaultdict(list)
    for r in runs:
        grouped[(r.dhtp_batch_no or "-").strip()].append(r)

    # ✅ Build stable client colors (soft background)
    def client_color(name: str) -> str:
        n = (name or "").strip().lower()
        if not n:
            return "#f7f7f7"
        h = hashlib.md5(n.encode("utf-8")).hexdigest()
        hue = int(h[:2], 16) * 360 // 255
        # very light HSL-like pastel using a fixed palette trick
        # (keeps consistent color per client)
        return f"hsl({hue}, 70%, 95%)"

    client_bg = {}
    for r in runs:
        cn = (r.client_name or "").strip()
        if cn and cn not in client_bg:
            client_bg[cn] = client_color(cn)

    # ✅ Convert dict to list (for template)
    batch_cards = []
    for batch_no, items in grouped.items():
        # Sort processes inside each batch in a nice order
        order = {"LINER": 1, "REINFORCEMENT": 2, "COVER": 3}
        items_sorted = sorted(items, key=lambda x: order.get((x.process or "").upper(), 99))

        # take summary info from first row
        client_name = items_sorted[0].client_name if items_sorted else ""
        po_number = items_sorted[0].po_number if items_sorted else ""

        batch_cards.append({
            "batch_no": batch_no,
            "client_name": client_name,
            "po_number": po_number,
            "runs": items_sorted,
        })

    # Sort batches newest-first by newest run created_at
    batch_cards.sort(
        key=lambda b: b["runs"][0].created_at if b["runs"] else datetime.min,
        reverse=True,
    )

    return templates.TemplateResponse(
        "run_list.html",
        {
            "request": request,
            "user": user,
            "q": q_clean,
            "view": view,
            "batch_cards": batch_cards,
            "client_bg": client_bg,
        },
    )




@app.get("/runs/new", response_class=HTMLResponse)
def run_new_get(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    if (user.role or "").upper() not in ["MANAGER", "RUN_CREATOR"]:
        raise HTTPException(403, "Manager only")

    return templates.TemplateResponse("run_new.html", {"request": request, "user": user, "error": ""})



@app.get("/mrr/{lot_id}/inspection")
def old_inspection_redirect(lot_id: int):
    return RedirectResponse(f"/mrr/{lot_id}/inspection/new", status_code=302)

@app.post("/runs/new")
def run_new_post(
    request: Request,
    session: Session = Depends(get_session),
    process: str = Form(...),
    dhtp_batch_no: str = Form(...),
    client_name: str = Form(...),
    po_number: str = Form(...),
    itp_number: str = Form(...),
    pipe_specification: str = Form(...),
    raw_material_spec: str = Form(...),
    total_length_m: float = Form(0.0),
    allow_duplicate: str = Form(""),
):
    user = get_current_user(request, session)
    if (user.role or "").upper() not in ["MANAGER", "RUN_CREATOR"]:
        raise HTTPException(403, "Manager only")

     

    process = process.upper().strip()
    if process not in PROCESS_PARAMS:
        return templates.TemplateResponse("run_new.html", {"request": request, "user": user, "error": "Invalid process"})

    existing_open = session.exec(
        select(ProductionRun).where(
            ProductionRun.dhtp_batch_no == dhtp_batch_no,
            ProductionRun.process == process,
            ProductionRun.status == "OPEN",
        )
    ).first()
    if existing_open and allow_duplicate != "1":
        return templates.TemplateResponse(
            "run_new.html",
            {
                "request": request,
                "user": user,
                "error": f"There is already an OPEN {process} run for Batch {dhtp_batch_no}. If you really need a second line, tick 'Allow duplicate line'."
            },
        )

    run = ProductionRun(
        process=process,
        dhtp_batch_no=dhtp_batch_no,
        client_name=client_name,
        po_number=po_number,
        itp_number=itp_number,
        pipe_specification=pipe_specification,
        raw_material_spec=raw_material_spec,
        total_length_m=total_length_m or 0.0,
    )
    session.add(run)
    session.commit()
    session.refresh(run)

    for idx, (key, label, unit) in enumerate(PROCESS_PARAMS[process]):
        session.add(RunParameter(run_id=run.id, param_key=key, label=label, unit=unit, display_order=idx))
    session.commit()

    return RedirectResponse("/dashboard", status_code=302)

@app.get("/mrr/{lot_id}/inspection/id/{inspection_id}", response_class=HTMLResponse)
def shipment_inspection_form(
    lot_id: int,
    inspection_id: int,
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    inspection = session.get(MrrReceivingInspection, inspection_id)
    if not inspection or inspection.ticket_id != lot_id:
        raise HTTPException(404, "Shipment inspection not found")

    # Header source of truth:
    # - PO = ticket
    # - Report/DN/Qty/Unit = shipment record
    try:
        data = json.loads(inspection.inspection_json or "{}")
    except Exception:
        data = {}

    # Ensure report_no exists even for older drafts
    if not getattr(inspection, "report_no", None):
        data["report_no"] = data.get("report_no") or generate_report_no(lot_id, 1)
    else:
        data["report_no"] = inspection.report_no

    # Mirror shipment fields into json
    data["delivery_note_no"] = inspection.delivery_note_no or ""
    data["qty_arrived"] = inspection.qty_arrived if inspection.qty_arrived is not None else ""
    data["qty_unit"] = inspection.qty_unit or "KG"

    # ✅ NEW: load photos for this inspection and group them
    photos = session.exec(
        select(MrrInspectionPhoto)
        .where(
            (MrrInspectionPhoto.ticket_id == lot_id) &
            (MrrInspectionPhoto.inspection_id == inspection_id)
        )
        .order_by(MrrInspectionPhoto.created_at.asc())
    ).all()

    photo_groups: Dict[str, List[MrrInspectionPhoto]] = {}
    for p in photos:
        g = (p.group_name or "General").strip() or "General"
        photo_groups.setdefault(g, []).append(p)

    return templates.TemplateResponse(
        "mrr_inspection.html",
        {
            "request": request,
            "user": user,
            "lot": lot,
            "inspection": inspection,
            "inspection_data": data,
            "photo_groups": photo_groups,                 # ✅ NEW
            "photo_error": request.query_params.get("photo_error", ""),  # ✅ NEW
        },
    )


@app.get("/runs/{run_id}", response_class=HTMLResponse)
def run_view(run_id: int, request: Request, session: Session = Depends(get_session)):
    try:
        user = get_current_user(request, session)

        run = session.get(ProductionRun, run_id)
        if not run:
            raise HTTPException(404, "Run not found")

        machines = session.exec(select(RunMachine).where(RunMachine.run_id == run_id)).all()
        params = session.exec(
            select(RunParameter).where(RunParameter.run_id == run_id).order_by(RunParameter.display_order)
        ).all()

        days = get_days_for_run(session, run_id)

        selected_day = None
        day_q = request.query_params.get("day")
        if day_q:
            try:
                selected_day = date.fromisoformat(day_q)
            except Exception:
                selected_day = None

        if selected_day is None:
            selected_day = days[-1] if days else date.today()

        entries = session.exec(
            select(InspectionEntry)
            .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date == selected_day)
            .order_by(InspectionEntry.created_at)
        ).all()
        
        # ✅ NEW: for the UI to know which slots have an InspectionEntry
        slot_entry_ids: Dict[str, int] = {}
        for e in entries:
            if not e.slot_time or e.slot_time not in SLOTS:
                continue
            slot_entry_ids[e.slot_time] = e.id



        users = session.exec(select(User)).all()
        user_map = {u.id: u for u in users}

        slot_inspectors: Dict[str, str] = {s: "" for s in SLOTS}
        grid: Dict[str, Dict[str, dict]] = {s: {} for s in SLOTS}

        for e in entries:
            if not e.slot_time or e.slot_time not in SLOTS:
                continue

            if e.inspector_id and e.inspector_id in user_map:
                slot_inspectors[e.slot_time] = user_map[e.inspector_id].display_name or ""
            else:
                slot_inspectors[e.slot_time] = ""

            # ✅ NEW: actual times per slot (for header display)
            slot_actual_times: Dict[str, str] = {s: "" for s in SLOTS}
            for e in entries:
                if not e.slot_time or e.slot_time not in SLOTS:
                    continue
                slot_actual_times[e.slot_time] = e.actual_time or ""


            vals = session.exec(select(InspectionValue).where(InspectionValue.entry_id == e.id)).all()
            for v in vals:
                grid[e.slot_time][v.param_key] = {
                    "value_id": v.id,
                    "value": v.value,
                    "out": bool(v.is_out_of_spec),
                    "note": v.spec_note or "",
                    "pending_value": v.pending_value,
                    "pending_status": v.pending_status or "",
                }

        trace_today = get_day_latest_trace(session, run_id, selected_day)
        carry = get_last_known_trace_before_day(session, run_id, selected_day)

        raw_batches = trace_today["raw_batches"] or ([carry["raw"]] if carry["raw"] else [])
        tools = trace_today["tools"] or carry["tools"]

        progress = get_progress_percent(session, run)

        return templates.TemplateResponse(
            "run_view.html",
            {
                "request": request,
                "user": user,
                "run": run,
                "machines": machines,
                "params": params,
                "days": days,
                "selected_day": selected_day,
                "grid": grid,
                "slot_inspectors": slot_inspectors,
                "slot_entry_ids": slot_entry_ids, 
                "image_url": IMAGE_MAP.get(run.process, ""),
                "progress": progress,
                "raw_batches": raw_batches,
                "tools": tools,
                "slot_actual_times": slot_actual_times,
               
            },
        )

    except Exception:
        # TEMP DEBUG: show the real error on the page
        return HTMLResponse(
            "<pre style='white-space:pre-wrap;font-size:14px'>"
            + traceback.format_exc()
            + "</pre>",
            status_code=500,
        )

@app.post("/runs/{run_id}/close")
def run_close(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)  # only manager can close

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    run.status = "CLOSED"
    session.add(run)
    session.commit()

    return RedirectResponse(f"/runs/{run_id}", status_code=302)

from datetime import datetime
from fastapi import HTTPException
from starlette.responses import RedirectResponse

@app.post("/runs/{run_id}/approve")
def run_approve(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)  # only managers can approve

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    # Require CLOSED before approving (keep or remove depending on your rule)
    if (run.status or "").upper() != "CLOSED":
        raise HTTPException(status_code=400, detail="Run must be CLOSED before approving")

    run.status = "APPROVED"
    run.approved_by_user_id = user.id
    run.approved_by_user_name = user.display_name or ""
    run.approved_at_utc = datetime.utcnow()

    session.add(run)
    session.commit()

    return RedirectResponse(url=f"/runs/{run_id}", status_code=302)


@app.post("/runs/{run_id}/reopen")
def run_reopen(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)  # only manager can reopen

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    # Reopen allowed from CLOSED or APPROVED
    run.status = "OPEN"

    # If it was approved before, clear approval fields so it behaves like a normal open run again
    run.approved_by_user_id = None
    run.approved_by_user_name = ""
    run.approved_at_utc = None

    session.add(run)
    session.commit()

    return RedirectResponse(f"/runs/{run_id}", status_code=302)


@app.get("/runs/{run_id}/edit", response_class=HTMLResponse)
def run_edit_get(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)
        

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    machines = session.exec(select(RunMachine).where(RunMachine.run_id == run_id)).all()
    params = session.exec(
        select(RunParameter).where(RunParameter.run_id == run_id).order_by(RunParameter.display_order)
    ).all()

    return templates.TemplateResponse(
        "run_edit.html",
        {"request": request, "user": user, "run": run, "machines": machines, "params": params, "error": ""},
    )


@app.post("/runs/{run_id}/edit")
async def run_edit_post(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)
        

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    form = await request.form()

    run.client_name = str(form.get("client_name", "")).strip()
    run.po_number = str(form.get("po_number", "")).strip()
    run.itp_number = str(form.get("itp_number", "")).strip()
    run.pipe_specification = str(form.get("pipe_specification", "")).strip()
    run.raw_material_spec = str(form.get("raw_material_spec", "")).strip()
    try:
        run.total_length_m = float(form.get("total_length_m") or 0.0)
    except Exception:
        run.total_length_m = 0.0

    session.add(run)
    session.commit()

    existing = session.exec(select(RunMachine).where(RunMachine.run_id == run_id)).all()
    for m in existing:
        session.delete(m)
    session.commit()

    def _m(name_key: str, tag_key: str):
        name = str(form.get(name_key, "")).strip()
        tag = str(form.get(tag_key, "")).strip()
        if name:
            session.add(RunMachine(run_id=run_id, machine_name=name, machine_tag=tag))

    _m("machine1_name", "machine1_tag")
    _m("machine2_name", "machine2_tag")
    _m("machine3_name", "machine3_tag")
    _m("machine4_name", "machine4_tag")
    _m("machine5_name", "machine5_tag")
    session.commit()

    params = session.exec(select(RunParameter).where(RunParameter.run_id == run_id)).all()
    for p in params:
        new_label = str(form.get(f"label_{p.param_key}", "")).strip()
        if new_label:
            p.label = new_label

        rule = str(form.get(f"rule_{p.param_key}", "")).strip().upper()
        if rule not in ["", "RANGE", "MIN_ONLY", "MAX_ONLY"]:
            rule = ""
        p.rule = rule

        set_raw = form.get(f"set_{p.param_key}", "")
        tol_raw = form.get(f"tol_{p.param_key}", "")
        
        set_v = _safe_float(set_raw)
        tol_v = _safe_float(tol_raw)
        
        # convert Set/Tolerance to Min/Max (keeps old system working)
        if p.rule == "RANGE":
            if set_v is None or tol_v is None:
                p.min_value = None
                p.max_value = None
            else:
                t = abs(tol_v)
                p.min_value = set_v - t
                p.max_value = set_v + t
        
        elif p.rule == "MAX_ONLY":
            # your example: temperature max 35 -> set=35, tol can be empty
            p.min_value = None
            p.max_value = set_v
        
        elif p.rule == "MIN_ONLY":
            p.min_value = set_v
            p.max_value = None
        
        else:
            p.min_value = None
            p.max_value = None


        session.add(p)

    session.commit()
    return RedirectResponse(f"/runs/{run.id}", status_code=302)


@app.get("/runs/{run_id}/entry/new", response_class=HTMLResponse)
def entry_new_get(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    params = session.exec(
        select(RunParameter).where(RunParameter.run_id == run_id).order_by(RunParameter.display_order)
    ).all()

    has_any = session.exec(select(InspectionEntry.id).where(InspectionEntry.run_id == run_id)).first() is not None
    error = request.query_params.get("error", "")

    approved_lots = session.exec(
        select(MaterialLot)
        .where(MaterialLot.status == "APPROVED", MaterialLot.lot_type == "RAW")
        .order_by(MaterialLot.batch_no)
    ).all()

    # ✅ TRUE check: does the run already have any batch event?
    has_any_event = session.exec(
        select(MaterialUseEvent.id).where(MaterialUseEvent.run_id == run_id).limit(1)
    ).first() is not None

    # ✅ Show current batch as the latest batch in the run (not today 00:00)
    today_lot = get_latest_material_lot_for_run(session, run_id)

    return templates.TemplateResponse(
        "entry_new.html",
        {
            "request": request,
            "user": user,
            "run": run,
            "params": params,
            "has_any": has_any,
            "error": error,
            "approved_lots": approved_lots,
            "has_any_event": has_any_event,
            "current_lot_preview": today_lot,
        },
    )


def get_latest_material_lot_for_run(session: Session, run_id: int):
    last_ev = session.exec(
        select(MaterialUseEvent)
        .where(MaterialUseEvent.run_id == run_id)
        .order_by(
            MaterialUseEvent.day.desc(),
            MaterialUseEvent.slot_time.desc(),
            MaterialUseEvent.created_at.desc(),
        )
    ).first()

    if not last_ev:
        return None
    return session.get(MaterialLot, last_ev.lot_id)


def apply_spec_check(param, v):
    """
    Returns (is_oos, note)
    - is_oos: True if value is out of spec
    - note: human-readable explanation
    """

    # Handle empty / missing values safely
    if v is None or (isinstance(v, str) and v.strip() == ""):
        return False, ""

    # Convert value to float if possible (common for measurements)
    try:
        val = float(v)
    except (TypeError, ValueError):
        # If it's not numeric, we can't spec-check it here
        return False, ""

    # Extract limits from param in a flexible way (dict or object)
    def get_attr(obj, *names):
        if obj is None:
            return None
        # dict style
        if isinstance(obj, dict):
            for n in names:
                if n in obj and obj[n] is not None:
                    return obj[n]
        # object style
        for n in names:
            if hasattr(obj, n):
                x = getattr(obj, n)
                if x is not None:
                    return x
        return None

    min_v = get_attr(param, "min_value", "min", "lower", "low_limit")
    max_v = get_attr(param, "max_value", "max", "upper", "high_limit")

    # Try to convert min/max if they exist
    try:
        min_v = float(min_v) if min_v is not None else None
    except (TypeError, ValueError):
        min_v = None
    try:
        max_v = float(max_v) if max_v is not None else None
    except (TypeError, ValueError):
        max_v = None

    # Spec checks
    if min_v is not None and val < min_v:
        return True, f"Below min ({val} < {min_v})"
    if max_v is not None and val > max_v:
        return True, f"Above max ({val} > {max_v})"

    return False, ""
   
@app.post("/runs/{run_id}/entry/new")
async def entry_new_post(
    run_id: int,
    request: Request,
    session: Session = Depends(get_session),
    actual_date: str = Form(...),
    actual_time: str = Form(...),
    operator_1: str = Form(""),
    operator_2: str = Form(""),
    operator_annular_12: str = Form(""),
    operator_int_ext_34: str = Form(""),
    remarks: str = Form(""),
    tool1_name: str = Form(""),
    tool1_serial: str = Form(""),
    tool1_calib_due: str = Form(""),
    tool2_name: str = Form(""),
    tool2_serial: str = Form(""),
    tool2_calib_due: str = Form(""),
    start_lot_id: str = Form(""),   # ✅ for first ever entry
):
    user = get_current_user(request, session)
    forbid_boss(user)

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    if run.status in ["CLOSED", "APPROVED"] and user.role != "MANAGER":
        raise HTTPException(403, "Run is not open")

    slot_time, day_add = slot_from_time_str(actual_time)
    day_obj = date.fromisoformat(actual_date) + timedelta(days=day_add)

    # prevent duplicate slot entry
    existing_for_slot = session.exec(
        select(InspectionEntry).where(
            InspectionEntry.run_id == run_id,
            InspectionEntry.actual_date == day_obj,
            InspectionEntry.slot_time == slot_time
        )
    ).first()
    if existing_for_slot:
        msg = "This timing slot is already inspected. Please confirm the time, or use Edit to change the existing record."
        return RedirectResponse(f"/runs/{run_id}/entry/new?error={msg}", status_code=302)

    form = await request.form()

    # batch-change checkbox + selected lot
    batch_changed = str(form.get("batch_changed", "")).strip() == "1"
    new_lot_id_raw = str(form.get("new_lot_id", "")).strip()
    
    # ✅ If user selected a new lot from dropdown, treat it as batch changed (even if checkbox not ticked)
    if new_lot_id_raw.isdigit():
        batch_changed = True


    # check if run has ANY material event yet
    has_any_event = session.exec(
        select(MaterialUseEvent.id).where(MaterialUseEvent.run_id == run_id).limit(1)
    ).first() is not None

    # If this is the FIRST entry EVER (no event exists), require start_lot_id
    if not has_any_event:
        if not str(start_lot_id).isdigit():
            msg = "Please select the STARTING approved RAW batch (first entry only)."
            return RedirectResponse(f"/runs/{run_id}/entry/new?error={msg}", status_code=302)

        lot = session.get(MaterialLot, int(start_lot_id))
        if (not lot) or (lot.status != "APPROVED") or (getattr(lot, "lot_type", "RAW") != "RAW"):
            msg = "Selected starting batch is not an APPROVED RAW batch."
            return RedirectResponse(f"/runs/{run_id}/entry/new?error={msg}", status_code=302)
            

        # Create the inspection entry first (NO batch yet)
    entry = InspectionEntry(
        run_id=run_id,
        actual_date=day_obj,
        actual_time=actual_time,
        slot_time=slot_time,
        inspector_id=user.id,
        operator_1=operator_1,
        operator_2=operator_2,
        operator_annular_12=operator_annular_12,
        operator_int_ext_34=operator_int_ext_34,
        remarks=remarks,
        tool1_name=tool1_name,
        tool1_serial=tool1_serial,
        tool1_calib_due=tool1_calib_due,
        tool2_name=tool2_name,
        tool2_serial=tool2_serial,
        tool2_calib_due=tool2_calib_due,
    )
    session.add(entry)
    session.commit()
    session.refresh(entry)

    # ✅ FIRST entry: create starting MaterialUseEvent at this slot
    if not has_any_event:
        session.add(MaterialUseEvent(
            run_id=run_id,
            day=day_obj,
            slot_time=slot_time,
            lot_id=int(start_lot_id),
            created_by_user_id=user.id,
        ))
        session.commit()

    # ✅ Batch changed: create new event
    if batch_changed:
        if not new_lot_id_raw.isdigit():
            msg = "Please select the NEW approved RAW batch when 'batch changed' is checked."
            return RedirectResponse(f"/runs/{run_id}/entry/new?error={msg}", status_code=302)

        new_lot = session.get(MaterialLot, int(new_lot_id_raw))
        if (not new_lot) or (new_lot.status != "APPROVED") or (getattr(new_lot, "lot_type", "RAW") != "RAW"):
            msg = "Selected NEW batch is not an APPROVED RAW batch."
            return RedirectResponse(f"/runs/{run_id}/entry/new?error={msg}", status_code=302)

        session.add(MaterialUseEvent(
            run_id=run_id,
            day=day_obj,
            slot_time=slot_time,
            lot_id=int(new_lot_id_raw),
            created_by_user_id=user.id,
        ))
        session.commit()

    # ✅ NOW we can read the current batch (because the events exist) and save it into the entry
    current_lot = get_current_material_lot_for_slot(session, run_id, day_obj, slot_time)
    entry.raw_material_batch_no = (current_lot.batch_no or "").strip() if current_lot else ""
    session.add(entry)
    session.commit()


    # save values (unchanged logic)
    params = session.exec(select(RunParameter).where(RunParameter.run_id == run_id)).all()
    by_key = {p.param_key: p for p in params}

    for key, param in by_key.items():
        raw = form.get(f"v_{key}")
        if raw in [None, ""]:
            continue
        v = _safe_float(raw)
        if v is None:
            continue
        is_oos, note = apply_spec_check(param, v)
        session.add(InspectionValue(
            entry_id=entry.id,
            param_key=key,
            value=v,
            is_out_of_spec=is_oos,
            spec_note=note,
        ))

    session.commit()
    return RedirectResponse(f"/runs/{run_id}?day={entry.actual_date.isoformat()}", status_code=302)


@app.get("/runs/{run_id}/entry/{slot_time}/fill/{param_key}", response_class=HTMLResponse)
def fill_missing_value_get(
    run_id: int,
    slot_time: str,
    param_key: str,
    request: Request,
    day: str,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    forbid_boss(user)

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    # permissions
    if run.status in ["CLOSED", "APPROVED"] and (user.role or "").upper() != "MANAGER":
        raise HTTPException(403, "Run is closed")

    day_obj = date.fromisoformat(day)

    entry = session.exec(
        select(InspectionEntry).where(
            InspectionEntry.run_id == run_id,
            InspectionEntry.actual_date == day_obj,
            InspectionEntry.slot_time == slot_time,
        )
    ).first()
    if not entry:
        raise HTTPException(404, "No inspection entry for this slot/day")

    # param
    param = session.exec(
        select(RunParameter).where(RunParameter.run_id == run_id, RunParameter.param_key == param_key)
    ).first()
    if not param:
        raise HTTPException(404, "Parameter not found")

    # already exists? send to normal edit
    existing = session.exec(
        select(InspectionValue).where(InspectionValue.entry_id == entry.id, InspectionValue.param_key == param_key)
    ).first()
    if existing:
        return RedirectResponse(f"/values/{existing.id}/edit", status_code=302)

    return templates.TemplateResponse(
        "value_fill.html",
        {"request": request, "user": user, "run": run, "entry": entry, "param": param, "error": ""},
    )

@app.post("/runs/{run_id}/entry/{slot_time}/fill/{param_key}")
async def fill_missing_value_post(
    run_id: int,
    slot_time: str,
    param_key: str,
    request: Request,
    day: str = Form(...),
    new_value: str = Form(...),
    note: str = Form(""),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    forbid_boss(user)

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    # permissions
    if run.status in ["CLOSED", "APPROVED"] and (user.role or "").upper() != "MANAGER":
        raise HTTPException(403, "Run is closed")

    day_obj = date.fromisoformat(day)

    entry = session.exec(
        select(InspectionEntry).where(
            InspectionEntry.run_id == run_id,
            InspectionEntry.actual_date == day_obj,
            InspectionEntry.slot_time == slot_time,
        )
    ).first()
    if not entry:
        raise HTTPException(404, "No inspection entry for this slot/day")

    param = session.exec(
        select(RunParameter).where(RunParameter.run_id == run_id, RunParameter.param_key == param_key)
    ).first()
    if not param:
        raise HTTPException(404, "Parameter not found")

    # prevent duplicates
    existing = session.exec(
        select(InspectionValue).where(InspectionValue.entry_id == entry.id, InspectionValue.param_key == param_key)
    ).first()
    if existing:
        return RedirectResponse(f"/runs/{run_id}?day={day_obj.isoformat()}", status_code=302)

    v = _safe_float(new_value)
    if v is None:
        return RedirectResponse(f"/runs/{run_id}?day={day_obj.isoformat()}", status_code=302)

    is_oos, spec_note = apply_spec_check(param, v)

    new_iv = InspectionValue(
        entry_id=entry.id,
        param_key=param_key,
        value=v,
        is_out_of_spec=is_oos,
        spec_note=spec_note,
    )
    session.add(new_iv)
    session.commit()
    session.refresh(new_iv)

    # optional: store audit (if your audit table allows it)
    session.add(InspectionValueAudit(
        inspection_value_id=new_iv.id,
        action="CREATED",
        old_value=None,
        new_value=v,
        by_user_id=user.id,
        by_user_name=user.display_name,
        note=note or "",
    ))
    session.commit()

    return RedirectResponse(f"/runs/{run_id}?day={day_obj.isoformat()}", status_code=302)


# ===== VALUE EDIT + APPROVAL (kept as your working logic) =====
@app.get("/values/{value_id}/edit", response_class=HTMLResponse)
def value_edit_get(value_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)

    v = session.get(InspectionValue, value_id)
    if not v:
        raise HTTPException(404, "Value not found")
    entry = session.get(InspectionEntry, v.entry_id)
    run = session.get(ProductionRun, entry.run_id) if entry else None
    if not entry or not run:
        raise HTTPException(404, "Run/Entry not found")

    if run.status in ["CLOSED", "APPROVED"] and user.role != "MANAGER":
        raise HTTPException(403, "Run is not open")

    param = session.exec(
        select(RunParameter).where(RunParameter.run_id == run.id, RunParameter.param_key == v.param_key)
    ).first()

    return templates.TemplateResponse(
        "value_edit.html",
        {"request": request, "user": user, "run": run, "entry": entry, "param": param, "v": v, "error": ""},
    )


@app.post("/values/{value_id}/edit")
async def value_edit_post(
    value_id: int,
    request: Request,
    session: Session = Depends(get_session),
    new_value: str = Form(...),
    note: str = Form(""),
):
    user = get_current_user(request, session)
    forbid_boss(user)

    v = session.get(InspectionValue, value_id)
    if not v:
        raise HTTPException(404, "Value not found")
    entry = session.get(InspectionEntry, v.entry_id)
    run = session.get(ProductionRun, entry.run_id) if entry else None
    if not entry or not run:
        raise HTTPException(404, "Run/Entry not found")

    if run.status in ["CLOSED", "APPROVED"] and user.role != "MANAGER":
        raise HTTPException(403, "Run is not open")

    nv = _safe_float(new_value)
    if nv is None:
        return RedirectResponse(f"/values/{value_id}/edit", status_code=302)

    v.pending_value = nv
    v.pending_status = "PENDING"
    v.pending_by_user_id = user.id
    v.pending_at = datetime.utcnow()
    session.add(v)

    session.add(InspectionValueAudit(
        inspection_value_id=v.id,
        action="PROPOSED",
        old_value=v.value,
        new_value=nv,
        by_user_id=user.id,
        by_user_name=user.display_name,
        note=note or "",
    ))

    session.commit()
    return RedirectResponse(f"/runs/{run.id}?day={entry.actual_date.isoformat()}", status_code=302)


@app.get("/runs/{run_id}/pending", response_class=HTMLResponse)
def pending_list(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)
        

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    pending_items = []
    entries = session.exec(select(InspectionEntry).where(InspectionEntry.run_id == run_id)).all()
    entry_ids = [e.id for e in entries]
    if entry_ids:
        vals = session.exec(
            select(InspectionValue).where(
                InspectionValue.entry_id.in_(entry_ids),
                InspectionValue.pending_status == "PENDING"
            )
        ).all()

        params = session.exec(select(RunParameter).where(RunParameter.run_id == run_id)).all()
        pmap = {p.param_key: p for p in params}
        emap = {e.id: e for e in entries}

        for v in vals:
            pending_items.append({"value": v, "entry": emap.get(v.entry_id), "param": pmap.get(v.param_key)})

    return templates.TemplateResponse(
        "pending_list.html",
        {"request": request, "user": user, "run": run, "items": pending_items},
    )


@app.post("/values/{value_id}/approve")
def value_approve(value_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)
        

    v = session.get(InspectionValue, value_id)
    if not v:
        raise HTTPException(404, "Value not found")
    entry = session.get(InspectionEntry, v.entry_id)
    run = session.get(ProductionRun, entry.run_id) if entry else None
    if not entry or not run:
        raise HTTPException(404, "Run/Entry not found")

    if v.pending_status != "PENDING" or v.pending_value is None:
        return RedirectResponse(f"/runs/{run.id}/pending", status_code=302)

    old = v.value
    new = v.pending_value

    v.value = new
    v.pending_status = "APPROVED"
    session.add(v)

    session.add(InspectionValueAudit(
        inspection_value_id=v.id,
        action="APPROVED",
        old_value=old,
        new_value=new,
        by_user_id=user.id,
        by_user_name=user.display_name,
        note="",
    ))

    v.pending_value = None
    v.pending_status = ""
    v.pending_by_user_id = None
    v.pending_at = None
    session.add(v)

    session.commit()
    return RedirectResponse(f"/runs/{run.id}/pending", status_code=302)


@app.post("/values/{value_id}/reject")
def value_reject(value_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    require_manager(user)
       

    v = session.get(InspectionValue, value_id)
    if not v:
        raise HTTPException(404, "Value not found")
    entry = session.get(InspectionEntry, v.entry_id)
    run = session.get(ProductionRun, entry.run_id) if entry else None
    if not entry or not run:
        raise HTTPException(404, "Run/Entry not found")

    if v.pending_status != "PENDING":
        return RedirectResponse(f"/runs/{run.id}/pending", status_code=302)

    session.add(InspectionValueAudit(
        inspection_value_id=v.id,
        action="REJECTED",
        old_value=v.value,
        new_value=v.pending_value,
        by_user_id=user.id,
        by_user_name=user.display_name,
        note="",
    ))

    v.pending_value = None
    v.pending_status = ""
    v.pending_by_user_id = None
    v.pending_at = None
    session.add(v)

    session.commit()
    return RedirectResponse(f"/runs/{run.id}/pending", status_code=302)
from openpyxl.cell.cell import MergedCell
import subprocess
import tempfile
from pathlib import Path
from fastapi.responses import Response


def _set_cell_safe(ws, addr: str, value, number_format: str | None = None):
    """
    Write value to Excel, even if the target address is part of a merged range.
    Also optionally apply number_format to the actual written cell.
    """
    target_addr = addr
    for rng in ws.merged_cells.ranges:
        if addr in rng:
            target_addr = rng.coord.split(":")[0]  # top-left
            break

    c = ws[target_addr]
    c.value = value
    if number_format:
        c.number_format = number_format



def _clone_sheet_no_drawings(wb, src_ws, title: str):
    """
    Clone a worksheet WITHOUT drawings/images (prevents crash).
    Preserves:
      - values
      - styles
      - merged cells
      - row/col dimensions
    """
    dst = wb.create_sheet(title=title)

    for col, dim in src_ws.column_dimensions.items():
        dst.column_dimensions[col].width = dim.width
    for row, dim in src_ws.row_dimensions.items():
        dst.row_dimensions[row].height = dim.height

    for merged_range in list(src_ws.merged_cells.ranges):
        dst.merge_cells(str(merged_range))

    for row in src_ws.iter_rows():
        for cell in row:
            new_cell = dst.cell(row=cell.row, column=cell.col_idx, value=cell.value)
            if cell.has_style:
                new_cell._style = cell._style
                new_cell.number_format = cell.number_format
                new_cell.font = cell.font
                new_cell.border = cell.border
                new_cell.fill = cell.fill
                new_cell.alignment = cell.alignment
                new_cell.protection = cell.protection

    return dst


def convert_xlsx_bytes_to_pdf_bytes(xlsx_bytes: bytes) -> bytes:
    """
    Uses LibreOffice headless to convert XLSX -> PDF.
    Requires 'soffice' available in the runtime image.
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        xlsx_path = tmpdir / "report.xlsx"
        out_dir = tmpdir / "out"
        out_dir.mkdir(parents=True, exist_ok=True)

        xlsx_path.write_bytes(xlsx_bytes)

        cmd = [
            "soffice",
            "--headless",
            "--nologo",
            "--nofirststartwizard",
            "--invisible",
            "--convert-to", "pdf",
            "--outdir", str(out_dir),
            str(xlsx_path),
        ]
        # capture output for debugging
        # run conversion and capture output
        try:
            r = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except FileNotFoundError:
            raise RuntimeError("PDF export failed: LibreOffice 'soffice' is not installed / not found on this server.")
        except subprocess.CalledProcessError as e:
            out = (e.stdout or b"").decode("utf-8", errors="ignore")
            err = (e.stderr or b"").decode("utf-8", errors="ignore")
            raise RuntimeError(
                "PDF export failed: LibreOffice conversion error.\n\n"
                f"CMD: {' '.join(cmd)}\n\nSTDOUT:\n{out}\n\nSTDERR:\n{err}\n"
            )

        # find produced pdf
        pdfs = list(out_dir.glob("*.pdf"))
        if not pdfs:
            out = (r.stdout or b"").decode("utf-8", errors="ignore")
            err = (r.stderr or b"").decode("utf-8", errors="ignore")
            raise RuntimeError(
                "PDF conversion failed: LibreOffice produced no PDF output.\n\n"
                f"CMD: {' '.join(cmd)}\n\nSTDOUT:\n{out}\n\nSTDERR:\n{err}\n"
            )

        pdf_path = pdfs[0]
        pdf_bytes = pdf_path.read_bytes()

        # ✅ critical validation
        if not pdf_bytes or len(pdf_bytes) < 10:
            out = (r.stdout or b"").decode("utf-8", errors="ignore")
            err = (r.stderr or b"").decode("utf-8", errors="ignore")
            raise RuntimeError(
                "PDF conversion failed: LibreOffice created an EMPTY PDF (0 bytes).\n\n"
                f"PDF path: {pdf_path}\n"
                f"PDF size: {len(pdf_bytes)} bytes\n\n"
                f"CMD: {' '.join(cmd)}\n\nSTDOUT:\n{out}\n\nSTDERR:\n{err}\n"
            )

        # optional but very helpful: check pdf signature
        if not pdf_bytes.startswith(b"%PDF"):
            out = (r.stdout or b"").decode("utf-8", errors="ignore")
            err = (r.stderr or b"").decode("utf-8", errors="ignore")
            head = pdf_bytes[:80]
            raise RuntimeError(
                "PDF conversion failed: output is not a valid PDF.\n\n"
                f"PDF path: {pdf_path}\n"
                f"PDF size: {len(pdf_bytes)} bytes\n"
                f"First bytes: {head!r}\n\n"
                f"CMD: {' '.join(cmd)}\n\nSTDOUT:\n{out}\n\nSTDERR:\n{err}\n"
            )

        return pdf_bytes


def build_one_day_workbook_bytes(run_id: int, day: date, session: Session) -> bytes:
    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    template_path = TEMPLATE_XLSX_MAP.get(run.process)
    if not template_path or not os.path.exists(template_path):
        raise HTTPException(404, f"Template not found: {template_path}")

    # Load the template fresh (images/logos stay)
    wb = openpyxl.load_workbook(template_path)
    ws = wb.worksheets[0]

    # ✅ Print setup (keep 1-page)
    apply_pdf_page_setup(ws)
    apply_specs_to_template(ws, run, session)
    

    # ✅ Per-process coordinates (THIS was missing and causing crashes)
    if run.process in ["LINER", "COVER"]:
        col_start = 5  # E
        date_row = 20
        time_row = 21
        inspector_row = 38
        op1_row = 39
        op2_row = 40
        row_map = ROW_MAP_LINER_COVER

        # Header cells (LINER/COVER)
        _set_cell_safe(ws, "E5", run.dhtp_batch_no)
        _set_cell_safe(ws, "I5", run.client_name)
        _set_cell_safe(ws, "I6", run.po_number)
        _set_cell_safe(ws, "E6", run.pipe_specification)
        _set_cell_safe(ws, "E7", run.raw_material_spec)
        _set_cell_safe(ws, "E9", run.itp_number)

    else:  # REINFORCEMENT
        col_start = 6   # F
        date_row = 19
        time_row = 20
        inspector_row = 35
        op1_row = 36
        op2_row = 37
        row_map = ROW_MAP_REINF

        # ✅ IMPORTANT:
        # Put ONLY the reinforcement header cells here (do NOT write E5/I5 again)
        # Use the real cells from your reinforcement.xlsx template
        _set_cell_safe(ws, "D4", run.dhtp_batch_no)
        _set_cell_safe(ws, "I4", run.client_name)
        _set_cell_safe(ws, "I5", run.po_number)
        _set_cell_safe(ws, "D5", run.pipe_specification)
        _set_cell_safe(ws, "D6", run.raw_material_spec)
        _set_cell_safe(ws, "D8", run.itp_number)

    # Machines Used
    machines = session.exec(select(RunMachine).where(RunMachine.run_id == run_id)).all()
    for idx in range(5):
        r = 4 + idx
        name = machines[idx].machine_name if idx < len(machines) else ""
        tag = machines[idx].machine_tag if (idx < len(machines) and machines[idx].machine_tag) else ""
        _set_cell_safe(ws, f"M{r}", name)
        _set_cell_safe(ws, f"P{r}", tag)
    
    # ✅ Date row (IMPORTANT: format as DATE so it doesn't show 12:00 AM)
    for slot_idx, slot in enumerate(SLOTS):
        col = openpyxl.utils.get_column_letter(col_start + slot_idx)
        _set_cell_safe(ws, f"{col}{date_row}", day, number_format="m/d/yyyy")
    
    # ✅ Time row: leave blank by default (we will fill actual times from entries)
    for slot_idx, _slot in enumerate(SLOTS):
        col = openpyxl.utils.get_column_letter(col_start + slot_idx)
        _set_cell_safe(ws, f"{col}{time_row}", "", number_format="h:mm")
    


    # Trace for THIS day: raw batch + tools
    trace_today = get_day_latest_trace(session, run_id, day)
    carry = get_last_known_trace_before_day(session, run_id, day)

    raw_batches = trace_today["raw_batches"] or ([carry["raw"]] if carry["raw"] else [])
    raw_str = ", ".join([x for x in raw_batches if x])
    if raw_str:
        if run.process in ["LINER", "COVER"]:
            _set_cell_safe(ws, "E8", raw_str)   # LINER/COVER: Raw Material Batch No.
        else:
            _set_cell_safe(ws, "D7", raw_str)   # REINFORCEMENT (keep as-is)


    tools = trace_today["tools"] or carry["tools"]
    for t_idx in range(2):
        r = 8 + t_idx
        if t_idx < len(tools):
            name, serial, calib = tools[t_idx]
            _set_cell_safe(ws, f"G{r}", name or "")
            _set_cell_safe(ws, f"I{r}", serial or "")
            _set_cell_safe(ws, f"K{r}", calib or "")

    apply_specs_to_template(ws, run, session)

    # Fill inspector/operators per slot + values
    day_entries = session.exec(
        select(InspectionEntry)
        .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date == day)
        .order_by(InspectionEntry.created_at)
    ).all()

    user_map = {u.id: u for u in session.exec(select(User)).all()}

    for e in day_entries:
        if e.slot_time not in SLOTS:
            continue
        slot_idx = SLOTS.index(e.slot_time)
        col = openpyxl.utils.get_column_letter(col_start + slot_idx)
        # ✅ write ACTUAL time under the slot header (export uses actual time)
        try:
            hh, mm = (e.actual_time or "00:00").split(":")
            _set_cell_safe(ws, f"{col}{time_row}", dtime(int(hh), int(mm)), number_format="h:mm")
        except Exception:
            # if actual_time is weird, keep it blank instead of crashing export
            pass


        inspector_name = user_map.get(e.inspector_id).display_name if e.inspector_id in user_map else ""
        _set_cell_safe(ws, f"{col}{inspector_row}", inspector_name)

        if run.process in ["LINER", "COVER"]:
            _set_cell_safe(ws, f"{col}{op1_row}", e.operator_1 or "")
            _set_cell_safe(ws, f"{col}{op2_row}", e.operator_2 or "")
        else:
            _set_cell_safe(ws, f"{col}{op1_row}", e.operator_annular_12 or "")
            _set_cell_safe(ws, f"{col}{op2_row}", e.operator_int_ext_34 or "")

        vals = session.exec(select(InspectionValue).where(InspectionValue.entry_id == e.id)).all()
        for v in vals:
            r = row_map.get(v.param_key)
            if not r:
                continue
            _set_cell_safe(ws, f"{col}{r}", v.value)

    out = BytesIO()
    wb.save(out)
    out.seek(0)
    return out.getvalue()


from pypdf import PdfReader, PdfWriter
from pypdf import Transformation
from io import BytesIO
import os






def build_export_xlsx_bytes(run_id: int, request: Request, session: Session) -> tuple[bytes, str]:
    """
    Build the XLSX export in memory and return (bytes, filename_base).
    This is used by BOTH /export/xlsx and /export/pdf.
    """
    user = get_current_user(request, session)

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    template_path = TEMPLATE_XLSX_MAP.get(run.process)
    if not template_path or not os.path.exists(template_path):
        raise HTTPException(404, f"Template not found: {template_path}")

    days = get_days_for_run(session, run_id)
    if not days:
        raise HTTPException(400, "No entries to export")

    base_wb = openpyxl.load_workbook(template_path)
    base_ws = base_wb.worksheets[0]

    # per process coordinates
    if run.process in ["LINER", "COVER"]:
        col_start = 5  # E
        date_row = 20
        inspector_row = 38
        op1_row = 39
        op2_row = 40
        row_map = ROW_MAP_LINER_COVER
    else:
        col_start = 6  # F
        date_row = 20
        inspector_row = 36
        op1_row = 37
        op2_row = 38
        row_map = ROW_MAP_REINF

    machines = session.exec(select(RunMachine).where(RunMachine.run_id == run_id)).all()
    user_map = {u.id: u for u in session.exec(select(User)).all()}

    for i, day in enumerate(days):
        title = f"Day {i+1} ({day.isoformat()})"

        if i == 0:
            ws = base_ws
            ws.title = title
        else:
            ws = _clone_sheet_no_drawings(base_wb, base_ws, title)

        # ----- HEADER (fixed cells) -----
        _set_cell_safe(ws, "E5", run.dhtp_batch_no)      # Batch
        _set_cell_safe(ws, "I5", run.client_name)        # Client
        _set_cell_safe(ws, "I6", run.po_number)          # PO
        _set_cell_safe(ws, "E6", run.pipe_specification) # Pipe Spec
        _set_cell_safe(ws, "E7", run.raw_material_spec)  # Raw Spec
        _set_cell_safe(ws, "E9", run.itp_number)         # ITP

        # ----- Machines (M4:P8) -----
        for idx in range(5):
            r = 4 + idx
            name = machines[idx].machine_name if idx < len(machines) else ""
            tag = machines[idx].machine_tag if (idx < len(machines) and machines[idx].machine_tag) else ""
            _set_cell_safe(ws, f"M{r}", name)
            _set_cell_safe(ws, f"P{r}", tag)

        # ----- Day trace (raw batch + tools) -----
        trace_today = get_day_latest_trace(session, run_id, day)
        carry = get_last_known_trace_before_day(session, run_id, day)

        raw_batches = trace_today["raw_batches"] or ([carry["raw"]] if carry["raw"] else [])
        raw_str = ", ".join([x for x in raw_batches if x])
        if raw_str:
            if run.process in ["LINER", "COVER"]:
                _set_cell_safe(ws, "E8", raw_str)
            else:
                _set_cell_safe(ws, "D7", raw_str)


        tools = trace_today["tools"] or carry["tools"]
        for t_idx in range(2):
            r = 8 + t_idx
            if t_idx < len(tools):
                name, serial, calib = tools[t_idx]
                _set_cell_safe(ws, f"G{r}", name or "")
                _set_cell_safe(ws, f"I{r}", serial or "")
                _set_cell_safe(ws, f"K{r}", calib or "")

        # ----- Date row for each time slot -----
        for slot_idx, slot in enumerate(SLOTS):
            col = openpyxl.utils.get_column_letter(col_start + slot_idx)
            _set_cell_safe(ws, f"{col}{date_row}", day)
        # ----- Time header row: blank by default (we will write actual times if entry exists) -----
        time_row = 21
        for slot_idx, _slot in enumerate(SLOTS):
            col = openpyxl.utils.get_column_letter(col_start + slot_idx)
            _set_cell_safe(ws, f"{col}{time_row}", "", number_format="h:mm")

        # ✅ print setup (important for PDF export)
        apply_pdf_page_setup(ws)
                

        
        # ----- Fill per-slot inspector/operators + values -----
        day_entries = session.exec(
            select(InspectionEntry)
            .where(InspectionEntry.run_id == run_id, InspectionEntry.actual_date == day)
            .order_by(InspectionEntry.created_at)
        ).all()

        for e in day_entries:
            if e.slot_time not in SLOTS:
                continue

            slot_idx = SLOTS.index(e.slot_time)
            col = openpyxl.utils.get_column_letter(col_start + slot_idx)

            inspector_name = user_map.get(e.inspector_id).display_name if e.inspector_id in user_map else ""
            _set_cell_safe(ws, f"{col}{inspector_row}", inspector_name)

            if run.process in ["LINER", "COVER"]:
                _set_cell_safe(ws, f"{col}{op1_row}", e.operator_1 or "")
                _set_cell_safe(ws, f"{col}{op2_row}", e.operator_2 or "")
            else:
                _set_cell_safe(ws, f"{col}{op1_row}", e.operator_annular_12 or "")
                _set_cell_safe(ws, f"{col}{op2_row}", e.operator_int_ext_34 or "")

            vals = session.exec(select(InspectionValue).where(InspectionValue.entry_id == e.id)).all()
            for v in vals:
                r = row_map.get(v.param_key)
                if not r:
                    continue
                _set_cell_safe(ws, f"{col}{r}", v.value)

            # ✅ write ACTUAL time for this slot
            try:
                hh, mm = (e.actual_time or "00:00").split(":")
                _set_cell_safe(ws, f"{col}{time_row}", dtime(int(hh), int(mm)), number_format="h:mm")
            except Exception:
                pass


    out = BytesIO()
    base_wb.save(out)
    out.seek(0)

    filename_base = f"{run.process}_{run.dhtp_batch_no}_ALL_DAYS"
    return out.getvalue(), filename_base
    

# =========================
# MRR EXPORT (per MaterialLot)
# =========================

def build_mrr_xlsx_bytes(lot_id: int, session: Session) -> bytes:
    """
    Build an MRR XLSX directly in code (no template file needed).
    This avoids missing-template issues and makes export stable.
    """
    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    docs = session.exec(
        select(MrrDocument)
        .where(MrrDocument.ticket_id == lot_id)
        .order_by(MrrDocument.created_at.asc())
    ).all()

    receiving = session.exec(
        select(MrrReceiving).where(MrrReceiving.ticket_id == lot_id)
    ).first()

    shipments = session.exec(
        select(MrrReceivingInspection)
        .where(
            (MrrReceivingInspection.ticket_id == lot_id) &
            (MrrReceivingInspection.inspector_confirmed == True)
        )
        .order_by(MrrReceivingInspection.created_at.asc())
    ).all()

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "MRR"

    # Basic formatting
    ws["A1"] = f"MRR Ticket #{lot.id}"
    ws["A1"].font = openpyxl.styles.Font(bold=True, size=16)

    row = 3
    def put(label, value):
        nonlocal row
        ws[f"A{row}"] = label
        ws[f"B{row}"] = value
        ws[f"A{row}"].font = openpyxl.styles.Font(bold=True)
        row += 1

    put("Type", lot.lot_type or "")
    put("Batch No", lot.batch_no or "")
    put("Status", lot.status or "")
    put("Material", lot.material_name or "")
    put("Supplier", lot.supplier_name or "")
    put("PO Number", lot.po_number or "")
    put("PO Quantity", f"{lot.quantity or 0} {lot.quantity_unit or ''}".strip())

    # received_total is stored normalized in KG for weight units
    unit = (lot.quantity_unit or "KG").upper().strip()
    if unit in ["PC", "PCS"]:
        put("Received Total", f"{lot.received_total or 0} {unit}")
    else:
        put("Received Total", f"{lot.received_total or 0} KG (normalized)")

    row += 1

    # Documentation status
    ws[f"A{row}"] = "Documentation"
    ws[f"A{row}"].font = openpyxl.styles.Font(bold=True, size=12)
    row += 1

    if receiving:
        put("Saved", "YES")
        cleared = bool(getattr(receiving, "inspector_confirmed_po", False) or getattr(receiving, "manager_confirmed_po", False))
        put("Cleared", "YES" if cleared else "NO")

        put("Inspector PO No.", getattr(receiving, "inspector_po_number", "") or "")

        # Receiving doc qty fields (these DO exist in your model)
        doc_qty = getattr(receiving, "qty_arrived", None)
        doc_unit = getattr(receiving, "qty_unit", "KG") or "KG"
        put("Arrived Qty (Doc)", f"{doc_qty if doc_qty is not None else ''} {doc_unit}".strip())

        is_partial = bool(getattr(receiving, "is_partial_delivery", False))
        put("Partial Delivery", "YES" if is_partial else "NO")
        put("Mismatch/Partial Reason", getattr(receiving, "qty_mismatch_reason", "") or "")

        put("Received Date", str(getattr(receiving, "received_date", "") or ""))
        put("Remarks (Doc)", getattr(receiving, "remarks", "") or "")
    else:
        put("Saved", "NO")
        put("Cleared", "NO")

    row += 1

    # Documents table
    ws[f"A{row}"] = "Documents"
    ws[f"A{row}"].font = openpyxl.styles.Font(bold=True, size=12)
    row += 1

    headers = ["Type", "Name", "Number", "Uploaded By", "Uploaded At"]
    for col, h in enumerate(headers, start=1):
        cell = ws.cell(row=row, column=col, value=h)
        cell.font = openpyxl.styles.Font(bold=True)
    row += 1

    for d in docs:
        ws.cell(row=row, column=1, value=d.doc_type or "")
        ws.cell(row=row, column=2, value=d.doc_name or "")
        ws.cell(row=row, column=3, value=d.doc_number or "")
        ws.cell(row=row, column=4, value=d.uploaded_by_user_name or "")
        ws.cell(row=row, column=5, value=str(getattr(d, "created_at", "") or ""))
        row += 1

    row += 1

    # Shipments table
    ws[f"A{row}"] = "Submitted Shipments"
    ws[f"A{row}"].font = openpyxl.styles.Font(bold=True, size=12)
    row += 1

    ship_headers = ["#", "DN", "Arrived Qty", "Unit", "Report No", "Submitted", "Manager Approved"]
    for col, h in enumerate(ship_headers, start=1):
        cell = ws.cell(row=row, column=col, value=h)
        cell.font = openpyxl.styles.Font(bold=True)
    row += 1

    for idx, s in enumerate(shipments, start=1):
        ws.cell(row=row, column=1, value=idx)
        ws.cell(row=row, column=2, value=s.delivery_note_no or "")
        ws.cell(row=row, column=3, value=float(s.qty_arrived or 0))
        ws.cell(row=row, column=4, value=s.qty_unit or "")
        ws.cell(row=row, column=5, value=s.report_no or "")
        ws.cell(row=row, column=6, value="YES" if s.inspector_confirmed else "NO")
        ws.cell(row=row, column=7, value="YES" if s.manager_approved else "NO")
        row += 1

    # Column widths
    ws.column_dimensions["A"].width = 22
    ws.column_dimensions["B"].width = 40
    ws.column_dimensions["C"].width = 18
    ws.column_dimensions["D"].width = 20
    ws.column_dimensions["E"].width = 22
    ws.column_dimensions["F"].width = 18
    ws.column_dimensions["G"].width = 18

    apply_pdf_page_setup(ws)

    out = BytesIO()
    wb.save(out)
    out.seek(0)
    return out.getvalue()


def build_mrr_photo_appendix_pdf_bytes(lot_id: int, session: Session) -> bytes:
    """
    Build a PDF appendix containing all photo evidence (grouped) for submitted shipments.
    Appended to the base MRR PDF.
    """
    submitted_inspections = session.exec(
        select(MrrReceivingInspection)
        .where(
            (MrrReceivingInspection.ticket_id == lot_id) &
            (MrrReceivingInspection.inspector_confirmed == True)
        )
        .order_by(MrrReceivingInspection.created_at.asc())
    ).all()

    insp_ids = [i.id for i in submitted_inspections if i.id is not None]
    if not insp_ids:
        return b""

    photos = session.exec(
        select(MrrInspectionPhoto)
        .where(
            (MrrInspectionPhoto.ticket_id == lot_id) &
            (MrrInspectionPhoto.inspection_id.in_(insp_ids))
        )
        .order_by(MrrInspectionPhoto.created_at.asc())
    ).all()

    if not photos:
        return b""

    insp_by_id = {i.id: i for i in submitted_inspections if i.id is not None}

    grouped: Dict[int, Dict[str, List[MrrInspectionPhoto]]] = {}
    for p in photos:
        grouped.setdefault(int(p.inspection_id), {})
        g = (p.group_name or "General").strip() or "General"
        grouped[int(p.inspection_id)].setdefault(g, []).append(p)

    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    page_w, page_h = A4
    margin = 46  # ~0.65 inch

    def header(title: str, subtitle: str = ""):
        y = page_h - margin
        c.setFont("Helvetica-Bold", 16)
        c.drawString(margin, y, title)
        y -= 18
        if subtitle:
            c.setFont("Helvetica", 10)
            c.drawString(margin, y, subtitle)
            y -= 14
        c.setLineWidth(0.8)
        c.line(margin, y, page_w - margin, y)
        return y - 16

    y = header("MRR Photo Evidence Appendix", f"Ticket #{lot_id}")
    c.setFont("Helvetica", 10)
    c.drawString(margin, y, "This appendix contains photo evidence attached to submitted shipment inspections.")
    c.showPage()

    max_w = page_w - 2 * margin
    max_h = 320  # keep room for captions

    for iid in insp_ids:
        insp = insp_by_id.get(iid)
        if not insp:
            continue

        dn = (insp.delivery_note_no or "").strip() or "-"
        rep = (insp.report_no or "").strip() or "-"

        y = header("Shipment Inspection Photos", f"DN: {dn} | Report: {rep} | Inspection ID: {iid}")

        for gname, items in grouped.get(iid, {}).items():
            if y < margin + 160:
                c.showPage()
                y = header("Shipment Inspection Photos", f"DN: {dn} | Report: {rep} | Inspection ID: {iid}")

            c.setFont("Helvetica-Bold", 12)
            c.drawString(margin, y, f"Group: {gname}")
            y -= 16

            for p in items:
                path = p.file_path or ""
                if not path or not os.path.exists(path):
                    continue

                caption = (p.caption or "").strip()

                needed = max_h + (40 if caption else 20)
                if y < margin + needed:
                    c.showPage()
                    y = header("Shipment Inspection Photos", f"DN: {dn} | Report: {rep} | Inspection ID: {iid}")
                    c.setFont("Helvetica-Bold", 12)
                    c.drawString(margin, y, f"Group: {gname}")
                    y -= 16

                try:
                    img = ImageReader(path)
                    iw, ih = img.getSize()
                    if iw <= 0 or ih <= 0:
                        continue
                    scale = min(max_w / iw, max_h / ih)
                    dw = iw * scale
                    dh = ih * scale

                    c.drawImage(img, margin, y - dh, width=dw, height=dh, preserveAspectRatio=True, mask="auto")
                    y -= dh + 8

                    if caption:
                        c.setFont("Helvetica", 9)
                        c.drawString(margin, y, caption[:180])
                        y -= 14
                    else:
                        y -= 8

                    c.setLineWidth(0.3)
                    c.line(margin, y, page_w - margin, y)
                    y -= 12
                except Exception:
                    continue

        c.showPage()

    c.save()
    out = buf.getvalue()
    buf.close()
    return out


def merge_pdf_bytes(base_pdf: bytes, appendix_pdf: bytes) -> bytes:
    if not appendix_pdf:
        return base_pdf

    base_reader = PdfReader(BytesIO(base_pdf))
    app_reader = PdfReader(BytesIO(appendix_pdf))

    w = PdfWriter()
    for p in base_reader.pages:
        w.add_page(p)
    for p in app_reader.pages:
        w.add_page(p)

    out = BytesIO()
    w.write(out)
    out.seek(0)
    return out.getvalue()


@app.get("/mrr/{lot_id}/export/xlsx")
def mrr_export_xlsx(lot_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)  # allow any logged-in reviewer

    xlsx_bytes = build_mrr_xlsx_bytes(lot_id, session)
    filename = f"MRR_{lot_id}.xlsx"

    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/mrr/{lot_id}/export/pdf")
def mrr_export_pdf(lot_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)  # allow any logged-in reviewer

    # 1) Build XLSX
    xlsx_bytes = build_mrr_xlsx_bytes(lot_id, session)

    # 2) Convert to PDF (existing pipeline)
    pdf_bytes = convert_xlsx_bytes_to_pdf_bytes(xlsx_bytes)

    # 3) Append photos
    appendix = build_mrr_photo_appendix_pdf_bytes(lot_id, session)
    final_pdf = merge_pdf_bytes(pdf_bytes, appendix)

    filename = f"MRR_{lot_id}.pdf"
    return Response(
        content=final_pdf,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

        # =========================
# MRR MANAGER APPROVAL
# =========================

@app.post("/mrr/{lot_id}/inspection/id/{inspection_id}/approve")
def mrr_approve_receiving_inspection(
    lot_id: int,
    inspection_id: int,
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    require_manager(user)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    if (lot.status or "").upper() == "CANCELED":
        return RedirectResponse(f"/mrr/{lot_id}?error=Ticket%20is%20canceled", status_code=303)

    insp = session.get(MrrReceivingInspection, inspection_id)
    if not insp or insp.ticket_id != lot_id:
        raise HTTPException(404, "MRR Inspection not found")

    # Must be submitted first
    if not insp.inspector_confirmed:
        return RedirectResponse(
            f"/mrr/{lot_id}?error=Inspection%20must%20be%20submitted%20before%20approval",
            status_code=303,
        )

    # Approve this inspection
    insp.manager_approved = True
    session.add(insp)

    # =========================
    # Recompute totals from APPROVED shipments (source of truth)
    # =========================
    approved_shipments = session.exec(
        select(MrrReceivingInspection).where(
            (MrrReceivingInspection.ticket_id == lot_id)
            & (MrrReceivingInspection.inspector_confirmed == True)
            & (MrrReceivingInspection.manager_approved == True)
        )
    ).all()

    po_unit = (lot.quantity_unit or "KG").upper().strip()
    try:
        po_qty = float(lot.quantity or 0.0)
    except Exception:
        po_qty = 0.0

    approved_total = 0.0

    # If PO is PCS/PC => sum as-is in PCS
    if po_unit in ["PC", "PCS"]:
        for s in approved_shipments:
            try:
                approved_total += float(s.qty_arrived or 0.0)
            except Exception:
                pass

        remaining = po_qty - approved_total
        if remaining < 0:
            remaining = 0.0

        lot.received_total = approved_total  # PCS total
        lot.status = "APPROVED" if remaining <= 0 else ("PARTIAL" if approved_total > 0 else "PENDING")

    else:
        # Weight-based => normalize everything into KG
        po_kg = normalize_qty_to_kg(po_qty, po_unit)

        for s in approved_shipments:
            try:
                approved_total += float(normalize_qty_to_kg(float(s.qty_arrived or 0.0), s.qty_unit or "KG"))
            except Exception:
                pass

        remaining_kg = po_kg - approved_total
        if remaining_kg < 0:
            remaining_kg = 0.0

        lot.received_total = approved_total  # KG normalized total
        lot.status = "APPROVED" if remaining_kg <= 0 else ("PARTIAL" if approved_total > 0 else "PENDING")

    session.add(lot)
    session.commit()

    return RedirectResponse(f"/mrr/{lot_id}", status_code=303)

@app.post("/mrr/{lot_id}/inspection/id/{inspection_id}/unapprove")
def mrr_unapprove_receiving_inspection(
    lot_id: int,
    inspection_id: int,
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    require_manager(user)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    if (lot.status or "").upper() == "CANCELED":
        return RedirectResponse(f"/mrr/{lot_id}?error=Ticket%20is%20canceled", status_code=303)

    insp = session.get(MrrReceivingInspection, inspection_id)
    if not insp or insp.ticket_id != lot_id:
        raise HTTPException(404, "MRR Inspection not found")

    # Only unapprove if it was approved already
    if not insp.manager_approved:
        return RedirectResponse(
            f"/mrr/{lot_id}?error=This%20inspection%20is%20not%20approved",
            status_code=303,
        )

    # Unapprove
    insp.manager_approved = False
    session.add(insp)

    # =========================
    # Recompute totals after unapprove
    # =========================
    approved_shipments = session.exec(
        select(MrrReceivingInspection).where(
            (MrrReceivingInspection.ticket_id == lot_id)
            & (MrrReceivingInspection.inspector_confirmed == True)
            & (MrrReceivingInspection.manager_approved == True)
        )
    ).all()

    po_unit = (lot.quantity_unit or "KG").upper().strip()
    try:
        po_qty = float(lot.quantity or 0.0)
    except Exception:
        po_qty = 0.0

    approved_total = 0.0

    if po_unit in ["PC", "PCS"]:
        for s in approved_shipments:
            try:
                approved_total += float(s.qty_arrived or 0.0)
            except Exception:
                pass

        remaining = po_qty - approved_total
        if remaining < 0:
            remaining = 0.0

        lot.received_total = approved_total
        # If nothing approved anymore -> go back to PENDING. Else PARTIAL.
        lot.status = "PARTIAL" if approved_total > 0 else "PENDING"

    else:
        po_kg = normalize_qty_to_kg(po_qty, po_unit)

        for s in approved_shipments:
            try:
                approved_total += float(normalize_qty_to_kg(float(s.qty_arrived or 0.0), s.qty_unit or "KG"))
            except Exception:
                pass

        remaining_kg = po_kg - approved_total
        if remaining_kg < 0:
            remaining_kg = 0.0

        lot.received_total = approved_total  # KG normalized
        lot.status = "PARTIAL" if approved_total > 0 else "PENDING"

    session.add(lot)
    session.commit()

    return RedirectResponse(f"/mrr/{lot_id}", status_code=303)


@app.get("/runs/{run_id}/export/xlsx")
def export_xlsx(run_id: int, request: Request, session: Session = Depends(get_session)):
    xlsx_bytes, filename_base = build_export_xlsx_bytes(run_id, request, session)

    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename_base}.xlsx"'},
    )




from fastapi import HTTPException
from starlette.responses import Response

@app.get("/runs/{run_id}/export/pdf")
def export_pdf(run_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)

    run = session.get(ProductionRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    days = get_days_for_run(session, run_id)
    if not days:
        raise HTTPException(400, "No entries to export")

    writer = PdfWriter()

    for day in days:
        # 1) Build 1-day excel
        xlsx_bytes = build_one_day_workbook_bytes(run_id, day, session)

        # 2) Convert to PDF
        pdf_bytes = convert_xlsx_bytes_to_pdf_bytes(xlsx_bytes)

        # 3) Stamp approval if approved
        if (run.status or "").upper() == "APPROVED":
            pdf_bytes = stamp_approval_on_pdf(
                pdf_bytes,
                approved_by=getattr(run, "approved_by_user_name", "") or "",
                approved_at_utc=getattr(run, "approved_at_utc", None),
            )

        # 4) Merge pages
        reader = PdfReader(BytesIO(pdf_bytes))
        for page in reader.pages:
            writer.add_page(page)

    # 5) Write final output ONCE
    out = BytesIO()
    writer.write(out)
    out.seek(0)

    filename = f"{run.process}_{run.dhtp_batch_no}_ALL_DAYS.pdf"
    return Response(
        content=out.getvalue(),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )



def apply_pdf_page_setup(ws):
    # A4 Portrait
    ws.page_setup.orientation = ws.ORIENTATION_PORTRAIT
    ws.page_setup.paperSize = ws.PAPERSIZE_A4

    # ✅ Back to "everything in 1 page"
    ws.page_setup.fitToWidth = 1
    ws.page_setup.fitToHeight = 1
    ws.sheet_properties.pageSetUpPr.fitToPage = True

    # Margins (keep as before)
    ws.page_margins.left = 0.25
    ws.page_margins.right = 0.25
    ws.page_margins.top = 0.35
    ws.page_margins.bottom = 0.70


# =========================
# MRR PHOTO EVIDENCE
# =========================

def _safe_ext(filename: str) -> str:
    name = (filename or "").lower().strip()
    if "." not in name:
        return ".jpg"
    ext = "." + name.split(".")[-1]
    if ext not in [".jpg", ".jpeg", ".png", ".webp"]:
        return ".jpg"
    return ext


@app.get("/mrr/photos/{photo_id}/view")
def mrr_photo_view(photo_id: int, request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)

    p = session.get(MrrInspectionPhoto, photo_id)
    if not p:
        raise HTTPException(404, "Photo not found")

    # (optional) basic permission gate: any logged-in user can view
    # You can tighten later by checking ticket access rules if needed.

    if not p.file_path or not os.path.exists(p.file_path):
        raise HTTPException(404, "Photo file missing")

    mt, _ = mimetypes.guess_type(p.file_path)
    return FileResponse(p.file_path, media_type=mt or "image/jpeg")


@app.post("/mrr/{lot_id}/inspection/id/{inspection_id}/photos/upload")
async def mrr_photo_upload(
    lot_id: int,
    inspection_id: int,
    request: Request,
    session: Session = Depends(get_session),
    group_name: str = Form(...),
    caption: str = Form(""),
    photos: List[UploadFile] = File(...),
):
    user = get_current_user(request, session)
    forbid_boss(user)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    insp = session.get(MrrReceivingInspection, inspection_id)
    if not insp or insp.ticket_id != lot_id:
        raise HTTPException(404, "Shipment inspection not found")

    # Prevent edits after submit
    if insp.inspector_confirmed:
        return RedirectResponse(
            f"/mrr/{lot_id}/inspection/id/{inspection_id}?photo_error=Inspection%20already%20submitted.%20Photos%20cannot%20be%20changed.",
            status_code=303,
        )

    g = (group_name or "General").strip() or "General"
    cap = (caption or "").strip()

    if not photos or len(photos) == 0:
        return RedirectResponse(
            f"/mrr/{lot_id}/inspection/id/{inspection_id}?photo_error=Please%20select%20at%20least%20one%20photo",
            status_code=303,
        )

    # Store under: DATA_DIR/mrr_photos/<ticket>/<inspection>/
    base = os.path.join(MRR_PHOTO_DIR, f"ticket_{lot_id}", f"insp_{inspection_id}")
    os.makedirs(base, exist_ok=True)

    for f in photos:
        # basic image validation
        ct = (f.content_type or "").lower()
        if not ct.startswith("image/"):
            continue

        ext = _safe_ext(f.filename)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        out_path = os.path.join(base, f"{ts}{ext}")

        content = await f.read()
        with open(out_path, "wb") as w:
            w.write(content)

        rec = MrrInspectionPhoto(
            ticket_id=lot_id,
            inspection_id=inspection_id,
            group_name=g,
            caption=cap,
            file_path=out_path,
            uploaded_by_user_id=user.id,
            uploaded_by_user_name=user.display_name,
        )
        session.add(rec)

    session.commit()

    return RedirectResponse(f"/mrr/{lot_id}/inspection/id/{inspection_id}", status_code=303)


@app.post("/mrr/{lot_id}/inspection/id/{inspection_id}/photos/{photo_id}/delete")
def mrr_photo_delete(
    lot_id: int,
    inspection_id: int,
    photo_id: int,
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    forbid_boss(user)

    lot = session.get(MaterialLot, lot_id)
    if not lot:
        raise HTTPException(404, "MRR Ticket not found")

    insp = session.get(MrrReceivingInspection, inspection_id)
    if not insp or insp.ticket_id != lot_id:
        raise HTTPException(404, "Shipment inspection not found")

    # Prevent edits after submit
    if insp.inspector_confirmed:
        return RedirectResponse(f"/mrr/{lot_id}/inspection/id/{inspection_id}", status_code=303)

    p = session.get(MrrInspectionPhoto, photo_id)
    if not p or p.ticket_id != lot_id or p.inspection_id != inspection_id:
        raise HTTPException(404, "Photo not found")

    # Delete file if exists
    try:
        if p.file_path and os.path.exists(p.file_path):
            os.remove(p.file_path)
    except Exception:
        pass

    session.delete(p)
    session.commit()

    return RedirectResponse(f"/mrr/{lot_id}/inspection/id/{inspection_id}", status_code=303)



































