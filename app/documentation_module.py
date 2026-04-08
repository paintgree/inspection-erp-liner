from __future__ import annotations

import io
import os
import mimetypes
import uuid
from datetime import datetime, date
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Session, select

from .db import get_session
from .models import ManagedDocument, User


router = APIRouter(prefix="/documentation", tags=["Documentation"])
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

BASE_DIR = Path(__file__).resolve().parent
DOCUMENT_LIBRARY_DIR = BASE_DIR / "uploaded_document_library"
DOCUMENT_LIBRARY_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED_DOC_EXTENSIONS = {".pdf", ".doc", ".docx"}


def _require_user(session: Session = Depends(get_session)) -> User:
    user = session.exec(select(User).order_by(User.id.asc())).first()
    if not user:
        raise HTTPException(status_code=401, detail="No users found.")
    return user


def _safe_filename(filename: str) -> str:
    raw = (filename or "").strip()
    if not raw:
        raw = "file"
    safe = "".join(c if c.isalnum() or c in {".", "-", "_"} else "_" for c in raw)
    return safe[:200]


def _save_uploaded_document(uploaded_file: UploadFile) -> dict:
    if uploaded_file is None:
        raise HTTPException(status_code=400, detail="No file uploaded.")

    original_name = _safe_filename(uploaded_file.filename or "file")
    suffix = Path(original_name).suffix.lower()

    if suffix not in ALLOWED_DOC_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"File type not allowed: {suffix or 'unknown'}",
        )

    stored_name = f"{uuid.uuid4().hex}.pdf"
    target_path = DOCUMENT_LIBRARY_DIR / stored_name

    size_bytes = 0
    with target_path.open("wb") as buffer:
        while True:
            chunk = uploaded_file.file.read(1024 * 1024)
            if not chunk:
                break
            size_bytes += len(chunk)
            buffer.write(chunk)

    content_type = uploaded_file.content_type or mimetypes.guess_type(original_name)[0] or "application/pdf"

    return {
        "original_filename": original_name,
        "stored_pdf_path": str(target_path),
        "content_type": content_type,
        "file_size_bytes": size_bytes,
    }


def _base_document_query(session: Session, library_type: str, search: str = "", category: str = "all"):
    statement = select(ManagedDocument).where(
        ManagedDocument.library_type == library_type
    )

    if category and category.lower() != "all":
        statement = statement.where(ManagedDocument.category == category)

    rows = session.exec(
        statement.order_by(ManagedDocument.updated_at.desc(), ManagedDocument.id.desc())
    ).all()

    if search:
        needle = search.strip().lower()
        filtered = []
        for row in rows:
            haystack = " ".join(
                [
                    row.code or "",
                    row.title or "",
                    row.subtitle or "",
                    row.category or "",
                    row.description or "",
                    row.revision or "",
                    row.extracted_text or "",
                ]
            ).lower()
            if needle in haystack:
                filtered.append(row)
        return filtered

    return rows


def _category_list(session: Session, library_type: str):
    rows = session.exec(
        select(ManagedDocument).where(ManagedDocument.library_type == library_type)
    ).all()

    return sorted(
        {
            (row.category or "").strip()
            for row in rows
            if (row.category or "").strip()
        }
    )


def _csv_escape(value) -> str:
    text = "" if value is None else str(value)
    text = text.replace('"', '""')
    return f'"{text}"'


def _build_csv_bytes(documents) -> bytes:
    lines = []
    lines.append("Code,Title,Category,Revision,Status,Issue Date,Review Date")

    for row in documents:
        parts = [
            _csv_escape(row.code or ""),
            _csv_escape(row.title or ""),
            _csv_escape(row.category or ""),
            _csv_escape(row.revision or ""),
            _csv_escape(row.status or ""),
            _csv_escape(row.issue_date or ""),
            _csv_escape(row.review_date or ""),
        ]
        lines.append(",".join(parts))

    return ("\n".join(lines) + "\n").encode("utf-8")


@router.get("")
def documentation_home():
    return RedirectResponse(url="/documentation/procedures", status_code=303)


@router.get("/procedures")
def documentation_procedures(
    request: Request,
    search: str = Query(default=""),
    category: str = Query(default="all"),
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),
):
    documents = _base_document_query(
        session=session,
        library_type="PROCEDURE",
        search=search,
        category=category,
    )

    categories = _category_list(session, "PROCEDURE")

    return templates.TemplateResponse(
        request=request,
        name="docs_procedures.html",
        context={
            "request": request,
            "user": user,
            "documents": documents,
            "search": search,
            "active_category": category,
            "categories": categories,
            "library_type": "PROCEDURE",
        },
    )


@router.get("/standards")
def documentation_standards(
    request: Request,
    search: str = Query(default=""),
    category: str = Query(default="all"),
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),
):
    documents = _base_document_query(
        session=session,
        library_type="STANDARD",
        search=search,
        category=category,
    )

    categories = _category_list(session, "STANDARD")

    return templates.TemplateResponse(
        request=request,
        name="docs_standards.html",
        context={
            "request": request,
            "user": user,
            "documents": documents,
            "search": search,
            "active_category": category,
            "categories": categories,
            "library_type": "STANDARD",
        },
    )


@router.get("/procedures/upload")
def procedure_upload_form(
    request: Request,
    user: User = Depends(_require_user),
):
    return templates.TemplateResponse(
        request=request,
        name="docs_upload_form.html",
        context={
            "request": request,
            "user": user,
            "library_type": "PROCEDURE",
            "page_title": "Upload New Procedure",
            "submit_url": "/documentation/procedures/upload",
        },
    )


@router.get("/standards/upload")
def standard_upload_form(
    request: Request,
    user: User = Depends(_require_user),
):
    return templates.TemplateResponse(
        request=request,
        name="docs_upload_form.html",
        context={
            "request": request,
            "user": user,
            "library_type": "STANDARD",
            "page_title": "Upload New Standard",
            "submit_url": "/documentation/standards/upload",
        },
    )


@router.post("/procedures/upload")
def procedure_upload_submit(
    code: str = Form(...),
    title: str = Form(...),
    subtitle: str = Form(""),
    category: str = Form("General"),
    description: str = Form(""),
    revision: str = Form("00"),
    status: str = Form("ACTIVE"),
    issue_date: Optional[date] = Form(None),
    review_date: Optional[date] = Form(None),
    uploaded_file: UploadFile = File(...),
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),
):
    saved = _save_uploaded_document(uploaded_file)

    row = ManagedDocument(
        library_type="PROCEDURE",
        code=(code or "").strip(),
        title=(title or "").strip(),
        subtitle=(subtitle or "").strip(),
        category=(category or "General").strip(),
        description=(description or "").strip(),
        revision=(revision or "00").strip(),
        status=(status or "ACTIVE").strip().upper(),
        issue_date=issue_date,
        review_date=review_date,
        source_filename=saved["original_filename"],
        stored_pdf_path=saved["stored_pdf_path"],
        extracted_text="",
        uploaded_by_user_id=getattr(user, "id", None),
        uploaded_by_user_name=(getattr(user, "display_name", "") or getattr(user, "username", "") or ""),
    )

    session.add(row)
    session.commit()

    return RedirectResponse(url="/documentation/procedures", status_code=303)


@router.post("/standards/upload")
def standard_upload_submit(
    code: str = Form(...),
    title: str = Form(...),
    subtitle: str = Form(""),
    category: str = Form("General"),
    description: str = Form(""),
    revision: str = Form("00"),
    status: str = Form("ACTIVE"),
    issue_date: Optional[date] = Form(None),
    review_date: Optional[date] = Form(None),
    uploaded_file: UploadFile = File(...),
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),
):
    saved = _save_uploaded_document(uploaded_file)

    row = ManagedDocument(
        library_type="STANDARD",
        code=(code or "").strip(),
        title=(title or "").strip(),
        subtitle=(subtitle or "").strip(),
        category=(category or "General").strip(),
        description=(description or "").strip(),
        revision=(revision or "00").strip(),
        status=(status or "ACTIVE").strip().upper(),
        issue_date=issue_date,
        review_date=review_date,
        source_filename=saved["original_filename"],
        stored_pdf_path=saved["stored_pdf_path"],
        extracted_text="",
        uploaded_by_user_id=getattr(user, "id", None),
        uploaded_by_user_name=(getattr(user, "display_name", "") or getattr(user, "username", "") or ""),
    )

    session.add(row)
    session.commit()

    return RedirectResponse(url="/documentation/standards", status_code=303)


@router.get("/view/{document_id}")
def view_document(
    document_id: int,
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),
):
    row = session.get(ManagedDocument, document_id)
    if not row:
        raise HTTPException(status_code=404, detail="Document not found.")

    if not row.stored_pdf_path or not os.path.exists(row.stored_pdf_path):
        raise HTTPException(status_code=404, detail="Stored PDF not found.")

    return FileResponse(
        path=row.stored_pdf_path,
        media_type="application/pdf",
        filename=f"{row.code or 'document'}.pdf",
    )


@router.get("/procedures/export")
def export_procedures(
    search: str = Query(default=""),
    category: str = Query(default="all"),
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),
):
    documents = _base_document_query(
        session=session,
        library_type="PROCEDURE",
        search=search,
        category=category,
    )

    csv_bytes = _build_csv_bytes(documents)

    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=procedures_library_list.csv"},
    )


@router.get("/standards/export")
def export_standards(
    search: str = Query(default=""),
    category: str = Query(default="all"),
    session: Session = Depends(get_session),
    user: User = Depends(_require_user),
):
    documents = _base_document_query(
        session=session,
        library_type="STANDARD",
        search=search,
        category=category,
    )

    csv_bytes = _build_csv_bytes(documents)

    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=standards_library_list.csv"},
    )
