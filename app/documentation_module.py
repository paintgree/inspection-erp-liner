from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Session, select

from .db import get_session
from .models import ManagedDocument, User


router = APIRouter(prefix="/documentation", tags=["Documentation"])
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

BASE_DIR = Path(__file__).resolve().parent
DOCUMENT_LIBRARY_DIR = BASE_DIR / "uploaded_document_library"
DOCUMENT_LIBRARY_DIR.mkdir(parents=True, exist_ok=True)


def _require_user(session: Session = Depends(get_session)) -> User:
    user = session.exec(select(User).order_by(User.id.asc())).first()
    if not user:
        raise HTTPException(status_code=401, detail="No users found.")
    return user


def _touch_document(row: ManagedDocument) -> None:
    row.updated_at = datetime.utcnow()


def _base_document_query(session: Session, library_type: str, search: str = "", category: str = "all"):
    statement = select(ManagedDocument).where(
        ManagedDocument.library_type == library_type
    )

    if category and category.lower() != "all":
        statement = statement.where(ManagedDocument.category == category)

    rows = session.exec(statement.order_by(ManagedDocument.updated_at.desc(), ManagedDocument.id.desc())).all()

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

    categories = sorted(
        {
            (row.category or "").strip()
            for row in session.exec(
                select(ManagedDocument).where(ManagedDocument.library_type == "PROCEDURE")
            ).all()
            if (row.category or "").strip()
        }
    )

    return templates.TemplateResponse(
        "docs_procedures.html",
        {
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

    categories = sorted(
        {
            (row.category or "").strip()
            for row in session.exec(
                select(ManagedDocument).where(ManagedDocument.library_type == "STANDARD")
            ).all()
            if (row.category or "").strip()
        }
    )

    return templates.TemplateResponse(
        "docs_standards.html",
        {
            "request": request,
            "user": user,
            "documents": documents,
            "search": search,
            "active_category": category,
            "categories": categories,
            "library_type": "STANDARD",
        },
    )
