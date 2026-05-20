"""Authentification utilisateur et isolation par entreprise_id."""

import os
from typing import Optional

import bcrypt
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy import true
from sqlalchemy.orm import Session
from starlette.middleware.sessions import SessionMiddleware

AUTH_PUBLIC_PREFIXES = (
    "/login",
    "/api/auth/login",
    "/api/auth/logout",
    "/css/",
    "/img/",
    "/files/",
    "/app/files/",
)

AUTH_PUBLIC_EXACT = frozenset({"/login", "/api/auth/login", "/api/auth/logout"})

WEBHOOK_PATH_PREFIXES = (
    "/api/actions/webhook",
    "/api/factures/webhook",
    "/api/factures/confirm",
    "/api/devis/webhook",
    "/api/devis/confirm",
)

SESSION_SECRET = os.getenv(
    "SESSION_SECRET", "changez-moi-en-production-via-env-SESSION_SECRET"
)
DEFAULT_ENTREPRISE_ID = int(os.getenv("DEFAULT_ENTREPRISE_ID", "1"))


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(
            password.encode("utf-8"), password_hash.encode("utf-8")
        )
    except (ValueError, TypeError):
        return False


def is_public_path(path: str) -> bool:
    if path in AUTH_PUBLIC_EXACT:
        return True
    return any(path.startswith(prefix) for prefix in AUTH_PUBLIC_PREFIXES)


def is_webhook_path(path: str) -> bool:
    if path in (
        "/api/factures/webhook",
        "/api/factures/confirm",
        "/api/devis/webhook",
        "/api/devis/confirm",
        "/api/actions/webhook",
    ):
        return True
    if path.startswith("/api/devis/") and path.endswith("/update_webhook"):
        return True
    if path.startswith("/api/devis/") and path.endswith("/facture_webhook"):
        return True
    return False


def webhook_entreprise_id() -> int:
    return DEFAULT_ENTREPRISE_ID


def scoped(db: Session, model, entreprise_id: int):
    return db.query(model).filter(model.entreprise_id == entreprise_id)


def get_one(db: Session, model, pk: int, entreprise_id: int, detail: str = "Ressource non trouvée"):
    row = (
        db.query(model)
        .filter(model.id == pk, model.entreprise_id == entreprise_id)
        .first()
    )
    if not row:
        raise HTTPException(status_code=404, detail=detail)
    return row


def get_session_user(request: Request, db: Session, utilisateur_model):
    user_id = request.session.get("user_id")
    if not user_id:
        return None
    user = (
        db.query(utilisateur_model)
        .filter(
            utilisateur_model.id == user_id,
            utilisateur_model.actif == true(),
        )
        .first()
    )
    if not user:
        request.session.clear()
    return user


def resolve_entreprise_id(request: Request, db: Session, utilisateur_model) -> int:
    if is_webhook_path(request.url.path):
        return webhook_entreprise_id()
    user = get_session_user(request, db, utilisateur_model)
    if not user:
        raise HTTPException(status_code=401, detail="Non authentifié.")
    request.state.current_user = user
    return user.entreprise_id


def setup_session_middleware(app):
    app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET, max_age=86400 * 14)


def setup_auth_middleware(app, utilisateur_model, session_factory):
    @app.middleware("http")
    async def tenant_auth_middleware(request: Request, call_next):
        path = request.url.path
        if is_public_path(path):
            return await call_next(request)

        if is_webhook_path(path):
            request.state.entreprise_id = webhook_entreprise_id()
            return await call_next(request)

        db = session_factory()
        try:
            user = get_session_user(request, db, utilisateur_model)
        finally:
            db.close()

        if not user:
            if path.startswith("/api/"):
                return JSONResponse(
                    status_code=401, content={"detail": "Non authentifié."}
                )
            return RedirectResponse(url="/login", status_code=303)

        request.state.current_user = user
        request.state.entreprise_id = user.entreprise_id
        return await call_next(request)
