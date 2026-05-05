import asyncio
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from database import get_db
from models.entities import User, WbToken
from routes.deps import current_user
from services.background_sync import create_sync_job, trigger_sync_now
from services.sync import quick_bootstrap_wb_data
from utils.security import decrypt_text, encrypt_text
from wb_api.client import WbApiError, WbClient, WbInvalidToken, normalize_token
from wb_api.permissions import REQUIRED_PERMISSIONS

router = APIRouter(prefix="/wb-token", tags=["wb-token"])


class TokenIn(BaseModel):
    token: str


@router.post("")
async def connect_token(
    payload: TokenIn,
    user: User = Depends(current_user),
    db: Session = Depends(get_db),
):
    token_text = normalize_token(payload.token)
    if not token_text:
        raise HTTPException(status_code=400, detail="Вставьте WB API-токен")

    wb_token = WbToken(user_id=user.id, encrypted_token=encrypt_text(token_text), is_active=False)
    db.add(wb_token)
    db.flush()

    await _check_and_store(db, wb_token, token_text, user.id)
    if wb_token.token_status == "invalid":
        raise HTTPException(status_code=400, detail="WB API-токен неправильный. Текущее подключение не изменено.")

    db.execute(
        update(WbToken)
        .where(WbToken.user_id == user.id, WbToken.id != wb_token.id)
        .values(is_active=False, sync_in_progress=False)
    )
    wb_token.is_active = True
    db.commit()
    db.refresh(wb_token)
    bootstrap = await quick_bootstrap_wb_data(db, user.id, wb_token, token_text)
    db.refresh(wb_token)

    # Start full background sync immediately
    job = create_sync_job(db, user.id, wb_token.id, sync_type="initial_full_sync")
    asyncio.create_task(trigger_sync_now(job.id, user.id))

    return _token_response(wb_token, bootstrap)


@router.get("")
def token_status(user: User = Depends(current_user), db: Session = Depends(get_db)):
    wb_token = _active_token(db, user.id)
    if not wb_token:
        return _empty_response()
    return _token_response(wb_token)


@router.post("/check")
async def check_current_token(user: User = Depends(current_user), db: Session = Depends(get_db)):
    wb_token = _active_token(db, user.id)
    if not wb_token:
        return _empty_response()

    token_text = decrypt_text(wb_token.encrypted_token)
    await _check_and_store(db, wb_token, token_text, user.id)
    db.refresh(wb_token)
    return _token_response(wb_token)


@router.delete("")
def delete_current_token(user: User = Depends(current_user), db: Session = Depends(get_db)):
    wb_token = _active_token(db, user.id)
    if wb_token:
        wb_token.is_active = False
        wb_token.sync_in_progress = False
        db.commit()
    return _empty_response()


async def _check_and_store(db: Session, wb_token: WbToken, token_text: str, user_id: int) -> None:
    client = WbClient(token_text, db=db, user_id=user_id, wb_token_id=wb_token.id)
    try:
        check = await client.check_token()
        wb_token.shop_name = check.shop_name
        wb_token.token_status = check.status
        wb_token.permissions = check.permissions
    except WbInvalidToken:
        wb_token.token_status = "invalid"
        wb_token.permissions = _empty_permissions()
    except WbApiError:
        wb_token.token_status = "api_error"
        wb_token.permissions = _empty_permissions()

    wb_token.last_checked_at = datetime.now(timezone.utc).replace(tzinfo=None)
    if wb_token.token_status == "invalid":
        wb_token.is_active = False
        wb_token.sync_in_progress = False
    db.commit()


def _active_token(db: Session, user_id: int) -> WbToken | None:
    return db.scalar(
        select(WbToken)
        .where(WbToken.user_id == user_id, WbToken.is_active.is_(True), WbToken.token_status != "invalid")
        .order_by(WbToken.id.desc())
    )


def _token_response(wb_token: WbToken, bootstrap: dict | None = None):
    permissions = wb_token.permissions or _empty_permissions()
    connected = wb_token.is_active and wb_token.token_status != "invalid"
    message = bootstrap.get("message") if bootstrap else None
    return {
        "connected": connected,
        "status": wb_token.token_status,
        "shop_name": wb_token.shop_name,
        "permissions": permissions,
        "last_checked_at": wb_token.last_checked_at.isoformat() if wb_token.last_checked_at else None,
        "last_sync_at": wb_token.last_sync_at.isoformat() if wb_token.last_sync_at else None,
        "message": message or _message(wb_token.token_status, permissions),
        "bootstrap": bootstrap,
    }


def _empty_response():
    return {
        "connected": False,
        "status": "invalid",
        "shop_name": None,
        "permissions": _empty_permissions(),
        "last_checked_at": None,
        "last_sync_at": None,
        "message": "WB API-токен не подключён.",
    }


def _empty_permissions() -> dict:
    return {
        "items": {},
        "missing": [meta["title"] for meta in REQUIRED_PERMISSIONS.values()],
        "affected": [meta["affects"] for meta in REQUIRED_PERMISSIONS.values()],
    }


def _message(status: str, permissions: dict) -> str:
    if status == "active":
        return "Токен активен. Для точной аналитики загрузите финансовый отчёт WB."
    if status == "limited":
        missing = ", ".join(permissions.get("missing", [])) or "часть разделов WB API"
        affected = ", ".join(permissions.get("affected", [])) or "часть данных"
        return f"Токен подключён, но не хватает прав: {missing}. Эти данные могут не работать: {affected}."
    if status == "rate_limited":
        return "Токен подключён. WB временно ограничил запросы. Для точной аналитики загрузите финансовый отчёт WB."
    if status == "api_error":
        return "Токен подключён. WB API временно недоступен. Для точной аналитики загрузите финансовый отчёт WB."
    return "Токен неправильный или не подключён."
