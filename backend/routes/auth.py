import os
import secrets
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr
from sqlalchemy import select
from sqlalchemy.orm import Session

from database import get_db
from models.entities import PasswordResetToken, User
from routes.deps import current_user
from services.subscription import create_trial, serialize
from utils.security import create_access_token, hash_password, verify_password

router = APIRouter(prefix="/auth", tags=["auth"])


class AuthIn(BaseModel):
    email: EmailStr
    password: str
    name: str | None = None


@router.post("/register")
def register(payload: AuthIn, db: Session = Depends(get_db)):
    if len(payload.password) < 6:
        raise HTTPException(status_code=400, detail="Пароль должен быть не менее 6 символов")
    if db.scalar(select(User).where(User.email == payload.email)):
        raise HTTPException(status_code=400, detail="Пользователь с таким email уже есть")
    user = User(name=payload.name or payload.email.split("@")[0], email=payload.email, password_hash=hash_password(payload.password))
    db.add(user)
    db.commit()
    db.refresh(user)
    create_trial(db, user)
    return {"access_token": create_access_token(str(user.id)), "token_type": "bearer", "has_wb_token": False}


@router.post("/login")
def login(payload: AuthIn, db: Session = Depends(get_db)):
    user = db.scalar(select(User).where(User.email == payload.email))
    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=400, detail="Неверный email или пароль")
    if not user.subscription:
        create_trial(db, user)
    return {"access_token": create_access_token(str(user.id)), "token_type": "bearer", "has_wb_token": user.wb_token is not None}


@router.get("/me")
def me(user: User = Depends(current_user), db: Session = Depends(get_db)):
    db.refresh(user)
    wb_token = user.wb_token
    return {
        "id": user.id,
        "name": user.name,
        "email": user.email,
        "is_admin": user.is_admin,
        "has_wb_token": wb_token is not None,
        "token_status": wb_token.token_status if wb_token else None,
        "subscription": serialize(user.subscription),
    }


class ForgotPasswordIn(BaseModel):
    email: EmailStr


@router.post("/forgot-password")
def forgot_password(payload: ForgotPasswordIn, db: Session = Depends(get_db)):
    user = db.scalar(select(User).where(User.email == payload.email))
    # Не раскрываем есть ли пользователь
    if not user:
        return {"message": "Если email зарегистрирован, ссылка для сброса отправлена."}

    # Инвалидируем старые токены
    old_tokens = db.scalars(select(PasswordResetToken).where(PasswordResetToken.user_id == user.id, PasswordResetToken.used == False)).all()  # noqa: E712
    for t in old_tokens:
        t.used = True

    token_value = secrets.token_urlsafe(48)
    expires = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=2)
    reset_token = PasswordResetToken(user_id=user.id, token=token_value, expires_at=expires)
    db.add(reset_token)
    db.commit()

    # TODO: отправить token_value по email когда будет настроен email-сервис
    return {"message": "Если email зарегистрирован, ссылка для сброса отправлена."}


class ResetPasswordIn(BaseModel):
    token: str
    new_password: str


@router.post("/reset-password")
def reset_password(payload: ResetPasswordIn, db: Session = Depends(get_db)):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    reset_token = db.scalar(
        select(PasswordResetToken).where(
            PasswordResetToken.token == payload.token,
            PasswordResetToken.used == False,  # noqa: E712
            PasswordResetToken.expires_at > now,
        )
    )
    if not reset_token:
        raise HTTPException(status_code=400, detail="Токен недействителен или истёк")
    if len(payload.new_password) < 6:
        raise HTTPException(status_code=400, detail="Пароль должен быть не менее 6 символов")

    user = db.get(User, reset_token.user_id)
    user.password_hash = hash_password(payload.new_password)
    reset_token.used = True
    db.commit()
    return {"message": "Пароль успешно изменён. Войдите с новым паролем."}
