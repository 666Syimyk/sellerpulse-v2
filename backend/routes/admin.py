"""
Админские маршруты. Доступны только пользователям с is_admin=True.
"""
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr
from sqlalchemy import select
from sqlalchemy.orm import Session

from config import get_settings
from database import get_db
from models.entities import Subscription, SubscriptionHistory, SubscriptionStatus, User
from routes.deps import admin_user
from services.subscription import create_trial, is_active, serialize

router = APIRouter(prefix="/admin", tags=["admin"])


class PromoteIn(BaseModel):
    email: EmailStr
    secret: str


@router.post("/promote")
def promote_to_admin(payload: PromoteIn, db: Session = Depends(get_db)):
    settings = get_settings()
    if payload.secret != settings.admin_secret:
        raise HTTPException(status_code=403, detail="Неверный секрет")
    user = db.scalar(select(User).where(User.email == payload.email))
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    user.is_admin = True
    db.commit()
    return {"ok": True, "email": user.email, "is_admin": user.is_admin}


@router.get("/users")
def list_users(db: Session = Depends(get_db), _: User = Depends(admin_user)):
    users = db.scalars(select(User).order_by(User.id)).all()
    histories = db.scalars(select(SubscriptionHistory).order_by(SubscriptionHistory.created_at.desc(), SubscriptionHistory.id.desc())).all()
    users_by_id = {u.id: u for u in users}
    history_by_user_id: dict[int, list[dict]] = {}
    for item in histories:
        if item.user_id not in users_by_id:
            continue
        history_by_user_id.setdefault(item.user_id, [])
        if len(history_by_user_id[item.user_id]) >= 5:
            continue
        admin = users_by_id.get(item.admin_user_id) if item.admin_user_id else None
        history_by_user_id[item.user_id].append({
            "id": item.id,
            "previous_status": item.previous_status,
            "new_status": item.new_status,
            "days_added": item.days_added,
            "previous_end_at": item.previous_end_at.isoformat() if item.previous_end_at else None,
            "new_end_at": item.new_end_at.isoformat() if item.new_end_at else None,
            "notes": item.notes,
            "admin_name": admin.name if admin else None,
            "created_at": item.created_at.isoformat(),
        })

    result = []
    for u in users:
        sub = db.scalar(select(Subscription).where(Subscription.user_id == u.id))
        result.append({
            "id": u.id,
            "name": u.name,
            "email": u.email,
            "is_admin": u.is_admin,
            "created_at": u.created_at.isoformat(),
            "subscription": serialize(sub),
            "subscription_history": history_by_user_id.get(u.id, []),
        })
    return result


class SubscriptionPatch(BaseModel):
    status: str  # trial | active | expired | cancelled
    days: int | None = None  # продлить на N дней от сейчас
    notes: str | None = None


class BulkSubscriptionPatch(SubscriptionPatch):
    user_ids: list[int]


def _subscription_end_at(sub: Subscription) -> datetime | None:
    if sub.status == SubscriptionStatus.trial.value:
        return sub.trial_end
    if sub.status == SubscriptionStatus.active.value:
        return sub.paid_until
    return None


def _apply_subscription_patch(
    db: Session,
    admin: User,
    user_id: int,
    payload: SubscriptionPatch,
) -> dict:
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    sub = db.scalar(select(Subscription).where(Subscription.user_id == user_id))
    if not sub:
        sub = create_trial(db, user)

    now = datetime.now(timezone.utc).replace(tzinfo=None)

    if payload.status not in [s.value for s in SubscriptionStatus]:
        raise HTTPException(status_code=400, detail="Неверный статус")

    previous_status = sub.status
    previous_end_at = _subscription_end_at(sub)
    days_added = payload.days if payload.status in {SubscriptionStatus.active.value, SubscriptionStatus.trial.value} else None

    sub.status = payload.status
    if payload.notes is not None:
        sub.notes = payload.notes

    if payload.status == SubscriptionStatus.trial.value and payload.days:
        sub.trial_end = now + timedelta(days=payload.days)
        sub.paid_until = None
    elif payload.status == SubscriptionStatus.active.value:
        base = sub.paid_until if sub.paid_until and sub.paid_until > now else now
        sub.paid_until = base + timedelta(days=payload.days or 30)
        sub.trial_end = None
    elif payload.status in {SubscriptionStatus.expired.value, SubscriptionStatus.cancelled.value}:
        sub.trial_end = None
        sub.paid_until = None

    history_entry = SubscriptionHistory(
        user_id=user.id,
        admin_user_id=admin.id,
        previous_status=previous_status,
        new_status=sub.status,
        days_added=days_added,
        previous_end_at=previous_end_at,
        new_end_at=_subscription_end_at(sub),
        notes=payload.notes if payload.notes is not None else sub.notes,
    )
    db.add(history_entry)

    return {"ok": True, "subscription": serialize(sub)}


@router.patch("/users/{user_id}/subscription")
def patch_subscription(user_id: int, payload: SubscriptionPatch, db: Session = Depends(get_db), admin: User = Depends(admin_user)):
    result = _apply_subscription_patch(db, admin, user_id, payload)
    db.commit()
    return result


@router.patch("/subscriptions/bulk")
def bulk_patch_subscription(payload: BulkSubscriptionPatch, db: Session = Depends(get_db), admin: User = Depends(admin_user)):
    if not payload.user_ids:
        raise HTTPException(status_code=400, detail="Не выбраны пользователи")

    unique_user_ids = list(dict.fromkeys(payload.user_ids))
    updated = []
    for user_id in unique_user_ids:
        _apply_subscription_patch(db, admin, user_id, payload)
        updated.append(user_id)

    db.commit()
    return {"ok": True, "updated_user_ids": updated, "count": len(updated)}


@router.patch("/users/{user_id}/admin")
def toggle_admin(user_id: int, db: Session = Depends(get_db), _: User = Depends(admin_user)):
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    user.is_admin = not user.is_admin
    db.commit()
    return {"ok": True, "is_admin": user.is_admin}


@router.get("/stats")
def get_stats(db: Session = Depends(get_db), _: User = Depends(admin_user)):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    soon = now + timedelta(days=7)

    users = db.scalars(select(User)).all()
    subs = db.scalars(select(Subscription)).all()
    subs_by_user = {s.user_id: s for s in subs}

    total = len(users)
    active = sum(1 for s in subs if s.status == SubscriptionStatus.active.value and (s.paid_until is None or s.paid_until > now))
    trial = sum(1 for s in subs if s.status == SubscriptionStatus.trial.value and (s.trial_end is None or s.trial_end > now))
    expired = sum(1 for s in subs if not is_active(s))
    no_sub = total - len(subs)

    expiring_soon = []
    for u in users:
        sub = subs_by_user.get(u.id)
        if not sub or not is_active(sub):
            continue
        end = sub.trial_end if sub.status == SubscriptionStatus.trial.value else sub.paid_until
        if end and end <= soon:
            expiring_soon.append({
                "id": u.id,
                "name": u.name,
                "email": u.email,
                "status": sub.status,
                "days_left": max(0, (end - now).days),
                "end_date": end.isoformat(),
            })

    expiring_soon.sort(key=lambda x: x["days_left"])

    new_today = sum(1 for u in users if u.created_at.date() == now.date())
    new_week = sum(1 for u in users if (now - u.created_at).days <= 7)

    return {
        "total_users": total,
        "active_paid": active,
        "active_trial": trial,
        "expired": expired,
        "no_subscription": no_sub,
        "expiring_soon": expiring_soon,
        "new_today": new_today,
        "new_week": new_week,
        "mrr_estimate": active * 990,
    }
