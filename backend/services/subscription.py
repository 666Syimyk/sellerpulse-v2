from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from config import get_settings
from models.entities import Subscription, SubscriptionStatus, User


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def create_trial(db: Session, user: User) -> Subscription:
    settings = get_settings()
    trial_end = _now() + timedelta(days=settings.trial_days)
    sub = Subscription(user_id=user.id, status=SubscriptionStatus.trial.value, trial_end=trial_end)
    db.add(sub)
    db.commit()
    db.refresh(sub)
    return sub


def is_active(sub: Subscription | None) -> bool:
    if sub is None:
        return False
    now = _now()
    if sub.status == SubscriptionStatus.trial.value:
        return sub.trial_end is None or sub.trial_end > now
    if sub.status == SubscriptionStatus.active.value:
        return sub.paid_until is None or sub.paid_until > now
    return False


def days_left(sub: Subscription | None) -> int | None:
    if sub is None:
        return None
    now = _now()
    if sub.status == SubscriptionStatus.trial.value and sub.trial_end:
        delta = (sub.trial_end - now).days
        return max(0, delta)
    if sub.status == SubscriptionStatus.active.value and sub.paid_until:
        delta = (sub.paid_until - now).days
        return max(0, delta)
    return None


def serialize(sub: Subscription | None) -> dict:
    if sub is None:
        return {"status": "no_subscription", "active": False, "days_left": None, "trial_end": None, "paid_until": None}
    return {
        "status": sub.status,
        "active": is_active(sub),
        "days_left": days_left(sub),
        "trial_end": sub.trial_end.isoformat() if sub.trial_end else None,
        "paid_until": sub.paid_until.isoformat() if sub.paid_until else None,
        "notes": sub.notes,
    }
