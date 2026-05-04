"""
Проверяет активность подписки для защищённых маршрутов.
Публичные пути (/auth/*, /health, /subscription, /admin/*) пропускаются.
"""
import json

from sqlalchemy import select
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from database import SessionLocal
from models.entities import Subscription, User
from services.subscription import is_active
from utils.security import decode_access_token

_PUBLIC_PREFIXES = ["/auth/", "/health", "/subscription", "/admin/", "/docs", "/openapi"]


def _is_public(path: str) -> bool:
    return any(path.startswith(p) for p in _PUBLIC_PREFIXES) or path in {"/health"}


class SubscriptionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if _is_public(request.url.path):
            return await call_next(request)

        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return await call_next(request)

        token = auth_header.split(" ", 1)[1]
        try:
            user_id = decode_access_token(token)
        except Exception:
            return await call_next(request)
        if not user_id:
            return await call_next(request)

        session_factory = getattr(request.app.state, "session_factory", SessionLocal)
        with session_factory() as db:
            user = db.get(User, int(user_id))
            if not user:
                return await call_next(request)
            sub = db.scalar(select(Subscription).where(Subscription.user_id == user.id))

        if not is_active(sub):
            return Response(
                content=json.dumps({"detail": "subscription_expired", "message": "Подписка истекла. Продлите доступ."}),
                status_code=402,
                media_type="application/json",
            )

        return await call_next(request)
