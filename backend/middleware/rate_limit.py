"""
Простой in-memory rate limiter. Не требует Redis.
Ограничивает количество запросов с одного IP к защищённым путям.
"""
import asyncio
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, max_requests: int = 10, window_seconds: int = 60, paths: list[str] | None = None):
        super().__init__(app)
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.paths = paths or []
        self._buckets: dict[str, list[datetime]] = defaultdict(list)
        self._lock = asyncio.Lock()

    async def dispatch(self, request: Request, call_next):
        if not any(request.url.path.startswith(p) for p in self.paths):
            return await call_next(request)

        ip = request.client.host if request.client else "unknown"
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        window_start = now - timedelta(seconds=self.window_seconds)

        async with self._lock:
            self._buckets[ip] = [t for t in self._buckets[ip] if t > window_start]
            if len(self._buckets[ip]) >= self.max_requests:
                return Response(
                    content=json.dumps({"detail": "Слишком много попыток. Подождите немного и попробуйте снова."}),
                    status_code=429,
                    media_type="application/json",
                )
            self._buckets[ip].append(now)

        return await call_next(request)
