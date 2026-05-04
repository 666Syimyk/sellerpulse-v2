import asyncio
import logging
from contextlib import asynccontextmanager

import sentry_sdk
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import get_settings
from database import Base, engine, run_sqlite_migrations
from middleware.rate_limit import RateLimitMiddleware
from middleware.subscription import SubscriptionMiddleware
from models import entities  # noqa: F401
from routes import admin, auth, dashboard, financial_report, products, subscription, sync, wb_token
from services.background_sync import run_sync_worker_loop
from services.scheduler import start_scheduler, stop_scheduler

settings = get_settings()

if settings.sentry_dsn:
    sentry_sdk.init(dsn=settings.sentry_dsn, traces_sample_rate=0.1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logging.getLogger("services.sync").setLevel(logging.INFO)
logging.getLogger("services.background_sync").setLevel(logging.INFO)
logging.getLogger("services.scheduler").setLevel(logging.INFO)
logging.getLogger("wb_api.client").setLevel(logging.INFO)
Base.metadata.create_all(bind=engine)
run_sqlite_migrations()


@asynccontextmanager
async def lifespan(app: FastAPI):
    worker_task = asyncio.create_task(run_sync_worker_loop())
    if settings.enable_scheduler:
        start_scheduler()
    yield
    worker_task.cancel()
    if settings.enable_scheduler:
        stop_scheduler()


app = FastAPI(title="SellerPulse v2 API", lifespan=lifespan)

app.add_middleware(SubscriptionMiddleware)
app.add_middleware(
    RateLimitMiddleware,
    max_requests=5,
    window_seconds=60,
    paths=["/auth/login", "/auth/register"],
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.frontend_origin, "http://localhost:5173", "http://127.0.0.1:5173"],
    allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1):\d+$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(subscription.router)
app.include_router(admin.router)
app.include_router(wb_token.router)
app.include_router(dashboard.router)
app.include_router(products.router)
app.include_router(financial_report.router)
app.include_router(sync.router)


@app.get("/health")
def health():
    return {"ok": True}
