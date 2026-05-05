import asyncio
import logging
from pathlib import Path
from contextlib import asynccontextmanager

import sentry_sdk
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

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
    worker_task = None
    if settings.run_sync_worker_in_web:
        worker_task = asyncio.create_task(run_sync_worker_loop())
    if settings.enable_scheduler:
        start_scheduler()
    yield
    if worker_task:
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
    allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1):\d+$|^https://.*\.onrender\.com$|^https://.*\.github\.io$",
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


frontend_dist = Path(__file__).resolve().parents[1] / "frontend" / "dist"
if frontend_dist.exists():
    assets_dir = frontend_dist / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

    @app.get("/", include_in_schema=False)
    def spa_index():
        return FileResponse(frontend_dist / "index.html")

    @app.get("/{full_path:path}", include_in_schema=False)
    def spa_fallback(full_path: str):
        candidate = frontend_dist / full_path
        if candidate.is_file():
            return FileResponse(candidate)
        return FileResponse(frontend_dist / "index.html")
