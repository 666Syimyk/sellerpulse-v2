"""
Background sync service — step-by-step WB API data sync with SyncJob progress tracking.
Designed for FastAPI BackgroundTasks; architecture allows migration to Celery later.
"""
import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from config import get_settings
from database import SessionLocal
from models.entities import SyncJob, SyncStep, WbToken
from services.periods import period_dates
from services.sync import (
    _clear_expenses,
    _clear_orders,
    _clear_sales,
    _clear_stocks,
    _max_row_date,
    _preserve_expense_fields,
    _restore_expense_fields,
    _save_advertising,
    _save_financial_report,
    _save_orders,
    _save_products,
    _save_sales,
    _save_sales_from_financial_report,
    _save_stocks,
)
from utils.security import decrypt_text
from wb_api.client import WbApiError, WbClient, WbInvalidToken, WbLimitedPermission, WbRateLimited

logger = logging.getLogger(__name__)

STEP_NAMES = [
    "token_check",
    "products",
    "stocks",
    "sales",
    "orders",
    "finance_reports",
    "advertising",
    "dashboard_calc",
]

STEP_PROGRESS = {
    "token_check": 5,
    "products": 15,
    "stocks": 25,
    "sales": 45,
    "orders": 55,
    "finance_reports": 80,
    "advertising": 90,
    "dashboard_calc": 100,
}

# Retry delays in seconds: immediate, 10s, 30s, 60s
RETRY_DELAYS = [0, 10, 30, 60]
ACTIVE_JOB_TTL = timedelta(minutes=90)
settings = get_settings()


# ─── Public API ───────────────────────────────────────────────────────────────

def create_sync_job(db: Session, user_id: int, wb_token_id: int | None, sync_type: str = "manual_sync") -> SyncJob:
    """Create a new SyncJob and cancel any stale queued/running jobs."""
    if sync_type == "auto_sync":
        active_job = db.scalar(
            select(SyncJob)
            .where(
                SyncJob.user_id == user_id,
                SyncJob.wb_token_id == wb_token_id,
                SyncJob.status.in_(["queued", "running", "partial"]),
            )
            .order_by(SyncJob.id.desc())
        )
        if active_job:
            active_since = active_job.started_at or active_job.created_at
            if active_since and (_now() - active_since) <= ACTIVE_JOB_TTL:
                return active_job

    stale = db.scalars(
        select(SyncJob).where(SyncJob.user_id == user_id, SyncJob.status.in_(["queued", "running"]))
    ).all()
    for s in stale:
        s.status = "cancelled"
        if s.wb_token_id:
            stale_token = db.get(WbToken, s.wb_token_id)
            if stale_token:
                stale_token.sync_in_progress = False

    job = SyncJob(
        user_id=user_id,
        wb_token_id=wb_token_id,
        status="queued",
        sync_type=sync_type,
        progress_percent=0,
    )
    db.add(job)
    db.flush()

    if wb_token_id:
        wb_token = db.get(WbToken, wb_token_id)
        if wb_token:
            wb_token.sync_in_progress = True

    for step_name in STEP_NAMES:
        db.add(SyncStep(sync_job_id=job.id, step_name=step_name, status="pending"))

    db.commit()
    db.refresh(job)
    return job


def get_latest_sync_status(db: Session, user_id: int) -> dict | None:
    job = db.scalar(
        select(SyncJob).where(SyncJob.user_id == user_id).order_by(SyncJob.id.desc())
    )
    if not job:
        return None
    _expire_stale_job(db, job)
    return _job_to_dict(db, job)


def claim_next_sync_job(db: Session) -> tuple[int, int] | None:
    stmt = select(SyncJob).where(SyncJob.status == "queued").order_by(SyncJob.id.asc())
    if db.bind and db.bind.dialect.name != "sqlite":
        stmt = stmt.with_for_update(skip_locked=True)

    job = db.scalar(stmt)
    if not job:
        return None

    job.status = "running"
    if not job.started_at:
        job.started_at = _now()
    if job.wb_token_id:
        wb_token = db.get(WbToken, job.wb_token_id)
        if wb_token:
            wb_token.sync_in_progress = True
    db.commit()
    return job.id, job.user_id


async def trigger_sync_now(job_id: int, user_id: int) -> None:
    """Immediately claim and run a queued sync job (called right after job creation)."""
    db = SessionLocal()
    try:
        job = db.get(SyncJob, job_id)
        if not job or job.status != "queued":
            return
        job.status = "running"
        if not job.started_at:
            job.started_at = _now()
        if job.wb_token_id:
            wb_token = db.get(WbToken, job.wb_token_id)
            if wb_token:
                wb_token.sync_in_progress = True
        db.commit()
    except Exception as exc:
        logger.exception("trigger_sync_now failed to claim job=%s: %s", job_id, exc)
        db.close()
        return
    db.close()
    await run_background_sync(job_id, user_id)


async def resume_interrupted_jobs() -> None:
    """On server start, reset any jobs stuck in 'running' state from a previous process."""
    db = SessionLocal()
    try:
        stuck = db.scalars(
            select(SyncJob).where(SyncJob.status.in_(["running", "partial"]))
        ).all()
        for job in stuck:
            job.status = "queued"
            job.last_error = None
            if job.wb_token_id:
                wb_token = db.get(WbToken, job.wb_token_id)
                if wb_token:
                    wb_token.sync_in_progress = False
            for step in job.steps:
                if step.status == "running":
                    step.status = "pending"
                    step.started_at = None
        if stuck:
            logger.info("Resumed %s interrupted sync jobs on startup", len(stuck))
        db.commit()
    except Exception as exc:
        logger.exception("resume_interrupted_jobs failed: %s", exc)
    finally:
        db.close()


async def run_sync_worker_loop() -> None:
    logger.info("Sync worker started, poll=%ss", settings.sync_worker_poll_seconds)
    await resume_interrupted_jobs()
    while True:
        db = SessionLocal()
        try:
            claimed = claim_next_sync_job(db)
        except Exception as exc:
            logger.exception("Sync worker failed while claiming job: %s", exc)
            claimed = None
        finally:
            db.close()

        if not claimed:
            await asyncio.sleep(settings.sync_worker_poll_seconds)
            continue

        job_id, user_id = claimed
        logger.info("Sync worker picked job=%s user_id=%s", job_id, user_id)
        await run_background_sync(job_id, user_id)


async def run_background_sync(job_id: int, user_id: int) -> None:
    """Executes one queued sync job in a dedicated DB session."""
    db = SessionLocal()
    try:
        await _execute_sync(db, job_id, user_id)
    except Exception as exc:
        logger.exception("Sync job %s crashed: %s", job_id, exc)
        try:
            job = db.get(SyncJob, job_id)
            if job:
                job.status = "failed"
                job.last_error = str(exc)[:1024]
                job.finished_at = _now()
                if job.wb_token_id:
                    wb_token = db.get(WbToken, job.wb_token_id)
                    if wb_token:
                        wb_token.sync_in_progress = False
                db.commit()
        except Exception:
            pass
    finally:
        db.close()


# ─── Core sync logic ──────────────────────────────────────────────────────────

async def _execute_sync(db: Session, job_id: int, user_id: int) -> None:
    job = db.get(SyncJob, job_id)
    if not job:
        logger.error("Sync job %s not found", job_id)
        return

    if job.status != "running":
        job.status = "running"
        if not job.started_at:
            job.started_at = _now()
        db.commit()

    wb_token = _active_token(db, user_id)
    if not wb_token:
        _fail_job(db, job, "WB API-токен не подключён")
        if job.wb_token_id:
            token_for_job = db.get(WbToken, job.wb_token_id)
            if token_for_job:
                token_for_job.sync_in_progress = False
            db.commit()
        return

    wb_token.sync_in_progress = True
    db.commit()

    try:
        token_text = decrypt_text(wb_token.encrypted_token)
        client = WbClient(token_text, db=db, user_id=user_id, wb_token_id=wb_token.id)
        # Sync from start of last month to today so both "month" and "last_month" periods have data
        date_from, _ = period_dates("last_month")
        _, date_to = period_dates("month")

        logger.info("Sync job %s starting user_id=%s type=%s", job_id, user_id, job.sync_type)

        # ── token_check ──────────────────────────────────────────────────────────
        step = _get_step(db, job_id, "token_check")
        _start_step(db, job, step)
        if wb_token.token_status == "invalid":
            _fail_step(db, step, "Токен WB недействителен")
            _fail_job(db, job, "Токен WB недействителен")
            return
        _complete_step(db, job, step, progress=STEP_PROGRESS["token_check"])

        # ── products ─────────────────────────────────────────────────────────────
        step = _get_step(db, job_id, "products")
        _start_step(db, job, step)
        products_rows, status, error = await _fetch_with_retry(client.fetch_products)
        if products_rows is not None:
            saved = _save_products(db, user_id, wb_token.id, products_rows)
            db.commit()
            _complete_step(db, job, step, received=len(products_rows), saved=saved, progress=STEP_PROGRESS["products"])
        else:
            _skip_step(db, job, step, error, progress=STEP_PROGRESS["products"])
            if status == "invalid":
                _update_token_status(db, wb_token, "invalid")
                _fail_job(db, job, error)
                return
            _update_token_status(db, wb_token, status)

        # ── stocks ───────────────────────────────────────────────────────────────
        step = _get_step(db, job_id, "stocks")
        _start_step(db, job, step)
        stocks_rows, status, error = await _fetch_with_retry(client.fetch_stocks)
        if stocks_rows is not None:
            _clear_stocks(db, user_id, wb_token.id)
            saved = _save_stocks(db, user_id, wb_token.id, stocks_rows)
            db.commit()
            _complete_step(db, job, step, received=len(stocks_rows), saved=saved, progress=STEP_PROGRESS["stocks"])
        else:
            _skip_step(db, job, step, error, progress=STEP_PROGRESS["stocks"])

        # Mark as partial — we have products + stocks now
        if job.status == "running":
            job.status = "partial"
            db.commit()

        # ── sales ────────────────────────────────────────────────────────────────
        step = _get_step(db, job_id, "sales")
        _start_step(db, job, step)
        sales_rows, _status, error = await _fetch_with_retry(lambda: client.fetch_sales(date_from, date_to))
        if sales_rows is not None:
            sales_date_to = _max_row_date(sales_rows, ("date", "sale_dt", "saleDt")) or date_to
            eff_to = max(date_to, sales_date_to)
            _clear_sales(db, user_id, wb_token.id, date_from, eff_to)
            saved = _save_sales(db, user_id, wb_token.id, sales_rows, date_from, eff_to)
            db.commit()
            _complete_step(db, job, step, received=len(sales_rows), saved=saved, progress=STEP_PROGRESS["sales"])
        else:
            _skip_step(db, job, step, error, progress=STEP_PROGRESS["sales"])

        # ── orders ───────────────────────────────────────────────────────────────
        step = _get_step(db, job_id, "orders")
        _start_step(db, job, step)
        orders_rows, _status, error = await _fetch_with_retry(lambda: client.fetch_orders(date_from, date_to))
        if orders_rows is not None:
            orders_date_to = _max_row_date(orders_rows, ("date", "order_dt", "orderDt")) or date_to
            eff_to = max(date_to, orders_date_to)
            _clear_orders(db, user_id, wb_token.id, date_from, eff_to)
            saved = _save_orders(db, user_id, wb_token.id, orders_rows, date_from, eff_to)
            db.commit()
            _complete_step(db, job, step, received=len(orders_rows), saved=saved, progress=STEP_PROGRESS["orders"])
        else:
            _skip_step(db, job, step, error, progress=STEP_PROGRESS["orders"])

        # ── finance_reports ──────────────────────────────────────────────────────
        step = _get_step(db, job_id, "finance_reports")
        _start_step(db, job, step)
        preserved: dict = {}
        finance_rows, _status, error = await _fetch_with_retry(lambda: client.fetch_financial_report(date_from, date_to))
        if finance_rows is not None:
            preserved = _preserve_expense_fields(db, user_id, wb_token.id, date_from, date_to, ("advertising", "tax"))
            _clear_expenses(db, user_id, wb_token.id, date_from, date_to)
            saved = _save_financial_report(db, user_id, wb_token.id, finance_rows, date_from, date_to)
            sales_step = _get_step(db, job_id, "sales")
            if (sales_step.records_saved or 0) == 0:
                _clear_sales(db, user_id, wb_token.id, date_from, date_to)
                _save_sales_from_financial_report(db, user_id, wb_token.id, finance_rows, date_from, date_to)
            db.commit()
            _complete_step(db, job, step, received=len(finance_rows), saved=saved, progress=STEP_PROGRESS["finance_reports"])
        else:
            _skip_step(db, job, step, error, progress=STEP_PROGRESS["finance_reports"])

        # ── advertising ──────────────────────────────────────────────────────────
        step = _get_step(db, job_id, "advertising")
        _start_step(db, job, step)
        adv_rows, _status, error = await _fetch_with_retry(lambda: client.fetch_advertising(date_from, date_to))
        if adv_rows is not None:
            saved = _save_advertising(db, user_id, wb_token.id, adv_rows, date_from, date_to)
            db.commit()
            _complete_step(db, job, step, received=len(adv_rows), saved=saved, progress=STEP_PROGRESS["advertising"])
        else:
            _skip_step(db, job, step, error, progress=STEP_PROGRESS["advertising"])

        # Restore preserved expense fields (tax always; advertising if adv step failed)
        if preserved:
            restore_fields = ["tax"]
            adv_step = _get_step(db, job_id, "advertising")
            if adv_step.status != "completed":
                restore_fields.append("advertising")
            _restore_expense_fields(db, user_id, wb_token.id, preserved, restore_fields)
            db.commit()

        wb_token.last_sync_at = _now()
        db.commit()

        # ── dashboard_calc ───────────────────────────────────────────────────────
        step = _get_step(db, job_id, "dashboard_calc")
        _start_step(db, job, step)
        try:
            from services.dashboard import calculate_dashboard
            for p in ("today", "week", "month", "last_month"):
                calculate_dashboard(db, user_id, p)
            _complete_step(db, job, step, progress=STEP_PROGRESS["dashboard_calc"])
        except Exception as exc:
            _skip_step(db, job, step, str(exc)[:512], progress=STEP_PROGRESS["dashboard_calc"])
            logger.warning("Dashboard calc failed in sync job %s: %s", job_id, exc)

        # ── Finalise job status ───────────────────────────────────────────────────
        db.refresh(job)
        failed_critical = any(s.step_name == "token_check" and s.status == "failed" for s in job.steps)
        if failed_critical:
            job.status = "failed"
        else:
            completed_count = sum(1 for s in job.steps if s.status == "completed")
            job.status = "completed" if completed_count == len(job.steps) else "partial"
        job.finished_at = _now()
        db.commit()

        logger.info(
            "Sync job %s finished status=%s user_id=%s progress=%s%%",
            job_id, job.status, user_id, job.progress_percent,
        )
    finally:
        wb_token.sync_in_progress = False
        db.commit()


# ─── Retry wrapper ────────────────────────────────────────────────────────────

async def _fetch_with_retry(fetcher) -> tuple[list | None, str, str]:
    """
    Retry fetcher with exponential backoff on rate-limit and API errors.
    Returns (rows, status, error_message). status: "ok" | "invalid" | "limited" | "rate_limited" | "api_error"
    """
    last_exc: Exception | None = None
    for attempt, delay in enumerate(RETRY_DELAYS):
        if delay > 0:
            logger.info("Sync retry attempt=%s, sleeping=%ss", attempt, delay)
            await asyncio.sleep(delay)
        try:
            rows = await fetcher()
            return rows, "ok", ""
        except WbRateLimited as exc:
            last_exc = exc
            logger.warning("WB rate-limited attempt=%s: %s", attempt, exc)
            if attempt < len(RETRY_DELAYS) - 1:
                continue
        except WbInvalidToken as exc:
            return None, "invalid", str(exc)
        except WbLimitedPermission as exc:
            return None, "limited", str(exc)
        except WbApiError as exc:
            last_exc = exc
            logger.warning("WB api_error attempt=%s: %s", attempt, exc)
            if attempt < len(RETRY_DELAYS) - 1:
                continue
    return None, "rate_limited", str(last_exc) if last_exc else "WB вернул ошибку"


# ─── Step helpers ─────────────────────────────────────────────────────────────

def _get_step(db: Session, job_id: int, step_name: str) -> SyncStep:
    return db.scalar(select(SyncStep).where(SyncStep.sync_job_id == job_id, SyncStep.step_name == step_name))


def _start_step(db: Session, job: SyncJob, step: SyncStep) -> None:
    step.status = "running"
    step.started_at = _now()
    job.current_step = step.step_name
    db.commit()


def _complete_step(
    db: Session, job: SyncJob, step: SyncStep,
    received: int | None = None, saved: int | None = None, progress: int = 0,
) -> None:
    step.status = "completed"
    step.finished_at = _now()
    step.records_received = received
    step.records_saved = saved
    job.progress_percent = progress
    db.commit()


def _skip_step(db: Session, job: SyncJob, step: SyncStep, error: str | None, progress: int = 0) -> None:
    step.status = "skipped"
    step.finished_at = _now()
    step.error_message = (error or "")[:1024]
    job.progress_percent = progress
    db.commit()


def _fail_step(db: Session, step: SyncStep, error: str) -> None:
    step.status = "failed"
    step.finished_at = _now()
    step.error_message = error[:1024]
    db.commit()


def _fail_job(db: Session, job: SyncJob, error: str) -> None:
    job.status = "failed"
    job.last_error = error[:1024]
    job.finished_at = _now()
    db.commit()


def _update_token_status(db: Session, wb_token: WbToken, status: str) -> None:
    if status == "invalid":
        wb_token.token_status = "invalid"
        wb_token.is_active = False
    elif status == "rate_limited":
        wb_token.token_status = "rate_limited"
    elif status == "limited" and wb_token.token_status == "active":
        wb_token.token_status = "limited"
    elif status == "api_error" and wb_token.token_status in ("active", "api_error"):
        wb_token.token_status = "api_error"
    db.commit()


# ─── Utilities ────────────────────────────────────────────────────────────────

def _active_token(db: Session, user_id: int) -> WbToken | None:
    return db.scalar(
        select(WbToken)
        .where(WbToken.user_id == user_id, WbToken.is_active.is_(True), WbToken.token_status != "invalid")
        .order_by(WbToken.id.desc())
    )


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _job_to_dict(db: Session, job: SyncJob) -> dict:
    steps = sorted(job.steps, key=lambda s: s.id)
    return {
        "job_id": job.id,
        "status": job.status,
        "sync_type": job.sync_type,
        "progress_percent": job.progress_percent,
        "current_step": job.current_step,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "finished_at": job.finished_at.isoformat() if job.finished_at else None,
        "last_error": job.last_error,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "steps": [
            {
                "step_name": s.step_name,
                "status": s.status,
                "records_received": s.records_received,
                "records_saved": s.records_saved,
                "error_message": s.error_message,
                "started_at": s.started_at.isoformat() if s.started_at else None,
                "finished_at": s.finished_at.isoformat() if s.finished_at else None,
            }
            for s in steps
        ],
    }


def _expire_stale_job(db: Session, job: SyncJob) -> None:
    if job.status not in {"queued", "running", "partial"}:
        return
    active_since = job.started_at or job.created_at
    if not active_since or (_now() - active_since) <= ACTIVE_JOB_TTL:
        return

    job.status = "failed"
    job.last_error = "Синхронизация была прервана или зависла. Запустите её повторно."
    job.finished_at = _now()

    if job.wb_token_id:
        wb_token = db.get(WbToken, job.wb_token_id)
        if wb_token:
            wb_token.sync_in_progress = False

    running_steps = [step for step in job.steps if step.status == "running"]
    for step in running_steps:
        step.status = "failed"
        step.finished_at = _now()
        if not step.error_message:
            step.error_message = "Шаг прерван из-за устаревшей синхронизации"

    db.commit()
