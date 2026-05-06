"""
Автосинхронизация: каждые N часов синхронизирует данные всех активных пользователей.
Запускается при старте FastAPI через lifespan.
"""
import asyncio
import logging
from datetime import datetime, timezone

from database import SessionLocal
from models.entities import WbToken
from services.background_sync import create_sync_job, find_retryable_sync_job
from sqlalchemy import select

logger = logging.getLogger(__name__)

SYNC_INTERVAL_HOURS = 6
_task: asyncio.Task | None = None


async def _sync_all_users() -> None:
    db = SessionLocal()
    try:
        tokens = db.scalars(
            select(WbToken).where(
                WbToken.is_active.is_(True),
                WbToken.token_status != "invalid",
                WbToken.sync_in_progress.is_(False),
            )
        ).all()
        logger.info("Auto-sync: найдено %s активных токенов", len(tokens))
        for token in tokens:
            try:
                retry_from = find_retryable_sync_job(db, token.user_id, token.id)
                sync_type = "retry_partial" if retry_from else "auto_sync"
                job = create_sync_job(db, token.user_id, token.id, sync_type=sync_type, retry_from=retry_from)
                logger.info("Auto-sync: job queued=%s user_id=%s type=%s", job.id, token.user_id, sync_type)
            except Exception as exc:
                logger.exception("Auto-sync: ошибка для user_id=%s: %s", token.user_id, exc)
    finally:
        db.close()


async def _scheduler_loop() -> None:
    await asyncio.sleep(60)
    while True:
        try:
            logger.info("Auto-sync: начало планового цикла %s", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
            await _sync_all_users()
        except Exception as exc:
            logger.exception("Auto-sync: сбой цикла: %s", exc)
        await asyncio.sleep(SYNC_INTERVAL_HOURS * 3600)


def start_scheduler() -> None:
    global _task
    _task = asyncio.create_task(_scheduler_loop())
    logger.info("Auto-sync: планировщик запущен, интервал %s ч.", SYNC_INTERVAL_HOURS)


def stop_scheduler() -> None:
    global _task
    if _task and not _task.done():
        _task.cancel()
        logger.info("Auto-sync: планировщик остановлен")
