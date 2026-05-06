import asyncio

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from sqlalchemy.orm import Session

from database import get_db
from models.entities import User
from routes.deps import current_user
from services.background_sync import create_sync_job, find_retryable_sync_job, get_latest_sync_status, trigger_sync_now
from services.dashboard import calculate_dashboard
from services.export import generate_excel
from services.sync import _active_token

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("")
def dashboard(period: str = "month", user: User = Depends(current_user), db: Session = Depends(get_db)):
    try:
        return calculate_dashboard(db, user.id, period)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="\u041d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u043d\u044b\u0439 \u043f\u0435\u0440\u0438\u043e\u0434 dashboard") from exc


@router.get("/export")
def export_excel(period: str = "month", user: User = Depends(current_user), db: Session = Depends(get_db)):
    try:
        data = calculate_dashboard(db, user.id, period)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="\u041d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u043d\u044b\u0439 \u043f\u0435\u0440\u0438\u043e\u0434") from exc
    xlsx_bytes = generate_excel(data)
    filename = f"sellerpulse_{period}.xlsx"
    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/sync")
async def sync(
    period: str = "month",
    user: User = Depends(current_user),
    db: Session = Depends(get_db),
):
    wb_token = _active_token(db, user.id)
    if not wb_token:
        return {
            "status": "error",
            "message": "WB API-\u0442\u043e\u043a\u0435\u043d \u043d\u0435 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0451\u043d. \u041f\u043e\u0434\u043a\u043b\u044e\u0447\u0438\u0442\u0435 \u0442\u043e\u043a\u0435\u043d \u0438\u043b\u0438 \u0437\u0430\u0433\u0440\u0443\u0437\u0438\u0442\u0435 \u0444\u0438\u043d\u0430\u043d\u0441\u043e\u0432\u044b\u0439 \u043e\u0442\u0447\u0451\u0442 WB.",
            "sync_job_id": None,
        }

    retry_from = find_retryable_sync_job(db, user.id, wb_token.id)
    retryable = retry_from is not None
    sync_type = "retry_partial" if retryable else "manual_sync"

    job = create_sync_job(
        db,
        user.id,
        wb_token.id,
        sync_type=sync_type,
        retry_from=retry_from if retryable else None,
    )
    asyncio.create_task(trigger_sync_now(job.id, user.id))

    return {
        "status": "queued",
        "message": (
            "\u041f\u043e\u0432\u0442\u043e\u0440 \u043f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u043d\u044b\u0445 \u0448\u0430\u0433\u043e\u0432 \u0437\u0430\u043f\u0443\u0449\u0435\u043d. \u0414\u0430\u043d\u043d\u044b\u0435 \u0434\u043e\u0433\u0440\u0443\u0437\u044f\u0442\u0441\u044f \u043f\u043e\u0441\u0442\u0435\u043f\u0435\u043d\u043d\u043e."
            if retryable
            else "\u0421\u0438\u043d\u0445\u0440\u043e\u043d\u0438\u0437\u0430\u0446\u0438\u044f \u0437\u0430\u043f\u0443\u0449\u0435\u043d\u0430. \u0414\u0430\u043d\u043d\u044b\u0435 \u0431\u0443\u0434\u0443\u0442 \u043e\u0431\u043d\u043e\u0432\u043b\u044f\u0442\u044c\u0441\u044f \u043f\u043e\u0441\u0442\u0435\u043f\u0435\u043d\u043d\u043e."
        ),
        "sync_job_id": job.id,
        "sync_status": get_latest_sync_status(db, user.id),
    }
