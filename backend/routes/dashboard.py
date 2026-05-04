from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from sqlalchemy.orm import Session

from database import get_db
from models.entities import User
from routes.deps import current_user
from services.background_sync import create_sync_job, get_latest_sync_status
from services.dashboard import calculate_dashboard
from services.export import generate_excel
from services.sync import _active_token

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("")
def dashboard(period: str = "month", user: User = Depends(current_user), db: Session = Depends(get_db)):
    try:
        return calculate_dashboard(db, user.id, period)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Неизвестный период dashboard") from exc


@router.get("/export")
def export_excel(period: str = "month", user: User = Depends(current_user), db: Session = Depends(get_db)):
    try:
        data = calculate_dashboard(db, user.id, period)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Неизвестный период") from exc
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
            "message": "WB API-токен не подключён. Подключите токен или загрузите финансовый отчёт WB.",
            "sync_job_id": None,
        }

    job = create_sync_job(db, user.id, wb_token.id, sync_type="manual_sync")

    return {
        "status": "queued",
        "message": "Синхронизация запущена. Данные будут обновляться постепенно.",
        "sync_job_id": job.id,
        "sync_status": get_latest_sync_status(db, user.id),
    }
