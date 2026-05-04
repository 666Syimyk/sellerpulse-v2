from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from database import get_db
from models.entities import User
from routes.deps import current_user
from services.background_sync import get_latest_sync_status

router = APIRouter(prefix="/sync", tags=["sync"])


@router.get("/status")
def sync_status(user: User = Depends(current_user), db: Session = Depends(get_db)):
    status = get_latest_sync_status(db, user.id)
    if not status:
        return {"status": "not_started", "progress_percent": 0, "steps": []}
    return status
