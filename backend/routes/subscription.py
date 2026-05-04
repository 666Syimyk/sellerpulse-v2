from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from database import get_db
from models.entities import User
from routes.deps import current_user
from services.subscription import serialize

router = APIRouter(prefix="/subscription", tags=["subscription"])


@router.get("")
def get_subscription(user: User = Depends(current_user), db: Session = Depends(get_db)):
    db.refresh(user)
    return serialize(user.subscription)
