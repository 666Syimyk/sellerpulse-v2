from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from pydantic import BaseModel
from sqlalchemy.orm import Session

from database import get_db
from models.entities import User
from routes.deps import current_user
from services.financial_report import (
    FinancialReportError,
    apply_report_settings,
    clear_latest_report,
    latest_report,
    process_upload,
    recalculate_report,
    update_report_item_cost,
    update_report_tax,
    validate_upload,
)

router = APIRouter(prefix="/financial-report", tags=["financial-report"])


class ReportTaxPayload(BaseModel):
    tax: float | None = None


class ReportItemCostPayload(BaseModel):
    cost_price: float | None = None


@router.get("")
def get_latest_report(user: User = Depends(current_user), db: Session = Depends(get_db)):
    report = latest_report(db, user.id)
    return report or {"report": None, "items": [], "message": "Финансовый отчёт ещё не загружен."}


@router.post("/upload")
async def upload_financial_report(
    file: UploadFile = File(...),
    user: User = Depends(current_user),
    db: Session = Depends(get_db),
):
    file_name = file.filename or "financial-report"
    if not file_name.lower().endswith((".xlsx", ".xls", ".csv", ".zip")):
        raise HTTPException(status_code=400, detail="Не удалось прочитать файл. Проверьте формат Excel/CSV.")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Файл пустой или не содержит данных.")

    try:
        return process_upload(db, user.id, file_name, content)
    except FinancialReportError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/validate")
async def validate_financial_report(
    file: UploadFile = File(...),
    user: User = Depends(current_user),
):
    file_name = file.filename or "financial-report"
    if not file_name.lower().endswith((".xlsx", ".xls", ".csv", ".zip")):
        raise HTTPException(status_code=400, detail="Не удалось прочитать файл. Проверьте формат Excel/CSV.")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Файл пустой или не содержит данных.")

    try:
        return validate_upload(file_name, content)
    except FinancialReportError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{report_id}/recalculate")
def recalculate(report_id: int, user: User = Depends(current_user), db: Session = Depends(get_db)):
    report = recalculate_report(db, user.id, report_id)
    if not report:
        raise HTTPException(status_code=404, detail="Финансовый отчёт не найден.")
    return report


@router.patch("/{report_id}/tax")
def save_report_tax(
    report_id: int,
    payload: ReportTaxPayload,
    user: User = Depends(current_user),
    db: Session = Depends(get_db),
):
    if payload.tax is not None and (payload.tax < 0 or payload.tax > 100):
        raise HTTPException(status_code=400, detail="Налог должен быть от 0 до 100%.")
    report = update_report_tax(db, user.id, report_id, payload.tax)
    if not report:
        raise HTTPException(status_code=404, detail="Финансовый отчёт не найден.")
    return report


@router.patch("/{report_id}/items/{item_id}/cost-price")
def save_report_item_cost(
    report_id: int,
    item_id: int,
    payload: ReportItemCostPayload,
    user: User = Depends(current_user),
    db: Session = Depends(get_db),
):
    if payload.cost_price is not None and payload.cost_price < 0:
        raise HTTPException(status_code=400, detail="Себестоимость не может быть отрицательной.")
    report = update_report_item_cost(db, user.id, report_id, item_id, payload.cost_price)
    if not report:
        raise HTTPException(status_code=404, detail="Строка финансового отчёта не найдена.")
    return report


class ApplySettingsPayload(BaseModel):
    global_cost_price: float | None = None
    tax_percent: float | None = None
    global_advertising: float | None = None
    global_other_expenses: float | None = None


@router.post("/{report_id}/apply-settings")
def apply_settings(
    report_id: int,
    payload: ApplySettingsPayload,
    user: User = Depends(current_user),
    db: Session = Depends(get_db),
):
    if payload.global_cost_price is not None and payload.global_cost_price < 0:
        raise HTTPException(status_code=400, detail="Себестоимость не может быть отрицательной.")
    if payload.tax_percent is not None and (payload.tax_percent < 0 or payload.tax_percent > 100):
        raise HTTPException(status_code=400, detail="Налог должен быть от 0 до 100%.")
    result = apply_report_settings(
        db, user.id, report_id,
        payload.global_cost_price,
        payload.tax_percent,
        payload.global_advertising,
        payload.global_other_expenses,
    )
    if not result:
        raise HTTPException(status_code=404, detail="Финансовый отчёт не найден.")
    return result


@router.delete("")
def clear_report(user: User = Depends(current_user), db: Session = Depends(get_db)):
    removed = clear_latest_report(db, user.id)
    return {"ok": True, "removed": removed}
