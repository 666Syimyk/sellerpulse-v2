import logging
from datetime import datetime, timedelta, timezone
from typing import Callable, Coroutine

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from models.entities import Expense, Order, Product, Sale, Stock, WbToken
from services.periods import period_dates
from utils.security import decrypt_text
from wb_api.client import WbApiError, WbClient, WbInvalidToken, WbLimitedPermission, WbRateLimited

SYNC_STALE_AFTER = timedelta(minutes=10)
DATA_ACCURACY_EXACT = "Точные данные WB"
DATA_ACCURACY_ESTIMATED = "Оценочный расчёт"

logger = logging.getLogger(__name__)


async def sync_wb_data(db: Session, user_id: int, period: str = "month") -> dict:
    date_from, date_to = period_dates(period)
    result = _sync_result(period, date_from, date_to)
    wb_token = _active_token(db, user_id)
    if not wb_token:
        result["message"] = "WB API-токен не подключён"
        result["errors"].append({"source": "token", "status": "missing", "message": result["message"]})
        return result

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    if wb_token.sync_in_progress:
        lock_age = now - wb_token.updated_at if wb_token.updated_at else None
        if lock_age and lock_age > SYNC_STALE_AFTER:
            wb_token.sync_in_progress = False
            db.commit()
        else:
            result["message"] = "Синхронизация уже идёт. Подождите немного."
            result["errors"].append({"source": "sync", "status": "in_progress", "message": result["message"]})
            return result

    wb_token.sync_in_progress = True
    db.commit()

    token = decrypt_text(wb_token.encrypted_token)
    client = WbClient(token, db=db, user_id=user_id, wb_token_id=wb_token.id)
    sources = result["sources"]
    errors = result["errors"]

    try:
        logger.info(
            "WB sync started user_id=%s wb_token_id=%s period=%s date_from=%s date_to=%s",
            user_id,
            wb_token.id,
            period,
            date_from,
            date_to,
        )

        products = await _fetch_source(sources, errors, "products", client.fetch_products)
        if products is not None:
            result["products_count"] = len(products)
            result["saved_products"] = _save_products(db, user_id, wb_token.id, products)
            logger.info("WB sync saved source=products received=%s saved=%s", len(products), result["saved_products"])

        sales = await _fetch_source(sources, errors, "sales", lambda: client.fetch_sales(date_from, date_to))
        if sales is not None:
            result["sales_count"] = len(sales)
            sales_date_to = _max_row_date(sales, ("date", "sale_dt", "saleDt")) or date_to
            _clear_sales(db, user_id, wb_token.id, date_from, max(date_to, sales_date_to))
            result["saved_sales"] = _save_sales(db, user_id, wb_token.id, sales, date_from, max(date_to, sales_date_to))
            logger.info("WB sync saved source=sales received=%s saved=%s", len(sales), result["saved_sales"])

        orders = await _fetch_source(sources, errors, "orders", lambda: client.fetch_orders(date_from, date_to))
        if orders is not None:
            result["orders_count"] = len(orders)
            orders_date_to = _max_row_date(orders, ("date", "order_dt", "orderDt")) or date_to
            _clear_orders(db, user_id, wb_token.id, date_from, max(date_to, orders_date_to))
            result["saved_orders"] = _save_orders(db, user_id, wb_token.id, orders, date_from, max(date_to, orders_date_to))
            logger.info("WB sync saved source=orders received=%s saved=%s", len(orders), result["saved_orders"])

        stocks = await _fetch_source(sources, errors, "stocks", client.fetch_stocks)
        if stocks is not None:
            result["stocks_count"] = len(stocks)
            _clear_stocks(db, user_id, wb_token.id)
            result["saved_stocks"] = _save_stocks(db, user_id, wb_token.id, stocks)
            logger.info("WB sync saved source=stocks received=%s saved=%s", len(stocks), result["saved_stocks"])

        preserved_expense_fields: dict[tuple[int, object], dict] = {}
        finance_rows = await _fetch_source(sources, errors, "finance", lambda: client.fetch_financial_report(date_from, date_to))
        if finance_rows is not None:
            result["finance_count"] = len(finance_rows)
            preserved_expense_fields = _preserve_expense_fields(db, user_id, wb_token.id, date_from, date_to, ("advertising", "tax"))
            _clear_expenses(db, user_id, wb_token.id, date_from, date_to)
            result["saved_finance"] = _save_financial_report(db, user_id, wb_token.id, finance_rows, date_from, date_to)
            if result["saved_sales"] == 0:
                _clear_sales(db, user_id, wb_token.id, date_from, date_to)
                result["saved_sales"] = _save_sales_from_financial_report(db, user_id, wb_token.id, finance_rows, date_from, date_to)
            logger.info("WB sync saved source=finance received=%s saved=%s", len(finance_rows), result["saved_finance"])

        advertising_rows = await _fetch_source(sources, errors, "advertising", lambda: client.fetch_advertising(date_from, date_to))
        if advertising_rows is not None:
            result["advertising_count"] = len(advertising_rows)
            result["saved_advertising"] = _save_advertising(db, user_id, wb_token.id, advertising_rows, date_from, date_to)
            logger.info("WB sync saved source=advertising received=%s saved=%s", len(advertising_rows), result["saved_advertising"])

        if preserved_expense_fields:
            restore_fields = ["tax"]
            if advertising_rows is None:
                restore_fields.append("advertising")
            result["restored_expense_fields"] = _restore_expense_fields(db, user_id, wb_token.id, preserved_expense_fields, restore_fields)
            logger.info("WB sync restored expense fields count=%s fields=%s", result["restored_expense_fields"], restore_fields)

        ok_count = sum(1 for source in sources.values() if source["ok"])
        saved_count = (
            result["saved_products"]
            + result["saved_sales"]
            + result["saved_orders"]
            + result["saved_stocks"]
            + result["saved_finance"]
            + result["saved_advertising"]
            + result["restored_expense_fields"]
        )

        if any(source.get("status") == "invalid" for source in sources.values()):
            wb_token.token_status = "invalid"
            wb_token.is_active = False
        elif any(source.get("status") == "rate_limited" for source in sources.values()):
            wb_token.token_status = "rate_limited"
        elif any(source.get("status") == "api_error" for source in sources.values()) and ok_count == 0:
            wb_token.token_status = "api_error"
        elif any(source.get("status") == "limited" for source in sources.values()) and wb_token.token_status == "active":
            wb_token.token_status = "limited"

        wb_token.last_sync_at = now
        result["sync_success"] = ok_count > 0
        result["ok"] = result["sync_success"]
        result["message"] = _sync_message(saved_count, ok_count, len(sources))
        db.commit()

        logger.info(
            "WB sync finished user_id=%s wb_token_id=%s ok_sources=%s saved=%s errors=%s",
            user_id,
            wb_token.id,
            ok_count,
            saved_count,
            len(errors),
        )
        return result
    finally:
        wb_token.sync_in_progress = False
        db.commit()


async def quick_bootstrap_wb_data(db: Session, user_id: int, wb_token: WbToken, token_text: str) -> dict:
    result = {
        "ok": False,
        "message": "",
        "products_count": 0,
        "saved_products": 0,
        "errors": [],
        "sources": {},
    }
    client = WbClient(token_text, db=db, user_id=user_id, wb_token_id=wb_token.id)
    products = await _fetch_source(result["sources"], result["errors"], "products", client.fetch_products)
    if products is not None:
        result["products_count"] = len(products)
        result["saved_products"] = _save_products(db, user_id, wb_token.id, products)
        result["ok"] = True
        result["message"] = (
            "Токен подключён. Товары WB загружены. Для точной аналитики загрузите финансовый отчёт WB."
            if result["saved_products"]
            else "Токен подключён. WB не вернул товары. Для точной аналитики загрузите финансовый отчёт WB."
        )
    else:
        status = result["sources"].get("products", {}).get("status")
        if status == "rate_limited":
            wb_token.token_status = "rate_limited"
            result["message"] = "Токен подключён. WB временно ограничил запросы. Для точной аналитики загрузите финансовый отчёт WB."
        elif status == "limited" and wb_token.token_status == "active":
            wb_token.token_status = "limited"
            result["message"] = "Токен подключён, но не хватает прав на товары. Для точной аналитики загрузите финансовый отчёт WB."
        elif status == "invalid":
            wb_token.token_status = "invalid"
            wb_token.is_active = False
            result["message"] = "WB API-токен неправильный."
        else:
            wb_token.token_status = "api_error"
            result["message"] = "Токен подключён. WB API временно недоступен. Для точной аналитики загрузите финансовый отчёт WB."

    db.commit()
    return result


def _sync_result(period: str, date_from, date_to) -> dict:
    return {
        "ok": False,
        "sync_success": False,
        "message": "",
        "period": period,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "products_count": 0,
        "sales_count": 0,
        "orders_count": 0,
        "stocks_count": 0,
        "finance_count": 0,
        "advertising_count": 0,
        "saved_products": 0,
        "saved_sales": 0,
        "saved_orders": 0,
        "saved_stocks": 0,
        "saved_finance": 0,
        "saved_advertising": 0,
        "restored_expense_fields": 0,
        "dashboard_recalculated": False,
        "errors": [],
        "sources": {},
    }


def _sync_message(saved_count: int, ok_count: int, sources_count: int) -> str:
    if saved_count == 0 and ok_count == 0:
        return "WB временно ограничил запросы или не вернул данные. Для точной аналитики загрузите финансовый отчёт WB."
    if saved_count == 0:
        return "WB ответил, но за выбранный период вернул пустые списки. Для точной аналитики загрузите финансовый отчёт WB."
    if ok_count == sources_count:
        return "Синхронизация завершена."
    return "Синхронизация завершена частично. Для недоступных разделов показываем последние сохранённые данные или «Нет данных WB»."


async def _fetch_source(sources: dict, errors: list[dict], name: str, fetcher: Callable[[], Coroutine]) -> list[dict] | None:
    logger.info("WB sync fetching source=%s", name)
    try:
        rows = await fetcher()
        sources[name] = {"ok": True, "rows": len(rows), "status": "ok"}
        logger.info("WB sync fetched source=%s rows=%s status=ok", name, len(rows))
        return rows
    except WbRateLimited as exc:
        _source_error(sources, errors, name, "rate_limited", exc)
    except (WbInvalidToken, WbLimitedPermission) as exc:
        status = "invalid" if isinstance(exc, WbInvalidToken) else "limited"
        _source_error(sources, errors, name, status, exc)
    except WbApiError as exc:
        _source_error(sources, errors, name, "api_error", exc)
    return None


def _source_error(sources: dict, errors: list[dict], name: str, status: str, exc: WbApiError) -> None:
    status_code = getattr(exc, "status_code", None)
    endpoint = getattr(exc, "endpoint", None)
    sources[name] = {"ok": False, "rows": 0, "status": status, "status_code": status_code, "endpoint": endpoint, "error": str(exc)}
    errors.append({"source": name, "status": status, "status_code": status_code, "endpoint": endpoint, "message": str(exc)})
    logger.warning(
        "WB sync source failed source=%s status=%s status_code=%s endpoint=%s error=%s",
        name,
        status,
        status_code,
        endpoint,
        exc,
    )


def _active_token(db: Session, user_id: int) -> WbToken | None:
    return db.scalar(
        select(WbToken)
        .where(WbToken.user_id == user_id, WbToken.is_active.is_(True), WbToken.token_status != "invalid")
        .order_by(WbToken.id.desc())
    )


def _clear_sales(db: Session, user_id: int, wb_token_id: int, date_from, date_to) -> None:
    db.execute(delete(Sale).where(Sale.user_id == user_id, Sale.wb_token_id == wb_token_id, Sale.date >= date_from, Sale.date <= date_to))


def _clear_orders(db: Session, user_id: int, wb_token_id: int, date_from, date_to) -> None:
    db.execute(delete(Order).where(Order.user_id == user_id, Order.wb_token_id == wb_token_id, Order.date >= date_from, Order.date <= date_to))


def _clear_expenses(db: Session, user_id: int, wb_token_id: int, date_from, date_to) -> None:
    db.execute(delete(Expense).where(Expense.user_id == user_id, Expense.wb_token_id == wb_token_id, Expense.date >= date_from, Expense.date <= date_to))


def _clear_stocks(db: Session, user_id: int, wb_token_id: int) -> None:
    db.execute(delete(Stock).where(Stock.user_id == user_id, Stock.wb_token_id == wb_token_id))


def _preserve_expense_fields(db: Session, user_id: int, wb_token_id: int, date_from, date_to, fields: tuple[str, ...]) -> dict[tuple[int, object], dict]:
    preserved: dict[tuple[int, object], dict] = {}
    expenses = db.scalars(
        select(Expense).where(
            Expense.user_id == user_id,
            Expense.wb_token_id == wb_token_id,
            Expense.date >= date_from,
            Expense.date <= date_to,
        )
    ).all()
    for expense in expenses:
        values = {field: getattr(expense, field) for field in fields if getattr(expense, field) is not None}
        if values:
            preserved[(expense.nm_id, expense.date)] = values
    return preserved


def _restore_expense_fields(db: Session, user_id: int, wb_token_id: int, preserved: dict[tuple[int, object], dict], fields: list[str]) -> int:
    restored = 0
    for (nm_id, expense_date), values in preserved.items():
        selected_values = {field: values[field] for field in fields if field in values}
        if not selected_values:
            continue
        expense = db.scalar(select(Expense).where(Expense.wb_token_id == wb_token_id, Expense.nm_id == nm_id, Expense.date == expense_date))
        if not expense:
            db.add(
                Expense(
                    user_id=user_id,
                    wb_token_id=wb_token_id,
                    nm_id=nm_id,
                    date=expense_date,
                    data_accuracy=DATA_ACCURACY_ESTIMATED,
                    **selected_values,
                )
            )
            restored += len(selected_values)
            continue
        for field, value in selected_values.items():
            if getattr(expense, field) is None:
                setattr(expense, field, value)
                restored += 1
    return restored


def _save_products(db: Session, user_id: int, wb_token_id: int, rows: list[dict]) -> int:
    saved = 0
    for row in rows:
        nm_id = _int(row.get("nmID") or row.get("nmId"))
        if not nm_id:
            continue
        _upsert_product(
            db,
            user_id,
            wb_token_id,
            nm_id,
            row.get("vendorCode"),
            row.get("title") or row.get("name") or row.get("imtName"),
            row.get("brand"),
            row.get("subjectName") or row.get("object") or row.get("category"),
        )
        saved += 1
    return saved


def _save_sales(db: Session, user_id: int, wb_token_id: int, rows: list[dict], date_from, date_to) -> int:
    grouped: dict[tuple[int, object], dict] = {}
    for row in rows:
        nm_id = _int(row.get("nmId") or row.get("nmID") or row.get("nm_id"))
        sale_date = _date(row.get("date") or row.get("sale_dt") or row.get("saleDt"))
        if not nm_id or not sale_date or sale_date < date_from or sale_date > date_to:
            continue
        quantity = _signed_sale_quantity(row)
        if quantity <= 0:
            continue

        _upsert_product(db, user_id, wb_token_id, nm_id, row.get("supplierArticle") or row.get("sa_name"), _product_name(row), row.get("brand"), row.get("subject"))
        key = (nm_id, sale_date)
        item = grouped.setdefault(key, {"quantity": 0, "before": 0.0, "spp": 0.0, "after": 0.0})
        before = _first_float(row, "priceWithDisc", "retail_price_withdisc_rub", "retailPriceWithDisc", "retail_amount", "retailAmount")
        after = _first_float(row, "finishedPrice", "retail_price_withdisc_rub", "retailPriceWithDisc", "priceWithDisc", "forPay", "ppvzForPay")
        if after is None and before is not None:
            after = before
        spp = _first_float(row, "spp")
        if spp is None and before is not None and after is not None:
            spp = max(before - after, 0)
        item["quantity"] += quantity
        item["before"] += before or 0
        item["after"] += after or 0
        item["spp"] += spp or 0

    for (nm_id, sale_date), item in grouped.items():
        db.add(
            Sale(
                user_id=user_id,
                wb_token_id=wb_token_id,
                nm_id=nm_id,
                date=sale_date,
                quantity=item["quantity"],
                total_before_spp=item["before"],
                spp_amount=item["spp"],
                total_after_spp=item["after"],
            )
        )
    return len(grouped)


def _save_sales_from_financial_report(db: Session, user_id: int, wb_token_id: int, rows: list[dict], date_from, date_to) -> int:
    grouped: dict[tuple[int, object], dict] = {}
    for row in rows:
        nm_id = _int(row.get("nmId") or row.get("nmID") or row.get("nm_id"))
        sale_date = _date(row.get("saleDt") or row.get("sale_dt") or row.get("rrDate") or row.get("rr_dt"))
        quantity = _int(row.get("quantity")) or 0
        if not nm_id or not sale_date or sale_date < date_from or sale_date > date_to or quantity <= 0:
            continue
        if _is_return_row(row):
            continue

        _upsert_product(
            db,
            user_id,
            wb_token_id,
            nm_id,
            row.get("vendorCode") or row.get("sa_name"),
            _product_name(row),
            row.get("brand") or row.get("brandName"),
            row.get("subjectName"),
        )
        key = (nm_id, sale_date)
        item = grouped.setdefault(key, {"quantity": 0, "before": 0.0, "spp": 0.0, "after": 0.0})
        before = _first_float(row, "retailPriceWithDisc", "retail_price_withdisc_rub", "retailAmount")
        after = _first_float(row, "retailAmount", "retail_amount", "retailPriceWithDisc")
        spp = max(before - after, 0) if before is not None and after is not None else 0
        item["quantity"] += quantity
        item["before"] += before or 0
        item["after"] += after or 0
        item["spp"] += spp

    for (nm_id, sale_date), item in grouped.items():
        db.add(
            Sale(
                user_id=user_id,
                wb_token_id=wb_token_id,
                nm_id=nm_id,
                date=sale_date,
                quantity=item["quantity"],
                total_before_spp=item["before"],
                spp_amount=item["spp"],
                total_after_spp=item["after"],
            )
        )
    return len(grouped)


def _save_orders(db: Session, user_id: int, wb_token_id: int, rows: list[dict], date_from, date_to) -> int:
    grouped: dict[tuple[int, object], int] = {}
    for row in rows:
        nm_id = _int(row.get("nmId") or row.get("nmID") or row.get("nm_id"))
        order_date = _date(row.get("date") or row.get("order_dt") or row.get("orderDt"))
        if not nm_id or not order_date or order_date < date_from or order_date > date_to:
            continue
        if row.get("isCancel") is True:
            continue
        _upsert_product(db, user_id, wb_token_id, nm_id, row.get("supplierArticle") or row.get("sa_name"), _product_name(row), row.get("brand"), row.get("subject"))
        grouped[(nm_id, order_date)] = grouped.get((nm_id, order_date), 0) + int(row.get("quantity") or 1)

    for (nm_id, order_date), quantity in grouped.items():
        db.add(Order(user_id=user_id, wb_token_id=wb_token_id, nm_id=nm_id, date=order_date, quantity=quantity))
    return len(grouped)


def _save_stocks(db: Session, user_id: int, wb_token_id: int, rows: list[dict]) -> int:
    totals: dict[int, int] = {}
    for row in rows:
        nm_id = _int(row.get("nmId") or row.get("nmID") or row.get("nm_id"))
        if not nm_id:
            continue
        _upsert_product(db, user_id, wb_token_id, nm_id, row.get("supplierArticle") or row.get("sa_name"), _product_name(row), row.get("brand"), row.get("subject"))
        totals[nm_id] = totals.get(nm_id, 0) + int(row.get("quantity") or 0)

    for nm_id, quantity in totals.items():
        db.add(Stock(user_id=user_id, wb_token_id=wb_token_id, nm_id=nm_id, quantity=quantity))
    return len(totals)


def _save_financial_report(db: Session, user_id: int, wb_token_id: int, rows: list[dict], date_from, date_to) -> int:
    grouped: dict[tuple[int, object], dict] = {}
    for row in rows:
        nm_id = _int(row.get("nmId") or row.get("nmID") or row.get("nm_id"))
        report_date = _date(row.get("saleDt") or row.get("sale_dt") or row.get("rrDate") or row.get("rrDt") or row.get("rr_dt") or row.get("orderDt"))
        if not nm_id or not report_date or report_date < date_from or report_date > date_to:
            continue

        _upsert_product(db, user_id, wb_token_id, nm_id, row.get("vendorCode") or row.get("sa_name"), _product_name(row), row.get("brand") or row.get("brandName"), row.get("subjectName"))
        key = (nm_id, report_date)
        item = grouped.setdefault(
            key,
            {
                "commission": 0.0,
                "logistics": 0.0,
                "storage": 0.0,
                "returns": 0.0,
                "returns_qty": 0,
                "acquiring": 0.0,
                "spa": 0.0,
                "penalties": 0.0,
                "deductions": 0.0,
                "other_expenses": 0.0,
            },
        )
        item["commission"] += _expense_amount(row, "ppvzSalesCommission", "ppvz_sales_commission", "ppvzReward", "ppvz_reward")
        item["logistics"] += _expense_amount(row, "deliveryRub", "delivery_rub", "deliveryService") + _expense_amount(row, "rebillLogisticCost", "rebill_logistic_cost")
        item["storage"] += _expense_amount(row, "storageFee", "storage_fee", "paidStorage")
        item["returns"] += _expense_amount(row, "returnAmount", "return_amount")
        item["returns_qty"] += _return_quantity(row)
        item["acquiring"] += _expense_amount(row, "acquiringFee", "acquiring_fee")
        item["spa"] += _expense_amount(row, "supplierPromo", "supplier_promo", "sellerPromo")
        item["penalties"] += _expense_amount(row, "penalty")
        item["deductions"] += _expense_amount(row, "deduction")
        item["other_expenses"] += _expense_amount(row, "additionalPayment", "additional_payment") + _expense_amount(row, "acceptance", "paidAcceptance")

    for (nm_id, report_date), item in grouped.items():
        db.add(Expense(user_id=user_id, wb_token_id=wb_token_id, nm_id=nm_id, date=report_date, data_accuracy=DATA_ACCURACY_EXACT, **item))
    return len(grouped)


def _save_advertising(db: Session, user_id: int, wb_token_id: int, rows: list[dict], date_from, date_to) -> int:
    grouped: dict[tuple[int, object], float] = {}
    for campaign in rows:
        for day in campaign.get("days", []):
            stat_date = _date(day.get("date"))
            if not stat_date or stat_date < date_from or stat_date > date_to:
                continue
            for app in day.get("apps", []):
                for nm in app.get("nm", []):
                    nm_id = _int(nm.get("nmId") or nm.get("nmID"))
                    if not nm_id:
                        continue
                    grouped[(nm_id, stat_date)] = grouped.get((nm_id, stat_date), 0.0) + (_float(nm.get("sum")) or 0)

    for (nm_id, stat_date), amount in grouped.items():
        expense = db.scalar(select(Expense).where(Expense.wb_token_id == wb_token_id, Expense.nm_id == nm_id, Expense.date == stat_date))
        if expense:
            expense.advertising = (expense.advertising or 0) + amount
        else:
            db.add(Expense(user_id=user_id, wb_token_id=wb_token_id, nm_id=nm_id, date=stat_date, advertising=amount, data_accuracy=DATA_ACCURACY_ESTIMATED))

    db.flush()
    existing_expenses = db.scalars(
        select(Expense).where(
            Expense.user_id == user_id,
            Expense.wb_token_id == wb_token_id,
            Expense.date >= date_from,
            Expense.date <= date_to,
            Expense.advertising.is_(None),
        )
    ).all()
    for expense in existing_expenses:
        expense.advertising = 0.0
    return len(grouped)


def _upsert_product(db: Session, user_id: int, wb_token_id: int, nm_id: int, vendor_code: str | None, name: str | None, brand: str | None = None, category: str | None = None) -> None:
    product = db.scalar(select(Product).where(Product.user_id == user_id, Product.nm_id == nm_id))
    if not product:
        db.add(Product(user_id=user_id, wb_token_id=wb_token_id, nm_id=nm_id, vendor_code=vendor_code or "", name=name or f"Товар {nm_id}", brand=brand, category=category))
        db.flush()
        return
    if product.wb_token_id != wb_token_id:
        product.wb_token_id = wb_token_id
    if vendor_code and not product.vendor_code:
        product.vendor_code = vendor_code
    if name and (not product.name or product.name.startswith("Товар ") or product.name.startswith("РўРѕРІР°СЂ ")):
        product.name = name
    if brand and not product.brand:
        product.brand = brand
    if category and not product.category:
        product.category = category


def _product_name(row: dict) -> str | None:
    return row.get("title") or row.get("imtName") or row.get("subject") or row.get("subject_name") or row.get("brand") or row.get("brandName") or row.get("brand_name")


def _date(value):
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        try:
            return datetime.strptime(text[:10], "%Y-%m-%d").date()
        except ValueError:
            return None


def _max_row_date(rows: list[dict], keys: tuple[str, ...]):
    dates = []
    for row in rows:
        for key in keys:
            row_date = _date(row.get(key))
            if row_date:
                dates.append(row_date)
                break
    return max(dates) if dates else None


def _signed_sale_quantity(row: dict) -> int:
    if str(row.get("saleID") or "").startswith("R") or row.get("isReturn") is True:
        return -1
    quantity = row.get("quantity")
    return int(quantity) if quantity is not None else 1


def _return_quantity(row: dict) -> int:
    explicit = _int(row.get("returnQuantity") or row.get("return_quantity") or row.get("quantityReturn"))
    if explicit is not None:
        return explicit
    doc_type = str(row.get("docTypeName") or row.get("supplierOperName") or row.get("operationType") or "").lower()
    if "возврат" in doc_type or "return" in doc_type:
        return abs(_int(row.get("quantity") or row.get("deliveryAmount")) or 1)
    return 0


def _is_return_row(row: dict) -> bool:
    doc_type = str(row.get("docTypeName") or row.get("supplierOperName") or row.get("operationType") or "").lower()
    return "возврат" in doc_type or "return" in doc_type


def _first_float(row: dict, *keys: str) -> float | None:
    for key in keys:
        value = _float(row.get(key))
        if value is not None:
            return value
    return None


def _expense_amount(row: dict, *keys: str) -> float:
    value = _first_float(row, *keys)
    return abs(value) if value is not None else 0.0


def _int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float(value) -> float | None:
    try:
        if isinstance(value, str):
            value = value.replace(",", ".")
        return float(value)
    except (TypeError, ValueError):
        return None
