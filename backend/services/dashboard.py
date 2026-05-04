from collections import defaultdict
from decimal import Decimal, ROUND_HALF_EVEN
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from models.entities import DashboardCache, Expense, FinancialReport, FinancialReportItem, Order, Product, Sale, Stock, WbToken
from services.financial_report import build_items_from_source_rows, validation_is_ok
from services.periods import period_dates

EXPENSE_FIELDS = [
    "commission",
    "logistics",
    "storage",
    "returns",
    "acquiring",
    "spa",
    "advertising",
    "tax",
    "penalties",
    "deductions",
    "other_expenses",
]

WB_REQUIRED_FOR_EXACT_PROFIT = [
    "commission",
    "logistics",
    "storage",
    "returns",
    "acquiring",
    "spa",
    "advertising",
    "penalties",
    "deductions",
    "other_expenses",
]

EXACT_ACCURACY_VALUES = {"Точные данные WB"}
ESTIMATED_ACCURACY_VALUES = {"Оценочный расчёт"}


def calculate_dashboard(db: Session, user_id: int, period: str) -> dict:
    date_from, date_to = period_dates(period)
    report_dashboard = _dashboard_from_latest_financial_report(db, user_id, period, date_from, date_to)
    if report_dashboard is not None:
        return report_dashboard

    # If user has parsed financial report but none match the period, show explicit message
    has_any_report = db.scalar(
        select(FinancialReport)
        .where(FinancialReport.user_id == user_id, FinancialReport.source_rows_json.is_not(None))
        .order_by(FinancialReport.id.desc())
    )
    if has_any_report:
        return _no_report_for_period_dashboard(period, has_any_report)

    wb_token = _active_token(db, user_id)
    if not wb_token:
        return _empty_dashboard(period)

    products = {
        p.nm_id: p
        for p in db.scalars(select(Product).where(Product.user_id == user_id)).all()
    }
    sales = db.scalars(
        select(Sale).where(Sale.user_id == user_id, Sale.wb_token_id == wb_token.id, Sale.date >= date_from, Sale.date <= date_to)
    ).all()
    expenses = db.scalars(
        select(Expense).where(Expense.user_id == user_id, Expense.wb_token_id == wb_token.id, Expense.date >= date_from, Expense.date <= date_to)
    ).all()
    orders = db.scalars(
        select(Order).where(Order.user_id == user_id, Order.wb_token_id == wb_token.id, Order.date >= date_from, Order.date <= date_to)
    ).all()
    stocks = {
        s.nm_id: s
        for s in db.scalars(select(Stock).where(Stock.user_id == user_id, Stock.wb_token_id == wb_token.id)).all()
    }

    sales_by_nm = defaultdict(list)
    expenses_by_nm = defaultdict(list)
    for sale in sales:
        sales_by_nm[sale.nm_id].append(sale)
    for expense in expenses:
        expenses_by_nm[expense.nm_id].append(expense)

    all_nm_ids = sorted(set(products) | set(sales_by_nm) | set(expenses_by_nm) | set(stocks))
    if not all_nm_ids:
        return _empty_dashboard(period, wb_token)

    rows = []
    totals = defaultdict(list)
    sale_qty_total = 0
    sales_data_missing = not sales and wb_token.token_status in {"rate_limited", "api_error", "limited"}
    returns_qty_total = _sum_nullable([expense.returns_qty for expense in expenses])
    orders_qty_total = sum(order.quantity for order in orders)

    for nm_id in all_nm_ids:
        product = products.get(nm_id)
        item_sales = sales_by_nm.get(nm_id, [])
        item_expenses = expenses_by_nm.get(nm_id, [])
        sold_qty = sum(s.quantity for s in item_sales)
        shown_sold_qty = None if sales_data_missing else sold_qty
        before_spp = _sum_nullable([s.total_before_spp for s in item_sales])
        spp = _sum_nullable([s.spp_amount for s in item_sales])
        after_spp = _sum_nullable([s.total_after_spp for s in item_sales])
        expense_values = {field: _sum_nullable([getattr(e, field) for e in item_expenses]) for field in EXPENSE_FIELDS}

        cost_price_total = None if sales_data_missing else product.cost_price * sold_qty if product and product.cost_price is not None else None
        has_finance_rows = any(e.data_accuracy in EXACT_ACCURACY_VALUES for e in item_expenses)
        missing_expense_fields = [field for field in WB_REQUIRED_FOR_EXACT_PROFIT if expense_values[field] is None]
        has_missing_wb_expense = bool(missing_expense_fields) or not has_finance_rows

        profit = None
        if after_spp is not None and cost_price_total is not None and not has_missing_wb_expense:
            profit = after_spp - cost_price_total - sum((expense_values[field] or 0) for field in EXPENSE_FIELDS)

        advertising = expense_values["advertising"]
        stock_qty = stocks[nm_id].quantity if nm_id in stocks else None
        days_left = None
        if stock_qty is not None and sold_qty > 0:
            days_left = round(stock_qty / max(sold_qty / max((date_to - date_from).days + 1, 1), 0.01), 1)

        accuracy = _accuracy(item_expenses, has_missing_wb_expense)
        status, action = _product_status(product, profit, after_spp, stock_qty, days_left, has_missing_wb_expense)
        sale_qty_total += sold_qty

        row = {
            "vendor_code": product.vendor_code if product else "",
            "name": product.name if product else f"Товар {nm_id}",
            "brand": product.brand if product else None,
            "category": product.category if product else None,
            "nm_id": nm_id,
            "unit_cost_price": _money(product.cost_price) if product else None,
            "sold_qty": shown_sold_qty,
            "sales_sum": _money(before_spp),
            "before_spp": _money(before_spp),
            "spp": _money(spp),
            "after_spp": _money(after_spp),
            "cost_price": _money(cost_price_total),
            **{field: _money(value) for field, value in expense_values.items()},
            "profit": _money(profit),
            "profit_per_unit": _money(profit / sold_qty) if profit is not None and sold_qty else None,
            "margin": _percent(profit, after_spp),
            "drr": _percent(advertising, after_spp),
            "stock": stock_qty,
            "days_left": days_left,
            "status": status,
            "action": action,
            "data_accuracy": accuracy,
            "missing_expense_fields": missing_expense_fields,
        }
        rows.append(row)

        totals["sold_qty"].append(sold_qty)
        totals["before_spp"].append(before_spp)
        totals["spp"].append(spp)
        totals["after_spp"].append(after_spp)
        totals["profit"].append(profit)
        totals["advertising"].append(advertising)
        totals["cost_price"].append(cost_price_total)
        for field, value in expense_values.items():
            totals[field].append(value)

    after_spp_total = _sum_nullable(totals["after_spp"])
    profit_total = _sum_nullable(totals["profit"])
    advertising_total = _sum_nullable(totals["advertising"])
    expenses_block = {field: _money(_sum_nullable(totals[field])) for field in EXPENSE_FIELDS}
    expenses_block["cost_price"] = _money(_sum_nullable(totals["cost_price"]))

    result = {
        "period": period,
        "shop": {
            "name": wb_token.shop_name,
            "token_status": wb_token.token_status,
            "last_sync_at": wb_token.last_sync_at.isoformat() if wb_token.last_sync_at else None,
        },
        "data_source": {
            "type": "wb_api",
            "label": "Источник: WB API — предварительные данные",
            "is_exact": False,
            "message": "Предварительные данные WB API. Для точной сверки загрузите финансовый отчёт WB.",
        },
        "today_hint": period == "today",
        "metrics": {
            "sold_qty": None if sales_data_missing else sum(totals["sold_qty"]),
            "sales_sum": _money(_sum_nullable(totals["before_spp"])),
            "after_spp": _money(after_spp_total),
            "net_profit": _money(profit_total),
            "margin": _percent(profit_total, after_spp_total),
            "drr": _percent(advertising_total, after_spp_total),
            "returns_qty": returns_qty_total,
            "buyout_percent": _percent(sale_qty_total, orders_qty_total) if orders_qty_total else None,
        },
        "expenses": expenses_block,
        "products": rows,
        "calculated_at": datetime.now(timezone.utc).isoformat(),
    }

    cache = db.scalar(select(DashboardCache).where(DashboardCache.user_id == user_id, DashboardCache.wb_token_id == wb_token.id, DashboardCache.period == period))
    if cache:
        cache.data_json = result
    else:
        db.add(DashboardCache(user_id=user_id, wb_token_id=wb_token.id, period=period, data_json=result))
    db.commit()
    return result


def _dashboard_from_latest_financial_report(db: Session, user_id: int, period: str, date_from, date_to) -> dict | None:
    reports = db.scalars(
        select(FinancialReport)
        .where(
            FinancialReport.user_id == user_id,
            FinancialReport.validation_json.is_not(None),
            FinancialReport.source_rows_json.is_not(None),
        )
        .order_by(FinancialReport.id.desc())
    ).all()
    if period == "report":
        report = next((r for r in reports if validation_is_ok(r.validation_json)), None)
    else:
        report = next(
            (
                item
                for item in reports
                if validation_is_ok(item.validation_json)
                and item.period_start
                and item.period_end
                and item.period_start <= date_to
                and item.period_end >= date_from
            ),
            None,
        )
    if not report:
        return None

    items = build_items_from_source_rows(
        db, user_id, report.source_rows_json or [], date_from, date_to,
        report.manual_tax,
        manual_advertising=report.manual_advertising,
        manual_other_expenses=report.manual_other_expenses,
    )
    if not items:
        return None

    rows = []
    totals = defaultdict(list)
    for item in items:
        row = {
            "vendor_code": item["vendor_code"],
            "name": item["product_name"],
            "brand": None,
            "category": None,
            "nm_id": item["nm_id"],
            "unit_cost_price": _money(item["cost_price"]),
            "sold_qty": item["sold_qty"],
            "sales_sum": _money(item["sales_amount"]),
            "before_spp": _money(item["before_spp"]),
            "spp": _money(item["spp_amount"]),
            "after_spp": _money(item["after_spp"]),
            "to_pay": _money(item["to_pay"]),
            "cost_price": _money(item["total_cost_price"]),
            **{field: _money(item.get(field)) for field in EXPENSE_FIELDS},
            "profit": _money(item["profit"]),
            "profit_per_unit": _money(item["profit_per_unit"]),
            "margin": item["margin"],
            "drr": item["drr"],
            "stock": None,
            "days_left": None,
            "orders_qty": item.get("orders_qty"),
            "buyout_rate": item.get("buyout_rate"),
            "status": item["status"],
            "action": item["action"],
            "data_accuracy": "Точные данные WB",
            "missing_expense_fields": [],
        }
        rows.append(row)

        totals["sold_qty"].append(item["sold_qty"])
        totals["sales_amount"].append(item["sales_amount"])
        totals["before_spp"].append(item["before_spp"])
        totals["after_spp"].append(item["after_spp"])
        totals["to_pay"].append(item["to_pay"])
        totals["profit"].append(item["profit"])
        totals["advertising"].append(item["advertising"])
        totals["cost_price"].append(item["total_cost_price"])
        for field in EXPENSE_FIELDS:
            totals[field].append(item.get(field))

    after_spp_total = _sum_nullable(totals["after_spp"])
    to_pay_total = _sum_nullable(totals["to_pay"])
    profit_total = _sum_nullable(totals["profit"]) if all(item["profit"] is not None for item in items) else None
    advertising_total = _sum_nullable(totals["advertising"])
    expenses_block = {field: _money(_sum_nullable(totals[field])) for field in EXPENSE_FIELDS}
    expenses_block["cost_price"] = _money(_sum_nullable(totals["cost_price"]))
    filtered_start = max(report.period_start, date_from) if report.period_start else date_from
    filtered_end = min(report.period_end, date_to) if report.period_end else date_to

    return {
        "period": period,
        "shop": {
            "name": "Финансовый отчёт WB",
            "token_status": "active",
            "last_sync_at": report.created_at.isoformat() if report.created_at else None,
        },
        "data_source": {
            "type": "financial_report",
            "label": "Источник: финансовый отчёт WB — точные данные",
            "is_exact": True,
            "file_name": report.file_name,
            "period_start": filtered_start.isoformat() if filtered_start else None,
            "period_end": filtered_end.isoformat() if filtered_end else None,
            "report_period_start": report.period_start.isoformat() if report.period_start else None,
            "report_period_end": report.period_end.isoformat() if report.period_end else None,
            "validation": report.validation_json,
            "message": "Dashboard построен из строк финансового отчёта WB, отфильтрованных по выбранному периоду.",
        },
        "today_hint": False,
        "metrics": {
            "sold_qty": sum(item["sold_qty"] for item in items),
            "sales_sum": _money(_sum_nullable(totals["sales_amount"])),
            "orders_qty": None,
            "orders_sum": None,
            "after_spp": _money(after_spp_total),
            "to_pay": _money(to_pay_total),
            "net_profit": _money(profit_total),
            "margin": _percent(profit_total, to_pay_total),
            "drr": _percent(advertising_total, after_spp_total),
            "returns_qty": None,
            "buyout_percent": None,
        },
        "expenses": expenses_block,
        "products": rows,
        "calculated_at": datetime.now(timezone.utc).isoformat(),
    }


def _active_token(db: Session, user_id: int) -> WbToken | None:
    return db.scalar(
        select(WbToken)
        .where(WbToken.user_id == user_id, WbToken.is_active.is_(True), WbToken.token_status != "invalid")
        .order_by(WbToken.id.desc())
    )


def _empty_dashboard(period: str, wb_token: WbToken | None = None) -> dict:
    return {
        "period": period,
        "shop": {
            "name": wb_token.shop_name if wb_token else None,
            "token_status": wb_token.token_status if wb_token else "invalid",
            "last_sync_at": wb_token.last_sync_at.isoformat() if wb_token and wb_token.last_sync_at else None,
        },
        "data_source": {
            "type": "none",
            "label": "Нет точных данных — загрузите финансовый отчёт WB",
            "is_exact": False,
            "message": (
                "Токен подключён, но WB API пока не вернул данные. Загрузите финансовый отчёт WB, чтобы получить точные расчёты."
                if wb_token
                else "Нет данных WB. Загрузите финансовый отчёт WB, чтобы получить точные расчёты."
            ),
        },
        "today_hint": period == "today",
        "metrics": {
            "sold_qty": None,
            "sales_sum": None,
            "after_spp": None,
            "to_pay": None,
            "net_profit": None,
            "margin": None,
            "drr": None,
            "returns_qty": None,
            "buyout_percent": None,
        },
        "expenses": {field: None for field in [*EXPENSE_FIELDS, "cost_price"]},
        "products": [],
        "calculated_at": datetime.now(timezone.utc).isoformat(),
    }


def _no_report_for_period_dashboard(period: str, latest_report: FinancialReport) -> dict:
    report_period = ""
    if latest_report.period_start and latest_report.period_end:
        report_period = f" ({latest_report.period_start.strftime('%d.%m.%Y')} — {latest_report.period_end.strftime('%d.%m.%Y')})"
    return {
        "period": period,
        "shop": {"name": None, "token_status": "active", "last_sync_at": None},
        "data_source": {
            "type": "no_report_for_period",
            "label": "Нет финансового отчёта за выбранный период",
            "is_exact": False,
            "file_name": latest_report.file_name,
            "message": f"Загруженный отчёт{report_period} не покрывает выбранный период. Загрузите финансовый отчёт WB за нужный период.",
        },
        "today_hint": False,
        "metrics": {
            "sold_qty": None, "sales_sum": None, "after_spp": None, "to_pay": None,
            "net_profit": None, "margin": None, "drr": None, "returns_qty": None, "buyout_percent": None,
        },
        "expenses": {field: None for field in [*EXPENSE_FIELDS, "cost_price"]},
        "products": [],
        "calculated_at": datetime.now(timezone.utc).isoformat(),
    }


def _sum_nullable(values: list[float | int | None]) -> float | int | None:
    present = [value for value in values if value is not None]
    if not present:
        return None
    return sum(present)


def _money(value: float | None) -> float | None:
    if value is None:
        return None
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN))


def _percent(part: float | None, total: float | None) -> float | None:
    if part is None or not total:
        return None
    return round(part / total * 100, 1)


def _accuracy(item_expenses: list[Expense], has_missing_wb_expense: bool) -> str:
    if not item_expenses or has_missing_wb_expense:
        return "Нет данных WB"
    unique_accuracy = {e.data_accuracy for e in item_expenses}
    if unique_accuracy and unique_accuracy.issubset(EXACT_ACCURACY_VALUES):
        return "Точные данные WB"
    if unique_accuracy and unique_accuracy.issubset(EXACT_ACCURACY_VALUES | ESTIMATED_ACCURACY_VALUES):
        return "Оценочный расчёт"
    return "Оценочный расчёт"


def _product_status(product, profit, after_spp, stock_qty, days_left, has_missing_wb_expense):
    if product is None or product.cost_price is None:
        return "Нет себестоимости", "Указать себестоимость"
    if has_missing_wb_expense:
        return "Нет данных WB", "Проверить карточку товара"
    if days_left is not None and days_left <= 7:
        return "Заканчивается остаток", "Пополнить остаток"
    if profit is not None and profit < 0:
        return "В минусе", "Поднять цену"
    margin = _percent(profit, after_spp)
    if margin is not None and margin < 15:
        return "Низкая маржа", "Снизить расходы"
    return "В плюсе", "Проверить рекламу"
