import io
import pytest
from services.financial_report import (
    parse_financial_report,
    build_items,
    EmptyFinancialReport,
    MissingColumns,
)


def _make_csv(rows: list[str]) -> bytes:
    return "\n".join(rows).encode("utf-8-sig")


HEADER = (
    "Код номенклатуры;Артикул поставщика;Название;Тип документа;"
    "Обоснование для оплаты;Кол-во;Цена розничная;"
    "Вайлдберриз реализовал Товар (Пр);"
    "К перечислению Продавцу за реализованный Товар;"
    "Вознаграждение Вайлдберриз (ВВ), без НДС;"
    "Услуги по доставке товара покупателю;Хранение;"
    "Общая сумма штрафов;Удержания;Операции на приемке;"
    "Компенсация платёжных услуг/Комиссия за интеграцию платёжных сервисов;"
    "Количество возврата;Дата продажи"
)

DATA_ROW = "123456;ART-001;Товар тест;Продажа;Оплата заказа покупателю;1;1500;1200;900;120;80;30;0;0;0;20;0;2024-01-15"
RETURN_ROW = "123456;ART-001;Товар тест;Возврат;Возврат покупателю;1;1500;-1200;-900;-120;0;0;0;0;0;0;1;2024-01-16"


def test_parse_basic_csv():
    content = _make_csv([HEADER, DATA_ROW])
    report = parse_financial_report("report.csv", content)
    assert report.rows_count == 1
    assert len(report.items) == 1
    item = report.items[0]
    assert item["nm_id"] == 123456
    assert item["vendor_code"] == "ART-001"
    assert item["sold_qty"] == 1


def test_parse_sales_amount():
    content = _make_csv([HEADER, DATA_ROW])
    report = parse_financial_report("report.csv", content)
    item = report.items[0]
    assert item["sales_amount"] == 1200.0
    assert item["to_pay"] == 900.0
    assert item["commission"] == 120.0
    assert item["logistics"] == 80.0
    assert item["storage"] == 30.0


def test_parse_period():
    content = _make_csv([HEADER, DATA_ROW])
    report = parse_financial_report("report.csv", content)
    from datetime import date
    assert report.period_start == date(2024, 1, 15)
    assert report.period_end == date(2024, 1, 15)


def test_parse_two_rows_same_product():
    row2 = "123456;ART-001;Товар тест;Продажа;Оплата заказа покупателю;2;1500;2400;1800;240;160;60;0;0;0;40;0;2024-01-16"
    content = _make_csv([HEADER, DATA_ROW, row2])
    report = parse_financial_report("report.csv", content)
    assert len(report.items) == 1
    assert report.items[0]["sold_qty"] == 3
    assert report.items[0]["to_pay"] == 2700.0


def test_parse_empty_file():
    with pytest.raises(EmptyFinancialReport):
        parse_financial_report("report.csv", b"")


def test_parse_missing_columns():
    bad = _make_csv(["Код номенклатуры;Название", "123;Test"])
    with pytest.raises(MissingColumns):
        parse_financial_report("report.csv", bad)


def test_build_items_profit_with_cost(db):
    from database import SessionLocal
    parsed_items = [{
        "nm_id": 999,
        "vendor_code": "TST",
        "product_name": "Test",
        "sold_qty": 2,
        "sales_amount": 2000.0,
        "before_spp": 3000.0,
        "after_spp": 2000.0,
        "to_pay": 1600.0,
        "commission": 200.0,
        "logistics": 100.0,
        "storage": 50.0,
        "returns": 0.0,
        "acquiring": 20.0,
        "spa": 0.0,
        "advertising": None,
        "penalties": 0.0,
        "deductions": 0.0,
        "other_expenses": None,
        "orders_qty": 2,
    }]
    items = build_items(db, user_id=9999, parsed_items=parsed_items)
    assert len(items) == 1
    item = items[0]
    assert item["tax"] is None
    assert item["profit"] is None  # нет себестоимости


def test_build_items_manual_tax(db):
    parsed_items = [{
        "nm_id": 998,
        "vendor_code": "TAX",
        "product_name": "Tax Test",
        "sold_qty": 1,
        "sales_amount": 1000.0,
        "before_spp": 1500.0,
        "after_spp": 1000.0,
        "to_pay": 800.0,
        "commission": 100.0,
        "logistics": 50.0,
        "storage": 20.0,
        "returns": 0.0,
        "acquiring": 0.0,
        "spa": 0.0,
        "advertising": None,
        "penalties": 0.0,
        "deductions": 0.0,
        "other_expenses": None,
        "orders_qty": 1,
    }]
    items = build_items(db, user_id=9999, parsed_items=parsed_items, manual_tax=6.0)
    item = items[0]
    assert item["tax"] == round(800.0 * 0.06, 2)
