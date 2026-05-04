from services.dashboard import _sum_nullable, _money, _percent, _product_status


def test_sum_nullable_all_none():
    assert _sum_nullable([None, None]) is None


def test_sum_nullable_mixed():
    assert _sum_nullable([1.0, None, 2.0]) == 3.0


def test_sum_nullable_all_values():
    assert _sum_nullable([1, 2, 3]) == 6


def test_money_rounds():
    assert _money(1.005) == 1.0
    assert _money(1.555) == 1.56
    assert _money(None) is None


def test_percent_normal():
    assert _percent(25.0, 100.0) == 25.0


def test_percent_zero_total():
    assert _percent(10.0, 0.0) is None


def test_percent_none():
    assert _percent(None, 100.0) is None
    assert _percent(10.0, None) is None


def test_product_status_no_cost():
    status, action = _product_status(None, None, None, None, None, False)
    assert status == "Нет себестоимости"


def test_product_status_missing_wb():
    class FakeProduct:
        cost_price = 100.0
    status, action = _product_status(FakeProduct(), None, None, None, None, True)
    assert status == "Нет данных WB"


def test_product_status_low_stock():
    class FakeProduct:
        cost_price = 100.0
    status, action = _product_status(FakeProduct(), 500.0, 1000.0, 5, 3.0, False)
    assert status == "Заканчивается остаток"


def test_product_status_loss():
    class FakeProduct:
        cost_price = 100.0
    status, action = _product_status(FakeProduct(), -100.0, 800.0, 50, 30.0, False)
    assert status == "В минусе"


def test_product_status_low_margin():
    class FakeProduct:
        cost_price = 100.0
    status, action = _product_status(FakeProduct(), 50.0, 1000.0, 50, 30.0, False)
    assert status == "Низкая маржа"


def test_product_status_good():
    class FakeProduct:
        cost_price = 100.0
    status, action = _product_status(FakeProduct(), 300.0, 1000.0, 50, 30.0, False)
    assert status == "В плюсе"
