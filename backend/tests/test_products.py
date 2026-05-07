def test_list_products_empty(auth_client):
    resp = auth_client.get("/products")
    assert resp.status_code == 200
    assert resp.json() == []


def test_update_cost_price(auth_client):
    resp = auth_client.put("/products/12345/cost-price", json={"cost_price": 150.0})
    assert resp.status_code == 200
    data = resp.json()
    assert data["nm_id"] == 12345
    assert data["cost_price"] == 150.0


def test_update_tax_rate(auth_client):
    resp = auth_client.put("/products/12345/cost-price", json={"cost_price": 150.0, "tax_rate": 6.0})
    assert resp.status_code == 200
    assert resp.json()["tax_rate"] == 6.0

    products = auth_client.get("/products").json()
    found = next((p for p in products if p["nm_id"] == 12345), None)
    assert found["tax_rate"] == 6.0


def test_clear_tax_rate(auth_client):
    auth_client.put("/products/12345/cost-price", json={"cost_price": 150.0, "tax_rate": 6.0})
    resp = auth_client.put("/products/12345/cost-price", json={"cost_price": 150.0, "tax_rate": None})
    assert resp.status_code == 200
    assert resp.json()["tax_rate"] is None


def test_update_tax_rate_out_of_range(auth_client):
    resp = auth_client.put("/products/12345/cost-price", json={"cost_price": 150.0, "tax_rate": 101.0})
    assert resp.status_code == 400


def test_update_cost_price_negative(auth_client):
    resp = auth_client.put("/products/12345/cost-price", json={"cost_price": -1.0})
    assert resp.status_code == 400


def test_update_cost_price_null(auth_client):
    auth_client.put("/products/77777/cost-price", json={"cost_price": 100.0})
    resp = auth_client.put("/products/77777/cost-price", json={"cost_price": None})
    assert resp.status_code == 200
    assert resp.json()["cost_price"] is None


def test_list_products_after_update(auth_client):
    auth_client.put("/products/99999/cost-price", json={"cost_price": 200.0, "vendor_code": "ART-99"})
    resp = auth_client.get("/products")
    assert resp.status_code == 200
    products = resp.json()
    found = next((p for p in products if p["nm_id"] == 99999), None)
    assert found is not None
    assert found["cost_price"] == 200.0
    assert found["vendor_code"] == "ART-99"


def test_products_isolated_between_users(client):
    client.post("/auth/register", json={"email": "user_a@example.com", "password": "pass123"})
    resp_a = client.post("/auth/login", json={"email": "user_a@example.com", "password": "pass123"})
    token_a = resp_a.json()["access_token"]

    client.post("/auth/register", json={"email": "user_b@example.com", "password": "pass123"})
    resp_b = client.post("/auth/login", json={"email": "user_b@example.com", "password": "pass123"})
    token_b = resp_b.json()["access_token"]

    client.put("/products/55555/cost-price", json={"cost_price": 111.0},
               headers={"Authorization": f"Bearer {token_a}"})

    resp = client.get("/products", headers={"Authorization": f"Bearer {token_b}"})
    products_b = resp.json()
    assert not any(p["nm_id"] == 55555 for p in products_b)


def test_product_upsert_reuses_existing_user_product(db):
    from models.entities import Product
    from services.sync import _upsert_product

    db.add(Product(user_id=1, wb_token_id=1, nm_id=123456, vendor_code="OLD", name="Old name"))
    db.commit()

    _upsert_product(db, user_id=1, wb_token_id=9, nm_id=123456, vendor_code="NEW", name="New name")
    db.commit()

    products = db.query(Product).filter(Product.user_id == 1, Product.nm_id == 123456).all()
    assert len(products) == 1
    assert products[0].wb_token_id == 9
    assert products[0].vendor_code == "OLD"


def test_list_products_shows_only_active_token_products(auth_client, db):
    from models.entities import Product, User, WbToken

    user = db.query(User).filter_by(email="test@example.com").first()
    active_token = WbToken(user_id=user.id, encrypted_token="x", token_status="active", is_active=True, shop_name="Active")
    old_token = WbToken(user_id=user.id, encrypted_token="y", token_status="active", is_active=False, shop_name="Old")
    db.add_all([active_token, old_token])
    db.flush()

    db.add_all([
        Product(user_id=user.id, wb_token_id=active_token.id, nm_id=101, vendor_code="ACTIVE", name="Active product"),
        Product(user_id=user.id, wb_token_id=old_token.id, nm_id=202, vendor_code="OLD", name="Old product"),
        Product(user_id=user.id, wb_token_id=None, nm_id=303, vendor_code="MANUAL", name="Manual product"),
    ])
    db.commit()

    resp = auth_client.get("/products")
    assert resp.status_code == 200
    nm_ids = {item["nm_id"] for item in resp.json()}
    assert nm_ids == {101, 303}


def test_list_products_without_active_token_hides_old_token_products(auth_client, db):
    from models.entities import Product, User, WbToken

    user = db.query(User).filter_by(email="test@example.com").first()
    old_token = WbToken(user_id=user.id, encrypted_token="y", token_status="active", is_active=False, shop_name="Old")
    db.add(old_token)
    db.flush()

    db.add_all([
        Product(user_id=user.id, wb_token_id=old_token.id, nm_id=202, vendor_code="OLD", name="Old product"),
        Product(user_id=user.id, wb_token_id=None, nm_id=303, vendor_code="MANUAL", name="Manual product"),
    ])
    db.commit()

    resp = auth_client.get("/products")
    assert resp.status_code == 200
    nm_ids = {item["nm_id"] for item in resp.json()}
    assert nm_ids == {303}
