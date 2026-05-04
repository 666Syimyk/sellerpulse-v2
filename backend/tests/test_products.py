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
    client.post("/auth/register", json={"email": "user_a@example.com", "password": "pass"})
    resp_a = client.post("/auth/login", json={"email": "user_a@example.com", "password": "pass"})
    token_a = resp_a.json()["access_token"]

    client.post("/auth/register", json={"email": "user_b@example.com", "password": "pass"})
    resp_b = client.post("/auth/login", json={"email": "user_b@example.com", "password": "pass"})
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
