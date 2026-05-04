def test_register(client):
    resp = client.post("/auth/register", json={"email": "new@example.com", "password": "pass123", "name": "New"})
    assert resp.status_code == 200
    data = resp.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"
    assert data["has_wb_token"] is False


def test_register_duplicate(client):
    client.post("/auth/register", json={"email": "dup@example.com", "password": "pass123"})
    resp = client.post("/auth/register", json={"email": "dup@example.com", "password": "pass123"})
    assert resp.status_code == 400


def test_login(client):
    client.post("/auth/register", json={"email": "login@example.com", "password": "pass123"})
    resp = client.post("/auth/login", json={"email": "login@example.com", "password": "pass123"})
    assert resp.status_code == 200
    assert "access_token" in resp.json()


def test_login_wrong_password(client):
    client.post("/auth/register", json={"email": "wrongpass@example.com", "password": "correct"})
    resp = client.post("/auth/login", json={"email": "wrongpass@example.com", "password": "wrong"})
    assert resp.status_code == 400


def test_me(auth_client):
    resp = auth_client.get("/auth/me")
    assert resp.status_code == 200
    data = resp.json()
    assert data["email"] == "test@example.com"
    assert "has_wb_token" in data


def test_me_no_token(client):
    resp = client.get("/auth/me")
    assert resp.status_code == 401
