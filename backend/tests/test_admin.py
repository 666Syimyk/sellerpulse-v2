from sqlalchemy import select

from models.entities import SubscriptionHistory, User


def _make_admin(client, email="admin@example.com", password="adminpass"):
    client.post("/auth/register", json={"email": email, "password": password, "name": "Admin"})
    with client.app.state.session_factory() as db:
        admin = db.scalar(select(User).where(User.email == email))
        admin.is_admin = True
        db.commit()
    resp = client.post("/auth/login", json={"email": email, "password": password})
    token = resp.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


def test_admin_subscription_history_and_users_payload(client):
    admin_headers = _make_admin(client)
    client.post("/auth/register", json={"email": "member@example.com", "password": "memberpass", "name": "Member"})

    patch = client.patch(
        "/admin/users/2/subscription",
        headers=admin_headers,
        json={"status": "active", "days": 30, "notes": "Оплата в Telegram"},
    )
    assert patch.status_code == 200
    assert patch.json()["subscription"]["status"] == "active"

    users = client.get("/admin/users", headers=admin_headers)
    assert users.status_code == 200
    member = next(item for item in users.json() if item["email"] == "member@example.com")
    assert member["subscription"]["status"] == "active"
    assert member["subscription_history"]
    latest = member["subscription_history"][0]
    assert latest["new_status"] == "active"
    assert latest["days_added"] == 30
    assert latest["notes"] == "Оплата в Telegram"
    assert latest["admin_name"] == "Admin"

    with client.app.state.session_factory() as db:
        history_rows = db.scalars(select(SubscriptionHistory).where(SubscriptionHistory.user_id == member["id"])).all()
        assert len(history_rows) == 1


def test_admin_bulk_subscription_update(client):
    admin_headers = _make_admin(client)
    client.post("/auth/register", json={"email": "bulk1@example.com", "password": "memberpass", "name": "Bulk 1"})
    client.post("/auth/register", json={"email": "bulk2@example.com", "password": "memberpass", "name": "Bulk 2"})

    resp = client.patch(
        "/admin/subscriptions/bulk",
        headers=admin_headers,
        json={"user_ids": [2, 3], "status": "active", "days": 90, "notes": "Массовое продление"},
    )
    assert resp.status_code == 200
    assert resp.json()["count"] == 2

    users = client.get("/admin/users", headers=admin_headers).json()
    bulk1 = next(item for item in users if item["email"] == "bulk1@example.com")
    bulk2 = next(item for item in users if item["email"] == "bulk2@example.com")
    assert bulk1["subscription"]["status"] == "active"
    assert bulk2["subscription"]["status"] == "active"
    assert bulk1["subscription_history"][0]["days_added"] == 90
    assert bulk2["subscription_history"][0]["notes"] == "Массовое продление"
