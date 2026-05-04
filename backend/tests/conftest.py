import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Base, get_db
from main import app
from middleware.rate_limit import RateLimitMiddleware

TEST_DB_URL = "sqlite:///./test.db"

engine_test = create_engine(TEST_DB_URL, connect_args={"check_same_thread": False})
SessionTest = sessionmaker(autocommit=False, autoflush=False, bind=engine_test)


def _clear_rate_limit_buckets():
    node = app.middleware_stack
    while node is not None:
        if isinstance(node, RateLimitMiddleware):
            with node._lock:
                node._buckets.clear()
            return
        node = getattr(node, "app", None)


@pytest.fixture(scope="session", autouse=True)
def setup_db():
    Base.metadata.drop_all(bind=engine_test)
    Base.metadata.create_all(bind=engine_test)
    yield
    Base.metadata.drop_all(bind=engine_test)


@pytest.fixture
def db():
    Base.metadata.drop_all(bind=engine_test)
    Base.metadata.create_all(bind=engine_test)
    session = SessionTest()
    try:
        yield session
    finally:
        session.rollback()
        session.close()


@pytest.fixture
def client(db):
    def override_get_db():
        try:
            yield db
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db
    app.state.session_factory = SessionTest
    with TestClient(app, raise_server_exceptions=False) as c:
        _clear_rate_limit_buckets()
        yield c
        _clear_rate_limit_buckets()
    del app.state.session_factory
    app.dependency_overrides.clear()


@pytest.fixture
def auth_client(client):
    client.post("/auth/register", json={"email": "test@example.com", "password": "testpass123", "name": "Test"})
    resp = client.post("/auth/login", json={"email": "test@example.com", "password": "testpass123"})
    token = resp.json()["access_token"]
    client.headers["Authorization"] = f"Bearer {token}"
    return client
