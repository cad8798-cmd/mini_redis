from fastapi.testclient import TestClient

from app.main import app


def test_set_success_template() -> None:
    client = TestClient(app)
    response = client.post("/v1/kv/set", json={"key": "user:1", "value": "kim"})

    assert response.status_code == 200
    assert response.json() == {"success": True, "data": {"stored": True}}


def test_get_failure_template() -> None:
    client = TestClient(app)
    response = client.get("/v1/kv/get", params={"key": "missing:key"})

    assert response.status_code == 404
    assert response.json() == {
        "success": False,
        "error": {"code": "KEY_NOT_FOUND", "message": "key not found"},
    }


def test_exists_success_template() -> None:
    client = TestClient(app)
    client.post("/v1/kv/set", json={"key": "user:2", "value": "lee"})
    response = client.get("/v1/kv/exists", params={"key": "user:2"})

    assert response.status_code == 200
    assert response.json() == {"success": True, "data": {"exists": True}}


def test_set_rejects_key_without_namespace() -> None:
    client = TestClient(app)
    response = client.post("/v1/kv/set", json={"key": "user", "value": "kim"})

    assert response.status_code == 400
    assert response.json() == {
        "success": False,
        "error": {
            "code": "INVALID_INPUT",
            "message": "key must use namespace format (<prefix>:<name>)",
        },
    }


def test_get_rejects_invalid_prefix_query() -> None:
    client = TestClient(app)
    response = client.get("/v1/kv/get", params={"key": "user::1"})

    assert response.status_code == 400
    assert response.json() == {
        "success": False,
        "error": {
            "code": "INVALID_INPUT",
            "message": "prefix cannot contain empty namespace segments",
        },
    }
