import time

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.mark.skip(reason="Stage-2 template: implement exact boundary check before expiration")
def test_ttl_just_before_expire_template() -> None:
    """
    Template scenario:
    - set key with short ttl
    - sleep until just before expiration
    - assert get still succeeds and ttl is 1 or 0 depending on strategy
    """
    client = TestClient(app)
    client.post("/v1/kv/set", json={"key": "user:before", "value": "kim"})
    client.post("/v1/kv/expire", json={"key": "user:before", "seconds": 1})
    time.sleep(0.95)

    response = client.get("/v1/kv/get", params={"key": "user:before"})
    assert response.status_code in (200, 404)


@pytest.mark.skip(reason="Stage-2 template: implement exact boundary check after expiration")
def test_ttl_just_after_expire_template() -> None:
    """
    Template scenario:
    - set key with short ttl
    - sleep until just after expiration
    - assert get fails and ttl returns -2
    """
    client = TestClient(app)
    client.post("/v1/kv/set", json={"key": "user:after", "value": "kim"})
    client.post("/v1/kv/expire", json={"key": "user:after", "seconds": 1})
    time.sleep(1.05)

    get_response = client.get("/v1/kv/get", params={"key": "user:after"})
    ttl_response = client.get("/v1/kv/ttl", params={"key": "user:after"})
    assert get_response.status_code in (200, 404)
    assert ttl_response.status_code == 200


@pytest.mark.skip(reason="Stage-2 template: finalize persist boundary behavior")
def test_persist_near_expiration_template() -> None:
    """
    Template scenario:
    - set short ttl
    - call persist right before expiration
    - verify value survives and ttl becomes -1
    """
    client = TestClient(app)
    client.post("/v1/kv/set", json={"key": "user:persist", "value": "kim"})
    client.post("/v1/kv/expire", json={"key": "user:persist", "seconds": 1})
    time.sleep(0.8)

    persist_response = client.post("/v1/kv/persist", json={"key": "user:persist"})
    assert persist_response.status_code == 200
