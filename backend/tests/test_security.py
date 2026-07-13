import os
import sys
from pathlib import Path

from fastapi.testclient import TestClient

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))
os.environ.setdefault("MONGO_URI", "mongodb://127.0.0.1:27017")

from main import app  # noqa: E402
import main  # noqa: E402
from security import settings  # noqa: E402


client = TestClient(app)


def test_liveness_does_not_require_database():
    response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers.get("x-request-id")


def test_mutation_rejects_untrusted_browser_origin():
    response = client.post(
        "/change-streams/stop",
        headers={"Origin": "https://site-malicioso.example"},
    )
    assert response.status_code == 403


def test_configured_token_is_required_for_mutations():
    previous = settings.demo_admin_token
    object.__setattr__(settings, "demo_admin_token", "test-secret")
    try:
        denied = client.post("/change-streams/stop")
        allowed = client.post("/change-streams/stop", headers={"X-Demo-Token": "test-secret"})
    finally:
        object.__setattr__(settings, "demo_admin_token", previous)
    assert denied.status_code == 401
    assert allowed.status_code == 200


def test_index_field_allowlist_rejects_operator_injection():
    response = client.post("/reindexacao/create?fields=%24where")
    assert response.status_code == 422


def test_lookup_limit_is_bounded_before_database_access():
    response = client.get("/aggregations/lookup?limit=100000")
    assert response.status_code == 422


def test_preflight_reports_required_collections(monkeypatch):
    class FakeDatabase:
        @staticmethod
        def list_collection_names():
            return ["produtos", "avaliacoes"]

    monkeypatch.setattr(main, "readiness", lambda: (True, "MongoDB conectado"))
    monkeypatch.setattr(main, "db", FakeDatabase())
    response = client.get("/preflight")
    assert response.status_code == 200
    assert response.json()["ready"] is True
    assert response.json()["checks"]["collection_produtos"]["ok"] is True
