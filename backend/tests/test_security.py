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


class FakeDatabase:
    @staticmethod
    def list_collection_names():
        return ["produtos", "avaliacoes"]


def test_preflight_reports_required_collections(monkeypatch):
    monkeypatch.setattr(main, "readiness", lambda: (True, "MongoDB conectado"))
    monkeypatch.setattr(main, "db", FakeDatabase())
    # As checagens do módulo Streaming falam com Atlas/Kafka de verdade; aqui o
    # foco é o reporte das coleções da POC.
    monkeypatch.setattr(main.streaming, "preflight_checks", dict)
    response = client.get("/preflight")
    assert response.status_code == 200
    assert response.json()["ready"] is True
    assert response.json()["checks"]["collection_produtos"]["ok"] is True


def test_preflight_reprova_quando_o_streaming_precisa_de_acao(monkeypatch):
    """A coleção cheia antes da demo tem que reprovar o pré-voo, não passar batido."""
    monkeypatch.setattr(main, "readiness", lambda: (True, "MongoDB conectado"))
    monkeypatch.setattr(main, "db", FakeDatabase())
    monkeypatch.setattr(
        main.streaming, "preflight_checks",
        lambda: {"streaming_colecao": {"ok": False, "message": "5000000 documentos — rode o Reset"}},
    )
    response = client.get("/preflight")
    assert response.status_code == 503
    assert response.json()["ready"] is False


def test_preflight_nao_reprova_por_kafka_ou_asp_ausentes(monkeypatch):
    """Kafka e ASP são opcionais: aparecem no diagnóstico, mas não travam o pré-voo."""
    monkeypatch.setattr(main, "readiness", lambda: (True, "MongoDB conectado"))
    monkeypatch.setattr(main, "db", FakeDatabase())
    monkeypatch.setattr(
        main.streaming, "preflight_checks",
        lambda: {
            "streaming_kafka": {"ok": False, "message": "Connect indisponível"},
            "streaming_asp": {"ok": False, "message": "ASP_ENABLED=false"},
        },
    )
    response = client.get("/preflight")
    assert response.status_code == 200
    assert response.json()["ready"] is True
