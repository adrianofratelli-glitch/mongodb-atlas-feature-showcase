from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi import HTTPException

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))
os.environ.setdefault("MONGO_URI", "mongodb://127.0.0.1:27017")

import settings as settings_module  # noqa: E402
from routers import hot_cold, reindexacao, schema_validation  # noqa: E402


def test_int_env_aplica_limites(monkeypatch):
    monkeypatch.setenv("TEST_LIMIT", "-10")
    assert settings_module._int_env("TEST_LIMIT", 50, 1, 100) == 1
    monkeypatch.setenv("TEST_LIMIT", "999")
    assert settings_module._int_env("TEST_LIMIT", 50, 1, 100) == 100
    monkeypatch.setenv("TEST_LIMIT", "invalido")
    assert settings_module._int_env("TEST_LIMIT", 50, 1, 100) == 50


def test_insert_valido_exige_schema_ativo(monkeypatch):
    monkeypatch.setattr(schema_validation, "_col_exists", lambda: False)
    with pytest.raises(HTTPException) as exc:
        schema_validation.insert_valid()
    assert exc.value.status_code == 409


def test_atlas_request_aceita_qualquer_status_2xx(monkeypatch):
    class Response:
        status_code = 202
        text = ""

    monkeypatch.setattr(hot_cold, "ATLAS_PUBLIC_KEY", "public")
    monkeypatch.setattr(hot_cold, "ATLAS_PRIVATE_KEY", "private")
    monkeypatch.setattr(hot_cold, "ATLAS_PROJECT_ID", "project")
    monkeypatch.setattr(hot_cold.requests, "request", lambda *_args, **_kwargs: Response())

    assert hot_cold._atlas_request("POST", "https://example.test") == {}


def test_online_archive_aceita_id_da_api_v2(monkeypatch):
    monkeypatch.setattr(
        hot_cold,
        "_atlas_request",
        lambda *_args, **_kwargs: {
            "results": [{
                "id": "archive-v2",
                "state": "ACTIVE",
                "collName": "produtos",
                "criteria": {"dateField": "created_at", "expireAfterDays": 365},
            }]
        },
    )

    result = hot_cold.list_online_archives()
    assert result["archives"][0]["id"] == "archive-v2"


def test_simulacao_online_archive_usa_corte_movel_de_365_dias(monkeypatch):
    class Collection:
        pipeline = None

        def aggregate(self, pipeline):
            self.pipeline = pipeline
            return [{"hot": 1, "cold": 1}]

        @staticmethod
        def estimated_document_count():
            return 2

    collection = Collection()
    monkeypatch.setattr(hot_cold, "db", {hot_cold.COLLECTION: collection})

    hot_cold.archive_simulation()

    cutoff = collection.pipeline[1]["$group"]["hot"]["$sum"]["$cond"][0]["$gte"][1]
    expected = datetime.now(timezone.utc) - timedelta(days=365)
    assert abs((cutoff - expected).total_seconds()) < 2


def test_nome_de_indice_diferencia_opcoes():
    assert reindexacao._index_name(["preco"]) == "preco_1"
    assert reindexacao._index_name(["preco"], partial=True) == "preco_1_partial"
    assert reindexacao._index_name(["preco"], sparse=True) == "preco_1_sparse"


def test_indice_equivalente_compara_key_e_opcoes(monkeypatch):
    class Collection:
        @staticmethod
        def list_indexes():
            return [
                {"name": "preco_normal", "key": {"preco": 1}},
                {
                    "name": "preco_parcial_existente",
                    "key": {"preco": 1},
                    "partialFilterExpression": {"em_estoque": True},
                },
            ]

    monkeypatch.setattr(reindexacao, "db", {reindexacao.COLLECTION: Collection()})

    assert reindexacao._equivalent_index_name(
        [("preco", 1)], False, {"em_estoque": True}
    ) == "preco_parcial_existente"
