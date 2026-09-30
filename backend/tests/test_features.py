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
from routers import change_streams, hot_cold, reindexacao, schema_validation  # noqa: E402


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


def test_start_change_stream_espera_cursor_abrir(monkeypatch):
    import threading

    entrou = threading.Event()

    class Stream:
        def __enter__(self):
            entrou.set()
            return self

        def __exit__(self, *_args):
            return False

        @staticmethod
        def try_next():
            return None

    class Collection:
        @staticmethod
        def watch(*_args, **_kwargs):
            return Stream()

    class Database:
        @staticmethod
        def list_collection_names():
            return []

        @staticmethod
        def create_collection(*_args, **_kwargs):
            return None

        @staticmethod
        def __getitem__(_name):
            return Collection()

    change_streams.stop_watch()
    monkeypatch.setattr(change_streams, "db", Database())
    resposta = change_streams.start_watch()
    try:
        assert resposta["status"] == "watching"
        assert entrou.is_set()
    finally:
        change_streams.stop_watch()


def test_previa_hotcold_declara_cluster_e_usa_politica_vigente(monkeypatch):
    corte = datetime(2024, 9, 8, tzinfo=timezone.utc)
    filtros = []
    class Cursor:
        def limit(self, n):
            assert n == 3
            return []
    class Colecao:
        def find(self, filtro, projecao):
            filtros.append(filtro)
            return Cursor()
    monkeypatch.setattr(hot_cold, 'db', {hot_cold.COLLECTION: Colecao()})
    monkeypatch.setattr(hot_cold, '_cutoff_atual', lambda: (corte, 730))
    r = hot_cold.query_transparent(categoria='Eletrônicos')
    assert filtros == [
        {'categoria': 'Eletrônicos', 'created_at': {'$gte': corte}},
        {'categoria': 'Eletrônicos', 'created_at': {'$lt': corte}},
    ]
    assert r['simulacao'] is True
    assert r['cutoff_dias'] == 730
    assert 'não consulta dados arquivados' in r['explanation']
    assert '$gte' in r['query_used'] and '$lt' in r['query_used']


def test_insert_valido_resposta_serializavel_depois_da_escrita(monkeypatch):
    import json
    from bson import ObjectId
    from types import SimpleNamespace
    from fastapi.encoders import jsonable_encoder
    class Collection:
        def insert_one(self, doc):
            doc['_id'] = ObjectId()
            return SimpleNamespace(inserted_id=doc['_id'])
    monkeypatch.setattr(schema_validation, '_col_exists', lambda: True)
    monkeypatch.setattr(schema_validation, '_has_validator', lambda: True)
    monkeypatch.setattr(schema_validation, 'db', {schema_validation.COL: Collection()})
    result = schema_validation.insert_valid()
    assert result['status'] == 'aceito'
    assert '_id' not in result['document']
    json.dumps(jsonable_encoder(result))


def test_passo_sem_schema_nao_grava_parcialmente_se_validador_ativo(monkeypatch):
    monkeypatch.setattr(schema_validation, '_col_exists', lambda: True)
    monkeypatch.setattr(schema_validation, '_has_validator', lambda: True)
    monkeypatch.setattr(schema_validation, 'db', {})  # nenhuma escrita deve ocorrer
    with pytest.raises(HTTPException) as exc:
        schema_validation.step2_insert()
    assert exc.value.status_code == 409


def test_erro_operacional_nao_e_evidencia_de_rejeicao_pelo_schema(monkeypatch):
    from pymongo.errors import OperationFailure
    class Collection:
        def insert_one(self, doc):
            raise OperationFailure('Unauthorized', code=13)
    monkeypatch.setattr(schema_validation, '_col_exists', lambda: True)
    monkeypatch.setattr(schema_validation, '_has_validator', lambda: True)
    monkeypatch.setattr(schema_validation, 'db', {schema_validation.COL: Collection()})
    with pytest.raises(OperationFailure):
        schema_validation.step4_insert_invalid(scenario='preco_negativo')


@pytest.mark.parametrize('codigo', [121, 12370902])
def test_constraint_recusa_promocao_com_dados_legados(monkeypatch, codigo):
    from pymongo.errors import OperationFailure

    commands = []
    class Collection:
        def count_documents(self, *_args, **_kwargs): return 4
    class Database:
        def __getitem__(self, _name): return Collection()
        def command(self, command, *_args, **kwargs):
            commands.append((command, kwargs))
            if kwargs.get('validationLevel') == 'constraint':
                raise OperationFailure('existing documents violate validator', code=codigo)

    monkeypatch.setattr(schema_validation, '_col_exists', lambda: True)
    monkeypatch.setattr(schema_validation, '_has_validator', lambda: True)
    monkeypatch.setattr(schema_validation, '_options', lambda: {'validationLevel': 'strict'})
    monkeypatch.setattr(schema_validation, '_supports_constraint', lambda: True)
    monkeypatch.setattr(schema_validation, 'db', Database())

    result = schema_validation.step4_try_constraint()
    assert result['status'] == 'bloqueado_por_legado'
    assert result['legacy_invalid_count'] == 4
    assert commands[0][1]['prepareConstraintValidationLevel'] is True
    assert commands[1][1]['validationLevel'] == 'constraint'


def test_reparo_de_legado_atualiza_apenas_os_quatro_documentos_da_demo(monkeypatch):
    updates = []
    class Result:
        modified_count = 1
    class Collection:
        def update_many(self, filtro, atualizacao):
            updates.append((filtro, atualizacao))
            return Result()
        def count_documents(self, *_args, **_kwargs): return 0
    class Database:
        def __getitem__(self, _name): return Collection()

    monkeypatch.setattr(schema_validation, '_col_exists', lambda: True)
    monkeypatch.setattr(schema_validation, '_has_validator', lambda: True)
    monkeypatch.setattr(schema_validation, 'db', Database())
    result = schema_validation.step5_repair_legacy()

    assert result == {'status': 'corrigidos', 'corrigidos': 4, 'restantes': 0}
    assert len(updates) == 4
    assert all(filtro.keys() == {'_demo_invalid'} for filtro, _ in updates)
    assert all('_demo_invalid' in atualizacao['$unset'] for _, atualizacao in updates)
