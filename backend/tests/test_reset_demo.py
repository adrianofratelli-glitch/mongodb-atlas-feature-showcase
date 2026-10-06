"""reset_demo.py: escopo do que é apagado e guarda do banco da demo (sem cluster)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))

spec = importlib.util.spec_from_file_location("reset_demo", ROOT / "scripts" / "reset_demo.py")
reset_demo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reset_demo)


class Col:
    def __init__(self, n=0, indexes=()):
        self.n = n
        self.idx = ["_id_", *indexes]
        self.dropped = False
        self.created = []

    def drop(self):
        self.dropped = True

    def list_indexes(self):
        return [{"name": i} for i in self.idx]

    def drop_index(self, name):
        self.idx.remove(name)

    def create_index(self, keys, **kw):
        self.created.append(kw.get("name") or keys)

    def estimated_document_count(self):
        return self.n


class Db(dict):
    name = "POC_test"
    client = None

    def list_collection_names(self):
        return list(self)

    def __missing__(self, k):
        self[k] = Col()
        return self[k]


def test_reset_nao_dropa_colecoes_compartilhadas_nem_indices_alheios(monkeypatch):
    db = Db()
    db["produtos"] = Col(200_000, ["marca_1", "categoria_1", "demo01_preco_1", "produto_id_1", "cat_total_av_idx"])
    db["avaliacoes"] = Col(50_000)
    db["schema_demo"] = Col(5)
    db["outra_pov"] = Col(10)
    chamadas = {}

    class Cleanup:
        @staticmethod
        def cleanup(client, streaming_db, ttl, geo_db):
            chamadas["streaming"] = (streaming_db, geo_db)
            return ["transacoes"]

    monkeypatch.setattr(reset_demo, "_load_cleanup", lambda: Cleanup)
    rel = reset_demo.reset(db, "pix_test", "geo_test", 300, 100_000, 20_000)

    assert rel["colecoes_dropadas"] == ["schema_demo"]
    assert db["schema_demo"].dropped and not db["produtos"].dropped and not db["outra_pov"].dropped
    assert rel["indices_removidos"] == ["demo01_preco_1"]
    assert "marca_1" in db["produtos"].idx
    assert rel["seed"].startswith("mantido")
    assert chamadas["streaming"] == ("pix_test", "geo_test")


def test_reset_recusa_banco_da_demo_sem_consentimento(monkeypatch):
    monkeypatch.delenv("ALLOW_DEMO_DB_WRITE", raising=False)
    monkeypatch.setenv("MONGO_URI", "mongodb://" + "127.0.0.1:1")
    monkeypatch.setenv("MONGO_DB", "POC")
    monkeypatch.setenv("STREAMING_DB", "pix_test")
    monkeypatch.setenv("GEO_DB", "geo_test")

    class NaoConecta:
        def __init__(self, *a, **k):
            pass

        def __getitem__(self, k):
            return Db()

        def close(self):
            pass

    monkeypatch.setattr(reset_demo, "MongoClient", NaoConecta)
    monkeypatch.setattr(reset_demo, "reset", lambda *a, **k: pytest.fail("não deveria escrever"))
    with pytest.raises(SystemExit):
        reset_demo.main([])


def test_check_aponta_colecao_vazia():
    db = Db()
    db["produtos"] = Col(0)
    problemas = reset_demo.check(db)
    assert any("produtos vazia" in p for p in problemas)
