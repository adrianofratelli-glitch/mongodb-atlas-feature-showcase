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
    monkeypatch.setenv("STREAMING_DB", "showcase_pix_test")
    monkeypatch.setenv("GEO_DB", "showcase_geo_test")

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


def _carregar_cleanup():
    spec = importlib.util.spec_from_file_location("cleanup_streaming_t", ROOT / "scripts" / "cleanup-streaming-data.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _ClienteProibido:
    def __init__(self, *a, **k):
        pytest.fail("não deveria conectar")


@pytest.mark.parametrize("streaming, geo", [
    ("pix_test", "showcase_geo_test"),          # *_test genérico de outra PoV
    ("showcase_pix_test", "geo_test"),          # geo_test = banco live do geo-showcase
])
def test_reset_recusa_streaming_e_geo_que_nao_sao_proprios(monkeypatch, streaming, geo):
    monkeypatch.delenv("ALLOW_DEMO_DB_WRITE", raising=False)
    monkeypatch.setenv("MONGO_URI", "mongodb://" + "127.0.0.1:1")
    monkeypatch.setenv("MONGO_DB", "showcase_poc_test")
    monkeypatch.setenv("STREAMING_DB", streaming)
    monkeypatch.setenv("GEO_DB", geo)
    monkeypatch.setattr(reset_demo, "MongoClient", lambda *a, **k: _ClienteNulo())
    monkeypatch.setattr(reset_demo, "reset", lambda *a, **k: pytest.fail("não deveria escrever"))
    with pytest.raises(SystemExit) as erro:
        reset_demo.main([])
    assert "showcase_*_test" in str(erro.value)


class _ClienteNulo:
    def __getitem__(self, k):
        return Db()

    def close(self):
        pass


@pytest.mark.parametrize("variavel, valor", [
    ("STREAMING_DB", "pix"), ("STREAMING_DB", "pix_test"),
    ("GEO_DB", "geo"), ("GEO_DB", "geo_test"),
])
def test_cleanup_main_recusa_banco_que_nao_e_proprio(monkeypatch, variavel, valor):
    cleanup = _carregar_cleanup()
    monkeypatch.delenv("ALLOW_DEMO_DB_WRITE", raising=False)
    monkeypatch.setenv("MONGO_URI", "mongodb://" + "127.0.0.1:1")
    monkeypatch.setenv("STREAMING_DB", "showcase_pix_test")
    monkeypatch.setenv("GEO_DB", "showcase_geo_test")
    monkeypatch.setenv(variavel, valor)
    monkeypatch.setattr(cleanup, "MongoClient", _ClienteProibido)
    monkeypatch.setattr(cleanup, "cleanup", lambda *a, **k: pytest.fail("não deveria limpar"))
    with pytest.raises(SystemExit) as erro:
        cleanup.main()
    assert variavel in str(erro.value)


def test_cleanup_main_aceita_banco_da_demo_com_a_flag(monkeypatch, tmp_path):
    cleanup = _carregar_cleanup()
    env = tmp_path / ".env"
    env.write_text("MONGO_URI=mongodb://127.0.0.1:1\n", encoding="utf-8")   # sem STREAMING_DB/GEO_DB: padrão pix/geo
    monkeypatch.setattr(cleanup, "ENV_FILE", env)
    monkeypatch.setenv("ALLOW_DEMO_DB_WRITE", "1")
    monkeypatch.setenv("MONGO_URI", "mongodb://" + "127.0.0.1:1")
    monkeypatch.setenv("STREAMING_DB", "pix")
    monkeypatch.setenv("GEO_DB", "geo")
    chamadas = []

    class ClienteFalso:
        def __init__(self, *a, **k):
            pass

        def close(self):
            pass

    monkeypatch.setattr(cleanup, "MongoClient", ClienteFalso)
    monkeypatch.setattr(cleanup, "cleanup", lambda c, db, ttl, geo: chamadas.append((db, geo)) or [])
    assert cleanup.main() == 0
    assert chamadas == [("pix", "geo")]


def _env_demo(tmp_path, linhas: str):
    arquivo = tmp_path / ".env"
    arquivo.write_text("MONGO_URI=mongodb://127.0.0.1:1\n" + linhas, encoding="utf-8")
    return arquivo


@pytest.mark.parametrize("variavel, valor", [("GEO_DB", "geo_test"), ("STREAMING_DB", "geo")])
def test_flag_recusa_banco_alheio_exportado_no_shell(monkeypatch, tmp_path, variavel, valor):
    """(a) Com a flag, um nome que não é o do backend/.env é recusado."""
    cleanup = _carregar_cleanup()
    monkeypatch.setattr(cleanup, "ENV_FILE", _env_demo(tmp_path, "STREAMING_DB=pix\nGEO_DB=geo\n"))
    monkeypatch.setenv("ALLOW_DEMO_DB_WRITE", "1")
    monkeypatch.setenv("STREAMING_DB", "pix")
    monkeypatch.setenv("GEO_DB", "geo")
    monkeypatch.setenv(variavel, valor)
    monkeypatch.setattr(cleanup, "MongoClient", _ClienteProibido)
    monkeypatch.setattr(cleanup, "cleanup", lambda *a, **k: pytest.fail("não deveria limpar"))
    with pytest.raises(SystemExit) as erro:
        cleanup.main()
    assert "backend/.env" in str(erro.value) and valor in str(erro.value)


def test_ambiente_nao_sobrepoe_o_env_na_decisao_da_flag(tmp_path, monkeypatch):
    """(b) O banco autorizado pela flag vem do arquivo, mesmo com override no ambiente."""
    cleanup = _carregar_cleanup()
    arquivo = _env_demo(tmp_path, "GEO_DB=geo_demo\n")
    monkeypatch.setenv("GEO_DB", "geo_test")
    assert cleanup.banco_da_demo("GEO_DB", arquivo) == "geo_demo"
    assert cleanup.banco_da_demo("STREAMING_DB", arquivo) == "pix"      # padrão do código
    monkeypatch.setenv("ALLOW_DEMO_DB_WRITE", "1")
    with pytest.raises(SystemExit):
        cleanup.assert_banco_proprio("geo_test", "GEO_DB", arquivo)


def test_flag_aceita_o_banco_do_env(tmp_path, monkeypatch):
    """(c) Com a flag, o nome definido no backend/.env passa; showcase_*_test passa sempre."""
    cleanup = _carregar_cleanup()
    arquivo = _env_demo(tmp_path, "STREAMING_DB=pix_demo\nGEO_DB=geo_demo\n")
    monkeypatch.setenv("ALLOW_DEMO_DB_WRITE", "1")
    cleanup.assert_banco_proprio("pix_demo", "STREAMING_DB", arquivo)
    cleanup.assert_banco_proprio("geo_demo", "GEO_DB", arquivo)
    cleanup.assert_banco_proprio("showcase_geo_test", "GEO_DB", arquivo)


def test_reset_com_flag_recusa_geo_alheio(monkeypatch, tmp_path):
    """O reset_demo usa a mesma regra (o load_dotenv dele deixa o ambiente sobrepor)."""
    monkeypatch.setenv("ALLOW_DEMO_DB_WRITE", "1")
    monkeypatch.setenv("MONGO_URI", "mongodb://" + "127.0.0.1:1")
    monkeypatch.setenv("MONGO_DB", "showcase_poc_test")
    monkeypatch.setenv("STREAMING_DB", "pix")
    monkeypatch.setenv("GEO_DB", "geo_test")
    cleanup = _carregar_cleanup()
    monkeypatch.setattr(cleanup, "ENV_FILE", _env_demo(tmp_path, ""))
    monkeypatch.setattr(reset_demo, "_load_cleanup", lambda: cleanup)
    monkeypatch.setattr(reset_demo, "MongoClient", lambda *a, **k: _ClienteNulo())
    monkeypatch.setattr(reset_demo, "reset", lambda *a, **k: pytest.fail("não deveria escrever"))
    with pytest.raises(SystemExit) as erro:
        reset_demo.main([])
    assert "geo_test" in str(erro.value)
