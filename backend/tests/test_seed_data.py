import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from seed_data import gerar_avaliacao  # noqa: E402


def test_review_inherits_product_category():
    review = gerar_avaliacao([("produto-1", "Eletrônicos")])
    assert review["produto_id"] == "produto-1"
    assert review["categoria"] == "Eletrônicos"
    assert 1 <= review["nota"] <= 5


# ── Idempotência (achado P1 da revisão 2026-10: reexecutar duplicava dados) ──
import pytest  # noqa: E402

import seed_data  # noqa: E402


class FakeCol:
    def __init__(self):
        self.docs = {}
        self.indexes = set()

    def bulk_write(self, ops, ordered=False):
        for op in ops:
            filtro, doc = op._filter, op._doc
            chave = tuple(sorted(filtro.items()))
            self.docs[chave] = doc

    def create_index(self, keys, **kwargs):
        self.indexes.add(kwargs.get("name") or str(keys))

    def estimated_document_count(self):
        return len(self.docs)


class FakeDb(dict):
    def __missing__(self, key):
        self[key] = FakeCol()
        return self[key]


def test_seed_reexecutado_nao_duplica():
    db = FakeDb()
    seed_data.seed(500, 200, db=db, verbose=False)
    primeira = (db["produtos"].estimated_document_count(), db["avaliacoes"].estimated_document_count())
    seed_data.seed(500, 200, db=db, verbose=False)
    assert primeira == (500, 200)
    assert (db["produtos"].estimated_document_count(), db["avaliacoes"].estimated_document_count()) == primeira


def test_seed_e_deterministico():
    a, b = FakeDb(), FakeDb()
    seed_data.seed(50, 20, db=a, verbose=False)
    seed_data.seed(50, 20, db=b, verbose=False)
    assert set(a["produtos"].docs) == set(b["produtos"].docs)
    assert set(a["avaliacoes"].docs) == set(b["avaliacoes"].docs)


def test_seed_garante_indices_dos_modulos():
    db = FakeDb()
    seed_data.seed(10, 5, db=db, verbose=False)
    assert {"cat_total_av_idx", "total_av_idx", "destaque_idx", "recent_nota_idx"} <= (
        db["produtos"].indexes | db["avaliacoes"].indexes
    )


@pytest.mark.parametrize("nome,permitido", [
    ("POC_test", True), ("pix_test", True), ("POC", False), ("pix", False), ("POC_test_test", False), ("", False),
])
def test_guarda_do_banco_da_demo(monkeypatch, nome, permitido):
    monkeypatch.delenv("ALLOW_DEMO_DB_WRITE", raising=False)
    if permitido:
        seed_data.assert_writable_db(nome)
    else:
        with pytest.raises(SystemExit):
            seed_data.assert_writable_db(nome)


def test_guarda_aceita_demo_com_consentimento(monkeypatch):
    monkeypatch.setenv("ALLOW_DEMO_DB_WRITE", "1")
    seed_data.assert_writable_db("POC")
    with pytest.raises(SystemExit):
        seed_data.assert_writable_db("POC_test_test")


def test_indice_produto_id_nao_unico_de_outra_pov_nao_derruba_o_reset():
    """Achado no reset real do banco da demo: produto_id_1 existia sem unique."""
    from pymongo.errors import OperationFailure

    class Compartilhada(FakeCol):
        name = "produtos"

        def create_index(self, keys, **kwargs):
            if kwargs.get("unique"):
                raise OperationFailure("An existing index has the same name", code=86)
            super().create_index(keys, **kwargs)

    db = FakeDb()
    db["produtos"] = Compartilhada()
    seed_data.ensure_indexes(db)
    assert "cat_total_av_idx" in db["produtos"].indexes

    class Outro(FakeCol):
        name = "produtos"

        def create_index(self, keys, **kwargs):
            raise OperationFailure("not authorized", code=13)

    db2 = FakeDb()
    db2["produtos"] = Outro()
    with pytest.raises(OperationFailure):
        seed_data.ensure_indexes(db2)
