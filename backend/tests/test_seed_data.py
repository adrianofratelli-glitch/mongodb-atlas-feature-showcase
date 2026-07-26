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
