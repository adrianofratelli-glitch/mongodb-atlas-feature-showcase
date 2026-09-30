"""O pipeline publicado precisa ser exatamente o enviado ao banco."""
import os
import sys
from pathlib import Path
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault('MONGO_URI', 'mongodb://127.0.0.1:27017')
from routers import aggregations

@pytest.mark.parametrize('name', ['lookup_produtos_avaliacoes', 'facet_analytics', 'union_with', 'group_advanced', 'window_functions', 'bucket_auto'])
def test_resposta_publica_pipeline_executado(monkeypatch, name):
    calls = []
    class Collection:
        def aggregate(self, pipeline, **kwargs):
            calls.append(pipeline)
            return []
    monkeypatch.setattr(aggregations, 'db', {'produtos': Collection(), 'avaliacoes': Collection()})
    monkeypatch.setattr(aggregations, '_lookup_cache', {})
    fn = getattr(aggregations, name)
    result = fn(limit=5) if name == 'lookup_produtos_avaliacoes' else fn()
    assert result['pipeline'] == calls[0]
    if name == 'lookup_produtos_avaliacoes':
        cached = fn(limit=5)
        assert len(calls) == 1
        assert cached['cache']['reutilizado'] is True
        assert result['cache']['reutilizado'] is False
        assert cached['pipeline'] == calls[0]
