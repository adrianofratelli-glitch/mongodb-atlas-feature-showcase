#!/usr/bin/env bash
# Cria o Atlas Search index do módulo Geo via mongosh.
#
#   ./scripts/create_search_index_geo.sh
#
# Lê MONGO_URI de backend/.env. O índice leva alguns minutos para ficar
# READY; até lá o painel de busca da aba Geo renderiza "não configurado".
set -euo pipefail

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$RAIZ/backend/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "backend/.env não encontrado — copie backend/.env.example" >&2
  exit 1
fi

# Mesmo parser de scripts/ambiente.sh: tira aspas nas pontas e CR de arquivo
# salvo no Windows. Um valor com '#' ou quebra de linha quebra os dois.
le_env() { { grep -E "^$1=" "$ENV_FILE" || true; } | head -n1 | cut -d= -f2- | sed -e 's/^["'\'']//' -e 's/["'\'']$//' -e 's/\r$//'; }

MONGO_URI="$(le_env MONGO_URI)"
GEO_DB="$(le_env GEO_DB)"
GEO_DB="${GEO_DB:-geo}"
INDICE="${GEO_SEARCH_INDEX:-idx_geo_estabelecimento}"

if [[ -z "$MONGO_URI" ]]; then
  echo "MONGO_URI ausente em backend/.env" >&2
  exit 1
fi

if ! command -v mongosh >/dev/null 2>&1; then
  echo "mongosh não encontrado — instale o MongoDB Shell" >&2
  exit 1
fi

echo "Criando search index '$INDICE' em $GEO_DB.transacoes…"

mongosh "$MONGO_URI" --quiet --eval "
const db = db.getSiblingDB('$GEO_DB');
const nome = '$INDICE';
const existente = db.transacoes.getSearchIndexes(nome);
const definicao = {
  mappings: {
    dynamic: false,
    fields: {
      estabelecimento: {
        type: 'document',
        fields: {
          // Analisador pt-BR: stemming e stopwords em português.
          nome: { type: 'string', analyzer: 'lucene.portuguese' },
          // token para o filtro exato, stringFacet para a faceta.
          categoria: [{ type: 'token' }, { type: 'stringFacet' }]
        }
      },
      uf: [{ type: 'token' }, { type: 'stringFacet' }],
      local: { type: 'geo' }
    }
  }
};

if (existente.length > 0) {
  db.transacoes.updateSearchIndex(nome, definicao);
  print('índice existente atualizado: ' + nome);
} else {
  db.transacoes.createSearchIndex(nome, definicao);
  print('índice criado: ' + nome);
}
print('acompanhe o status com: db.transacoes.getSearchIndexes(\"' + nome + '\")');
"
