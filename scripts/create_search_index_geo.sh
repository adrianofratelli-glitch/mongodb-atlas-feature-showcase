#!/usr/bin/env bash
# Cria o Atlas Search index do módulo Geo via mongosh.
#
#   ./scripts/create_search_index_geo.sh
#
# Lê MONGO_URI de backend/.env e só retorna sucesso quando o índice está READY.
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
TIMEOUT_SEGUNDOS="${GEO_SEARCH_READY_TIMEOUT_SECONDS:-$(le_env GEO_SEARCH_READY_TIMEOUT_SECONDS)}"
TIMEOUT_SEGUNDOS="${TIMEOUT_SEGUNDOS:-600}"

if [[ -z "$MONGO_URI" ]]; then
  echo "MONGO_URI ausente em backend/.env" >&2
  exit 1
fi

if [[ ! "$TIMEOUT_SEGUNDOS" =~ ^[0-9]+$ ]] || (( TIMEOUT_SEGUNDOS < 2 )); then
  echo "GEO_SEARCH_READY_TIMEOUT_SECONDS precisa ser um inteiro >= 2" >&2
  exit 1
fi
TENTATIVAS=$(( (TIMEOUT_SEGUNDOS + 1) / 2 ))

if ! command -v mongosh >/dev/null 2>&1; then
  echo "mongosh não encontrado — instale o MongoDB Shell" >&2
  exit 1
fi

echo "Garantindo search index '$INDICE' em $GEO_DB.transacoes…"

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

const canonico = valor => {
  if (Array.isArray(valor)) return valor.map(canonico);
  if (valor && typeof valor === 'object') {
    return Object.keys(valor).sort().reduce((r, chave) => {
      r[chave] = canonico(valor[chave]);
      return r;
    }, {});
  }
  return valor;
};
const igual = (a, b) => JSON.stringify(canonico(a)) === JSON.stringify(canonico(b));

if (existente.length > 0) {
  const atual = existente[0].latestDefinition || existente[0].definition;
  if (atual && igual(atual, definicao)) {
    print('índice existente já está com a definição esperada: ' + nome);
  } else {
    db.transacoes.updateSearchIndex(nome, definicao);
    print('índice existente atualizado: ' + nome);
  }
} else {
  db.transacoes.createSearchIndex(nome, definicao);
  print('índice criado: ' + nome);
}

for (let tentativa = 1; tentativa <= $TENTATIVAS; tentativa++) {
  const indice = db.transacoes.getSearchIndexes(nome)[0];
  if (indice && indice.status === 'READY' && indice.queryable !== false) {
    print('índice READY e consultável: ' + nome);
    quit(0);
  }
  if (tentativa % 10 === 0) {
    print('aguardando READY (' + tentativa * 2 + 's)…');
  }
  sleep(2000);
}
print('ERRO: índice não chegou a READY em ${TIMEOUT_SEGUNDOS}s');
quit(2);
"
