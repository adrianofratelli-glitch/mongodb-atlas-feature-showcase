#!/usr/bin/env bash
#
# Derruba a infraestrutura local do módulo Streaming e remove o connector.
#
#   ./scripts/teardown-streaming.sh            # para os containers
#   ./scripts/teardown-streaming.sh --volumes  # remove também o volume de plugins
#
set -euo pipefail

BASE="$(cd "$(dirname "$0")/.." && pwd)"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_NAME="${CONNECT_CONNECTOR_NAME:-atlas-pix-source}"
STREAMING_DB="${STREAMING_DB:-pix}"

echo "▶ Removendo connectors '$CONNECTOR_NAME' e '$CONNECTOR_NAME-*' (se existirem)..."
curl -fsS "$CONNECT_URL/connectors" 2>/dev/null \
  | python3 -c 'import json,sys
base=sys.argv[1]
for name in json.load(sys.stdin):
    if name == base or name.startswith(base + "-"):
        print(name)' "$CONNECTOR_NAME" \
  | while IFS= read -r connector; do
      curl -fsS -X DELETE "$CONNECT_URL/connectors/$connector" >/dev/null 2>&1 || true
      echo "   removido: $connector"
    done || true

echo "▶ Derrubando os containers..."
if [[ "${1:-}" == "--volumes" ]]; then
  docker compose -f "$BASE/docker-compose.streaming.yml" down --volumes
  echo "   Volume de plugins removido — a próxima subida baixa o plugin de novo."
else
  docker compose -f "$BASE/docker-compose.streaming.yml" down
fi

echo "✅ Infraestrutura de streaming encerrada."
echo "   As coleções $STREAMING_DB.transacoes / metricas_janela / dlq continuam no Atlas;"
echo "   use POST /streaming/reset (ou o botão Reset na UI) para limpá-las."
