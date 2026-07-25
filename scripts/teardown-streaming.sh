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

echo "▶ Removendo o connector '$CONNECTOR_NAME' (se existir)..."
curl -fsS -X DELETE "$CONNECT_URL/connectors/$CONNECTOR_NAME" >/dev/null 2>&1 || true

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
