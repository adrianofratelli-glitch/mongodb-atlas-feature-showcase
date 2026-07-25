#!/usr/bin/env bash
#
# Registra o MongoDB Kafka source connector no Kafka Connect (COLUNA 2 do módulo
# Streaming). Lê MONGO_URI de backend/.env — a credencial nunca vai para o repo.
#
#   docker compose -f docker-compose.streaming.yml up -d
#   ./scripts/setup-kafka-connector.sh
#
set -euo pipefail

BASE="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$BASE/backend/.env"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_NAME="${CONNECT_CONNECTOR_NAME:-atlas-pix-source}"
STREAMING_DB="${STREAMING_DB:-pix}"
COLLECTION="transacoes"

fail() { echo "❌ $1" >&2; exit 1; }

command -v curl >/dev/null || fail "curl não encontrado."
[[ -f "$ENV_FILE" ]] || fail "backend/.env não encontrado. Copie backend/.env.example primeiro."

MONGO_URI="$(grep -E '^MONGO_URI=' "$ENV_FILE" | head -n1 | cut -d= -f2- | sed -e 's/^["'\'']//' -e 's/["'\'']$//' -e 's/\r$//')"
[[ -n "$MONGO_URI" ]] || fail "MONGO_URI ausente em backend/.env."

echo "▶ Aguardando o Kafka Connect em $CONNECT_URL ..."
ready=0
for _ in $(seq 1 60); do
  if curl -fsS "$CONNECT_URL/connectors" >/dev/null 2>&1; then ready=1; break; fi
  sleep 2
done
[[ "$ready" == "1" ]] || fail "Kafka Connect não respondeu. Verifique 'docker compose -f docker-compose.streaming.yml ps'."

curl -fsS "$CONNECT_URL/connector-plugins" | grep -q "MongoSourceConnector" ||
  fail "Plugin mongodb-kafka-connect ausente no Connect. Veja os logs: docker logs showcase-connect"

# PUT em /config é idempotente: cria o connector ou atualiza o existente.
config=$(cat <<JSON
{
  "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
  "connection.uri": "$MONGO_URI",
  "database": "$STREAMING_DB",
  "collection": "$COLLECTION",
  "topic.prefix": "atlas",
  "publish.full.document.only": "true",
  "startup.mode": "copy_existing",
  "change.stream.full.document": "updateLookup",
  "output.format.value": "json",
  "output.format.key": "json",
  "poll.await.time.ms": "500",
  "poll.max.batch.size": "1000",
  "tasks.max": "1",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.storage.StringConverter"
}
JSON
)

echo "▶ Registrando o connector '$CONNECTOR_NAME' ..."
curl -fsS -X PUT -H "Content-Type: application/json" \
  --data "$config" "$CONNECT_URL/connectors/$CONNECTOR_NAME/config" >/dev/null

sleep 3
echo "▶ Status:"
curl -fsS "$CONNECT_URL/connectors/$CONNECTOR_NAME/status"
echo ""
echo "✅ Connector registrado. Tópico: atlas.$STREAMING_DB.$COLLECTION"
echo "   Console do Redpanda: http://localhost:8085"
