#!/usr/bin/env bash
#
# Registra o(s) MongoDB Kafka source connector(s) no Kafka Connect (COLUNA 2 do
# módulo Streaming). Lê MONGO_URI de backend/.env — a credencial nunca vai para
# o repo.
#
#   ./scripts/setup-kafka-connector.sh          # 2 connectors, particionados
#   ./scripts/setup-kafka-connector.sh 1        # um connector só (limite: 1 task)
#   KAFKA_CONNECTORS=6 ./scripts/setup-kafka-connector.sh
#
# POR QUE MAIS DE UM: o source connector roda UMA task por coleção — um cursor
# de change stream só. Medido, isso satura em ~6.300 msg/s enquanto as outras
# colunas seguem a 9.500. A saída é a mesma da coluna 1: particionar. Cada
# connector filtra `particao` no próprio pipeline e todos publicam no MESMO
# tópico, então o consumidor não muda.
#
# POR QUE 2 E NÃO MAIS: cada connector é mais um cursor lendo o oplog, e eles
# competem com o processor do ASP. Medido a 9.500 TPS:
#   1 connector  -> Kafka 6.327 msg/s (atrás)      · ASP 9.483 tx/s
#   2 connectors -> Kafka acompanha o gerador      · ASP 8.894 tx/s
#   4 connectors -> Kafka 9.565 msg/s              · ASP 7.113 tx/s (o ASP perde)
#
set -euo pipefail

BASE="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$BASE/backend/.env"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_NAME="${CONNECT_CONNECTOR_NAME:-atlas-pix-source}"
STREAMING_DB="${STREAMING_DB:-pix}"
COLLECTION="transacoes"
# Precisa bater com STREAMING_CS_PARTICOES do backend: é o mesmo campo `particao`.
PARTICOES="${STREAMING_CS_PARTICOES:-10}"
CONNECTORS="${1:-${KAFKA_CONNECTORS:-2}}"

fail() { echo "❌ $1" >&2; exit 1; }

command -v curl >/dev/null || fail "curl não encontrado."
[[ -f "$ENV_FILE" ]] || fail "backend/.env não encontrado. Copie backend/.env.example primeiro."

MONGO_URI="$(grep -E '^MONGO_URI=' "$ENV_FILE" | head -n1 | cut -d= -f2- | sed -e 's/^["'\'']//' -e 's/["'\'']$//' -e 's/\r$//')"
[[ -n "$MONGO_URI" ]] || fail "MONGO_URI ausente em backend/.env."
[[ "$CONNECTORS" -ge 1 ]] || fail "Número de connectors inválido: $CONNECTORS"

echo "▶ Aguardando o Kafka Connect em $CONNECT_URL ..."
ready=0
for _ in $(seq 1 60); do
  if curl -fsS "$CONNECT_URL/connectors" >/dev/null 2>&1; then ready=1; break; fi
  sleep 2
done
[[ "$ready" == "1" ]] || fail "Kafka Connect não respondeu. Verifique ./scripts/kafka-local.sh status"

curl -fsS "$CONNECT_URL/connector-plugins" | grep -q "MongoSourceConnector" ||
  fail "Plugin mongodb-kafka-connect ausente no Connect. Veja os logs do Connect."

# Remove connectors de execuções anteriores (o layout pode ter mudado).
existentes="$(curl -fsS "$CONNECT_URL/connectors" || echo '[]')"
for antigo in $(printf '%s' "$existentes" | tr -d '[]"' | tr ',' ' '); do
  case "$antigo" in
    "$CONNECTOR_NAME"|"$CONNECTOR_NAME"-*)
      curl -fsS -X DELETE "$CONNECT_URL/connectors/$antigo" >/dev/null 2>&1 || true ;;
  esac
done
sleep 2

registra() { # nome, pipeline_json_escapado
  local nome="$1" filtro="$2"
  local config
  config=$(cat <<JSON
{
  "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
  "connection.uri": "$MONGO_URI",
  "database": "$STREAMING_DB",
  "collection": "$COLLECTION",
  "topic.prefix": "atlas",
  "publish.full.document.only": "true",
  "pipeline": "$filtro",
  "startup.mode": "copy_existing",
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
  curl -fsS -X PUT -H "Content-Type: application/json" \
    --data "$config" "$CONNECT_URL/connectors/$nome/config" >/dev/null
  echo "   ✓ $nome"
}

if [[ "$CONNECTORS" -eq 1 ]]; then
  echo "▶ Registrando 1 connector (todas as partições) ..."
  registra "$CONNECTOR_NAME" '[{\"$match\": {\"operationType\": \"insert\"}}]'
else
  echo "▶ Registrando $CONNECTORS connectors sobre $PARTICOES partições ..."
  for ((i = 0; i < CONNECTORS; i++)); do
    # Connector i cobre as partições p onde p % CONNECTORS == i.
    lista=""
    for ((p = i; p < PARTICOES; p += CONNECTORS)); do
      lista="${lista:+$lista, }$p"
    done
    [[ -n "$lista" ]] || continue
    filtro='[{\"$match\": {\"operationType\": \"insert\", \"fullDocument.particao\": {\"$in\": ['"$lista"']}}}]'
    registra "$CONNECTOR_NAME-$i" "$filtro"
  done
fi

sleep 5
echo "▶ Status:"
# Parsing em Python: o `tr` sobre o JSON produzia tokens espúrios.
CONNECT_URL="$CONNECT_URL" PREFIXO="$CONNECTOR_NAME" python3 - <<'PYSTATUS'
import json, os, urllib.request
base = os.environ["CONNECT_URL"].rstrip("/")
prefixo = os.environ["PREFIXO"]
nomes = json.loads(urllib.request.urlopen(f"{base}/connectors", timeout=5).read())
for nome in sorted(n for n in nomes if n == prefixo or n.startswith(prefixo + "-")):
    d = json.loads(urllib.request.urlopen(f"{base}/connectors/{nome}/status", timeout=5).read())
    tasks = ", ".join(t["state"] for t in d.get("tasks", [])) or "sem task"
    print(f"   {nome}: {d['connector']['state']} · {tasks}")
PYSTATUS
echo ""
echo "✅ Tópico: atlas.$STREAMING_DB.$COLLECTION (todos os connectors publicam no mesmo)"
