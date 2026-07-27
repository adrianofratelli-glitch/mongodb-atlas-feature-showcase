#!/usr/bin/env bash
#
# Registra o(s) MongoDB Kafka source connector(s) no Kafka Connect (COLUNA 2 do
# módulo Streaming). Lê MONGO_URI de backend/.env — a credencial nunca vai para
# o repo.
#
#   ./scripts/setup-kafka-connector.sh          # 1 connector, menor custo/complexidade
#   STREAMING_CS_PARTICOES=4 ./scripts/setup-kafka-connector.sh 2
#                                                # experimento com filtros disjuntos
#
# Um source connector por coleção já prova o conceito CDC → Kafka. Mais de um
# abre cursores adicionais com filtros disjuntos; é um experimento da PoV, não
# partição nativa nem recomendação de sizing.
#
set -euo pipefail

BASE="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$BASE/backend/.env"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_NAME="${CONNECT_CONNECTOR_NAME:-atlas-pix-source}"
STREAMING_DB="${STREAMING_DB:-pix}"
COLLECTION="transacoes"
# Precisa bater com STREAMING_CS_PARTICOES do backend: é o mesmo campo `particao`.
PARTICOES="${STREAMING_CS_PARTICOES:-1}"
CONNECTORS="${1:-${KAFKA_CONNECTORS:-1}}"

fail() { echo "❌ $1" >&2; exit 1; }

command -v curl >/dev/null || fail "curl não encontrado."
[[ -f "$ENV_FILE" ]] || fail "backend/.env não encontrado. Copie backend/.env.example primeiro."

MONGO_URI="$(grep -E '^MONGO_URI=' "$ENV_FILE" | head -n1 | cut -d= -f2- | sed -e 's/^["'\'']//' -e 's/["'\'']$//' -e 's/\r$//')"
[[ -n "$MONGO_URI" ]] || fail "MONGO_URI ausente em backend/.env."
[[ "$CONNECTORS" =~ ^[0-9]+$ ]] || fail "Número de connectors inválido: $CONNECTORS"
[[ "$PARTICOES" =~ ^[0-9]+$ ]] || fail "Número de partições inválido: $PARTICOES"
(( CONNECTORS >= 1 && CONNECTORS <= PARTICOES )) ||
  fail "Connectors deve ficar entre 1 e o número de partições ($PARTICOES)."

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
#
# Apagar o connector NÃO apaga o offset dele: o Connect guarda o resume token no
# tópico connect-offsets, indexado pelo nome. Recriado com o mesmo nome, ele
# retoma do token antigo — e `startup.mode: latest` só vale quando não existe
# offset guardado. Como o `up` dropa pix.transacoes, esse token aponta para um
# ponto do oplog que não existe mais e a task morre com ChangeStreamHistoryLost,
# deixando o connector RUNNING com a única task FAILED.
#
# Por isso a ordem é stop -> apaga offset -> delete. O endpoint de offsets exige
# o connector parado e existe a partir do Kafka Connect 3.6; se não existir, o
# `|| true` só nos devolve o comportamento antigo.
existentes="$(curl -fsS "$CONNECT_URL/connectors" || echo '[]')"
printf '%s' "$existentes" | python3 -c 'import json,sys
base=sys.argv[1]
for name in json.load(sys.stdin):
    if name == base or name.startswith(base + "-"):
        print(name)' "$CONNECTOR_NAME" |
  while IFS= read -r antigo; do
    curl -fsS -X PUT "$CONNECT_URL/connectors/$antigo/stop" >/dev/null 2>&1 || true
    for _ in $(seq 1 15); do
      curl -fsS "$CONNECT_URL/connectors/$antigo/status" 2>/dev/null |
        grep -q '"state":"STOPPED"' && break
      sleep 1
    done
    curl -fsS -X DELETE "$CONNECT_URL/connectors/$antigo/offsets" >/dev/null 2>&1 ||
      echo "   (não foi possível zerar o offset de $antigo — Connect < 3.6?)"
    curl -fsS -X DELETE "$CONNECT_URL/connectors/$antigo" >/dev/null 2>&1 || true
  done
sleep 2

registra() { # nome, pipeline_json_escapado
  local nome="$1" filtro="$2"
  local config
  config="$(MONGO_CONNECT_URI="$MONGO_URI" STREAM_DB="$STREAMING_DB" STREAM_COLL="$COLLECTION" \
    STREAM_FILTER="$filtro" python3 -c 'import json,os
print(json.dumps({
  "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
  "connection.uri": os.environ["MONGO_CONNECT_URI"],
  "database": os.environ["STREAM_DB"],
  "collection": os.environ["STREAM_COLL"],
  "topic.prefix": "atlas",
  "publish.full.document.only": "true",
  "pipeline": os.environ["STREAM_FILTER"],
  "startup.mode": "latest",
  "heartbeat.interval.ms": "10000",
  "heartbeat.topic.name": "__mongodb_heartbeats",
  "output.format.value": "json",
  "output.format.key": "json",
  "poll.await.time.ms": "500",
  "poll.max.batch.size": "1000",
  "tasks.max": "1",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.storage.StringConverter"
}))')"
  printf '%s' "$config" | curl -fsS -X PUT -H "Content-Type: application/json" \
    --data-binary @- "$CONNECT_URL/connectors/$nome/config" >/dev/null
  echo "   ✓ $nome"
}

if [[ "$CONNECTORS" -eq 1 ]]; then
  echo "▶ Registrando 1 connector (todas as partições) ..."
  registra "$CONNECTOR_NAME" '[{"$match": {"operationType": "insert"}}]'
else
  echo "▶ Registrando $CONNECTORS connectors sobre $PARTICOES partições ..."
  for ((i = 0; i < CONNECTORS; i++)); do
    # Connector i cobre as partições p onde p % CONNECTORS == i.
    lista=""
    for ((p = i; p < PARTICOES; p += CONNECTORS)); do
      lista="${lista:+$lista, }$p"
    done
    [[ -n "$lista" ]] || continue
    filtro='[{"$match": {"operationType": "insert", "fullDocument.particao": {"$in": ['"$lista"']}}}]'
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
