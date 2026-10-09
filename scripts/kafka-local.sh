#!/usr/bin/env bash
#
# COLUNA 2 do módulo Streaming SEM Docker — Kafka nativo (binários do Homebrew).
#
# Sobe um broker KRaft de um nó e um Kafka Connect distribuído com o plugin
# mongodb-kafka-connect, os dois como processos desta PoV:
#
#   ./scripts/kafka-local.sh up      # broker + connect + plugin
#   ./scripts/kafka-local.sh status
#   ./scripts/kafka-local.sh down    # encerra só o que este script iniciou
#
# Depois: ./scripts/setup-kafka-connector.sh  (registra o source connector)
#
# Pré-requisito: `brew install kafka` (só os binários; Java vem junto).
# O script NÃO usa `brew services`: registrar um serviço do launchd é efeito
# fora da PoV que sobrevive ao `down` e reinicia o Java sozinho. O broker roda
# com configuração, dados e log em $KAFKA_RUN_DIR, e o controller KRaft usa a
# porta $KAFKA_CONTROLLER_PORT (19093 por padrão, não a 9093 do Homebrew, que
# costuma estar ocupada por túnel SSH ou outro broker). Se já houver um broker
# escutando em 9092, ele é usado como está e o `down` não o encerra.
#
set -euo pipefail

PLUGIN_DIR="${KAFKA_PLUGIN_DIR:-$HOME/.local/share/mdb-showcase-kafka/plugins}"
RUN_DIR="${KAFKA_RUN_DIR:-$HOME/.local/share/mdb-showcase-kafka/run}"
MONGO_CONNECTOR_VERSION="${MONGO_CONNECTOR_VERSION:-1.15.0}"
BROKER="${KAFKA_BROKERS:-localhost:9092}"
CONNECT_PORT="${CONNECT_PORT:-8083}"
CONNECT_LOG="$RUN_DIR/connect.log"
CONNECTOR_NAME="${CONNECT_CONNECTOR_NAME:-atlas-pix-source}"
STREAMING_DB="${STREAMING_DB:-pix}"
STREAMING_COLLECTION="${STREAMING_COLLECTION:-transacoes}"
TOPIC="atlas.$STREAMING_DB.$STREAMING_COLLECTION"
CONSUMER_GROUP="${KAFKA_CONSUMER_GROUP:-showcase-pix-observer}"
KAFKA_BIN="${KAFKA_BIN:-/opt/homebrew/opt/kafka/bin}"
CONTROLLER_PORT="${KAFKA_CONTROLLER_PORT:-19093}"
BROKER_PORT="${BROKER##*:}"
BROKER_CFG="$RUN_DIR/broker.properties"
BROKER_DATA="$RUN_DIR/kraft-data"
BROKER_LOG="$RUN_DIR/broker.log"

fail() { echo "❌ $1" >&2; exit 1; }
# Só LISTEN: sem o filtro, uma conexão ESTABLISHED para a porta faz o script
# achar que o serviço está de pé quando não está.
porta_ativa() { lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1; }

sobe_broker() {
  if porta_ativa "$CONTROLLER_PORT"; then
    fail "Porta $CONTROLLER_PORT (controller KRaft) ocupada. Defina KAFKA_CONTROLLER_PORT com uma porta livre."
  fi
  cat > "$BROKER_CFG" <<PROPS
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:$CONTROLLER_PORT
listeners=PLAINTEXT://localhost:$BROKER_PORT,CONTROLLER://localhost:$CONTROLLER_PORT
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://localhost:$BROKER_PORT,CONTROLLER://localhost:$CONTROLLER_PORT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$BROKER_DATA
num.partitions=1
offsets.topic.replication.factor=1
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=6
PROPS
  if [[ ! -f "$BROKER_DATA/meta.properties" ]]; then
    echo "▶ Formatando o armazenamento KRaft em $BROKER_DATA (só na primeira vez)..."
    local cluster_id
    cluster_id="$("$KAFKA_BIN/kafka-storage" random-uuid)"
    "$KAFKA_BIN/kafka-storage" format --standalone -t "$cluster_id" -c "$BROKER_CFG" >/dev/null ||
      fail "Falha ao formatar $BROKER_DATA."
  fi
  echo "▶ Subindo o broker Kafka (KRaft, :$BROKER_PORT, controller :$CONTROLLER_PORT)..."
  LOG_DIR="$RUN_DIR/logs" nohup "$KAFKA_BIN/kafka-server-start" "$BROKER_CFG" > "$BROKER_LOG" 2>&1 &
  echo $! > "$RUN_DIR/broker.pid"
  for _ in $(seq 1 45); do porta_ativa "$BROKER_PORT" && break; sleep 1; done
  porta_ativa "$BROKER_PORT" || { tail -20 "$BROKER_LOG" >&2; fail "Broker não subiu. Log em $BROKER_LOG"; }
  echo "✅ Broker em $BROKER"
}

# Encerra um processo iniciado por este script, conferindo que o PID ainda é
# dele (PID reaproveitado pelo sistema não pode ser morto por engano).
encerra_pid() { # arquivo_pid padrão rótulo
  local arquivo="$1" padrao="$2" rotulo="$3" pid
  [[ -f "$arquivo" ]] || return 0
  pid="$(cat "$arquivo")"
  if ps -p "$pid" -o command= 2>/dev/null | grep -q "$padrao"; then
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 20); do ps -p "$pid" >/dev/null 2>&1 || break; sleep 0.5; done
    echo "▶ $rotulo encerrado."
  else
    echo "▶ PID antigo do $rotulo ignorado ($pid)."
  fi
  rm -f "$arquivo"
}

subir() {
  [[ -x "$KAFKA_BIN/connect-distributed" && -x "$KAFKA_BIN/kafka-server-start" ]] ||
    fail "Kafka ausente em $KAFKA_BIN. Instale com 'brew install kafka' (ou aponte KAFKA_BIN). Sem Kafka a demo web funciona; só a coluna 2 do módulo 07 fica 'não configurado'."
  mkdir -p "$PLUGIN_DIR" "$RUN_DIR"

  local jar="$PLUGIN_DIR/mongo-kafka-connect-$MONGO_CONNECTOR_VERSION-all.jar"
  if [[ ! -f "$jar" ]]; then
    echo "▶ Baixando mongodb-kafka-connect $MONGO_CONNECTOR_VERSION (só na primeira vez)..."
    local parcial="$jar.part"
    rm -f "$parcial"
    curl -fsSL --retry 3 -o "$parcial" \
      "https://repo1.maven.org/maven2/org/mongodb/kafka/mongo-kafka-connect/$MONGO_CONNECTOR_VERSION/mongo-kafka-connect-$MONGO_CONNECTOR_VERSION-all.jar" \
      || { rm -f "$parcial"; fail "Falha ao baixar o plugin."; }
    if command -v jar >/dev/null && ! jar tf "$parcial" >/dev/null 2>&1; then
      rm -f "$parcial"
      fail "O arquivo baixado não é um JAR válido."
    fi
    mv "$parcial" "$jar"
  fi

  if porta_ativa "$BROKER_PORT"; then
    echo "✅ Broker já escutando em $BROKER (não iniciado por este script)"
  else
    sobe_broker
  fi

  if porta_ativa "$CONNECT_PORT"; then
    echo "✅ Kafka Connect já está em :$CONNECT_PORT"
    return
  fi

  # Connect distribuído de um nó: replicação 1 nos tópicos internos.
  cat > "$RUN_DIR/connect-distributed.properties" <<PROPS
bootstrap.servers=$BROKER
group.id=showcase-connect
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
offset.storage.topic=_connect-offsets
offset.storage.replication.factor=1
config.storage.topic=_connect-configs
config.storage.replication.factor=1
status.storage.topic=_connect-status
status.storage.replication.factor=1
offset.flush.interval.ms=10000
listeners=HTTP://:$CONNECT_PORT
plugin.path=$PLUGIN_DIR
PROPS

  echo "▶ Subindo o Kafka Connect em :$CONNECT_PORT ..."
  nohup "$KAFKA_BIN/connect-distributed" \
    "$RUN_DIR/connect-distributed.properties" > "$CONNECT_LOG" 2>&1 &
  echo $! > "$RUN_DIR/connect.pid"

  for _ in $(seq 1 60); do
    curl -fsS "http://localhost:$CONNECT_PORT/connectors" >/dev/null 2>&1 && break
    sleep 2
  done
  curl -fsS "http://localhost:$CONNECT_PORT/connectors" >/dev/null 2>&1 \
    || { tail -20 "$CONNECT_LOG" >&2; fail "Connect não respondeu. Log em $CONNECT_LOG"; }

  curl -fsS "http://localhost:$CONNECT_PORT/connector-plugins" | grep -q MongoSourceConnector \
    || fail "Plugin do MongoDB não carregou. Confira $PLUGIN_DIR"

  echo "✅ Kafka Connect pronto com o plugin do MongoDB."
  echo "   Agora rode: ./scripts/setup-kafka-connector.sh"
}

estado() {
  porta_ativa "$BROKER_PORT" && echo "broker  : UP ($BROKER)" || echo "broker  : DOWN"
  if porta_ativa "$CONNECT_PORT"; then
    echo "connect : UP (http://localhost:$CONNECT_PORT)"
    curl -fsS "http://localhost:$CONNECT_PORT/connectors" 2>/dev/null | sed 's/^/  connectors: /'
    echo ""
  else
    echo "connect : DOWN"
  fi
}

derrubar() {
  echo "▶ Removendo connector e dados Kafka da PoV..."
  if porta_ativa "$CONNECT_PORT"; then
    # O corpo é lido para uma variável antes de ser interpretado: num teardown o
    # Connect pode estar encerrando e devolver algo que não é a lista JSON
    # esperada. Antes isso virava um traceback do json.load no meio da saída do
    # `down` — feio e, pior, indistinguível de uma falha real de limpeza. Agora
    # avisa em uma linha e mostra o começo do corpo, que é o que se precisa para
    # diagnosticar da próxima vez.
    local corpo
    corpo="$(curl -fsS --max-time 5 "http://localhost:$CONNECT_PORT/connectors" 2>/dev/null || true)"
    CONNECT_BODY="$corpo" python3 -c 'import json, os, sys
corpo = os.environ.get("CONNECT_BODY", "").strip()
if not corpo:
    sys.exit(0)
try:
    nomes = json.loads(corpo)
    if not isinstance(nomes, list):
        raise ValueError(f"esperava uma lista, veio {type(nomes).__name__}")
except Exception as exc:
    print(f"   (lista de connectors ilegível: {exc}; corpo: {corpo[:200]!r})", file=sys.stderr)
    sys.exit(0)
base = sys.argv[1]
for name in nomes:
    if name == base or (isinstance(name, str) and name.startswith(base + "-")):
        print(name)' "$CONNECTOR_NAME" |
      while IFS= read -r connector; do
        # Apagar o connector não apaga o offset dele; zerar aqui evita que o
        # próximo `up` retome de um resume token que já saiu do oplog.
        curl -fsS -X PUT "http://localhost:$CONNECT_PORT/connectors/$connector/stop" >/dev/null 2>&1 || true
        curl -fsS -X DELETE "http://localhost:$CONNECT_PORT/connectors/$connector/offsets" >/dev/null 2>&1 || true
        curl -fsS -X DELETE "http://localhost:$CONNECT_PORT/connectors/$connector" >/dev/null 2>&1 || true
      done || true
  fi
  if porta_ativa "$BROKER_PORT"; then
    "$KAFKA_BIN"/kafka-topics --bootstrap-server "$BROKER" \
      --delete --if-exists --topic "$TOPIC" >/dev/null 2>&1 || true
    "$KAFKA_BIN"/kafka-topics --bootstrap-server "$BROKER" \
      --delete --if-exists --topic "__mongodb_heartbeats" >/dev/null 2>&1 || true
    "$KAFKA_BIN"/kafka-consumer-groups --bootstrap-server "$BROKER" \
      --delete --group "$CONSUMER_GROUP" >/dev/null 2>&1 || true
  fi
  encerra_pid "$RUN_DIR/connect.pid" "connect-distributed" "Kafka Connect"
  encerra_pid "$RUN_DIR/broker.pid" "$BROKER_CFG" "broker Kafka"
  echo "✅ Kafka local encerrado."
}

case "${1:-up}" in
  up) subir ;;
  status) estado ;;
  down) derrubar ;;
  *) fail "Uso: $0 [up|status|down]" ;;
esac
