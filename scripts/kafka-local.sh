#!/usr/bin/env bash
#
# COLUNA 2 do módulo Streaming SEM Docker — Kafka nativo via Homebrew.
#
# Alternativa ao docker-compose.streaming.yml para máquinas sem Docker (ou com
# Wi-Fi ruim no auditório): usa o broker do Homebrew em modo KRaft e sobe um
# Kafka Connect distribuído com o plugin mongodb-kafka-connect.
#
#   ./scripts/kafka-local.sh up      # broker + connect + plugin
#   ./scripts/kafka-local.sh status
#   ./scripts/kafka-local.sh down
#
# Depois: ./scripts/setup-kafka-connector.sh  (registra o source connector)
#
set -euo pipefail

PLUGIN_DIR="${KAFKA_PLUGIN_DIR:-$HOME/.local/share/mdb-showcase-kafka/plugins}"
RUN_DIR="${KAFKA_RUN_DIR:-$HOME/.local/share/mdb-showcase-kafka/run}"
MONGO_CONNECTOR_VERSION="${MONGO_CONNECTOR_VERSION:-1.15.0}"
BROKER="${KAFKA_BROKERS:-localhost:9092}"
CONNECT_PORT="${CONNECT_PORT:-8083}"
CONNECT_LOG="$RUN_DIR/connect.log"

fail() { echo "❌ $1" >&2; exit 1; }
porta_ativa() { lsof -ti:"$1" >/dev/null 2>&1; }

subir() {
  command -v brew >/dev/null || fail "Homebrew não encontrado."
  [[ -x /opt/homebrew/opt/kafka/bin/connect-distributed ]] || fail "Kafka ausente. Rode: brew install kafka"
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

  if ! porta_ativa 9092; then
    echo "▶ Subindo o broker Kafka (Homebrew, KRaft)..."
    brew services start kafka >/dev/null
    for _ in $(seq 1 30); do porta_ativa 9092 && break; sleep 2; done
    porta_ativa 9092 || fail "Broker não subiu. Veja: brew services info kafka"
  fi
  echo "✅ Broker em $BROKER"

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
  nohup /opt/homebrew/opt/kafka/bin/connect-distributed \
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
  porta_ativa 9092 && echo "broker  : UP ($BROKER)" || echo "broker  : DOWN"
  if porta_ativa "$CONNECT_PORT"; then
    echo "connect : UP (http://localhost:$CONNECT_PORT)"
    curl -fsS "http://localhost:$CONNECT_PORT/connectors" 2>/dev/null | sed 's/^/  connectors: /'
    echo ""
  else
    echo "connect : DOWN"
  fi
}

derrubar() {
  if [[ -f "$RUN_DIR/connect.pid" ]]; then
    local pid
    pid="$(cat "$RUN_DIR/connect.pid")"
    if ps -p "$pid" -o command= 2>/dev/null | grep -q "connect-distributed"; then
      kill "$pid" 2>/dev/null || true
    else
      echo "▶ PID antigo do Connect ignorado ($pid)."
    fi
    rm -f "$RUN_DIR/connect.pid"
    echo "▶ Kafka Connect encerrado."
  fi
  brew services stop kafka >/dev/null 2>&1 || true
  echo "✅ Kafka local encerrado."
}

case "${1:-up}" in
  up) subir ;;
  status) estado ;;
  down) derrubar ;;
  *) fail "Uso: $0 [up|status|down]" ;;
esac
