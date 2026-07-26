#!/usr/bin/env bash
#
# Liga/desliga TUDO que a PoV consome, num comando só.
#
#   ./scripts/ambiente.sh up      # cluster + processor do ASP + Kafka local
#   ./scripts/ambiente.sh down    # deixa o ambiente sem custo de compute
#   ./scripts/ambiente.sh status
#
# O que custa e o que não custa quando está "down":
#   • Cluster Atlas pausado  — não cobra compute (armazenamento continua).
#   • Processor do ASP parado — o Atlas cobra POR PROCESSOR RODANDO, por segundo;
#     parado não cobra, e a Stream Processing Instance em si não é cobrada.
#     Por isso a SPI pode ficar de pé: o que liga/desliga é o processor.
#   • Kafka é local (Homebrew) — só consome a sua máquina.
#
set -euo pipefail

BASE="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$BASE/backend/.env"
[[ -f "$ENV_FILE" ]] || { echo "❌ backend/.env não encontrado." >&2; exit 1; }

# `|| true`: variável ausente no .env não pode derrubar o script sob `set -e`.
le_env() { { grep -E "^$1=" "$ENV_FILE" || true; } | head -n1 | cut -d= -f2- | sed -e 's/^["'\'']//' -e 's/["'\'']$//' -e 's/\r$//'; }

PUB="$(le_env ATLAS_PUBLIC_KEY)"
PRIV="$(le_env ATLAS_PRIVATE_KEY)"
PROJ="$(le_env ATLAS_PROJECT_ID)"
CLUSTER="$(le_env ATLAS_CLUSTER)"
PROCESSOR="${ASP_PROCESSOR_NAME:-$(le_env ASP_PROCESSOR_NAME)}"
PROCESSOR="${PROCESSOR:-pixJanelas10s}"
ASP_URI="$(le_env ASP_CONNECTION_STRING)"
ASP_CONNECTION="${ASP_CONNECTION_NAME:-$(le_env ASP_CONNECTION_NAME)}"
ASP_CONNECTION="${ASP_CONNECTION:-atlasCluster}"
STREAM_DB="${STREAMING_DB:-$(le_env STREAMING_DB)}"
STREAM_DB="${STREAM_DB:-pix}"
API="https://cloud.mongodb.com/api/atlas/v2"
ATLAS_MEDIA_TYPE="application/vnd.atlas.2025-03-12+json"
ACCEPT="Accept: $ATLAS_MEDIA_TYPE"

for required in PUB PRIV PROJ CLUSTER; do
  [[ -n "${!required}" ]] || { echo "❌ $required ausente em backend/.env." >&2; exit 1; }
done

atlas() { # método, caminho, [body]
  local m="$1" p="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -fsS --retry 2 --digest -u "$PUB:$PRIV" -X "$m" -H "$ACCEPT" \
      -H "Content-Type: $ATLAS_MEDIA_TYPE" --data "$body" "$API$p"
  else
    curl -fsS --retry 2 --digest -u "$PUB:$PRIV" -X "$m" -H "$ACCEPT" "$API$p"
  fi
}

estado_cluster() {
  atlas GET "/groups/$PROJ/clusters/$CLUSTER" |
    python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('stateName','?'), '| paused:', d.get('paused'), '|', [rc['electableSpecs']['instanceSize'] for s in d.get('replicationSpecs',[]) for rc in s['regionConfigs']])"
}

pausa_cluster() { # true|false
  atlas PATCH "/groups/$PROJ/clusters/$CLUSTER" "{\"paused\": $1}" >/dev/null
  echo "   cluster paused=$1 solicitado"
}

processor() { # stop|start
  local cmd="$1"
  "$BASE/backend/venv/bin/python" - "$cmd" "$PROCESSOR" "$ENV_FILE" <<'PY'
import sys
import time
acao, nome, env_file = sys.argv[1], sys.argv[2], sys.argv[3]
uri = ""
for linha in open(env_file):
    if linha.startswith("ASP_CONNECTION_STRING="):
        uri = linha.split("=", 1)[1].strip().strip("\"'")
if not uri:
    print("   ASP_CONNECTION_STRING ausente — nada a fazer no ASP")
    sys.exit(0)
from pymongo import MongoClient
c = MongoClient(uri, serverSelectionTimeoutMS=8000)
try:
    r = c.admin.command({"listStreamProcessors": 1})
    processors = r.get("streamProcessors", [])
    target = next((p for p in processors if p.get("name") == nome), None)
    if acao == "status":
        if not target:
            print(f"   processor {nome}: não encontrado")
        else:
            tier = target.get("effectiveTier") or target.get("tier") or "?"
            print(f"   processor {nome}: {target.get('state')} (tier {tier})")
    elif not target:
        print(f"   processor {nome}: não encontrado", file=sys.stderr)
        sys.exit(1)
    else:
        esperado = "STOPPED" if acao == "stop" else "STARTED"
        if target.get("state") == esperado:
            print(f"   processor {nome}: já estava {esperado}")
        else:
            cmd = "stopStreamProcessor" if acao == "stop" else "startStreamProcessor"
            c.admin.command({cmd: nome})
            for _ in range(120):
                atual = c.admin.command({"listStreamProcessors": 1})
                state = next(
                    (p.get("state") for p in atual.get("streamProcessors", [])
                     if p.get("name") == nome),
                    None,
                )
                if state == esperado:
                    print(f"   processor {nome}: {esperado}")
                    break
                time.sleep(0.5)
            else:
                print(f"   processor {nome} não chegou a {esperado}", file=sys.stderr)
                sys.exit(1)
finally:
    c.close()
PY
}

limpa_dados_pix() {
  "$BASE/backend/venv/bin/python" "$BASE/scripts/cleanup-streaming-data.py"
}

recria_processor() {
  if [[ -z "$ASP_URI" ]]; then
    echo "   ASP_CONNECTION_STRING ausente — processor não configurado"
    return 0
  fi
  command -v mongosh >/dev/null || {
    echo "❌ mongosh é necessário para criar o processor automaticamente." >&2
    return 1
  }
  echo "▶ Criando uma execução limpa do processor ASP..."
  ASP_RECREATE=true \
    ASP_CONNECTION_NAME="$ASP_CONNECTION" \
    ASP_PROCESSOR_NAME="$PROCESSOR" \
    STREAMING_DB="$STREAM_DB" \
    mongosh "$ASP_URI" --quiet --file "$BASE/scripts/setup-asp.js"
}

case "${1:-status}" in
  up)
    echo "▶ Ligando o ambiente..."
    pausa_cluster false
    echo "   aguardando o cluster ficar IDLE (pode levar alguns minutos)..."
    pronto=0
    for _ in $(seq 1 90); do
      if atlas GET "/groups/$PROJ/clusters/$CLUSTER" | grep -q '"stateName":"IDLE"'; then
        pronto=1
        break
      fi
      sleep 10
    done
    [[ "$pronto" == "1" ]] || { echo "❌ Cluster não chegou a IDLE no prazo." >&2; exit 1; }
    estado_cluster
    # Um ciclo de apresentação começa sem documentos, offsets de aplicação ou
    # estado de janela da rodada anterior.
    processor stop || true
    limpa_dados_pix
    recria_processor
    if "$BASE/scripts/kafka-local.sh" up; then
      "$BASE/scripts/setup-kafka-connector.sh" ||
        echo "   (connector falhou — a coluna Kafka mostra o diagnóstico)"
    else
      echo "   (Kafka opcional falhou — a coluna 2 renderiza 'não configurado')"
    fi
    echo "✅ Ambiente pronto."
    ;;
  down)
    echo "▶ Desligando o ambiente..."
    falhou=0
    processor stop || echo "   processor ausente ou já indisponível — seguindo com a limpeza"
    limpa_dados_pix || {
      falhou=1
      echo "❌ A limpeza direta dos dados PIX falhou." >&2
    }
    "$BASE/scripts/kafka-local.sh" down || falhou=1
    pausa_cluster true || falhou=1
    if [[ "$falhou" == "0" ]]; then
      echo "✅ Sem custo de compute: cluster pausado e processor parado."
      echo "   (armazenamento do cluster continua sendo cobrado)"
    else
      echo "⚠️ O desligamento foi solicitado, mas houve falha parcial; confira o status." >&2
      exit 1
    fi
    ;;
  status)
    echo "cluster : $(estado_cluster)"
    processor status
    "$BASE/scripts/kafka-local.sh" status
    ;;
  *)
    echo "Uso: $0 [up|down|status]" >&2; exit 1 ;;
esac
