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
            print(f"   processor {nome}: {acao} solicitado")
finally:
    c.close()
PY
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
    processor start
    "$BASE/scripts/kafka-local.sh" up || echo "   (Kafka opcional falhou — a coluna 2 renderiza 'não configurado')"
    echo "✅ Ambiente pronto."
    ;;
  down)
    echo "▶ Desligando o ambiente..."
    processor stop
    "$BASE/scripts/kafka-local.sh" down || true
    pausa_cluster true
    echo "✅ Sem custo de compute: cluster pausado e processor parado."
    echo "   (armazenamento do cluster continua sendo cobrado)"
    ;;
  status)
    echo "cluster : $(estado_cluster)"
    processor status
    "$BASE/scripts/kafka-local.sh" status
    ;;
  *)
    echo "Uso: $0 [up|down|status]" >&2; exit 1 ;;
esac
