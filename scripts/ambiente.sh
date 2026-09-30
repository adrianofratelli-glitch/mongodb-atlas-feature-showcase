#!/usr/bin/env bash
#
# Liga/desliga somente os processos da PoV. O cluster é responsabilidade do operador.
#
#   ./scripts/ambiente.sh up      # preflight + processor do ASP/Kafka quando ao vivo
#   ./scripts/ambiente.sh down    # para ASP/Kafka e limpa os dados PIX da rodada
#   ./scripts/ambiente.sh status
#
# O processor do ASP cobra por segundo enquanto estiver rodando;
#     parado não cobra, e a Stream Processing Instance em si não é cobrada.
# Kafka é local (Homebrew). Este script nunca pausa, retoma ou redimensiona Atlas.
#
set -euo pipefail

BASE="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$BASE/backend/.env"
[[ -f "$ENV_FILE" ]] || { echo "❌ backend/.env não encontrado." >&2; exit 1; }

# `|| true`: variável ausente no .env não pode derrubar o script sob `set -e`.
le_env() { { grep -E "^$1=" "$ENV_FILE" || true; } | head -n1 | cut -d= -f2- | sed -e 's/^["'\'']//' -e 's/["'\'']$//' -e 's/\r$//'; }

PROCESSOR="${ASP_PROCESSOR_NAME:-$(le_env ASP_PROCESSOR_NAME)}"
PROCESSOR="${PROCESSOR:-pixJanelas5s}"
# Segundo processor: sinal de risco geográfico em event time, sink em
# geo.sinais_ao_vivo. Um pipeline implantado tem um sink só, por isso são dois.
PROCESSOR_GEO="${ASP_GEO_PROCESSOR_NAME:-$(le_env ASP_GEO_PROCESSOR_NAME)}"
PROCESSOR_GEO="${PROCESSOR_GEO:-geoSinais30s}"
GEO_DB_NOME="${GEO_DB:-$(le_env GEO_DB)}"
GEO_DB_NOME="${GEO_DB_NOME:-geo}"
ASP_URI="$(le_env ASP_CONNECTION_STRING)"
ASP_CONNECTION="${ASP_CONNECTION_NAME:-$(le_env ASP_CONNECTION_NAME)}"
ASP_CONNECTION="${ASP_CONNECTION:-atlasCluster}"
ASP_TIER="${ASP_TIER:-$(le_env ASP_TIER)}"
ASP_TIER="${ASP_TIER:-SP10}"
STREAM_DB="${STREAMING_DB:-$(le_env STREAMING_DB)}"
STREAM_DB="${STREAM_DB:-pix}"
# A aba de Streaming usa ASP e Kafka reais no modo principal. STREAMING_AO_VIVO
# vem ligado pelo launcher e pode ser desligado com `overview --replay`.
AO_VIVO="${STREAMING_AO_VIVO:-$(le_env STREAMING_AO_VIVO)}"
AO_VIVO="${AO_VIVO:-0}"
[[ "$AO_VIVO" == "1" || "$AO_VIVO" == "true" ]] && AO_VIVO=1 || AO_VIVO=0

processor() { # stop|start|status [nome]
  local cmd="$1"
  local nome="${2:-$PROCESSOR}"
  "$BASE/backend/venv/bin/python" - "$cmd" "$nome" "$ENV_FILE" <<'PY'
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

verifica_demo() {
  echo "▶ Verificando artefatos pré-materializados da demo..."
  "$BASE/backend/venv/bin/python" "$BASE/scripts/seed_geo.py" --check
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
    ASP_TIER="$ASP_TIER" \
    STREAMING_DB="$STREAM_DB" \
    mongosh "$ASP_URI" --quiet --file "$BASE/scripts/setup-asp.js"

  echo "▶ Criando o processor de sinal geográfico em event time..."
  ASP_RECREATE=true \
    ASP_CONNECTION_NAME="$ASP_CONNECTION" \
    ASP_GEO_PROCESSOR_NAME="$PROCESSOR_GEO" \
    ASP_TIER="$ASP_TIER" \
    STREAMING_DB="$STREAM_DB" \
    GEO_DB="$GEO_DB_NOME" \
    mongosh "$ASP_URI" --quiet --file "$BASE/scripts/setup-asp-geo.js"
}

case "${1:-status}" in
  up)
    echo "▶ Preparando processos da PoV (o estado do cluster não será alterado)..."
    verifica_demo
    # Um ciclo de apresentação começa sem documentos, offsets de aplicação ou
    # estado de janela da rodada anterior. O processor é parado mesmo no modo
    # padrão: se sobrou ligado de uma gravação anterior, ele estaria cobrando.
    processor stop || true
    processor stop "$PROCESSOR_GEO" || true
    limpa_dados_pix

    if [[ "$AO_VIVO" == "1" ]]; then
      echo "▶ STREAMING_AO_VIVO=1 — montando ASP e Kafka para a execução ao vivo."
      recria_processor
      if "$BASE/scripts/kafka-local.sh" up; then
        "$BASE/scripts/setup-kafka-connector.sh" ||
          echo "   (connector falhou — a coluna Kafka mostra o diagnóstico)"
      else
        echo "   (Kafka opcional falhou — a coluna 2 renderiza 'não configurado')"
      fi
    else
      echo "   ASP e Kafka não subiram: modo replay selecionado."
    fi
    echo "✅ Ambiente pronto."
    ;;
  down)
    echo "▶ Desligando o ambiente..."
    falhou=0
    processor stop || echo "   processor ausente ou já indisponível — seguindo com a limpeza"
    processor stop "$PROCESSOR_GEO" || echo "   processor de geo ausente — seguindo"
    limpa_dados_pix || {
      falhou=1
      echo "❌ A limpeza direta dos dados PIX falhou." >&2
    }
    "$BASE/scripts/kafka-local.sh" down || falhou=1
    if [[ "$falhou" == "0" ]]; then
      echo "✅ Processos locais encerrados, processor parado e dados PIX removidos."
      echo "   cluster Atlas não foi alterado"
    else
      echo "⚠️ Desligamento parcial; confira o status." >&2
      exit 1
    fi
    ;;
  status)
    echo "cluster : não gerenciado pelo overview"
    verifica_demo || true
    processor status
    processor status "$PROCESSOR_GEO"
    "$BASE/scripts/kafka-local.sh" status
    ;;
  *)
    echo "Uso: $0 [up|down|status]" >&2; exit 1 ;;
esac
