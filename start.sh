#!/bin/bash
set -u

BASE="$(cd "$(dirname "$0")" && pwd)"
BACKEND_LOG="${TMPDIR:-/tmp}/mongodb-showcase-backend.log"
FRONTEND_LOG="${TMPDIR:-/tmp}/mongodb-showcase-frontend.log"

fail() {
  echo "❌ $1" >&2
  exit 1
}

cleanup() {
  kill "${BACKEND_PID:-}" "${FRONTEND_PID:-}" 2>/dev/null || true
}

wait_for_url() {
  local url="$1"
  local attempts="${2:-30}"
  for ((i = 1; i <= attempts; i++)); do
    if curl --fail --silent --max-time 2 "$url" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

command -v curl >/dev/null || fail "curl não encontrado."
command -v npm >/dev/null || fail "npm não encontrado."
[[ -x "$BASE/backend/venv/bin/uvicorn" ]] || fail "Virtualenv ausente. Siga a seção Backend do README."
[[ -d "$BASE/frontend/node_modules" ]] || fail "Dependências frontend ausentes. Execute npm install em frontend/."

echo "🍃 MongoDB Atlas Feature Showcase"
echo "=================================="

# Backend
echo "▶ Iniciando backend (porta 8002)..."
cd "$BASE/backend"
venv/bin/uvicorn main:app --host 127.0.0.1 --port 8002 --reload > "$BACKEND_LOG" 2>&1 &
BACKEND_PID=$!
echo "  PID: $BACKEND_PID"

if ! wait_for_url "http://127.0.0.1:8002/preflight" 30; then
  echo "❌ Backend não ficou pronto. Últimas linhas do log:" >&2
  tail -n 25 "$BACKEND_LOG" >&2
  cleanup
  exit 1
fi

# Frontend
echo "▶ Iniciando frontend (porta 5174)..."
cd "$BASE/frontend"
npm run dev -- --host 127.0.0.1 > "$FRONTEND_LOG" 2>&1 &
FRONTEND_PID=$!
echo "  PID: $FRONTEND_PID"

if ! wait_for_url "http://127.0.0.1:5174" 30; then
  echo "❌ Frontend não ficou pronto. Últimas linhas do log:" >&2
  tail -n 25 "$FRONTEND_LOG" >&2
  cleanup
  exit 1
fi
echo ""
echo "✅ POC rodando!"
echo "   Frontend: http://localhost:5174"
echo "   API:      http://localhost:8002"
echo ""
echo "Para parar: kill $BACKEND_PID $FRONTEND_PID"
echo "Preflight:  http://localhost:8002/preflight"

if [[ "${1:-}" == "--foreground" ]]; then
  trap cleanup INT TERM EXIT
  echo "Modo foreground ativo. Pressione Ctrl+C para encerrar."
  wait "$BACKEND_PID" "$FRONTEND_PID"
fi
