#!/usr/bin/env bash
# Preparação antecipada e mutável. Execute fora da janela da apresentação.
# O `overview` apenas verifica o resultado (reset_demo.py --check); não cria
# índices nem toca no cluster.
#
# Escreve no banco configurado em backend/.env. Se ele não terminar em `_test`,
# o reset recusa sem ALLOW_DEMO_DB_WRITE=1 — por exemplo:
#   ALLOW_DEMO_DB_WRITE=1 ./scripts/prepare-demo.sh
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "▶ Reset idempotente: dados, índices B-tree e coleções da rodada..."
"$BASE/backend/venv/bin/python" "$BASE/scripts/reset_demo.py"

echo "▶ Validando artefatos da apresentação..."
"$BASE/backend/venv/bin/python" "$BASE/scripts/reset_demo.py" --check

echo "✅ Demo pré-materializada. O overview fará somente este preflight rápido."
