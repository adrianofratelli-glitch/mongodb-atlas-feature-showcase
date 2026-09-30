#!/usr/bin/env bash
# Preparação antecipada e mutável. Execute fora da janela da apresentação.
# O `overview` apenas verifica o resultado; não cria índices nem toca no cluster.
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "▶ Materializando dataset e índices MongoDB do módulo Geo..."
"$BASE/backend/venv/bin/python" "$BASE/scripts/seed_geo.py" --ensure

echo "▶ Materializando e aguardando o índice Atlas Search..."
"$BASE/scripts/create_search_index_geo.sh"

echo "▶ Validando artefatos da apresentação..."
"$BASE/backend/venv/bin/python" "$BASE/scripts/seed_geo.py" --check

echo "✅ Demo pré-materializada. O overview fará somente este preflight rápido."
