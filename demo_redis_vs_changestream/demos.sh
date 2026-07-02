#!/bin/bash
# Roda as TRÊS demonstrações em sequência, com um único comando.
# Uso: ./demos.sh            (roda tudo)
#      ./demos.sh 1|2|3      (roda uma demo específica)
set -e
BASE="$(cd "$(dirname "$0")" && pwd)"
PY="$BASE/venv/bin/python"

if [ ! -x "$PY" ]; then
  echo "venv não encontrado. Rode primeiro:"
  echo "  python3 -m venv venv && ./venv/bin/pip install -r requirements.txt"
  exit 1
fi

cd "$BASE"
case "${1:-all}" in
  1) "$PY" demo_1_latencia.py ;;
  2) "$PY" demo_2_resiliencia.py ;;
  3) "$PY" demo_3_dualwrite.py ;;
  all)
     "$PY" demo_1_latencia.py
     "$PY" demo_2_resiliencia.py
     "$PY" demo_3_dualwrite.py
     ;;
  *) echo "uso: ./demos.sh [1|2|3|all]"; exit 1 ;;
esac
