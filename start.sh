#!/bin/bash
BASE="$(cd "$(dirname "$0")" && pwd)"

echo "🍃 MongoDB Atlas Feature Showcase"
echo "=================================="

# Backend
echo "▶ Iniciando backend (porta 8001)..."
cd "$BASE/backend"
venv/bin/uvicorn main:app --host 0.0.0.0 --port 8001 --reload > /tmp/mongodb-showcase-backend.log 2>&1 &
BACKEND_PID=$!
echo "  PID: $BACKEND_PID"

# Aguarda backend subir
sleep 2

# Frontend
echo "▶ Iniciando frontend (porta 5173)..."
cd "$BASE/frontend"
npm run dev > /tmp/mongodb-showcase-frontend.log 2>&1 &
FRONTEND_PID=$!
echo "  PID: $FRONTEND_PID"

sleep 3
echo ""
echo "✅ POC rodando!"
echo "   Frontend: http://localhost:5173"
echo "   API:      http://localhost:8001"
echo ""
echo "Para parar: kill $BACKEND_PID $FRONTEND_PID"
