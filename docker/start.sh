#!/bin/sh
set -e

cd /app/backend
# One worker only: the change-stream fan-out, the
# streaming run state and the preflight cache are in-process.
uvicorn main:app --host 127.0.0.1 --port 8000 --workers 1 &
nginx -g 'daemon off;'
