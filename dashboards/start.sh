#!/bin/sh
# Container entrypoint for Cloud Run: the cities API and Streamlit in one
# container. Flask listens on loopback only, so the API is never public;
# Streamlit takes the port Cloud Run assigns.
set -e

# `flask run` rather than `python citiesapi.py`: no debug reloader in prod.
flask --app citiesapi run --host 127.0.0.1 --port 5001 &

# Wait for the API to accept connections so the first page load does not hit
# a refused connection.
python - <<'PY'
import socket, time
for _ in range(60):
    try:
        socket.create_connection(("127.0.0.1", 5001), timeout=1).close()
        break
    except OSError:
        time.sleep(0.5)
PY

exec streamlit run dashboard.py \
  --server.port "${PORT:-8080}" \
  --server.address 0.0.0.0 \
  --server.headless true \
  --browser.gatherUsageStats false
