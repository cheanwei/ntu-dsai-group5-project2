# syntax=docker/dockerfile:1

# The dashboard image for Cloud Run: Streamlit plus the Flask cities API, both
# started by start.sh. Build context is dashboards/ (`gcloud run deploy
# --source dashboards`). No credentials go in the image: on Cloud Run the
# service account supplies them through the metadata server.

FROM python:3.12-slim

# Pinned for the same reason as orchestration/deploy/Dockerfile.
COPY --from=ghcr.io/astral-sh/uv:0.8.22 /uv /usr/local/bin/uv

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt ./
RUN uv pip install --system --no-cache -r requirements.txt

COPY . .
RUN chmod +x start.sh

EXPOSE 8080
CMD ["./start.sh"]
