# Dagster webserver / daemon image (§8, stretch).
# Deliberately minimal: this image runs the UI and the scheduler, not the
# pipeline. Heavy compute lives in BigQuery, and the project's own dependencies
# (dbt, dlt, pandas) are deliberately absent — see deploy/README.md.

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home

WORKDIR /opt/dagster/app

# Pinned to the versions in uv.lock. Bump both together.
RUN pip install \
      "dagster==1.13.20" \
      "dagster-webserver==1.13.20"

RUN mkdir -p ${DAGSTER_HOME}

# Fallback config for running this image without a mount. Compose bind-mounts a
# host DAGSTER_HOME over this directory, which shadows the copy — that is
# intended: the point is to read the history the host already wrote. There are
# no credentials in this file and none are baked into the image.
COPY orchestration/dagster.yaml ${DAGSTER_HOME}/dagster.yaml

EXPOSE 3000
