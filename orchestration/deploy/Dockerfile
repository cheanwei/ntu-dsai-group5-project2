# syntax=docker/dockerfile:1

# The image that *runs* the pipeline (§8).
#
# It carries the project's own dependencies — dlt, dbt, Great Expectations,
# pandas — and the project's code, so `orchestration.definitions` loads as a
# real code location and a run launched from the UI or by the daemon does the
# same work `dagster dev` and the CI workflow do. That is the whole difference
# from a console image: an image without dbt/dlt can show history, but it
# cannot materialise an asset.
#
# Two stages. The builder holds uv and its cache; the runtime gets the
# resolved virtualenv and nothing else, so the build machinery is not shipped.

FROM python:3.11-slim AS builder

# Pinned deliberately: an unpinned :latest here would make the resolver — the
# one component that decides every version in the image — the one component
# that is not itself versioned.
COPY --from=ghcr.io/astral-sh/uv:0.8.22 /uv /usr/local/bin/uv

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON=/usr/local/bin/python3.11

WORKDIR /opt/dagster/app

# Only the lockfile inputs, so this layer — the expensive one — is rebuilt when
# dependencies change and not when a model or an asset does.
COPY pyproject.toml uv.lock README.md ./

# --frozen: install exactly uv.lock, never re-resolve. The reproducibility
# claim in §8 rests on the lockfile, so an image that quietly resolved
# something newer would be the one place that claim stops holding.
# --no-dev: ruff, pytest and nbstripout are for a laptop, not this image.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev


FROM python:3.11-slim

# PYTHONPATH: pyproject.toml sets `package = false`, so nothing is installed
# into site-packages and `import orchestration` resolves the same way it does
# for pytest (`pythonpath = ["."]`) and for run_all.py (a sys.path insert) —
# off the repo root, which here is the working directory.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    DAGSTER_HOME=/opt/dagster/dagster_home \
    DBT_PROFILES_DIR=/opt/dagster/app/transform \
    PYTHONPATH=/opt/dagster/app

COPY --from=builder /opt/venv /opt/venv

WORKDIR /opt/dagster/app

# The three packages a run touches, and nothing else: no notebooks, no docs, no
# dashboards. .dockerignore drops the generated state inside them.
#
# There is no quality/ to copy: §7's assertions are dbt tests from
# dbt_expectations, so they ship inside transform/ (see orchestration/assets.py,
# "Layer 4"). The Great Expectations tree this used to copy was removed in
# 840b7da.
COPY ingestion ./ingestion
COPY transform ./transform
COPY orchestration ./orchestration
COPY pyproject.toml ./

# dbt_utils, resolved at build time so a run never waits on the package index —
# and so a network failure surfaces here rather than mid-materialisation.
# package-lock.yml pins the version; this is `dbt deps`, not a re-resolve.
RUN dbt deps --project-dir transform

# Where the Kaggle download lands before it is uploaded (resources.py,
# STAGING_DIR). Compose puts a named volume here so the 126 MB survives a
# container restart.
RUN mkdir -p /opt/dagster/app/data/staging ${DAGSTER_HOME}

# Fallback instance config for running this image without a mount. Compose
# bind-mounts a host DAGSTER_HOME over this directory, which shadows the copy;
# that is intended, and is what makes the UI show the history `dagster dev`
# already wrote. There are no credentials in this file and none in the image:
# the service-account key is bind-mounted at run time, never copied.
COPY orchestration/dagster.yaml ${DAGSTER_HOME}/dagster.yaml

EXPOSE 3000
