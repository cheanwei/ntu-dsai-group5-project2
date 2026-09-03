#!/usr/bin/env bash
# Provision the data plane: the raw-zone bucket, the two BigQuery datasets,
# and the one IAM binding that lets the service account write to the bucket.
#
#     ./scripts/provision_gcp.sh              # create everything, idempotently
#     ./scripts/provision_gcp.sh --dry-run    # print the calls, change nothing
#     ./scripts/provision_gcp.sh --grant-bigquery   # also grant the SA on BigQuery
#
# Run once per GCP project, by whoever owns it — not once per developer. The
# per-developer step is `scripts/bootstrap_env.py`, which writes .env and needs
# no cloud permissions at all. The two are deliberately separate: this script
# needs project-admin rights that five of six teammates do not have, and a
# setup command that 403s for most of the team is worse than one they never run.
#
# Configuration comes from .env at the repo root, which it reads itself (only
# the keys it needs, and without executing the file). Environment variables
# already set win, so ENV_FILE=other.env or GCP_PROJECT=... override it.
#
# AUTHENTICATION. This needs `gcloud auth login` — user credentials in
# gcloud's own store. It is *not* `gcloud auth application-default login`:
# ADC writes a separate file that only client libraries read, and neither
# `gcloud` nor `bq` looks at it. (Nothing here uses ADC either — the pipeline
# authenticates with the service-account key at GOOGLE_APPLICATION_CREDENTIALS,
# which google-auth resolves ahead of ADC. See ingestion/kaggle_to_gcs.py.)
#
# Idempotent: every step checks for its resource first, so re-running after a
# partial failure resumes rather than errors.
#
# LOCATION IS IMMUTABLE (§14). Everything is US multi-region, and a bucket or
# dataset created elsewhere cannot be moved — only deleted and recreated. So
# where a resource already exists this script verifies its location and stops
# if it disagrees, rather than carrying on to fail later with an error that
# appears to blame something else: a non-US bucket fails the load into a US
# dataset while looking like a bucket permissions problem.
#
# What it creates, in order:
#
#   APIs            storage, bigquery
#   Bucket          gs://$GCP_RAW_BUCKET, US, uniform bucket-level access
#   Datasets        $BIGQUERY_RAW_DATASET, $BIGQUERY_MARTS_DATASET
#   Bucket IAM      roles/storage.objectUser for the service account
#
# It does not create a dbt dataset. dbt-bigquery creates its own target dataset
# when the account holds bigquery.datasets.create, using the `location` in
# transform/profiles.yml — so `dbt build --target dev` needs nothing from here,
# whether it points at a shared project or at the developer's own.

set -euo pipefail

# gcloud prompts on stdin for things this script must decide for itself — most
# sharply, `describe` against a resource whose API is not yet enabled offers to
# enable it and waits. `exists()` below discards stderr, so that prompt is
# invisible *and* blocking. Disabling prompts globally answers them all.
export CLOUDSDK_CORE_DISABLE_PROMPTS=1

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${REPO}/.env}"
ENV_LOADED=""

# --- .env ------------------------------------------------------------------
# Read, never sourced. `source` executes the file, so a Kaggle token containing
# a backtick or $( ) would run as code, and it would export every unrelated
# variable in there besides. This lifts out the six keys the script actually
# uses and ignores the rest.
#
# The pipeline still loads nothing implicitly — that rule is about the modules
# in ingestion/ and orchestration/, where an ambient .env would make a run
# depend on which shell started it. This is a setup script run by hand once,
# and bootstrap_env.py already reads and writes the same file.
#
# Anything already exported wins, so `GCP_PROJECT=other ./scripts/provision_gcp.sh`
# still overrides, and `set -a; source .env` beforehand changes nothing.
load_env() {
  local key value line
  [[ -f "${ENV_FILE}" ]] || return 0
  for key in GCP_PROJECT GCP_LOCATION GCP_RAW_BUCKET BIGQUERY_RAW_DATASET \
             BIGQUERY_MARTS_DATASET GOOGLE_APPLICATION_CREDENTIALS SERVICE_ACCOUNT; do
    [[ -n "${!key:-}" ]] && continue
    line="$(grep -m1 "^${key}=" "${ENV_FILE}" 2>/dev/null || true)"
    [[ -n "${line}" ]] || continue
    value="${line#*=}"
    value="${value%$'\r'}"                      # a .env edited on Windows
    value="${value#[\"\']}"; value="${value%[\"\']}"
    [[ -n "${value}" ]] || continue
    export "${key}=${value}"
    ENV_LOADED=1
  done
}
load_env

# --- Configuration ---------------------------------------------------------
# Defaults match .env.example. Anything already exported wins, so a filled-in
# .env configures this script entirely.
PROJECT_ID="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
LOCATION="${GCP_LOCATION:-US}"
RAW_BUCKET="${GCP_RAW_BUCKET:-}"
RAW_DATASET="${BIGQUERY_RAW_DATASET:-olist_raw}"
MARTS_DATASET="${BIGQUERY_MARTS_DATASET:-olist_marts}"
# The account the pipeline runs as. Derived from the key file below when unset,
# since that file is the one place a developer reliably has it.
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-}"
KEY_FILE="${GOOGLE_APPLICATION_CREDENTIALS:-}"

DRY_RUN=0
GRANT_BIGQUERY=0
while (( $# )); do
  case "$1" in
    --dry-run)        DRY_RUN=1 ;;
    --grant-bigquery) GRANT_BIGQUERY=1 ;;
    # Two POSIX substitutions rather than one `s/^# \?//`: `\?` is a GNU
    # extension, and BSD sed reads it as a literal '?' and strips nothing.
    -h|--help) sed -n '2,46p' "${BASH_SOURCE[0]}" | sed -e 's/^# //' -e 's/^#$//'; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; exit 2 ;;
  esac
  shift
done

# --- Helpers ---------------------------------------------------------------
bold()  { printf '\033[1m%s\033[0m\n' "$*"; }
info()  { printf '  %s\n' "$*"; }
skip()  { printf '  \033[2m· %s\033[0m\n' "$*"; }
die()   { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }

quoted() {
  local out="" a
  for a in "$@"; do
    if [[ "$a" =~ ^[A-Za-z0-9_/.:=,@-]+$ ]]; then out+="$a "; else out+="$(printf '%q' "$a") "; fi
  done
  printf '%s' "${out% }"
}

# Every mutation goes through run(), so --dry-run is total rather than
# best-effort — there is no path that changes state without printing first.
run() {
  if (( DRY_RUN )); then
    printf '  \033[36m$ %s\033[0m\n' "$(quoted "$@")"
  else
    "$@"
  fi
}

# Same, for calls whose success output is noise — an IAM binding prints the
# entire resulting policy. The redirect has to live *inside* the branch: as a
# suffix on `run ... >/dev/null` it would swallow the dry-run echo as well.
run_quiet() {
  if (( DRY_RUN )); then
    printf '  \033[36m$ %s\033[0m\n' "$(quoted "$@")"
  else
    "$@" >/dev/null
  fi
}

# Confirmation line for a mutation that actually happened. During a dry run the
# echoed command is the whole story, and an unguarded "granted ..." after it
# reads as a report of something the run explicitly did not do.
done_() { (( DRY_RUN )) || info "$*"; }

# `describe`/`show` is the existence check throughout. Their stderr is noise
# when the answer is "no", which is the expected answer on a first run.
exists() { "$@" </dev/null >/dev/null 2>&1; }

# Top-level string field from JSON on stdin. python3 is a prerequisite of the
# project, but this script may run outside the venv, so fall back to sed.
json_get() {
  if command -v python3 >/dev/null 2>&1; then
    python3 -c 'import json,sys; print(json.load(sys.stdin).get(sys.argv[1], ""))' "$1"
  else
    sed -n "s/.*\"$1\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p" | head -1
  fi
}

# Location comparison is case-insensitive: `bq` reports US, `gcloud storage`
# reports US, but a hand-created resource may carry `us` and it is the same
# multi-region. A mismatch here is fatal, not cosmetic — §14.
same_location() { [[ "$(tr '[:lower:]' '[:upper:]' <<<"$1")" == "$(tr '[:lower:]' '[:upper:]' <<<"$2")" ]]; }

# --- Preflight -------------------------------------------------------------
command -v gcloud >/dev/null || die "gcloud not found. Install the Google Cloud CLI."
command -v bq     >/dev/null || die "bq not found. It ships with the Google Cloud CLI."

# gcloud and bq authenticate from gcloud's own credential store. ADC is a
# different file and neither reads it, so `application-default login` alone
# leaves this check failing with an account list that is empty.
gcloud auth list --filter=status:ACTIVE --format='value(account)' | grep -q . \
  || die "Not authenticated. Run: gcloud auth login
       (not \`gcloud auth application-default login\` — gcloud and bq do not read ADC)"

[[ -n "${PROJECT_ID}" ]] || die "No project. Set GCP_PROJECT in ${ENV_FILE}, or run:
       gcloud config set project <id>"
[[ -n "${RAW_BUCKET}" ]] || die "GCP_RAW_BUCKET is unset. Set it in ${ENV_FILE}:
       GCP_RAW_BUCKET=olist-raw-${PROJECT_ID}"

# A .env copied from .env.example and not filled in. Without this the run gets
# as far as trying to create gs://olist-raw-your-gcp-project-id, and the error
# it fails with talks about bucket naming rather than about your .env.
for placeholder in your-gcp-project-id olist-raw-your-gcp-project-id; do
  [[ "${PROJECT_ID}" == "${placeholder}" || "${RAW_BUCKET}" == "${placeholder}" ]] \
    && die "${ENV_FILE} still holds the .env.example placeholders. Fill it in first."
done

# The service account, and the one input a developer cannot invent. The key
# file names it, so read it from there when it was not passed explicitly.
if [[ -z "${SERVICE_ACCOUNT}" && -n "${KEY_FILE}" && -f "${KEY_FILE}" ]]; then
  SERVICE_ACCOUNT="$(json_get client_email <"${KEY_FILE}" || true)"
fi

bold "Provisioning the data plane"
if [[ -n "${ENV_LOADED}" ]]; then info "config     ${ENV_FILE}"; fi
info "project    ${PROJECT_ID}"
info "location   ${LOCATION} (immutable once created — §14)"
info "bucket     gs://${RAW_BUCKET}"
info "datasets   ${RAW_DATASET}, ${MARTS_DATASET}"
info "identity   ${SERVICE_ACCOUNT:-<unknown — step 4 will be skipped>}"
(( DRY_RUN )) && bold "DRY RUN — nothing will be changed"
echo

# --- 1. APIs ---------------------------------------------------------------
# Enabling an already-enabled API is a no-op, but a slow one, so ask first.
bold "1. APIs"
WANT_APIS=(storage.googleapis.com bigquery.googleapis.com)
ENABLED="$(gcloud services list --enabled --project "${PROJECT_ID}" --format='value(config.name)' 2>/dev/null || true)"
TO_ENABLE=()
for api in "${WANT_APIS[@]}"; do
  if grep -qx "${api}" <<<"${ENABLED}"; then skip "${api} already enabled"; else TO_ENABLE+=("${api}"); fi
done
if (( ${#TO_ENABLE[@]} )); then
  info "enabling: ${TO_ENABLE[*]}"
  run gcloud services enable "${TO_ENABLE[@]}" --project "${PROJECT_ID}"
fi

# --- 2. The raw-zone bucket ------------------------------------------------
# Uniform bucket-level access because every grant in this project is IAM: step
# 4 binds a role on the bucket, and with per-object ACLs still enabled an
# object written by another identity could carry permissions that binding does
# not describe. Public access prevention because the raw zone holds the whole
# dataset and nothing about it should ever be reachable anonymously.
bold "2. Raw-zone bucket"
if exists gcloud storage buckets describe "gs://${RAW_BUCKET}" --project "${PROJECT_ID}"; then
  actual="$(gcloud storage buckets describe "gs://${RAW_BUCKET}" --project "${PROJECT_ID}" --format='value(location)')"
  if same_location "${actual}" "${LOCATION}"; then
    skip "gs://${RAW_BUCKET} exists in ${actual}"
  else
    die "gs://${RAW_BUCKET} is in ${actual}, not ${LOCATION}.
       A bucket's location is immutable (§14) and a non-${LOCATION} bucket fails the
       load into a ${LOCATION} dataset with an error that appears to blame the bucket.
       Delete and recreate it, or point GCP_RAW_BUCKET at a new name."
  fi
else
  run gcloud storage buckets create "gs://${RAW_BUCKET}" \
    --location="${LOCATION}" \
    --uniform-bucket-level-access \
    --public-access-prevention \
    --project "${PROJECT_ID}"
fi

# --- 3. BigQuery datasets --------------------------------------------------
# Only the two the pipeline writes. A dbt dev dataset is deliberately absent:
# dbt creates its own target dataset, so making one here would be a second
# source of truth for a name only transform/profiles.yml actually uses.
bold "3. BigQuery datasets"
for ds in "${RAW_DATASET}" "${MARTS_DATASET}"; do
  if exists bq --format=none show --dataset "${PROJECT_ID}:${ds}"; then
    actual="$(bq --format=json show --dataset "${PROJECT_ID}:${ds}" 2>/dev/null | json_get location)"
    if [[ -z "${actual}" ]] || same_location "${actual}" "${LOCATION}"; then
      skip "${ds} exists${actual:+ in ${actual}}"
    else
      die "dataset ${ds} is in ${actual}, not ${LOCATION}.
       A dataset's location is immutable (§14). Delete and recreate it:
         bq rm -r -d ${PROJECT_ID}:${ds}"
    fi
  else
    run bq --location="${LOCATION}" mk --dataset "${PROJECT_ID}:${ds}"
    done_ "created ${ds}"
  fi
done

# --- 4. Bucket IAM ---------------------------------------------------------
# The failure this prevents comes late and reads as a bucket problem: a key
# minted for BigQuery carries no storage role, so the Kaggle download succeeds,
# then the first upload returns 403 naming storage.objects.create.
#
# On the bucket, not the project, so the account stays scoped to the raw zone.
# objectUser and not objectCreator: the upload needs create, and dlt needs get
# and list to read those same CSVs back during the load.
bold "4. Bucket IAM"
if [[ -z "${SERVICE_ACCOUNT}" ]]; then
  skip "no service account known — set GOOGLE_APPLICATION_CREDENTIALS (its"
  skip "client_email is read from the key file), or pass SERVICE_ACCOUNT=<email>."
  skip "Until then the pipeline will 403 on storage.objects.create."
else
  run_quiet gcloud storage buckets add-iam-policy-binding "gs://${RAW_BUCKET}" \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role=roles/storage.objectUser \
    --project "${PROJECT_ID}"
  done_ "granted roles/storage.objectUser on gs://${RAW_BUCKET}"
fi

# --- 5. BigQuery IAM (opt-in) ----------------------------------------------
# Off by default because the service account usually already holds these from
# whenever it was created; --grant-bigquery is for a freshly minted one.
#
# Project level, unlike step 4, and that is a real widening: jobUser has no
# lower scope — running a query is a project-level permission — and dataset-
# level dataEditor means patching each dataset's access list rather than one
# binding. Grant these by hand at dataset scope if the wider grant matters.
bold "5. BigQuery IAM"
if (( ! GRANT_BIGQUERY )); then
  skip "skipped — pass --grant-bigquery if the account is new and cannot load yet"
elif [[ -z "${SERVICE_ACCOUNT}" ]]; then
  skip "no service account known; nothing to grant"
else
  for role in roles/bigquery.dataEditor roles/bigquery.jobUser; do
    # --condition=None is required whenever the binding might be conditional;
    # without it gcloud prompts, which deadlocks a non-interactive run.
    run_quiet gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
      --member="serviceAccount:${SERVICE_ACCOUNT}" \
      --role="${role}" \
      --condition=None \
      --quiet
    done_ "granted ${role} on the project"
  done
fi

# --- Done ------------------------------------------------------------------
cat <<DONE

$(bold "$( (( DRY_RUN )) && echo "Dry run complete — nothing was changed." || echo "Provisioned." )")

  1. Fill in .env and load it — nothing loads it for you:
       uv run python scripts/bootstrap_env.py --name <your-short-name>
       set -a; source .env; set +a

  2. Run the pipeline:
       uv run python orchestration/run_all.py
       uv run dbt build --project-dir transform --target dev

     dbt creates its own dataset on first build; there is nothing to make here.

  To work in your own project instead of the shared one, point GCP_PROJECT at
  it and run this script again. Note that it is the whole pipeline, not just
  dbt: transform/models/staging/_sources.yml resolves sources to
  \$GCP_PROJECT.${RAW_DATASET}, so a personal project needs its own raw zone —
  its own bucket, its own service-account key, and its own Kaggle download.

  Non-engineers install nothing (§15). Grant them on their own Google account:
    gcloud projects add-iam-policy-binding ${PROJECT_ID} \\
      --member=user:<them>@example.com --role=roles/bigquery.dataViewer --condition=None
    gcloud projects add-iam-policy-binding ${PROJECT_ID} \\
      --member=user:<them>@example.com --role=roles/bigquery.jobUser --condition=None

  Teardown — both are irreversible and the bucket holds the whole raw zone:
    gcloud storage rm -r gs://${RAW_BUCKET}
    bq rm -r -d ${PROJECT_ID}:${RAW_DATASET}
DONE
