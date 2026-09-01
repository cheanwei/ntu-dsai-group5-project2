#!/usr/bin/env bash
# GCE startup script for `dagster-vm` — installs the host, not the app.
#
# GCE re-runs this on *every* boot, not just the first, so every step below is
# guarded and idempotent. It leaves the machine able to `docker compose up` an
# image pulled from Artifact Registry, and nothing more: the compose file, the
# .env and the service-account key all arrive later, from the deploy workflow.
#
# Progress and failures: /var/log/dagster-startup.log, and the serial console
# (`gcloud compute instances get-serial-port-output dagster-vm`).
#
# The AR host is read from instance metadata rather than hardcoded, so
# provision_vm.sh stays the single place the region is decided.

set -euo pipefail
exec > >(tee -a /var/log/dagster-startup.log) 2>&1
echo "=== startup-script begin $(date -Is) ==="

APP_DIR=/opt/dagster/app
MARKER=/opt/dagster/.provisioned

meta() {
  curl -fsS -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1" 2>/dev/null || true
}

AR_HOST="$(meta ar-host)"
: "${AR_HOST:=us-central1-docker.pkg.dev}"

# --- Swap ------------------------------------------------------------------
# An e2-micro has 1 GB. The webserver and the daemon each import dbt, dlt and
# pandas, and a materialisation adds a third copy of that working set; without
# swap the kernel OOM-kills whichever process asks for the page that tips it
# over, which in practice is the one doing the work. 2 GB of swap on the boot
# disk is slow, but slow is recoverable and a killed daemon is not.
if [[ ! -f /swapfile ]]; then
  echo "--- creating 2G swapfile"
  fallocate -l 2G /swapfile || dd if=/dev/zero of=/swapfile bs=1M count=2048
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  grep -q '^/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi
# Lean on swap early rather than at the moment of exhaustion.
sysctl -w vm.swappiness=60 >/dev/null
grep -q '^vm.swappiness' /etc/sysctl.d/99-dagster.conf 2>/dev/null \
  || echo 'vm.swappiness=60' > /etc/sysctl.d/99-dagster.conf

# --- Docker Engine + Compose v2 --------------------------------------------
# Docker's own apt repo, not Debian's `docker.io`: the compose *plugin*
# (`docker compose`, which is what the compose file and the deploy workflow
# invoke) is only published there. The standalone `docker-compose` v1 binary is
# end-of-life and parses `x-` anchors differently.
if ! command -v docker >/dev/null 2>&1; then
  echo "--- installing docker engine"
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y -qq ca-certificates curl gnupg
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -qq
  apt-get install -y -qq \
    docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

# --- Container log rotation ------------------------------------------------
# Written before docker is first started. The daemon is long-lived and chatty;
# unbounded json-file logs are the most likely way a 30 GB boot disk fills, and
# a full disk on this box looks like Dagster hanging rather than like a disk
# problem.
if [[ ! -f /etc/docker/daemon.json ]]; then
  echo "--- configuring docker log rotation"
  mkdir -p /etc/docker
  cat > /etc/docker/daemon.json <<'JSON'
{
  "log-driver": "json-file",
  "log-opts": { "max-size": "10m", "max-file": "3" }
}
JSON
  systemctl restart docker 2>/dev/null || true
fi

systemctl enable --now docker

# --- Artifact Registry credential helper -----------------------------------
# No registry credential is ever stored on this machine. It authenticates to
# Artifact Registry as its *attached* service account: gcloud's docker
# credential helper fetches a token from the metadata server on each pull.
# Configured for root because the deploy workflow runs compose under sudo.
if ! command -v gcloud >/dev/null 2>&1; then
  echo "--- installing google-cloud-cli"
  export DEBIAN_FRONTEND=noninteractive
  apt-get install -y -qq apt-transport-https ca-certificates gnupg curl
  curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
    > /etc/apt/sources.list.d/google-cloud-sdk.list
  apt-get update -qq
  apt-get install -y -qq google-cloud-cli
fi
echo "--- configuring docker credential helper for ${AR_HOST}"
gcloud auth configure-docker "${AR_HOST}" --quiet

# --- Layout ----------------------------------------------------------------
# app/          docker-compose.yml and .env, delivered by the deploy workflow
# dagster_home/ SQLite run history, event logs, schedule state (bind-mounted)
# secrets/      the service-account key, delivered by the deploy workflow
mkdir -p "${APP_DIR}" /opt/dagster/dagster_home /opt/dagster/secrets
chmod 700 /opt/dagster/secrets

# --- Weekly image prune ----------------------------------------------------
# Every deploy pulls a new SHA-tagged image (~2 GB). Nothing else reclaims the
# superseded ones, so the disk fills in a few weeks of daily deploys.
cat > /etc/cron.weekly/docker-prune <<'CRON'
#!/bin/sh
/usr/bin/docker image prune -af --filter "until=168h" >/dev/null 2>&1
CRON
chmod +x /etc/cron.weekly/docker-prune

touch "${MARKER}"
echo "=== startup-script complete $(date -Is) ==="
