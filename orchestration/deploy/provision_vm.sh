#!/usr/bin/env bash
# Provision `dagster-vm` — the always-on host the architecture doc (§8) said we
# would not build. Read that section before changing anything here: it explains
# why an e2-micro is marginal for this workload, and what was traded away to
# fit. The mitigations live in startup-script.sh (2 GB swap, log rotation) and
# in docker-compose.vm.yml (two services, SQLite, no Postgres, no per-run
# container).
#
#     ./provision_vm.sh                    # create everything, idempotently
#     ./provision_vm.sh --dry-run          # print the gcloud calls, change nothing
#     ./provision_vm.sh --adopt            # also fix an existing VM that has drifted
#     DEPLOY_SA=ci@proj.iam.gserviceaccount.com ./provision_vm.sh
#
# Idempotent: every step checks for its resource first, so re-running after a
# partial failure resumes rather than errors. Safe to run against an existing
# VM — it will not recreate or restart one.
#
# What it creates, in order:
#
#   API enablement          compute, artifactregistry, iap, oslogin
#   Artifact Registry repo  where the deploy workflow pushes the image
#   Service account         attached to the VM; pulls images, reaches GCS/BQ
#   Firewall: allow         tcp:22,3002 from IAP's range   (priority 800)
#   Firewall: deny          tcp:22,3002,3389 from anywhere (priority 900)
#   Firewall: allow         tcp:80 from anywhere — read-only UI (priority 850)
#   The VM                  e2-micro, external IP, startup-script.sh
#   Compose file            copied to /opt/dagster/app over the IAP tunnel
#
# EGRESS IS PUBLIC; INGRESS IS IAP-ONLY EXCEPT THE READ-ONLY UI. The VM has an
# external IP so it can reach kaggle.com — `kaggle_dataset` downloads 126 MB.
# The only port the internet reaches is 80, the `--read-only` webserver, which
# cannot launch runs. SSH and the full UI are closed by two rules, and the
# second one matters more than it looks:
#
#   800  ALLOW  tcp:22,3002  from 35.235.240.0/20  → tag dagster-vm
#   850  ALLOW  tcp:80       from 0.0.0.0/0        → tag dagster-vm  (read-only UI)
#   900  DENY   tcp:22,3002,3389  from 0.0.0.0/0   → tag dagster-vm
#  1000  ALLOW  tcp:22       from 0.0.0.0/0        → EVERY VM  (GCP's default)
#
# The default VPC ships `default-allow-ssh`, which opens port 22 to the whole
# internet for every instance on the network and carries no target tag. Give
# this VM a public address and that rule alone puts its SSH on the internet.
# The deny at 900 is evaluated first and is scoped to the dagster-vm tag, so it
# closes that hole for this machine without touching a shared rule other people
# on the project may be relying on.
#
# So SSH and the UI still arrive only over IAP, whose forwarders live in
# 35.235.240.0/20 and cannot be spoofed from outside GCP.
#
# WHAT THIS COSTS. Not zero, and the earlier claim in this file that Cloud NAT
# was "USD 1–2/month" was wrong — it omitted the NAT gateway's own external IP.
# The honest comparison, at 730 hours:
#
#   external IP on the VM, no NAT   $0.005/hr                        = $3.65/mo
#   no external IP + Cloud NAT      $0.0014/hr VM + $0.005/hr NAT IP
#                                   + $0.045/GiB × ~3.8 GiB          ≈ $4.84/mo
#
# Public is the cheaper of the two by about a dollar a month, because Cloud NAT
# needs a billable external address of its own. The free tier covers the
# e2-micro and its 30 GB disk; it does not cover either external IP. Stopping
# the VM releases the ephemeral address and stops that charge.

set -euo pipefail

# gcloud prompts on stdin for things this script must decide for itself — most
# sharply, `describe` against a resource whose API is not yet enabled offers to
# enable it and waits. `exists()` below discards stderr, so that prompt is
# invisible *and* blocking: the script simply stops, with no output and no
# error. Disabling prompts globally makes every such question answer itself.
export CLOUDSDK_CORE_DISABLE_PROMPTS=1

# --- Configuration ---------------------------------------------------------
# Every value is overridable from the environment. The defaults are what the
# deploy workflow assumes, so changing one here means changing it there too.
#
# us-central1: the always-free e2-micro is limited to us-west1, us-central1 and
# us-east1, and of the three us-central1 is the conventional pick for a US
# multi-region GCS bucket and BigQuery dataset (§14) — same continent, no
# cross-region egress on the raw-zone upload.
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${REGION:-us-central1}"
ZONE="${ZONE:-us-central1-a}"
VM_NAME="${VM_NAME:-dagster-vm}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-micro}"
# 30 GB pd-standard is the free-tier allowance. The image is ~2 GB, and the
# weekly prune in startup-script.sh keeps superseded ones from accumulating.
BOOT_DISK_SIZE="${BOOT_DISK_SIZE:-30GB}"
BOOT_DISK_TYPE="${BOOT_DISK_TYPE:-pd-standard}"
IMAGE_FAMILY="${IMAGE_FAMILY:-debian-12}"
IMAGE_PROJECT="${IMAGE_PROJECT:-debian-cloud}"
NETWORK="${NETWORK:-default}"
SUBNET="${SUBNET:-default}"
NETWORK_TAG="${NETWORK_TAG:-dagster-vm}"
AR_REPO="${AR_REPO:-dagster}"
SA_NAME="${SA_NAME:-dagster-vm}"
UI_PORT="${UI_PORT:-3002}"
FIREWALL_NAME="${FIREWALL_NAME:-allow-iap-dagster}"
DENY_FIREWALL_NAME="${DENY_FIREWALL_NAME:-deny-public-dagster}"
# The read-only webserver in docker-compose.vm.yml, published to the internet
# for people without GCP access. Must match that service's host port.
PUBLIC_UI_PORT="${PUBLIC_UI_PORT:-80}"
PUBLIC_FIREWALL_NAME="${PUBLIC_FIREWALL_NAME:-allow-public-dagster-readonly}"
# The service account the GitHub Actions deploy job authenticates as — the one
# behind the GCP_SA_KEY secret. Optional: pass it and the script grants it the
# five roles the deploy needs. Leave it unset and you grant them by hand.
DEPLOY_SA="${DEPLOY_SA:-}"

DRY_RUN=0
ADOPT=0
while (( $# )); do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --adopt)   ADOPT=1 ;;
    # Two POSIX substitutions rather than one `s/^# \?//`: `\?` is a GNU
    # extension, and BSD sed reads it as a literal '?' and strips nothing.
    -h|--help) sed -n '2,40p' "${BASH_SOURCE[0]}" | sed -e 's/^# //' -e 's/^#$//'; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; exit 2 ;;
  esac
  shift
done

SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
AR_HOST="${REGION}-docker.pkg.dev"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# IAP's TCP-forwarding source range. Fixed and documented by Google; traffic
# from it has already been through the IAP authorization check, which is what
# makes a rule this narrow equivalent to "authenticated Google users only".
IAP_RANGE="35.235.240.0/20"

# --- Helpers ---------------------------------------------------------------
bold()  { printf '\033[1m%s\033[0m\n' "$*"; }
info()  { printf '  %s\n' "$*"; }
skip()  { printf '  \033[2m· %s\033[0m\n' "$*"; }
die()   { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }

# Every mutation goes through run(), so --dry-run is total rather than
# best-effort — there is no path that changes state without printing first.
# Re-quotes an argv for display, so a --dry-run line is something you can paste
# back into a shell. Without it an argument containing spaces prints as several,
# and the printed command is not the command that would run.
quoted() {
  local out="" a
  for a in "$@"; do
    if [[ "$a" =~ ^[A-Za-z0-9_/.:=,@-]+$ ]]; then out+="$a "; else out+="$(printf '%q' "$a") "; fi
  done
  printf '%s' "${out% }"
}

run() {
  if (( DRY_RUN )); then
    printf '  \033[36m$ %s\033[0m\n' "$(quoted "$@")"
  else
    "$@"
  fi
}

# Same, for calls whose success output is noise — an IAM binding prints the
# entire resulting policy. The redirect has to live *inside* the branch: as a
# suffix on `run ... >/dev/null` it would swallow the dry-run echo as well,
# leaving --dry-run silently under-reporting what it would do.
run_quiet() {
  if (( DRY_RUN )); then
    printf '  \033[36m$ %s\033[0m\n' "$(quoted "$@")"
  else
    "$@" >/dev/null
  fi
}

# `gcloud ... describe` is the existence check throughout. Its stderr is noise
# when the answer is "no", which is the expected answer on a first run.
exists() { "$@" </dev/null >/dev/null 2>&1; }

# --- Preflight -------------------------------------------------------------
command -v gcloud >/dev/null || die "gcloud not found. Install the Google Cloud CLI."
[[ -n "${PROJECT_ID}" ]] || die "No project. Set PROJECT_ID or run: gcloud config set project <id>"
gcloud auth list --filter=status:ACTIVE --format='value(account)' | grep -q . \
  || die "Not authenticated. Run: gcloud auth login"
[[ -f "${HERE}/startup-script.sh" ]]       || die "startup-script.sh missing from ${HERE}"
[[ -f "${HERE}/docker-compose.vm.yml" ]]   || die "docker-compose.vm.yml missing from ${HERE}"

bold "Provisioning ${VM_NAME}"
info "project    ${PROJECT_ID}"
info "zone       ${ZONE} (${MACHINE_TYPE}, public egress, IAP-only ingress + public read-only UI)"
info "registry   ${AR_HOST}/${PROJECT_ID}/${AR_REPO}"
info "identity   ${SA_EMAIL}"
(( DRY_RUN )) && bold "DRY RUN — nothing will be changed"
echo

# --- 1. APIs ---------------------------------------------------------------
# Enabling an already-enabled API is a no-op, but it is a slow one, so ask
# first. `iap` is what terminates the SSH tunnel; `oslogin` is what authorises
# the SSH key against IAM instead of against project metadata.
bold "1. APIs"
WANT_APIS=(compute.googleapis.com artifactregistry.googleapis.com iap.googleapis.com oslogin.googleapis.com)
ENABLED="$(gcloud services list --enabled --project "${PROJECT_ID}" --format='value(config.name)' 2>/dev/null || true)"
TO_ENABLE=()
for api in "${WANT_APIS[@]}"; do
  if grep -qx "${api}" <<<"${ENABLED}"; then skip "${api} already enabled"; else TO_ENABLE+=("${api}"); fi
done
if (( ${#TO_ENABLE[@]} )); then
  info "enabling: ${TO_ENABLE[*]}"
  run gcloud services enable "${TO_ENABLE[@]}" --project "${PROJECT_ID}"
fi

# --- 2. Artifact Registry --------------------------------------------------
bold "2. Artifact Registry"
if exists gcloud artifacts repositories describe "${AR_REPO}" --location "${REGION}" --project "${PROJECT_ID}"; then
  skip "repository ${AR_REPO} exists"
else
  run gcloud artifacts repositories create "${AR_REPO}" \
    --repository-format=docker \
    --location="${REGION}" \
    --description="Dagster images for ${VM_NAME}" \
    --project "${PROJECT_ID}"
fi

# --- 3. The VM's identity --------------------------------------------------
# A dedicated account rather than the default Compute Engine one, which carries
# Editor on the whole project. These five roles are what a run actually needs:
# pull the image, write the raw zone, load and query BigQuery, ship logs.
#
# storage.objectAdmin, not storage.admin: the pipeline needs object-level
# access, and an objectAdmin account returns 403 on `storage.buckets.get` while
# still running correctly (deploy/README.md).
bold "3. Service account"
if exists gcloud iam service-accounts describe "${SA_EMAIL}" --project "${PROJECT_ID}"; then
  skip "${SA_EMAIL} exists"
else
  run gcloud iam service-accounts create "${SA_NAME}" \
    --display-name="Dagster VM" \
    --description="Identity for ${VM_NAME}: pulls images, runs the pipeline" \
    --project "${PROJECT_ID}"
fi

VM_ROLES=(
  roles/artifactregistry.reader
  roles/storage.objectAdmin
  roles/bigquery.dataEditor
  roles/bigquery.jobUser
  roles/logging.logWriter
)
for role in "${VM_ROLES[@]}"; do
  # --condition=None is required whenever the binding might be conditional;
  # without it gcloud prompts, which deadlocks a non-interactive run.
  run_quiet gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="${role}" \
    --condition=None \
    --quiet
  info "granted ${role}"
done

# The deploy job's own identity. Distinct from the VM's: it never touches GCS
# or BigQuery, it pushes an image and opens a tunnel.
#
# osAdminLogin rather than osLogin because the deploy runs `sudo docker
# compose` — /opt/dagster is root-owned, and osLogin alone grants no sudo.
# serviceAccountUser on the *VM's* account is the non-obvious one: SSHing into
# an instance that has a service account attached counts as acting as it, and
# without this the tunnel opens and the login is refused.
if [[ -n "${DEPLOY_SA}" ]]; then
  bold "3b. Deploy service account"
  for role in roles/iap.tunnelResourceAccessor roles/compute.osAdminLogin roles/compute.viewer; do
    run_quiet gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
      --member="serviceAccount:${DEPLOY_SA}" --role="${role}" --condition=None --quiet
    info "granted ${role}"
  done
  run_quiet gcloud iam service-accounts add-iam-policy-binding "${SA_EMAIL}" \
    --member="serviceAccount:${DEPLOY_SA}" \
    --role=roles/iam.serviceAccountUser --condition=None --quiet
  info "granted roles/iam.serviceAccountUser on ${SA_EMAIL}"
  run_quiet gcloud artifacts repositories add-iam-policy-binding "${AR_REPO}" \
    --location="${REGION}" \
    --member="serviceAccount:${DEPLOY_SA}" \
    --role=roles/artifactregistry.writer --quiet
  info "granted roles/artifactregistry.writer on ${AR_REPO}"
else
  bold "3b. Deploy service account"
  skip "DEPLOY_SA unset — grant iap.tunnelResourceAccessor, compute.osAdminLogin,"
  skip "compute.viewer, iam.serviceAccountUser and artifactregistry.writer by hand,"
  skip "or re-run with DEPLOY_SA=<the account behind GCP_SA_KEY>"
fi

# --- 4. Firewall -----------------------------------------------------------
# Three rules, all scoped to the dagster-vm tag. See the header for why the
# second one is not redundant: GCP's own default-allow-ssh opens port 22 to
# 0.0.0.0/0 for every instance on the default network, so on a VM with a public
# address the allow-list is only half the job.
#
# Priority is ascending-wins. 800 beats 900 beats GCP's default 1000, so IAP
# traffic is permitted, everything else on those ports is dropped, and the
# shared default rule is left alone for whatever else uses it.
bold "4. Firewall"
if exists gcloud compute firewall-rules describe "${FIREWALL_NAME}" --project "${PROJECT_ID}"; then
  skip "rule ${FIREWALL_NAME} exists"
else
  run gcloud compute firewall-rules create "${FIREWALL_NAME}" \
    --network="${NETWORK}" \
    --direction=INGRESS \
    --action=ALLOW \
    --priority=800 \
    --rules="tcp:22,tcp:${UI_PORT}" \
    --source-ranges="${IAP_RANGE}" \
    --target-tags="${NETWORK_TAG}" \
    --description="IAP TCP forwarding only: SSH and the Dagster UI" \
    --project "${PROJECT_ID}"
fi

# 3389 is RDP. Pointless on a Linux VM, but the default VPC opens it too, and a
# rule that closes "everything the defaults opened" is easier to reason about
# than one that closes the subset someone judged exploitable.
if exists gcloud compute firewall-rules describe "${DENY_FIREWALL_NAME}" --project "${PROJECT_ID}"; then
  skip "rule ${DENY_FIREWALL_NAME} exists"
else
  run gcloud compute firewall-rules create "${DENY_FIREWALL_NAME}" \
    --network="${NETWORK}" \
    --direction=INGRESS \
    --action=DENY \
    --priority=900 \
    --rules="tcp:22,tcp:${UI_PORT},tcp:3389" \
    --source-ranges="0.0.0.0/0" \
    --target-tags="${NETWORK_TAG}" \
    --description="Overrides the default VPC's internet-wide SSH/RDP allow for this VM" \
    --project "${PROJECT_ID}"
fi

# The one port that IS public: the --read-only webserver. It is not in the deny
# rule's port list, so this allow is what opens it; the default VPC has no rule
# for it. Safe to expose only because that server refuses every mutation —
# never point this rule at ${UI_PORT}, whose UI can launch runs.
if exists gcloud compute firewall-rules describe "${PUBLIC_FIREWALL_NAME}" --project "${PROJECT_ID}"; then
  skip "rule ${PUBLIC_FIREWALL_NAME} exists"
else
  run gcloud compute firewall-rules create "${PUBLIC_FIREWALL_NAME}" \
    --network="${NETWORK}" \
    --direction=INGRESS \
    --action=ALLOW \
    --priority=850 \
    --rules="tcp:${PUBLIC_UI_PORT}" \
    --source-ranges="0.0.0.0/0" \
    --target-tags="${NETWORK_TAG}" \
    --description="Public read-only Dagster UI (dagster-webserver --read-only)" \
    --project "${PROJECT_ID}"
fi

# Names the shared rules the deny above exists to neutralise. Informational —
# the deny already handles this VM — but worth printing, because "the default
# VPC lets the whole internet reach port 22 on everything untagged" is the kind
# of fact people assume is not true of their project.
#
# Filtered in awk rather than with --filter: gcloud pushes list filters to the
# API, which rejects `NOT targetTags:*` outright.
PERMISSIVE="$(gcloud compute firewall-rules list --project "${PROJECT_ID}" \
  --format='value[separator="|"](name,network.basename(),direction,sourceRanges.list(separator=";"),targetTags.list(separator=";"))' \
  2>/dev/null \
  | awk -F'|' -v net="${NETWORK}" \
      '$2==net && $3=="INGRESS" && $4 ~ /0\.0\.0\.0\/0/ && $5=="" {print $1}' || true)"
if [[ -n "${PERMISSIVE}" ]]; then
  info "untagged internet-wide INGRESS rules on '${NETWORK}': $(tr '\n' ' ' <<<"${PERMISSIVE}")"
  info "  they apply to every other VM on this network; ${DENY_FIREWALL_NAME} exempts this one"
fi

# --- 5. The VM -------------------------------------------------------------
# It gets an ephemeral external IP — that is what pays for egress to Kaggle,
# and it is cheaper than the Cloud NAT it replaces (see the header). Ingress is
# closed by the pair of rules above, not by the absence of an address.
# --scopes=cloud-platform delegates authorisation entirely to the IAM roles on
#   ${SA_EMAIL} above; the legacy per-scope model would have to be kept in sync
#   with them, and silently denies in a way that reads as a permissions bug.
# enable-oslogin=TRUE puts SSH access under IAM rather than under project SSH
#   keys, which is what lets the deploy job in with no key distribution at all.
bold "5. Instance"
if exists gcloud compute instances describe "${VM_NAME}" --zone "${ZONE}" --project "${PROJECT_ID}"; then
  # An existing VM is never recreated — it may hold run history nobody wants to
  # lose. But "it exists" is not "it is the machine described above", and the
  # three ways it can differ all fail *later* and obscurely:
  #
  #   no network tag  → the IAP firewall rule does not select it, so the deploy
  #                     hangs at the tunnel and reports a timeout, not a policy
  #                     problem
  #   no external IP  → no egress, so kaggle_dataset cannot reach kaggle.com
  #                     and the first asset in the graph fails on a timeout
  #   wrong identity  → the default Compute Engine account is project Editor;
  #                     the pull works, so nothing looks wrong, and the blast
  #                     radius of the box is the whole project
  #
  # So: report the drift with the command that closes it, and only act with
  # --adopt. Two of the fixes need a stop/start, which would be a rude thing to
  # do to a running daemon without being asked.
  skip "instance ${VM_NAME} exists — not recreating"
  read -r CUR_SA CUR_NAT CUR_TAGS CUR_OSLOGIN <<<"$(
    gcloud compute instances describe "${VM_NAME}" --zone "${ZONE}" --project "${PROJECT_ID}" \
      --format="value[separator=' '](
        serviceAccounts[0].email.yesno(no='NONE'),
        networkInterfaces[0].accessConfigs[0].natIP.yesno(no='NONE'),
        tags.items.list(separator=',').yesno(no='NONE'),
        metadata.items.filter(\"key:enable-oslogin\").firstof(value).yesno(no='NONE'))")"

  DRIFT=0
  fix() { DRIFT=1; printf '  \033[33m! %s\033[0m\n' "$1"; printf '      %s\n' "$2"; }

  [[ ",${CUR_TAGS}," == *",${NETWORK_TAG},"* ]] || fix \
    "no '${NETWORK_TAG}' network tag — the IAP firewall rule does not apply to it" \
    "gcloud compute instances add-tags ${VM_NAME} --zone ${ZONE} --tags=${NETWORK_TAG}"

  # Inverted from the no-address design: the address is now wanted, and its
  # absence means the pipeline cannot reach Kaggle. Ingress is closed by the
  # firewall pair, not by the lack of an address.
  [[ "${CUR_NAT}" != "NONE" ]] || fix \
    "no external IP — the VM has no egress, so kaggle_dataset cannot download" \
    "gcloud compute instances add-access-config ${VM_NAME} --zone ${ZONE}"

  [[ "${CUR_SA}" == "${SA_EMAIL}" ]] || fix \
    "runs as ${CUR_SA}, not ${SA_EMAIL} (needs a stop/start)" \
    "gcloud compute instances stop ${VM_NAME} --zone ${ZONE} && gcloud compute instances set-service-account ${VM_NAME} --zone ${ZONE} --service-account=${SA_EMAIL} --scopes=cloud-platform && gcloud compute instances start ${VM_NAME} --zone ${ZONE}"

  [[ "${CUR_OSLOGIN}" == "TRUE" ]] || fix \
    "OS Login not enabled — IAM cannot authorise the deploy job's SSH key" \
    "gcloud compute instances add-metadata ${VM_NAME} --zone ${ZONE} --metadata=enable-oslogin=TRUE,ar-host=${AR_HOST}"

  if (( DRIFT )); then
    if (( ADOPT )); then
      bold "5b. Adopting ${VM_NAME}"
      run gcloud compute instances add-tags "${VM_NAME}" --zone "${ZONE}" \
        --tags="${NETWORK_TAG}" --project "${PROJECT_ID}"
      if [[ "${CUR_NAT}" == "NONE" ]]; then
        run gcloud compute instances add-access-config "${VM_NAME}" --zone "${ZONE}" \
          --project "${PROJECT_ID}"
      fi
      run gcloud compute instances add-metadata "${VM_NAME}" --zone "${ZONE}" \
        --metadata="enable-oslogin=TRUE,ar-host=${AR_HOST}" \
        --metadata-from-file="startup-script=${HERE}/startup-script.sh" \
        --project "${PROJECT_ID}"
      if [[ "${CUR_SA}" != "${SA_EMAIL}" ]]; then
        # The identity is fixed only while the instance is TERMINATED. The
        # restart also re-runs the startup script, which is how the metadata
        # set just above takes effect.
        info "stopping to change the service account (this reboots the VM)"
        run gcloud compute instances stop "${VM_NAME}" --zone "${ZONE}" --project "${PROJECT_ID}"
        run gcloud compute instances set-service-account "${VM_NAME}" --zone "${ZONE}" \
          --service-account="${SA_EMAIL}" --scopes=cloud-platform --project "${PROJECT_ID}"
        run gcloud compute instances start "${VM_NAME}" --zone "${ZONE}" --project "${PROJECT_ID}"
      else
        info "rebooting so the new startup script runs"
        run gcloud compute instances reset "${VM_NAME}" --zone "${ZONE}" --project "${PROJECT_ID}"
      fi
    else
      info ""
      info "Run the commands above, or re-run this script with --adopt to apply"
      info "them. Two of them stop and start the VM. Nothing else here depends"
      info "on the fixes, so the remaining steps still run."
    fi
  else
    skip "configuration matches"
  fi
else
  run gcloud compute instances create "${VM_NAME}" \
    --zone="${ZONE}" \
    --machine-type="${MACHINE_TYPE}" \
    --image-family="${IMAGE_FAMILY}" \
    --image-project="${IMAGE_PROJECT}" \
    --boot-disk-size="${BOOT_DISK_SIZE}" \
    --boot-disk-type="${BOOT_DISK_TYPE}" \
    --subnet="${SUBNET}" \
    --tags="${NETWORK_TAG}" \
    --service-account="${SA_EMAIL}" \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --metadata="enable-oslogin=TRUE,ar-host=${AR_HOST}" \
    --metadata-from-file="startup-script=${HERE}/startup-script.sh" \
    --labels=app=dagster \
    --project "${PROJECT_ID}"
fi

# --- 8. Wait for the startup script ----------------------------------------
# The instance is RUNNING long before docker exists on it, and the deploy that
# follows will fail confusingly if it lands in that window. /opt/dagster/.provisioned
# is written on the last line of startup-script.sh, so it means "docker is
# installed and the credential helper is configured", not merely "booted".
bold "6. Waiting for host provisioning"
if (( DRY_RUN )); then
  skip "skipped in dry run"
else
  deadline=$(( SECONDS + 600 ))
  until gcloud compute ssh "${VM_NAME}" --zone "${ZONE}" --project "${PROJECT_ID}" \
          --tunnel-through-iap --quiet \
          --command='test -f /opt/dagster/.provisioned' >/dev/null 2>&1; do
    (( SECONDS < deadline )) || die "startup script did not finish in 10 minutes.
  Inspect it with:
    gcloud compute instances get-serial-port-output ${VM_NAME} --zone ${ZONE}
    gcloud compute ssh ${VM_NAME} --zone ${ZONE} --tunnel-through-iap --command 'sudo tail -50 /var/log/dagster-startup.log'"
    info "still installing… ($(( deadline - SECONDS ))s left)"
    sleep 20
  done
  info "docker and the AR credential helper are ready"
fi

# --- 9. The compose file ---------------------------------------------------
# Two hops because /opt/dagster/app is root-owned and scp arrives as the OS
# Login user. The deploy workflow does exactly the same thing, for the same
# reason; this copy is what makes the VM usable before the first deploy runs.
bold "7. docker-compose.yml"
if (( DRY_RUN )); then
  skip "skipped in dry run"
else
  gcloud compute scp "${HERE}/docker-compose.vm.yml" \
    "${VM_NAME}:/tmp/docker-compose.yml" \
    --zone "${ZONE}" --project "${PROJECT_ID}" --tunnel-through-iap --quiet
  gcloud compute ssh "${VM_NAME}" --zone "${ZONE}" --project "${PROJECT_ID}" \
    --tunnel-through-iap --quiet \
    --command='sudo install -o root -g root -m 644 /tmp/docker-compose.yml /opt/dagster/app/docker-compose.yml && rm -f /tmp/docker-compose.yml'
  info "installed at /opt/dagster/app/docker-compose.yml"
fi

# --- Done ------------------------------------------------------------------
cat <<DONE

$(bold "Provisioned.")

The VM is up and can run containers, but there is nothing to run yet: the
image, the .env and the service-account key are the deploy workflow's job.

  1. Set the GitHub Actions secrets the deploy workflow reads:
       GCP_SA_KEY  GCP_PROJECT  GCP_RAW_BUCKET  KAGGLE_API_TOKEN
     GCP_SA_KEY must be a key for an account holding the roles granted in
     step 3b$([[ -n "${DEPLOY_SA}" ]] && echo " — granted above to ${DEPLOY_SA}" || echo ", which were NOT granted (DEPLOY_SA was unset)").

  2. Run the deploy:
       gh workflow run deploy-dagster.yml
     or push to main.

  3. Open the full UI (IAP-only — the tunnel is the only way in):
       gcloud compute start-iap-tunnel ${VM_NAME} ${UI_PORT} \\
         --local-host-port=127.0.0.1:${UI_PORT} --zone ${ZONE} --project ${PROJECT_ID}
     then http://127.0.0.1:${UI_PORT}

     127.0.0.1, not localhost: on macOS localhost resolves to ::1 first, and
     the tunnel logs a stream of "[Errno 9] Bad file descriptor" on IPv6
     connection teardown. Noise, not failure — but avoidable.

  4. Share the read-only UI with people who have no GCP access:
       http://$(gcloud compute instances describe "${VM_NAME}" --zone "${ZONE}" --project "${PROJECT_ID}" \
         --format='value(networkInterfaces[0].accessConfigs[0].natIP)' 2>/dev/null || echo '<external-ip>')
     The address is ephemeral and changes if the VM is stopped; see README.md,
     "Public read-only UI", to reserve it.

  SSH:   gcloud compute ssh ${VM_NAME} --zone ${ZONE} --tunnel-through-iap
  Logs:  gcloud compute ssh ${VM_NAME} --zone ${ZONE} --tunnel-through-iap \\
           --command 'cd /opt/dagster/app && sudo docker compose logs -f daemon'

  Cost: the e2-micro and its 30 GB disk are free tier; its external IP is not,
  at \$0.005/hr (~\$3.65/mo). Stopping the VM releases the address and the
  charge:
    gcloud compute instances stop ${VM_NAME} --zone ${ZONE}

  Teardown:
    gcloud compute instances delete ${VM_NAME} --zone ${ZONE}
    gcloud compute firewall-rules delete ${FIREWALL_NAME} ${DENY_FIREWALL_NAME} ${PUBLIC_FIREWALL_NAME}
DONE
