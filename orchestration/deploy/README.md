# Dagster deployment

This runbook covers the local Docker Compose stack and the hosted GCP VM. Both
use one project image and two Dagster services:

| Service | Responsibility |
|---|---|
| `webserver` | UI, lineage, run submission, and history |
| `webserver-readonly` | Hosted only: public `--read-only` UI for people without GCP access |
| `daemon` | Schedule and sensor evaluation, queued-run execution, and run launching |

Both load `orchestration.definitions` and share a SQLite-backed
`DAGSTER_HOME`. There is no Postgres service, separate gRPC code server, or
per-run container.

## Run locally with Docker Compose

From this directory:

```bash
docker compose --env-file ../../.env up --build -d
docker compose --env-file ../../.env logs -f daemon
```

Open <http://localhost:3002>.

### Three details that prevent common failures

**Pass `--env-file` on every local Compose command.** Compose resolves
`${...}` expressions before containers start. The `env_file:` key populates
container variables later and cannot provide the host path for the
service-account volume mount.

**Keep the daemon running.** The default coordinator queues UI-submitted runs;
the daemon dequeues and launches them. If the webserver is healthy but runs stay
in `QUEUED`, inspect the daemon first:

```bash
docker compose --env-file ../../.env logs --tail=100 daemon
```

Restarting the daemon interrupts work executing inside it.

**Know which `DAGSTER_HOME` is mounted.** The local Compose file uses
`${DAGSTER_HOME:-..}`. A value exported by the shell takes precedence over the
value in `--env-file`, so two shells can show different run histories. Pin an
absolute path when history location matters:

```bash
DAGSTER_HOME=/absolute/path/to/orchestration \
  docker compose --env-file ../../.env up -d
```

### Data, credentials, and volumes

Runs are not simulations. A full materialization downloads Kaggle data, writes
the dated Cloud Storage prefix, replaces nine BigQuery raw tables, and builds
the dbt models and tests.

The service-account key named by `GOOGLE_APPLICATION_CREDENTIALS` is mounted
read-only at `/opt/dagster/secrets/gcp-sa.json`; it is never copied into the
image. `kaggle-cache` and `staging` are named volumes, so repeat runs can reuse
downloaded and extracted files.

Stop services while retaining volumes and history:

```bash
docker compose --env-file ../../.env down
```

Adding `-v` deletes the named Kaggle and staging volumes. It does not delete a
host directory mounted as `DAGSTER_HOME`.

## Hosted topology

`orchestration/deploy/docker-compose.vm.yml` runs the same two services on a VM named
`dagster-vm`:

- GitHub Actions builds and pushes the image; the VM only pulls it.
- The image is tagged with the source commit SHA.
- `/opt/dagster/dagster_home` preserves SQLite history and automation state.
- `/opt/dagster/secrets/gcp-sa.json` is mounted read-only.
- The daemon runs ingestion at 08:00 SGT and evaluates dbt automation
  conditions every 60 seconds.

The default `e2-micro` has 1 GB of memory. The deployment omits Postgres, a
separate code server, and per-run containers to reduce the resident footprint.
`orchestration/deploy/startup-script.sh` adds 2 GB of swap and bounded Docker logs. This makes the
small host practical for the project, but runs are slower and the machine
remains a single point of failure. Use `MACHINE_TYPE=e2-small` when reliability
is more important than staying within the smallest footprint.

## Provision the VM

Authenticate as a project administrator, then preview and apply the setup:

```bash
gcloud auth login
./provision_vm.sh --dry-run
DEPLOY_SA=<github-actions-service-account> ./provision_vm.sh
```

The script creates or validates:

- Compute Engine, Artifact Registry, IAP, and OS Login APIs
- the Artifact Registry repository
- the VM service account and project roles
- IAP-only allow rules and explicit public-ingress deny rules
- the VM, startup script, and hosted Compose file

The script is idempotent. An existing VM is inspected but not changed by
default. Use `--adopt` only when intentionally correcting its network tag,
external address, or attached service account; adoption can stop and restart
the instance.

### Identities and permissions

Three identities take part:

| Identity | Purpose | Authentication |
|---|---|---|
| `dagster-vm@` | Pull the image and run the hosted pipeline | Attached VM service account |
| Account behind `GCP_SA_KEY` | Build/push/deploy from GitHub Actions and run the on-demand pipeline | Repository secret |
| Operator | Provision and troubleshoot GCP resources | `gcloud auth login` |

The VM account receives:

| Role | Why it is needed |
|---|---|
| `roles/artifactregistry.reader` | Pull the deployment image |
| `roles/storage.objectAdmin` | Read and write raw-zone objects |
| `roles/bigquery.dataEditor` | Create and replace warehouse tables |
| `roles/bigquery.jobUser` | Submit BigQuery load and query jobs |
| `roles/logging.logWriter` | Send host/container logs to Cloud Logging |

It deliberately does not receive project-wide Storage Admin or BigQuery Admin.
Object and table operations are sufficient for the running pipeline.

When `DEPLOY_SA` is supplied, the provisioning script also grants the deploy
identity:

| Role or binding | Scope | Purpose |
|---|---|---|
| `roles/iap.tunnelResourceAccessor` | project | Open the deployment tunnel |
| `roles/compute.osAdminLogin` | project | SSH with the `sudo` access required under `/opt/dagster` |
| `roles/compute.viewer` | project | Resolve and inspect the VM |
| `roles/iam.serviceAccountUser` | VM service account | Connect to a VM carrying that identity |
| `roles/artifactregistry.writer` | Dagster repository | Push the image |

The on-demand pipeline workflow additionally needs access to Cloud Storage and
BigQuery. That data-plane authorization is separate from the deployment roles
above.

Audit project-level bindings:

```bash
gcloud projects get-iam-policy <project-id> \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:<service-account-email>" \
  --format="value(bindings.role)" | sort
```

Narrow `serviceAccountUser` and Artifact Registry bindings live on those
resources, so inspect their IAM policies separately when a project-level audit
looks complete but deployment still fails.

## Networking

The VM has an ephemeral external address for outbound Kaggle traffic. The only
port it accepts from the public internet is 80, the read-only UI. All three
firewall rules target the `dagster-vm` network tag:

| Priority | Action | Ports and source |
|---:|---|---|
| 800 | allow | TCP 22 and 3002 from IAP `35.235.240.0/20` |
| 850 | allow | TCP 80 from `0.0.0.0/0` (read-only UI) |
| 900 | deny | TCP 22, 3002, and 3389 from `0.0.0.0/0` |

The deny rule matters on the default VPC because a lower-priority shared rule
may allow public SSH. Since lower numeric priorities win, authenticated IAP
traffic is accepted at 800 and other traffic on those ports is rejected at
900 before a default priority-1000 rule can match.

The Dagster UI has no application-level authentication. Anyone who reaches the
full UI on 3002 can submit a pipeline run that uses project resources, so the
IAP boundary and public deny rule are required controls. Never widen the
850 rule to port 3002.

### Public read-only UI

People without GCP access use `webserver-readonly`: the same image, code
location and run history, started with `dagster-webserver --read-only`. They
can browse the asset graph, asset details, runs, and run logs. Dagster rejects
every mutation server-side: launching or re-executing runs, materializing
assets, and turning schedules or sensors on or off.

Share `http://<external-ip>`. Find the address with:

```bash
gcloud compute instances describe dagster-vm --zone=us-central1-a \
  --format='value(networkInterfaces[0].accessConfigs[0].natIP)'
```

Keep in mind:

- **Everything shown is public.** That includes run logs, asset metadata, dbt
  SQL, and the bucket and dataset names that appear in them. Secrets are not
  shown: the key file and `.env` values are not rendered by the UI.
- **Plain HTTP.** Browsers mark the page "Not secure". Nothing sensitive is
  submitted, because the page takes no input that changes state. For HTTPS, put
  a reverse proxy such as Caddy with a domain in front.
- **The address changes when the VM stops.** To keep a stable link, promote the
  current ephemeral address to a static one. A static address costs the same
  while the VM runs, but is billed while the VM is stopped too:

  ```bash
  IP=$(gcloud compute instances describe dagster-vm --zone=us-central1-a \
    --format='value(networkInterfaces[0].accessConfigs[0].natIP)')
  gcloud compute addresses create dagster-ui --region=us-central1 --addresses="$IP"
  ```

- **Memory.** The second webserver loads the code location too, adding a few
  hundred MB. Use `e2-small` or larger.
- **Turn it off** without touching the operator UI:

  ```bash
  gcloud compute firewall-rules delete allow-public-dagster-readonly
  ```

  To also free its memory, remove the service from `docker-compose.vm.yml` and
  redeploy; `--remove-orphans` stops the container.

### Cost boundary

The host is not cost-free merely because the machine and disk may qualify for a
free allowance. External IPv4 addresses, Artifact Registry storage, BigQuery
jobs, Cloud Storage, and network transfer can incur charges. Cloud pricing and
free-tier rules change, so this runbook intentionally does not preserve the
old fixed monthly estimate.

The practical cost controls are:

- stop the schedule when only the UI and history are needed
- stop the VM to stop scheduled work and release its ephemeral address
- remove superseded Artifact Registry images
- delete the deployment when it is no longer required

## Deploy

`.github/workflows/deploy-dagster.yml` runs for relevant code changes on
`main` or by manual dispatch:

```bash
gh workflow run deploy-dagster.yml
```

Required repository secrets:

- `GCP_PROJECT`
- `GCP_RAW_BUCKET`
- `KAGGLE_API_TOKEN`
- `GCP_SA_KEY`

Optional repository variables default to `US`, `olist_raw`, `olist_marts`,
and the `prod` dbt target.

### How a deployment works

1. The guard job decides whether deployment is enabled.
2. Required secrets are checked before the image build.
3. GitHub Actions builds and pushes
   `<region>-docker.pkg.dev/<project>/dagster/olist-dagster:<commit-sha>`.
4. The workflow delivers the Compose file, Dagster configuration, environment,
   and service-account key through IAP.
5. The VM pulls the immutable tag and recreates the two services.
6. The workflow checks container and UI health.

The VM does not retain a registry key. Docker authenticates through the
attached VM service account and the gcloud credential helper. The pipeline key
is staged into a root-owned directory with restrictive permissions, mounted at
runtime, and selected through `GOOGLE_APPLICATION_CREDENTIALS`.

The commit-SHA tag is operational, not cosmetic. Compose uses the image
reference to decide whether a container is stale; changing the SHA ensures a
newly pulled image causes recreation instead of leaving the previous container
running behind a floating tag.

### Workflow responsibilities

| Mechanism | Responsibility |
|---|---|
| `.github/workflows/deploy-dagster.yml` | Build the image, push it, and roll the VM; it does not run the pipeline |
| Hosted `daily_refresh` | Schedule ingestion at 08:00 SGT |
| Hosted automation sensor | Request dbt models after successful raw materialization |
| `.github/workflows/pipeline.yml` | Run the full graph manually and publish dbt Docs |

`.github/workflows/pipeline.yml` has no cron. A second schedule could race the hosted daemon on
the same replace-loaded objects and tables.

## Pause, rollback, and remove

`DEPLOY_DAGSTER` is a deployment kill switch:

```bash
gh variable set DEPLOY_DAGSTER --body false
gh variable delete DEPLOY_DAGSTER
gh workflow run deploy-dagster.yml -f force=true
```

`false`, `no`, `off`, and `0` pause automatic deployments. A forced
manual dispatch deploys once without changing the variable.

Pausing deployment only stops new images and VM restarts. It does not stop the
running VM, external-address charges, or `daily_refresh` on the already
deployed image.

There is no automated rollback job. To roll back code, redeploy a known-good
commit so the workflow produces and pins that commit's image. If the current
containers cannot start, set `DAGSTER_IMAGE` on the VM to an existing known-good
SHA tag and run `sudo docker compose up -d`.

Stop the host while retaining its disk and run history:

```bash
gcloud compute instances stop dagster-vm --zone=us-central1-a
```

Starting it again runs the startup script and Docker restarts services configured
with `restart: unless-stopped`. Verify the schedule and sensor state in the UI
after recovery because Dagster stores their running state in the instance.

Remove the host and firewall rules:

```bash
gcloud compute instances delete dagster-vm --zone=us-central1-a
gcloud compute firewall-rules delete allow-iap-dagster deny-public-dagster \
  allow-public-dagster-readonly
```

Deleting the VM removes its local Dagster history. Artifact Registry images and
other shared project resources are separate and must be reviewed independently.

## Access and troubleshooting

Start an IAP tunnel:

```bash
gcloud compute start-iap-tunnel dagster-vm 3002 \
  --local-host-port=127.0.0.1:3002 \
  --zone=us-central1-a
```

Open <http://127.0.0.1:3002> and keep the tunnel process running. Explicit IPv4
loopback avoids noisy IPv6 `localhost` behavior observed on some macOS setups.

### UI temporarily unavailable during deployment

The replacement webserver can take roughly a minute to import dbt, dlt, and
the project before it listens. An IAP `4003: failed to connect to backend`
during that window means the tunnel reached Google but nothing was accepting
port 3002 on the VM. The tunnel can remain open; wait, then reload.

When the error persists, inspect the listener and containers:

```bash
gcloud compute ssh dagster-vm \
  --zone=us-central1-a \
  --tunnel-through-iap \
  --command='sudo ss -lntp | grep 3002; sudo docker ps -a'
```

Then inspect startup and daemon logs:

```bash
gcloud compute ssh dagster-vm \
  --zone=us-central1-a \
  --tunnel-through-iap \
  --command='cd /opt/dagster/app && sudo docker compose logs --tail=100 webserver daemon'
```

A repeatedly restarting container with little application output can indicate
an out-of-memory kill. Check host logs and memory, then stop competing work or
move to a larger machine type.

### Runs remain queued

The webserver submits runs; the daemon executes them. Confirm that the daemon is
running and inspect its logs:

```bash
gcloud compute ssh dagster-vm \
  --zone=us-central1-a \
  --tunnel-through-iap \
  --command='cd /opt/dagster/app && sudo docker compose ps && sudo docker compose logs --tail=100 daemon'
```

Stopping the daemon also stops schedule and automation-condition evaluation, so
raw ingestion can no longer trigger the dbt layer until the service returns.

### Image pull or SSH authorization fails

- Image pull denied: check `artifactregistry.reader` on the VM identity and
  `artifactregistry.writer` on the deploy identity.
- IAP tunnel times out: check the VM tag, allow rule, external state, and
  `iap.tunnelResourceAccessor`.
- SSH connects but `sudo` fails: the deploy identity needs
  `compute.osAdminLogin`, not only `compute.osLogin`.
- Login is refused after the tunnel opens: check `serviceAccountUser` on the
  VM's attached service account.
