# Dagster deployment

The deployment runs the project image as two services:

| Service | Responsibility |
|---|---|
| `webserver` | Dagster UI, lineage, run submission, and history |
| `daemon` | Schedule/sensor evaluation and queued run execution |

Both load `orchestration.definitions` and share a SQLite-backed
`DAGSTER_HOME`. There is no Postgres service or separate code server.

## Run locally with Docker Compose

From this directory:

```bash
docker compose --env-file ../../.env up --build -d
docker compose --env-file ../../.env logs -f daemon
```

Open <http://localhost:3002>. `--env-file` is required because Compose must
resolve the service-account mount before containers start.

Runs use real Kaggle, Cloud Storage, and BigQuery credentials. Stop the stack
without deleting cached downloads or run history:

```bash
docker compose --env-file ../../.env down
```

Adding `-v` also deletes the Kaggle and staging volumes.

## Hosted topology

`docker-compose.vm.yml` runs the same two services on a GCP VM named
`dagster-vm`:

- GitHub Actions builds and pushes a SHA-tagged image.
- The VM pulls the image; it does not build locally.
- `/opt/dagster/dagster_home` preserves SQLite history and automation state.
- A service-account key is mounted read-only at runtime.
- The daemon runs ingestion at 08:00 SGT and evaluates dbt automation
  conditions every 60 seconds.

The default `e2-micro` is memory-constrained. A 2 GB swap file and log
rotation make the small deployment practical, but runs are slower and the
daemon remains a single point of failure.

## Provision the VM

Authenticate as a project administrator, then preview and apply the setup:

```bash
gcloud auth login
./provision_vm.sh --dry-run
DEPLOY_SA=<github-actions-service-account> ./provision_vm.sh
```

The script creates or validates:

- required Compute, Artifact Registry, IAP, and OS Login APIs
- an Artifact Registry repository
- the VM service account and least-privilege project roles
- IAP-only allow rules and explicit public-ingress deny rules
- the VM, startup script, and hosted Compose file

It is idempotent. Use `--adopt` only when intentionally bringing an existing
VM's network tag, external address, or service account back to the declared
configuration; that operation can stop and restart the VM.

## Deploy

The `deploy-dagster` workflow runs for relevant changes on `main` or by manual
dispatch:

```bash
gh workflow run deploy-dagster.yml
```

Required repository secrets:

- `GCP_PROJECT`
- `GCP_RAW_BUCKET`
- `KAGGLE_API_TOKEN`
- `GCP_SA_KEY`

Optional repository variables default to `US`, `olist_raw`, `olist_marts`,
and the `prod` dbt target. `DEPLOY_DAGSTER=false` pauses automatic deployments:

```bash
gh variable set DEPLOY_DAGSTER --body false
gh variable delete DEPLOY_DAGSTER
gh workflow run deploy-dagster.yml -f force=true
```

Pausing deployment does not stop the VM or the daily pipeline.

## Access and troubleshoot

The UI has no public route. Start an Identity-Aware Proxy tunnel:

```bash
gcloud compute start-iap-tunnel dagster-vm 3002 \
  --local-host-port=127.0.0.1:3002 \
  --zone=us-central1-a
```

Then open <http://127.0.0.1:3002>. During deployment, the UI can be unavailable
for about a minute while the new webserver imports the project.

Check containers and daemon logs through IAP:

```bash
gcloud compute ssh dagster-vm \
  --zone=us-central1-a \
  --tunnel-through-iap \
  --command='cd /opt/dagster/app && sudo docker compose ps && sudo docker compose logs --tail=100 daemon'
```

If UI runs stay queued, check the daemon first: it both dequeues runs and
evaluates the schedule and sensor.

## Security and cost boundaries

- The external address is for outbound Kaggle access; ports 22 and 3002 accept
  traffic only from IAP's forwarding range.
- The UI has no application-level authentication, so the public deny rule is
  required.
- Images contain code and dependencies only. Credentials arrive from Actions
  secrets and are mounted at runtime.
- Run history is durable on the VM disk but is not replicated or backed up.
- Stopping the VM releases its ephemeral address and stops scheduled runs.

Pause the host while keeping its disk and history:

```bash
gcloud compute instances stop dagster-vm --zone=us-central1-a
```

Remove the host and firewall rules:

```bash
gcloud compute instances delete dagster-vm --zone=us-central1-a
gcloud compute firewall-rules delete allow-iap-dagster deny-public-dagster
```

`pipeline.yml` is separate: it runs on demand to publish dbt Docs. It is not a
scheduler and does not compete with the hosted daemon.
