# orchestration/deploy/ — Dagster, running the pipeline

    docker compose --env-file ../../.env up --build

UI on `localhost:3002`. Two services off one image, both loading the code
location with `-m orchestration.definitions`:

| Service | Role |
|---|---|
| `webserver` | The UI. Submits runs, shows history and lineage. |
| `daemon` | **Executes every run**, and ticks `daily_refresh` once enabled. |

No database: Dagster's default storage is SQLite under `DAGSTER_HOME`
(`orchestration/dagster.yaml`), bind-mounted from the host.

## Three things that will bite you

**`--env-file` is required on every command.** Compose interpolates `${...}` in
`docker-compose.yml` at parse time, from the shell or `--env-file` only — the
`env_file:` key is read later, when containers start, and cannot supply a volume
path. A `.env` symlink here (`ln -s ../../.env .env`) is picked up
automatically and drops the flag, but it is gitignored, so it is per-clone.

**The daemon is not optional.** The default run coordinator is the queued one,
so a run submitted in the UI is only queued there — the daemon picks it up and
executes it. Stop the daemon and runs sit in `QUEUED` forever; restart it and
you kill whatever is running. Run logs: `docker compose logs daemon`.

**Which history you see depends on your shell.** The mount is
`${DAGSTER_HOME:-..}`, and Compose reads the shell before `--env-file`. With
`~/.zshrc` exporting `DAGSTER_HOME=$HOME/dagster_home` you get that instance;
without it you get `orchestration/`. A run you cannot find is usually this. Pin
it explicitly:

    DAGSTER_HOME=$HOME/dagster_home docker compose --env-file ../../.env up

## Runs are real, and cost real money

A full materialisation downloads 126 MB from Kaggle, writes nine CSVs to
`gs://$GCP_RAW_BUCKET/<today>/`, and replace-loads nine tables into
`$BIGQUERY_RAW_DATASET`, on your GCP bill. Idempotent (§4), but not a dry run.

The graph is `kaggle_dataset` → `gcs_raw_files` → nine `olist_raw/*` tables.
**dbt is in the graph now** — `dbt_models()` in
`assets.py` are still `TODO(A1)`/`TODO(B1)`. Both are installed in the image and
will run here once those are implemented.

`daily_refresh` (08:00 SGT, `AssetSelection.all()`) is off until you toggle it
on in the UI. GitHub Actions is still what fires the scheduled run in
production (§8).

## Credentials

Read from the same `.env` the laptop uses. The service-account key is
bind-mounted read-only from the host path in `GOOGLE_APPLICATION_CREDENTIALS`
to `/opt/dagster/secrets/gcp-sa.json` — never copied into the image, which is
also why `.dockerignore` excludes `*.json`.

The pipeline needs object-level GCS access, not bucket metadata: a
`roles/storage.objectAdmin` account returns 403 on `storage.buckets.get` and
still runs correctly.

## Volumes

`kaggle-cache` (kagglehub downloads) and `staging` (`data/staging`) are named
volumes, so a re-run does not pay the 126 MB again. `docker compose down -v`
discards both.

---

# dagster-vm — the same two services, hosted

Everything above runs Compose on your laptop. This section runs it on a GCP
`e2-micro` called `dagster-vm`, so the daemon is alive at 08:00 and the run
history in the UI is not whatever happens to be on the machine you opened it
from.

| | |
|---|---|
| `provision_vm.sh` | Creates the VM and everything around it. Run once, by hand. |
| `startup-script.sh` | Runs on the VM at every boot. Installs Docker, swap, the AR credential helper. |
| `docker-compose.vm.yml` | The hosted variant of `docker-compose.yml`. Pulls, does not build. |
| `.github/workflows/deploy-dagster.yml` | Builds the image, pushes it, rolls the VM. Runs on push to `main`. |

## Read §8 first

`docs/architecture-design.md` §8 argues against exactly this: an `e2-micro` has
1 GB, and the reference Dagster Compose topology needs 1.5–2.5 GB. That
arithmetic still holds — what makes this fit is that our Compose file is not
the reference one. No Postgres (SQLite under `DAGSTER_HOME`), no separate gRPC
code server (each service loads `-m orchestration.definitions` itself), and no
per-run container (the `DefaultRunLauncher` executes inside the daemon). That
is roughly 450–550 MB resident for the two services, plus whatever a
materialisation adds on top.

**It is still tight, and the mitigation is swap.** `startup-script.sh` puts 2 GB
on the boot disk. Expect a materialisation to be slower here than on a laptop;
expect it to complete. If it does not, the machine type is the dial —
`MACHINE_TYPE=e2-small ./provision_vm.sh` on a fresh VM, and it stops being
free.

This does not replace `pipeline.yml`, but it does take the schedule off it.
The daemon here holds the daily cron; `pipeline.yml` is `workflow_dispatch`
only and publishes the dbt docs site to Pages, which remains the durable
evidence §8 describes. Two schedulers firing `AssetSelection.all()` at the same
instant would race on the same BigQuery tables (`orchestration/schedules.py`).
The VM adds live UI and persistent history.

## Provisioning

    ./provision_vm.sh --dry-run                                  # see the plan
    DEPLOY_SA=<account behind GCP_SA_KEY> ./provision_vm.sh       # do it

`DEPLOY_SA` is optional in the sense that the script runs without it, and
required in the sense that the deploy does not. With it, the script grants the
workflows' account the five roles listed under [Permissions](#permissions)
below. Without it, step 3b skips them and says so — and the resulting deploy
validates its secrets, builds a 2 GB image, and fails on the push.

Idempotent — re-run it freely. An **existing** VM is never recreated, but it is
checked against the design, and the three ways it can differ each fail later
and obscurely:

| Drift | How it actually surfaces |
|---|---|
| No `dagster-vm` network tag | The IAP firewall rule does not select the instance, so the deploy hangs at the tunnel and times out. Looks like a network fault, is a policy one. |
| No external IP | `kaggle_dataset` cannot reach kaggle.com, so the first asset in the graph fails on a connection timeout — which reads as a Kaggle outage, not a missing address. |
| Default Compute Engine service account | Nothing fails — that account is project Editor, so every pull and query works. The blast radius of the box is the whole project. |

The script prints the fix for each. `--adopt` applies them; two involve a
stop/start, which is why it is opt-in rather than the default.

## Permissions

Three identities, and keeping them straight is most of the debugging. Every
failure below is one an IAM gap actually produces — none of them says
"permission denied" where you would look for it.

| Identity | Is | Authenticates via |
|---|---|---|
| `dagster-vm@` | The VM, and therefore the pipeline when it runs there | Attached to the instance; token from the metadata server |
| `bigquery-admin@` | Both GitHub Actions workflows — `deploy-dagster.yml` **and** `pipeline.yml` | The `GCP_SA_KEY` repository secret |
| you | `gcloud` on your laptop | `gcloud auth login` (project Owner) |

`bigquery-admin@` doing double duty is why it needs an odd-looking union of
roles: some are for pushing an image and opening a tunnel, others for running
the pipeline on an Actions runner. Splitting it into a `github-deploy@` and a
`github-ci@` would be tidier; it would also mean two keys and two secrets.

### The VM's identity — `dagster-vm@`

Granted by `provision_vm.sh` unconditionally, at project scope.

| Role | Why | Missing it looks like |
|---|---|---|
| `artifactregistry.reader` | Pull the image | `docker compose pull` fails with `denied`, on a VM that holds no registry credential to blame |
| `storage.objectAdmin` | Write nine CSVs to the raw zone | `gcs_raw_files` fails; note this is object-level, so `storage.buckets.get` still 403s and that is *not* the fault |
| `bigquery.dataEditor` | Replace-load nine tables | dlt fails on the first table |
| `bigquery.jobUser` | Run the load jobs at all | dataEditor without this fails at job submission, which reads as a dataset problem |
| `logging.logWriter` | Ship container logs to Cloud Logging | Silent; logs stay on the box only |

Deliberately *not* `storage.admin` or `bigquery.admin`: the pipeline needs
object and table access, not bucket or dataset administration.

### The workflows' identity — `bigquery-admin@`

`provision_vm.sh` grants the first five **only when given `DEPLOY_SA`**. It
skips them silently otherwise and says so at step 3b — which is easy to miss,
and produces a deploy that validates its secrets, builds a 2 GB image, and then
fails on the push.

| Role | Scope | Needed by | Missing it looks like |
|---|---|---|---|
| `iap.tunnelResourceAccessor` | project | deploy | `--tunnel-through-iap` times out — reads as a network or firewall fault |
| `compute.osAdminLogin` | project | deploy | SSH connects, then `sudo` is refused: `/opt/dagster` is root-owned. Plain `osLogin` grants no sudo and is not enough |
| `compute.viewer` | project | deploy | Cannot resolve the instance before connecting |
| `iam.serviceAccountUser` | **`dagster-vm@` only** | deploy | The tunnel opens and the login is refused. SSHing to an instance with an attached service account counts as *acting as* it |
| `artifactregistry.writer` | **`dagster` repo only** | deploy | The push fails after the full build has already run |
| `storage.objectAdmin` | project | `pipeline.yml` | `gcs_raw_files` fails on the runner. **Not granted by the script** — it belongs to the pipeline's identity, not the deployment's |

It already holds `roles/bigquery.admin`, which is broader than the
`dataEditor` + `jobUser` pair the VM gets. Pre-existing, and left alone.

### Granting them

The script, for the five deploy roles — idempotent, so a re-run adds only what
is missing:

    DEPLOY_SA=bigquery-admin@ntu-dsai-6-ycw.iam.gserviceaccount.com \
      ./provision_vm.sh

It then continues to step 6 and can sit for up to ten minutes on the SSH wait,
so the same bindings by hand, plus the one the script does not cover:

    PROJECT=ntu-dsai-6-ycw
    DEPLOY_SA=bigquery-admin@${PROJECT}.iam.gserviceaccount.com
    VM_SA=dagster-vm@${PROJECT}.iam.gserviceaccount.com

    for ROLE in roles/iap.tunnelResourceAccessor \
                roles/compute.osAdminLogin \
                roles/compute.viewer \
                roles/storage.objectAdmin; do
      gcloud projects add-iam-policy-binding "$PROJECT" \
        --member="serviceAccount:$DEPLOY_SA" --role="$ROLE" \
        --condition=None --quiet >/dev/null && echo "OK $ROLE"
    done

    gcloud iam service-accounts add-iam-policy-binding "$VM_SA" \
      --member="serviceAccount:$DEPLOY_SA" \
      --role=roles/iam.serviceAccountUser --condition=None --quiet

    gcloud artifacts repositories add-iam-policy-binding dagster \
      --location=us-central1 --member="serviceAccount:$DEPLOY_SA" \
      --role=roles/artifactregistry.writer --quiet

`--condition=None` is not decoration: without it gcloud asks whether the
binding should be conditional, and a prompt in a non-interactive shell hangs
with no output.

### Auditing them

    for SA in dagster-vm bigquery-admin; do
      echo "== $SA"
      gcloud projects get-iam-policy ntu-dsai-6-ycw \
        --flatten="bindings[].members" \
        --filter="bindings.members:serviceAccount:$SA@ntu-dsai-6-ycw.iam.gserviceaccount.com" \
        --format="value(bindings.role)" | sort
    done

Project-scope only. The two narrow bindings live on their own resources:

    gcloud iam service-accounts get-iam-policy dagster-vm@ntu-dsai-6-ycw.iam.gserviceaccount.com
    gcloud artifacts repositories get-iam-policy dagster --location=us-central1

## Networking: public egress, private ingress

The VM has an external IP. It is not reachable on it.

That combination is the point. Egress is what the pipeline needs — `kaggle_dataset`
downloads 126 MB from kaggle.com, and a VM with no external address has no route
to it. Ingress is what nobody needs: SSH and the UI both arrive over IAP TCP
forwarding, whose forwarders live in `35.235.240.0/20` and cannot be spoofed
from outside GCP.

Two firewall rules, both tagged `dagster-vm`, and **the second is not
redundant**:

| Priority | | | |
|---|---|---|---|
| 800 | ALLOW | `tcp:22,3002` from `35.235.240.0/20` | tag `dagster-vm` |
| 900 | DENY | `tcp:22,3002,3389` from `0.0.0.0/0` | tag `dagster-vm` |
| 1000 | ALLOW | `tcp:22` from `0.0.0.0/0` | **every VM** — GCP's default |

The default VPC ships `default-allow-ssh`, which opens port 22 to the entire
internet for every instance on the network and carries no target tag. Give this
VM a public address and that rule alone puts its SSH on the internet. Lower
priority wins, so the deny at 900 is evaluated before it, and being tag-scoped
it closes the hole for this machine without touching a shared rule that other
VMs on the project may rely on. `provision_vm.sh` prints the untagged
internet-wide rules it finds, so you can see what it is working around.

### What it costs, honestly

Not zero. An earlier draft of this file put Cloud NAT at "USD 1–2/month", which
was wrong — it omitted the NAT gateway's own external IP. At 730 hours:

| | |
|---|---|
| External IP on the VM, no NAT | `$0.005/hr` → **≈ $3.65/mo** |
| No external IP + Cloud NAT | `$0.0014/hr` VM + `$0.005/hr` NAT's own IP + `$0.045/GiB` × ~3.8 GiB → **≈ $4.84/mo** |

Public is the cheaper of the two, by about a dollar a month, precisely because
Cloud NAT needs a billable external address of its own. The free tier covers
the `e2-micro` and its 30 GB disk; it covers neither external IP.

Stopping the VM releases the ephemeral address and stops that charge:

    gcloud compute instances stop dagster-vm --zone us-central1-a

**The UI has no authentication.** That is why the deny rule is not optional and
why 3002 is not simply opened to your own IP as a convenience. Anyone who
reaches the UI can launch a materialisation that spends money in your GCP
project.

## Reaching it

The UI has no address. Open a tunnel and it is on localhost:

    gcloud compute start-iap-tunnel dagster-vm 3002 \
      --local-host-port=127.0.0.1:3002 --zone us-central1-a

    # then http://127.0.0.1:3002

**`127.0.0.1`, not `localhost`.** On macOS `localhost` resolves to `::1` first,
so the tunnel binds IPv6 and then logs a stream of these as the browser tears
connections down:

    ERROR: [17] Error during local connection to [('::1', 55035, 0, 0)]:
    [Errno 9] Bad file descriptor

They are noise, not failure — gcloud logs an ordinary closed socket at ERROR
level, and a browser closes sockets constantly (speculative connections,
keep-alive expiry, and Dagster's own GraphQL WebSocket reconnecting). Binding
IPv4 avoids them: the same session over `127.0.0.1` serves the page, GraphQL
and six concurrent requests with an empty log.

The `NumPy` warning gcloud prints on startup is unrelated. It affects tunnel
*upload* bandwidth, and browsing a UI is almost entirely download.

Keep the tunnel in the foreground — it is the connection, not a daemon.

### `4003: failed to connect to backend`

A different error, and a real one — but almost always transient:

    ERROR: [52] Error during local connection to [('127.0.0.1', 55942)]:
    Error while connecting [4003: 'failed to connect to backend'].
    (Failed to connect to port 3002)

This means the tunnel reached Google and Google could not reach port 3002 *on
the VM*. Nine times in ten you are tunnelling during a deploy. `docker compose
up -d` stops the old webserver before starting the new one, and the new one
then spends 40–90 seconds importing dbt, dlt and pandas before it binds — so
there is a window, once per deploy, where nothing is listening.

**There is no zero-downtime version of this on one 1 GB node.** Rolling a new
container up beside the old one means two webservers resident at once, and the
memory is not there. A minute of UI downtime per deploy is the cost of the
machine size, not a bug.

The tunnel process survives it — the failure is per-connection, and the
listener stays up — so wait and reload the page. If the tunnel itself exited,
restart it.

When it is *not* transient, check in this order:

    # is anything actually listening?
    gcloud compute ssh dagster-vm --zone us-central1-a --tunnel-through-iap \
      --command 'sudo ss -lntp | grep 3002; sudo docker ps -a'

    # did the webserver crash on boot? (OOM shows up here)
    gcloud compute ssh dagster-vm --zone us-central1-a --tunnel-through-iap \
      --command 'sudo docker logs --tail=50 olist-dagster-webserver-1'

A container in `Restarting` with nothing in the log is the OOM signature — see
[Read §8 first](#read-8-first) for the memory headroom this machine has.

    gcloud compute ssh dagster-vm --zone us-central1-a --tunnel-through-iap

    gcloud compute ssh dagster-vm --zone us-central1-a --tunnel-through-iap \
      --command 'cd /opt/dagster/app && sudo docker compose logs -f daemon'

`sudo` throughout: `/opt/dagster` is root-owned, and OS Login with
`roles/compute.osAdminLogin` is what grants it.

## Two workflows, and which one schedules

`pipeline.yml` and `deploy-dagster.yml` do different jobs and no longer overlap
— but they did, and the overlap was silent.

| | |
|---|---|
| `deploy-dagster.yml` | Builds the image, pushes it, rolls the VM. Runs nothing. Push to `main`, path-filtered. |
| `pipeline.yml` | Runs the pipeline once and publishes the dbt docs site to Pages. **`workflow_dispatch` only.** |
| `daily_refresh` on the VM | The scheduler. 08:00 SGT, `AssetSelection.all()`, `default_status=RUNNING`. |

**What changed.** §8 gave `pipeline.yml` the scheduler role because nothing else
was always on. Its cron was 00:00 UTC; `schedules.py` declares 08:00 SGT — the
same instant. Once a daemon is running on the VM, both fire `AssetSelection.all()`
simultaneously and race on replace-loads into the same nine BigQuery tables.
So the cron is gone, and so is `pipeline.yml`'s `push: main` trigger: a full run
is a 126 MB download, nine GCS writes and nine table loads, which is not what
every commit to main should spend.

`tests/test_orchestration_definitions.py` used to assert that `pipeline.yml`'s
cron matched the declared schedule. It now asserts that `pipeline.yml` declares
*no* cron, because restoring one is the failure mode that would be hardest to
attribute.

**The gap, stated rather than discovered:** nothing publishes reports after the
VM's nightly run. Dispatch `pipeline.yml` when you want current Data Docs, or
add a step that pulls the artifacts off the VM. The run history for scheduled
runs is now durable on the VM — which §8 said it could not be — but the
published reports are no longer automatic.

## Deploying

Push to `main` — the workflow is path-filtered to what actually lands in the
image — or `gh workflow run deploy-dagster.yml`.

It builds on a GitHub runner and the VM only pulls, because `uv sync` over
pandas, dbt and dlt on a shared-core `e2-micro` takes tens of minutes when it
does not run out of memory first.

Secrets and variables it reads:

| Repository secret | |
|---|---|
| `GCP_SA_KEY` | The deploy account's JSON key. Also written to the VM for dbt — see below. Already used by `pipeline.yml`. |
| `GCP_PROJECT`, `GCP_RAW_BUCKET`, `KAGGLE_API_TOKEN` | Already used by `pipeline.yml`. |

| Repository variable | Default |
|---|---|
| `DEPLOY_DAGSTER` | unset — deploys. Set to `false` to pause; see [Pausing deployments](#pausing-deployments) |
| `GCP_LOCATION` | `US` |
| `BIGQUERY_RAW_DATASET` | `olist_raw` |
| `BIGQUERY_MARTS_DATASET` | `olist_marts` |
| `DBT_TARGET` | `prod` — the profiles.yml target the scheduled run builds into |

### Three things worth knowing

**The image tag is the commit SHA, and that is load-bearing.** Compose decides
a container is stale by comparing image *references*. With a floating `:latest`
the pull would fetch new layers and `up -d` would leave the old container
running — a deploy that reports success while the previous code keeps serving.
The workflow writes the SHA into the VM's `.env`; that value changing is the
entire restart mechanism.

**Nothing on the VM holds a registry credential.** The image pull authenticates
as the VM's *attached* service account, through the gcloud Docker credential
helper the startup script configures for root, against a token from the
metadata server.

**The service-account key on the VM is `transform/profiles.yml`'s doing.** Every
target there is `method: service-account` with an explicit `keyfile`, so dbt
needs a file on disk even though the VM already has an identity that could
serve the same purpose. The workflow delivers it on every deploy to a 0700
directory as `root:0600`, staged through a 0700 directory in the login user's
home so it is never briefly world-readable in `/tmp`. Rotating it is a re-run
of the workflow, not a login to the machine. Adding a `method: oauth` target to
`profiles.yml` would let the key and its mount disappear entirely — that is a
profiles change, not a deployment one.

## Pausing deployments

    gh variable set DEPLOY_DAGSTER --body false     # pause
    gh variable delete DEPLOY_DAGSTER               # resume
    gh workflow run deploy-dagster.yml -f force=true  # deploy once anyway

A `guard` job reads it and the `deploy` job runs only if it says so. `false`,
`no`, `off` and `0` all pause (case-insensitively); **anything else deploys**,
including unset and unrecognised values. That asymmetry is deliberate — a kill
switch that failed toward "off" would silently disable deployment for anyone
who forked the repo or re-created its variables.

The switch is a whole job rather than an `if:` on `deploy` because a skipped job
renders as a grey tick with the condition hidden behind a hover. The failure
mode that buys you is someone pushing, seeing green, and assuming their change
is live. The guard job writes the decision, and how to undo it, into the run
summary instead.

### What it actually saves, and what it does not

Be clear about this before relying on it to control spend. Pausing deployments
stops:

- **~8 minutes of Actions runner time per push.** On a private repo that counts
  against the monthly free allowance.
- **A new ~2 GB image in Artifact Registry per deploy.** Worth watching: the
  weekly prune in `startup-script.sh` reclaims superseded images *on the VM*,
  and nothing prunes the registry. Every SHA-tagged version is retained, and
  Artifact Registry is free only to 0.5 GB. Ten deploys is ~20 GB.

It does **not** stop:

- **The external IP, ~$3.65/mo**, billed for as long as the VM is running.
- **`daily_refresh` at 08:00 SGT**, which keeps firing on the image already
  deployed — with the full Kaggle download, nine GCS writes and nine BigQuery
  replace-loads each time.

So this is a switch for "stop shipping changes", not for "stop spending". The
levers for spend, in increasing order of severity:

    # stop the nightly run, keep the UI and history
    #   toggle daily_refresh off in the UI, or:
    #   dagster schedule stop daily_refresh   (inside the daemon container)

    # stop everything; releases the IP, keeps the disk and all run history
    gcloud compute instances stop dagster-vm --zone us-central1-a

    # delete old images from the registry (keeps the two most recent)
    gcloud artifacts docker images list \
      us-central1-docker.pkg.dev/ntu-dsai-6-ycw/dagster/olist-dagster \
      --format='value(version)' --sort-by=~CREATE_TIME | tail -n +3 \
      | xargs -I{} gcloud artifacts docker images delete \
          us-central1-docker.pkg.dev/ntu-dsai-6-ycw/dagster/olist-dagster@{} --quiet

Stopping the VM is the one that matters. It releases the ephemeral address, so
the only remaining charge is the boot disk — 30 GB standard, inside the free
tier. Starting it again re-runs the startup script and the containers come back
on their own (`restart: unless-stopped` plus an enabled `docker.service`).

## Teardown

    gcloud compute instances delete dagster-vm --zone us-central1-a
    gcloud compute firewall-rules delete allow-iap-dagster deny-public-dagster

Deleting the instance releases its ephemeral IP, so nothing keeps billing
behind it. To pause rather than remove — the schedule stops firing, the history
survives, the IP charge stops:

    gcloud compute instances stop dagster-vm --zone us-central1-a

If you tear the VM down, restore the scheduler: `daily_refresh` is the only
thing firing the daily run now (see below).
