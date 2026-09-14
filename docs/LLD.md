<div align="center">

# 🔧 LLD — Real-Time Machine Telemetry Streaming Pipeline

**Repo:** `gcp-streaming-telemetry-pipeline` · **Project:** `project-streaming-telemetry`

</div>

---

## 📁 0. Repo Structure

```
gcp-streaming-telemetry-pipeline/
├── 📄 README.md
├── 📁 docs/
│   ├── 📄 HLD.md
│   └── 📄 LLD.md
├── 📁 terraform/
│   ├── 📁 envs/lab/
│   │   ├── 📄 main.tf          # networking, pubsub, bigquery, iam
│   │   ├── 📄 cicd.tf          # WIF, Artifact Registry, GCS buckets, required APIs
│   │   ├── 📄 backend.tf       # GCS remote state
│   │   └── 📄 variables.tf
│   └── 📁 modules/              # scaffolded, not yet populated — see §10
│       ├── 📁 pubsub/
│       ├── 📁 bigquery/
│       ├── 📁 networking/
│       ├── 📁 iam/
│       └── 📁 cloud-function/
├── 📁 simulator/
│   └── 📄 publisher.py          # telemetry simulator (temperature + vibration, DLQ test payloads)
├── 📁 dataflow/
│   ├── 📄 pipeline.py            # Beam pipeline (Flex Template source)
│   ├── 📄 test_pipeline.py       # 11 unit tests, Apache Beam TestPipeline
│   ├── 📄 metadata.json          # Flex Template spec
│   ├── 📄 Dockerfile             # template launcher image
│   └── 📄 requirements.txt
├── 📁 cloud-function/
│   └── 📁 gemini-diagnostics/
│       ├── 📄 main.py
│       ├── 📄 conftest.py        # stubs GCP SDKs so tests need no live credentials
│       ├── 📄 test_main.py       # 5 unit tests
│       └── 📄 requirements.txt
└── 📁 .github/
    └── 📁 workflows/
        └── 📄 deploy-pipeline.yml   # validate → test → apply → build → deploy
```

## 🌐 1. Networking

| Resource | Name | Notes |
|---|---|---|
| VPC | `telemetry-vpc` | `auto_create_subnetworks = false` |
| Subnet | `telemetry-subnet` | `10.10.0.0/24`, `private_ip_google_access = true` |
| Router | `telemetry-router` | for Cloud NAT |
| NAT | `telemetry-nat` | `AUTO_ONLY`, all subnet ranges |
| Firewall | `telemetry-allow-internal` | tcp+udp `12345-12346`, source `10.10.0.0/24` — required for Dataflow worker-to-worker comms on a private subnet |

> ⚠️ **Org-policy gotcha:** this org blocks external IPs on Compute Engine
> VMs by default. Dataflow's launcher VM requests one unless explicitly
> told not to — every Flex Template launch **must** pass
> `--disable-public-ips`, or the launcher VM fails to provision and the
> job dies in under a second with an empty pipeline graph.

## 📬 2. Pub/Sub

| Resource | Name | Notes |
|---|---|---|
| Topic | `telemetry-input-topic` | Main ingestion |
| Subscription | `telemetry-input-sub` | Pull, consumed by Dataflow, `ack_deadline_seconds = 30` |
| Topic | `telemetry-dlq-topic` | Malformed/failed payloads |
| Topic | `telemetry-alerts-topic` | Threshold-breach events. No explicit subscription in Terraform — the 2nd-gen Cloud Function's Eventarc trigger provisions its own push subscription on deploy |

**Raw payload schema (JSON):**
```json
{
  "machine_id": "MCH-PLANT-01-001",
  "temperature": 87.42,
  "vibration": 4.8,
  "timestamp": "2026-09-11T12:00:00Z"
}
```

**Alert payload schema (JSON):**
```json
{
  "machine_id": "MCH-PLANT-01-001",
  "avg_temperature": 82.1,
  "avg_vibration": 5.3,
  "window_end": "2026-09-11T12:05:00Z"
}
```

## 📊 3. BigQuery

Dataset: `telemetry_analytics` (`deletion_protection = false` — fast lab teardown/rebuild)

**Table: `telemetry_aggregates`**
| Column | Type | Mode |
|---|---|---|
| machine_id | STRING | REQUIRED |
| avg_temperature | FLOAT | REQUIRED |
| avg_vibration | FLOAT | REQUIRED |
| window_end | TIMESTAMP | REQUIRED |
| calculated_at | TIMESTAMP | REQUIRED |

**Table: `incident_log`**
| Column | Type | Mode |
|---|---|---|
| incident_id | STRING | REQUIRED |
| machine_id | STRING | REQUIRED |
| avg_temperature | FLOAT | REQUIRED |
| avg_vibration | FLOAT | REQUIRED |
| diagnostic | STRING | NULLABLE |
| detected_at | TIMESTAMP | REQUIRED |

## 🌊 4. Dataflow (Flex Template)

- Packaged as a Flex Template, built via Docker + `gcloud dataflow flex-template build`, deployed via `gcloud dataflow flex-template run`.
- Parsing uses `beam.pvalue.TaggedOutput` — malformed JSON or missing fields tagged `dlq`.
- 🪟 **Windowing: 1-minute tumbling (fixed) windows** — `beam.WindowInto(beam.window.FixedWindows(60))`.
- `CombinePerKey` aggregates both `temperature` and `vibration` per `machine_id` per window.
- Window-end timestamp read via `beam.DoFn.WindowParam`, with an explicit `Z` suffix on both timestamps for unambiguous UTC parsing:
  ```python
  class FormatBQFn(beam.DoFn):
      def process(self, element, window=beam.DoFn.WindowParam):
          machine_id, agg_metrics = element
          window_end_iso = window.end.to_utc_datetime().isoformat() + 'Z'
          calculated_at_iso = datetime.now(timezone.utc).isoformat()
          yield {
              "machine_id": machine_id,
              "avg_temperature": round(agg_metrics['avg_temperature'], 2),
              "avg_vibration": round(agg_metrics['avg_vibration'], 2),
              "window_end": window_end_iso,
              "calculated_at": calculated_at_iso,
          }
  ```
- 🎯 Breach filter: `avg_temperature > 80.0 OR avg_vibration > 5.0` (strict `>`, pinned by a unit test) → `telemetry-alerts-topic`.
- ⚖️ Autoscaling: `--autoscaling_algorithm=THROUGHPUT_BASED --max_num_workers=10`, `--enable_streaming_engine`.
- 🖥️ **Worker machine type: `e2-standard-2`**, not the default `n1-standard-2`. Switched after repeatedly hitting `ZONE_RESOURCE_POOL_EXHAUSTED` in `us-central1` — a genuine, temporary Compute Engine capacity shortage, not a config bug.
- 🔐 Worker network: `telemetry-subnet`, private IP only (`--disable-public-ips`), egress via `telemetry-nat`.
- 🔁 Job launched/updated idempotently: fixed job name `telemetry-pipeline-lab`; CI checks for an existing active job and passes `--update` if found, otherwise launches fresh.

## ⚡ 5. Cloud Function — `gemini-diagnostics`

- Trigger: **2nd-gen, Eventarc, on `telemetry-alerts-topic`** — not `telemetry-input-topic` (would fire on every raw message).
- Runtime: Python 3.12.
- Gemini client initialized against the Vertex AI backend, not the Developer API default:
  ```python
  from google import genai
  client = genai.Client(vertexai=True, project=PROJECT_ID, location="us-central1")
  ```
- Logic: parse alert → `generate_content(model="gemini-2.5-flash", ...)` → write to `incident_log` with `row_ids=[incident_id]` for dedup → structured log.
- 🔁 Idempotency: `incident_id` = SHA-256 hash of `machine_id` + `window_end`, truncated to 16 chars, actually passed as `row_ids`.
- Errors logged and swallowed (at-most-once trade-off for a lab); production would re-raise for Eventarc retry, relying on the idempotency key.

> ⚠️ **Eventarc invoker gotcha:** a gen2 Cloud Function triggered by Pub/Sub
> is a Cloud Run service underneath. Eventarc invokes it using the
> function's runtime SA (`sa-gemini-function`) — but that account needs
> `roles/run.invoker` **on the Cloud Run service itself**, separate from
> its own runtime permissions:
> ```bash
> gcloud run services add-iam-policy-binding gemini-diagnostics \
>   --region=us-central1 \
>   --member="serviceAccount:sa-gemini-function@project-streaming-telemetry.iam.gserviceaccount.com" \
>   --role="roles/run.invoker"
> ```
> Without this, every trigger fails with `IAM principal lacks {run.routes.invoke} permission` — a fully "successful" deploy, silently non-functional.

## 🔐 6. IAM (final state)

| Service Account | Roles | Purpose |
|---|---|---|
| `sa-dataflow-worker` | `dataflow.worker`, `pubsub.subscriber`, `pubsub.publisher`, `bigquery.dataEditor`, `artifactregistry.reader`, `pubsub.viewer` | Runs the Dataflow job; `artifactregistry.reader` needed to pull its own image; `pubsub.viewer` needed to inspect subscription config at startup |
| `sa-gemini-function` | `aiplatform.user`, `bigquery.dataEditor` (project) + `run.invoker` (Cloud Run resource) | Calls Gemini, writes incidents, must be independently authorized to be *invoked* |
| `sa-github-actions` | `editor`, `resourcemanager.projectIamAdmin`, `iam.serviceAccountAdmin`, `iam.serviceAccountUser`, `artifactregistry.writer`, `dataflow.developer`, `cloudfunctions.developer`, `iam.workloadIdentityPoolAdmin`, `run.admin` | CI/CD identity via WIF — broad by design for build velocity |
| `{project-number}@cloudbuild.gserviceaccount.com` | `cloudbuild.builds.builder` | Legacy Cloud Build SA — newer projects don't auto-grant this |
| `{project-number}-compute@developer.gserviceaccount.com` | `cloudbuild.builds.builder`, `artifactregistry.writer`, `logging.logWriter` | Since mid-2024, gen2 function/Cloud Build v2 builds actually run as **this** SA on newer projects |

All bindings are `google_project_iam_member` (project-scoped) except the Cloud Run `run.invoker` grant. A production hardening pass would scope the rest similarly.

**Required APIs** (declared via `google_project_service`): `compute`, `dataflow`, `pubsub`, `bigquery`, `aiplatform`, `cloudfunctions`, `run`, `cloudbuild`, `eventarc`, `artifactregistry`, `iamcredentials`, `sts`. `eventarc` was the one missing initially — its absence broke the Cloud Function deploy with an unrelated-looking error.

## 🔁 7. CI/CD Infrastructure

| Resource | Name | Notes |
|---|---|---|
| 📦 Artifact Registry | `telemetry-images` | Dataflow Flex Template launcher image |
| 🪣 GCS bucket | `project-streaming-telemetry-dataflow-staging` | Dataflow staging/temp + Flex Template spec |
| 🪣 GCS bucket | `project-streaming-telemetry-tfstate` | Terraform remote state — shared between local and CI |
| 🔐 Workload Identity Federation | pool `github-actions-pool`, provider `github-provider` | Keyless GitHub Actions auth, scoped via `attribute_condition` |

**`deploy-pipeline.yml` jobs, in order:**
1. ✅ `validate-terraform` — `fmt -check`, `init -backend=false`, `validate`. No GCP auth.
2. 🧪 `run-tests` — both unit suites. Gates everything after it.
3. 🏗️ `deploy-infrastructure` — `terraform apply` against shared GCS state.
4. 🐳 `build-dataflow-template` — Docker build, push, `flex-template build`.
5. 🚀 `deploy-application` — Cloud Function deploy (+ `run.invoker` grant), Dataflow launch/update.

## 🧪 8. Testing

Two unit test suites, ~4 seconds combined, no live GCP credentials needed:

**`dataflow/test_pipeline.py`** — 11 tests (Beam `TestPipeline` / `assert_that`):
- Parsing: valid payload, malformed JSON, missing field
- Averaging math: correct computation, accumulator merge, empty-accumulator edge case
- Breach logic: temp-only, vibration-only, both (still one alert), neither, exact-boundary (`80.0`/`5.0` is **not** a breach)

**`cloud-function/gemini-diagnostics/test_main.py`** — 5 tests, with `conftest.py` stubbing `google.genai`/`google.cloud.bigquery` in `sys.modules` before import:
- Successful incident logged with `row_ids` passed
- Same `machine_id` + `window_end` → same `incident_id`
- Malformed payload never calls Gemini/BigQuery
- Empty envelope doesn't raise
- BigQuery insert error logged, not raised

Both suites gate `deploy-infrastructure` in CI.

## 📈 9. Dashboard (Looker Studio)

- Two data sources (`telemetry_aggregates`, `incident_log`), both **Owner's Credentials** — viewable by anyone with the link, no GCP account needed.
- Time series charts, `machine_id` breakdown, `AVG` aggregation (not the default `SUM` — must be explicitly corrected).
- Incident table with **per-column conditional text-color formatting**: `avg_temperature > 80` and `avg_vibration > 5` independently red, matching the real OR-threshold logic.
- Public embed verified in an incognito/logged-out session before external use.

## 🏗️ 10. Terraform Layout (current)

```
terraform/
└── envs/
    └── lab/
        ├── main.tf        # networking, pubsub, bigquery, iam
        ├── cicd.tf        # WIF, Artifact Registry, GCS buckets, required APIs,
        │                  # Cloud Build / Compute default SA grants, run.admin
        ├── backend.tf     # GCS remote state
        └── variables.tf
```
Module split (`terraform/modules/{networking,pubsub,bigquery,iam,cloud-function}`) exists as empty scaffolding — not yet populated.

## ✅ 11. Resolved Decisions

- [x] Gemini SDK: `google-genai`, Vertex AI backend
- [x] GCP project: `project-streaming-telemetry`
- [x] Metric scope: temperature **and** vibration
- [x] Windowing: 1-minute tumbling
- [x] Threshold: `avg_temperature > 80.0 OR avg_vibration > 5.0`
- [x] Alert delivery: Eventarc-managed push subscription
- [x] Dataflow networking: `--disable-public-ips`; `e2-standard-2` machine type
- [x] Terraform state: GCS remote backend, shared local/CI
- [x] Testing: pytest + Beam `TestPipeline`, gating deploys
- [x] Dashboard: Looker Studio, public, Owner's-credentials

## 🚧 12. Still Open

- [ ] Terraform module refactor (currently flat in `envs/lab/`)
- [ ] IAM hardening — `sa-github-actions` currently broader than strictly necessary
- [ ] Multi-region resilience — a real zone-capacity shortage was hit; not yet mitigated beyond a machine-type change
