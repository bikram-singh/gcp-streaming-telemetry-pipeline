# LLD — Real-Time Machine Telemetry Streaming Pipeline (GCP)

## 1. Networking

| Resource | Name | Notes |
|---|---|---|
| VPC | `telemetry-vpc` | `auto_create_subnetworks = false` |
| Subnet | `telemetry-subnet` | `10.10.0.0/24`, `private_ip_google_access = true` |
| Router | `telemetry-router` | for Cloud NAT |
| NAT | `telemetry-nat` | `AUTO_ONLY`, all subnet ranges |
| Firewall | `telemetry-allow-internal` | tcp+udp `12345-12346`, source `10.10.0.0/24` — required for Dataflow worker-to-worker comms on a private subnet |

## 2. Pub/Sub

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

## 3. BigQuery

Dataset: `telemetry_analytics` (`deletion_protection = false` on all tables — fast lab teardown/rebuild)

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

## 4. Dataflow (Flex Template)

- Packaged as a Flex Template, deployed via `gcloud dataflow flex-template run`.
- Parsing uses `beam.pvalue.TaggedOutput` — malformed JSON or missing fields tagged `dlq`, written to `telemetry-dlq-topic`; valid records flow to windowing.
- **Windowing: 1-minute tumbling (fixed) windows** — `beam.WindowInto(beam.window.FixedWindows(60))`. Chosen over the originally-planned 5-minute sliding window to simplify Beam state management while keeping processing lightweight.
- `CombinePerKey` aggregates both `temperature` and `vibration` per `machine_id` per window (accumulator holds both running sums/counts).
- Window-end timestamp for the BigQuery row is read via `beam.DoFn.WindowParam` in a `ParDo` (not `beam.Map` — `beam.window.TimestampCombo` does not exist in Apache Beam and will raise `AttributeError`):
  ```python
  class FormatBQFn(beam.DoFn):
      def process(self, element, window=beam.DoFn.WindowParam):
          machine_id, (avg_temp, avg_vib) = element
          yield {
              "machine_id": machine_id,
              "avg_temperature": avg_temp,
              "avg_vibration": avg_vib,
              "window_end": window.end.to_utc_datetime().isoformat(),
              "calculated_at": window.end.to_utc_datetime().isoformat(),
          }
  ```
- Breach filter: `avg_temperature > 80.0 OR avg_vibration > 5.0` → publish to `telemetry-alerts-topic`.
- Autoscaling: `--autoscaling_algorithm=THROUGHPUT_BASED --max_num_workers=10`, `--enable_streaming_engine`.
- Worker network: `telemetry-subnet`, private IP only, egress via `telemetry-nat`.

## 5. Cloud Function — `gemini-diagnostics`

- Trigger: **2nd-gen, Eventarc, triggered on `telemetry-alerts-topic`** — not `telemetry-input-topic` (that would fire on every raw telemetry message instead of only on breaches).
- Runtime: Python 3.12.
- Gemini client must be initialized against the Vertex AI backend, not the Gemini Developer API default:
  ```python
  from google import genai
  client = genai.Client(vertexai=True, project=PROJECT_ID, location="us-central1")
  ```
  A bare `genai.Client()` defaults to the Developer API and requires an API key — it will fail auth given this project's IAM setup (`roles/aiplatform.user`, no API key anywhere).
- Logic: parse alert JSON → call `client.models.generate_content(model="gemini-2.5-flash", contents=prompt)` → write row to `incident_log` (via BigQuery client) → structured log for Cloud Monitoring.
- Idempotency: `incident_id` = hash of `machine_id` + `window_end`, to avoid duplicate rows on Eventarc redelivery.

## 6. IAM (least privilege, one SA per component)

| Service Account | Roles |
|---|---|
| `sa-dataflow-worker` | `roles/dataflow.worker`, `roles/pubsub.subscriber`, `roles/pubsub.publisher`, `roles/bigquery.dataEditor` |
| `sa-gemini-function` | `roles/aiplatform.user`, `roles/bigquery.dataEditor` |

All bindings are `google_project_iam_member` (project-scoped) — acceptable for the lab; a production hardening pass would scope these to specific topics/datasets via conditional IAM or resource-level bindings.

## 7. Terraform Layout (current)

```
terraform/
└── envs/
    └── lab/
        ├── main.tf        # all resources — networking, pubsub, bigquery, iam
        └── variables.tf
```
Module split (`terraform/modules/{networking,pubsub,bigquery,iam,cloud-function}`) exists as empty scaffolding — not yet populated. `main.tf` currently holds everything directly; refactor into modules is a follow-up, not a blocker.

## 8. Resolved Decisions (was "Open Decisions")

- [x] Gemini SDK: `google-genai` client, Vertex AI backend (`vertexai=True`).
- [x] GCP project ID: `project-streaming-telemetry`.
- [x] Metric scope: temperature **and** vibration, both tracked.
- [x] Naming: live Terraform naming is source of truth (`telemetry_analytics` / `telemetry_aggregates` / `telemetry-input-topic`, etc.) — this doc has been updated to match, not the other way around.
- [x] Windowing: 1-minute tumbling, not 5-minute sliding.
- [x] Threshold: `avg_temperature > 80.0 OR avg_vibration > 5.0`.
- [x] Alert delivery: Eventarc-managed push subscription (no explicit `google_pubsub_subscription` for alerts in Terraform).

## 9. Still Open

- [ ] `dataflow/pipeline.py`, `cloud-function/gemini-diagnostics/main.py`, `simulator/publisher.py` — not yet written against this finalized schema.
- [ ] `.github/workflows/` — CI/CD not yet built; infra so far applied via local `terraform apply`.
- [ ] Terraform module refactor (currently flat in `envs/lab/main.tf`).
