# gcp-streaming-telemetry-pipeline

Real-time machine telemetry (temperature + vibration) ingestion, 1-minute
tumbling-window aggregation, and Gemini 2.5 Flash-powered AI incident
diagnostics on GCP. Fully Terraform-managed, GitHub Actions CI/CD with
Workload Identity Federation, gated by a 16-test unit test suite. Same
build pattern as `gcp-cloud-armor-waf-ddos-lab` and
`gcp-network-connectivity-center`.

See [`docs/HLD.md`](docs/HLD.md) and [`docs/LLD.md`](docs/LLD.md) for full
design detail, including the real GCP deployment gotchas found and fixed
along the way (org-policy IP restrictions, Cloud Build default service
account permissions, Eventarc invoker requirements, regional capacity
shortages, and more).

**Live dashboard:** [Looker Studio — Machine Telemetry & AI Incident Monitoring](https://datastudio.google.com/embed/reporting/a6052335-bf85-4d05-a74e-df423a67dd15/page/xgk8F)

## Repo Structure

```
gcp-streaming-telemetry-pipeline/
├── README.md
├── docs/
│   ├── HLD.md
│   └── LLD.md
├── terraform/
│   ├── envs/lab/
│   │   ├── main.tf          # networking, pubsub, bigquery, iam
│   │   ├── cicd.tf          # WIF, Artifact Registry, GCS buckets, required APIs
│   │   ├── backend.tf       # GCS remote state
│   │   └── variables.tf
│   └── modules/              # scaffolded, not yet populated — see LLD §10
│       ├── pubsub/
│       ├── bigquery/
│       ├── networking/
│       ├── iam/
│       └── cloud-function/
├── simulator/
│   └── publisher.py          # telemetry simulator (temperature + vibration, DLQ test payloads)
├── dataflow/
│   ├── pipeline.py            # Beam pipeline (Flex Template source)
│   ├── test_pipeline.py       # 11 unit tests, Apache Beam TestPipeline
│   ├── metadata.json          # Flex Template spec
│   ├── Dockerfile             # template launcher image
│   └── requirements.txt
├── cloud-function/
│   └── gemini-diagnostics/
│       ├── main.py
│       ├── conftest.py        # stubs GCP SDKs so tests need no live credentials
│       ├── test_main.py       # 5 unit tests
│       └── requirements.txt
└── .github/
    └── workflows/
        └── deploy-pipeline.yml   # validate → test → apply → build → deploy
```

## Pipeline at a glance

Simulator → Pub/Sub → Dataflow (1-min tumbling window, dual-metric OR
threshold: `avg_temperature > 80.0` or `avg_vibration > 5.0`) → BigQuery
(`telemetry_aggregates`) → on breach → Pub/Sub alert → Cloud Function →
Gemini 2.5 Flash → BigQuery (`incident_log`) → Looker Studio dashboard.

## Local verification

```powershell
python simulator\publisher.py          # feed live telemetry
# ... let it run a few minutes, then Ctrl+C ...
python -m pytest dataflow\test_pipeline.py -v
python -m pytest cloud-function\gemini-diagnostics\test_main.py -v
```

## Build Order (for reference / rebuilding from scratch)

1. `terraform/envs/lab/main.tf` — foundational infra (networking, pubsub, bigquery, iam)
2. `terraform/envs/lab/cicd.tf` + `backend.tf` — WIF, Artifact Registry, GCS buckets, remote state
3. `dataflow/` — Flex Template pipeline + DLQ routing + unit tests
4. `cloud-function/gemini-diagnostics/` — alert → Gemini → BigQuery + unit tests
5. `simulator/` — traffic generator for demo/testing
6. `.github/workflows/deploy-pipeline.yml` — CI/CD wiring
7. Looker Studio dashboard + Medium article
