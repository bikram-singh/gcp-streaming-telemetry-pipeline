# gcp-streaming-telemetry-pipeline

Real-time machine telemetry ingestion, sliding-window aggregation, and
Gemini-powered anomaly diagnostics on GCP. Terraform + GitHub Actions,
same pattern as `gcp-cloud-armor-waf-ddos-lab` and `gcp-network-connectivity-center`.

See [`docs/HLD.md`](docs/HLD.md) and [`docs/LLD.md`](docs/LLD.md) for design detail.

## Repo Structure

```
gcp-streaming-telemetry-pipeline/
├── README.md
├── docs/
│   ├── HLD.md
│   ├── LLD.md
│   └── snapshots/              # console screenshots for the Medium article
├── terraform/
│   ├── envs/lab/
│   └── modules/
│       ├── pubsub/
│       ├── bigquery/
│       ├── networking/
│       ├── iam/
│       ├── dataflow-template-bucket/
│       └── cloud-function/
├── simulator/
│   └── publisher.py             # dummy IoT telemetry producer
├── dataflow/
│   ├── pipeline.py               # Beam pipeline (Flex Template source)
│   ├── metadata.json             # Flex Template spec
│   └── Dockerfile                # template launcher image
├── cloud-function/
│   └── gemini-diagnostics/
│       ├── main.py
│       └── requirements.txt
└── .github/
    └── workflows/
        ├── terraform-plan-apply.yml
        └── deploy-dataflow-template.yml
```

## Build Order

1. `terraform/modules/networking` + `iam` + `pubsub` + `bigquery` — foundational infra
2. `dataflow/` — Flex Template pipeline + DLQ routing
3. `cloud-function/gemini-diagnostics` — alert → Gemini → BigQuery
4. `simulator/` — traffic generator for demo
5. `.github/workflows/` — CI/CD wiring
6. `docs/snapshots` + Medium article
