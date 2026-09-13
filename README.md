<div align="center">

# 🌡️ GCP Streaming Telemetry Pipeline

### Real-Time Machine Monitoring · Apache Beam · BigQuery · Gemini 2.5 Flash · Google Cloud Platform

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://python.org)
[![Apache Beam](https://img.shields.io/badge/Apache_Beam-Dataflow-FF6F00?logo=apache&logoColor=white)](https://beam.apache.org)
[![BigQuery](https://img.shields.io/badge/BigQuery-Analytics-4285F4?logo=googlebigquery&logoColor=white)](https://cloud.google.com/bigquery)
[![Vertex AI](https://img.shields.io/badge/Vertex_AI-Gemini_2.5_Flash-8E44AD?logo=googlecloud&logoColor=white)](https://cloud.google.com/vertex-ai)
[![Terraform](https://img.shields.io/badge/Terraform-1.9-844FBA?logo=terraform&logoColor=white)](https://www.terraform.io)
[![Google Cloud](https://img.shields.io/badge/Google_Cloud-9_Services-4285F4?logo=googlecloud&logoColor=white)](https://cloud.google.com)
[![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub_Actions-2088FF?logo=githubactions&logoColor=white)](.github/workflows/deploy-pipeline.yml)
[![Tests](https://img.shields.io/badge/Unit_Tests-16_passing-2ECC71?logo=pytest&logoColor=white)](dataflow/test_pipeline.py)

---

*A real-time GCP streaming pipeline that watches simulated factory machine
telemetry (temperature + vibration), continuously computes per-machine
rolling averages, and — the moment a machine crosses a safety threshold —
automatically asks Gemini 2.5 Flash to diagnose the likely root cause and
logs it as an incident. Fully Terraform-managed, deployed through a
16-test-gated GitHub Actions pipeline, and live-tested against real GCP
infrastructure — not a static demo.*

</div>

---

## 📋 Table of Contents

- [Overview](#-overview)
- [The Problem, In Plain Terms](#-the-problem-in-plain-terms)
- [Architecture](#-architecture)
- [What It Does](#-what-it-does)
- [Repository Structure](#-repository-structure)
- [Prerequisites](#-prerequisites)
- [Setup](#-setup)
- [Testing](#-testing)
- [Live Verification — Real Incidents, Real Diagnostics](#-live-verification--real-incidents-real-diagnostics)
- [Real Deployment Gotchas (Found & Fixed)](#-real-deployment-gotchas-found--fixed)
- [Dashboard](#-dashboard)
- [Repository](#-repository)

---

## 🌐 Overview

This pipeline ingests machine telemetry, computes 1-minute tumbling-window
averages per machine for two metrics, and triggers an AI-generated
diagnostic the instant either metric crosses a safety line. It isn't a
static architecture demo — every service below was deployed for real,
debugged against real GCP errors, and verified with real breaches
triggering real Gemini output, logged to a real BigQuery table.

### 🔑 Key Facts

| Property | Value |
|---|---|
| ☁️ **Cloud Platform** | Google Cloud Platform |
| 🌊 **Stream Processing** | Cloud Dataflow, Apache Beam (Flex Template) |
| 📊 **Storage** | BigQuery (`telemetry_aggregates`, `incident_log`) |
| 🧠 **AI Model** | Gemini 2.5 Flash via Vertex AI (`google-genai` SDK) |
| 📬 **Messaging** | Cloud Pub/Sub (ingestion, DLQ, alerts) |
| 🏗️ **IaC** | Terraform 1.16, GCS remote state |
| 🔁 **CI/CD** | GitHub Actions, Workload Identity Federation (keyless) |
| 🧪 **Test Suite** | 16 unit tests (Beam `TestPipeline` + pytest), gating every deploy |
| 📈 **Dashboard** | Looker Studio, publicly embeddable |
| 🐍 **Language** | Python 3.12 |

---

## 🏭 The Problem, In Plain Terms

A factory floor full of machines, each reporting temperature and
vibration. Two things predict failure, if anyone's watching:

- 🌡️ **Rising temperature** → friction, cooling problems, overwork
- 📳 **Rising vibration** → bearing wear, imbalance, misalignment

Nobody watches a dashboard continuously, and "average temp is a bit
high" doesn't tell a technician *what to check first*. This system
watches every machine, every minute, and turns a breach into an actual
diagnostic sentence — before a human looks at it.

---

## 🏛️ Architecture

```
                              [ SIMULATED FACTORY TELEMETRY ]
                                          │
                         (JSON: machine_id, temperature, vibration, timestamp)
                                          │
                                          ▼
┌───────────────────────────────────────────────────────────────────────────────────────────┐
│  📥 INGESTION LAYER          (VPC: telemetry-vpc  /  Subnet: telemetry-subnet)             │
│                                   ┌──────────────────────────┐                             │
│                                   │        Pub/Sub           │                             │
│                                   │   telemetry-input-topic  │                             │
│                                   └────────────┬─────────────┘                             │
└────────────────────────────────────────────────┼───────────────────────────────────────────┘
                                                  ▼
┌───────────────────────────────────────────────────────────────────────────────────────────┐
│  🌊 STREAM PROCESSING LAYER   (Private workers, no public IP  /  Cloud NAT egress)         │
│                          ┌────────────────────────────────────┐                            │
│                          │      Dataflow Flex Template         │                            │
│                          │   (Apache Beam, e2-standard-2)      │                            │
│                          │  1-min tumbling window · avg_temp,  │                            │
│                          │  avg_vibration per machine_id       │                            │
│                          └──────────┬──────────────────┬───────┘                            │
└─────────────────────────────────────┼──────────────────┼─────────────────────────────────────┘
                    (Windowed Averages)│                  │ (Malformed Payload)
                                       ▼                  ▼
┌────────────────────────────────────────┐   ┌───────────────────────────────────────────┐
│  📊 ANALYTICS & STORAGE LAYER           │   │  ☠️ DEAD-LETTER QUEUE                     │
│  BigQuery: telemetry_analytics          │   │  Pub/Sub: telemetry-dlq-topic              │
│   └─ telemetry_aggregates               │   │                                            │
└────────────────────┬─────────────────────┘   └───────────────────────────────────────────┘
                      │
        (avg_temperature > 80.0°C  OR  avg_vibration > 5.0 mm/s)
                      ▼
┌───────────────────────────────────────────────────────────────────────────────────────────┐
│  🧠 AI & INCIDENT RESPONSE LAYER                                                           │
│  ┌────────────────────┐   ┌───────────────────────┐   ┌──────────────────────────────┐    │
│  │     Pub/Sub          │──►│   Cloud Function       │──►│      Gemini 2.5 Flash        │    │
│  │ telemetry-alerts-    │   │  gemini-diagnostics     │   │   (google-genai SDK,          │    │
│  │      topic           │   │  (gen2, Eventarc)       │   │    Vertex AI backend)         │    │
│  └────────────────────┘   └───────────────────────┘   └──────────────┬───────────────┘    │
│                                                                        ▼                     │
│                                              BigQuery: telemetry_analytics.incident_log      │
└───────────────────────────────────────────────────────────────────────────────────────────┘
```

### 🔄 Layer Breakdown

| Layer | Components |
|---|---|
| 📥 **Ingestion** | Pub/Sub topic, decoupling producers from processing |
| 🌊 **Stream Processing** | Dataflow Flex Template, Apache Beam, 1-min tumbling windows, dual-metric aggregation |
| 📊 **Storage** | BigQuery, clustered on `machine_id` |
| ☠️ **Error Handling** | Pub/Sub dead-letter topic for malformed payloads |
| 🧠 **Intelligence** | Cloud Function (Eventarc) → Gemini 2.5 Flash → persisted incident with idempotency key |
| 📈 **Visualization** | Looker Studio, live-connected to BigQuery |
| 🔁 **Delivery** | Terraform + GitHub Actions, Workload Identity Federation, 16-test gate |

---

## ✨ What It Does

| Capability | Description |
|---|---|
| 📡 **Continuous ingestion** | Simulated machine telemetry streamed via Pub/Sub, at configurable rate |
| 🪟 **Windowed aggregation** | 1-minute tumbling windows, per-machine average temperature and vibration |
| ☠️ **Malformed-data isolation** | Bad payloads routed to a DLQ instead of crashing the pipeline |
| 🚨 **Dual-metric breach detection** | `avg_temperature > 80.0` **OR** `avg_vibration > 5.0` — either signal alone triggers a response |
| 🧠 **AI root-cause diagnosis** | Gemini 2.5 Flash generates a real operational diagnostic per incident |
| 🔁 **Idempotent incident logging** | Deterministic hash key prevents duplicate incidents on retried delivery |
| 🏗️ **Fully Terraform-managed** | Networking, IAM, Pub/Sub, BigQuery, CI/CD infra — all as code |
| 🔐 **Keyless CI/CD** | Workload Identity Federation — no static service account keys anywhere |
| 🧪 **Test-gated deploys** | 16 unit tests must pass before any `terraform apply` or live deploy |
| 📈 **Public live dashboard** | Looker Studio, viewable without a GCP login |

---

## 📁 Repository Structure

```
gcp-streaming-telemetry-pipeline/
│
├── 📄 README.md
├── 📁 docs/
│   ├── 📄 HLD.md
│   └── 📄 LLD.md
│
├── 📁 terraform/
│   ├── 📁 envs/lab/
│   │   ├── 📄 main.tf          # networking, pubsub, bigquery, iam
│   │   ├── 📄 cicd.tf          # WIF, Artifact Registry, GCS buckets, required APIs
│   │   ├── 📄 backend.tf       # GCS remote state
│   │   └── 📄 variables.tf
│   └── 📁 modules/              # scaffolded, not yet populated
│       ├── 📁 pubsub/
│       ├── 📁 bigquery/
│       ├── 📁 networking/
│       ├── 📁 iam/
│       └── 📁 cloud-function/
│
├── 📁 simulator/
│   └── 📄 publisher.py          # telemetry simulator (temp + vibration, DLQ test payloads)
│
├── 📁 dataflow/
│   ├── 📄 pipeline.py            # Beam pipeline (Flex Template source)
│   ├── 📄 test_pipeline.py       # 11 unit tests, Apache Beam TestPipeline
│   ├── 📄 metadata.json          # Flex Template spec
│   ├── 📄 Dockerfile             # template launcher image
│   └── 📄 requirements.txt
│
├── 📁 cloud-function/
│   └── 📁 gemini-diagnostics/
│       ├── 📄 main.py
│       ├── 📄 conftest.py        # stubs GCP SDKs so tests need no live credentials
│       ├── 📄 test_main.py       # 5 unit tests
│       └── 📄 requirements.txt
│
└── 📁 .github/
    └── 📁 workflows/
        └── 📄 deploy-pipeline.yml   # validate → test → apply → build → deploy
```

---

## ✅ Prerequisites

| Requirement | Details |
|---|---|
| 🐍 **Python** | 3.12 (matches Dataflow's runtime and CI) |
| ☁️ **GCP Project** | Billing enabled, required APIs enabled (see `docs/LLD.md` §6) |
| 🏗️ **Terraform** | 1.16+, 64-bit build (32-bit crashes on large project numbers) |
| 🔐 **Application Default Credentials** | `gcloud auth application-default login` |
| 🧠 **Vertex AI access** | Gemini 2.5 Flash enabled in-region |

---

## ⚙️ Setup

```powershell
cd terraform\envs\lab
terraform init
terraform apply -auto-approve
```

Bootstrap CI/CD (one-time, before GitHub Actions can run):

```powershell
# cicd.tf provisions Workload Identity Federation, Artifact Registry,
# and the GCS buckets CI depends on — apply it locally first
terraform apply -auto-approve
```

Then set the four GitHub repo secrets/variables from the Terraform
outputs (`wif_provider`, `github_actions_sa_email`) — see `docs/LLD.md`
§7 for the exact values.

---

## 🧪 Testing

```powershell
python -m pytest dataflow\test_pipeline.py -v
python -m pytest cloud-function\gemini-diagnostics\test_main.py -v
```

| Suite | Tests | Covers |
|---|---|---|
| 🌊 `dataflow/test_pipeline.py` | 11 | Parsing/DLQ routing, averaging math, OR-threshold breach logic, exact-boundary edge case |
| 🧠 `cloud-function/gemini-diagnostics/test_main.py` | 5 | Successful incident logging, idempotency-key stability, malformed-payload handling, graceful BigQuery-error handling |

Both suites run in ~4 seconds combined, require no live GCP credentials,
and gate every CI deploy — a broken pipeline never reaches a real
resource.

---

## 🔍 Live Verification — Real Incidents, Real Diagnostics

Every component below was verified against real GCP state, not just a
green CI checkmark:

| Component | Verified Via |
|---|---|
| 📬 Pub/Sub ingestion | Real simulator run, messages confirmed flowing |
| 🌊 Dataflow job | `gcloud dataflow jobs describe` → `JOB_STATE_RUNNING`, real worker in `us-central1-a` |
| 📊 BigQuery aggregates | Direct query — real per-machine averages across multiple windows |
| ☠️ Dead-letter routing | Malformed test payloads confirmed isolated, pipeline never crashed |
| 🚨 Breach detection | Multiple genuine breaches fired naturally from simulated data (not forced) |
| 🧠 Gemini diagnostics | 7 real, distinct AI-generated incident diagnostics logged |
| 🔁 CI/CD pipeline | Full unattended run: test → apply → build → deploy, no manual intervention |
| 📈 Public dashboard | Loaded with live data in an incognito, logged-out browser session |

**A real diagnostic, generated during testing — not a curated example:**

> *"Elevated vibration exceeding its threshold (5.19 mm/s) strongly
> indicates a developing mechanical fault on MCH-PLANT-01-002, such as
> imbalance, misalignment, or bearing degradation. Immediately initiate
> a physical inspection..."*

---

## 🔧 Real Deployment Gotchas (Found & Fixed)

Most tutorials skip this part. This project didn't:

| Gotcha | Symptom | Fix |
|---|---|---|
| 🌐 Org policy blocks external IPs | Dataflow launcher fails in ~1s, empty pipeline graph | `--disable-public-ips` on every Flex Template launch |
| 🏗️ Cloud Build SA lost default permissions | `missing permission on the build service account` | Grant `roles/cloudbuild.builds.builder` to **both** the legacy Cloud Build SA and the Compute Engine default SA |
| 📦 Launcher VM can't pull its own image | `docker pull` denied, 4 retries, job dies | `roles/artifactregistry.reader` on `sa-dataflow-worker` |
| 🔔 Eventarc authenticates but can't invoke | `IAM principal lacks {run.routes.invoke} permission` | `roles/run.invoker` on the underlying **Cloud Run service**, not just the function's runtime SA |
| 🌍 Zone-level capacity shortage | `ZONE_RESOURCE_POOL_EXHAUSTED`, job fails after ~5 min of retries | Switch worker machine type to `e2-standard-2` (different capacity pool) |
| ⌨️ PowerShell backtick escaping | BigQuery table name silently corrupted mid-query | Use single-quoted (`@'...'@`) here-strings, not double-quoted |
| 🐍 Local Python too new for Beam | `grpcio-tools` build fails, `pkg_resources` missing | Trust CI's pinned Python 3.12 runner instead of fighting local 3.13/3.14 |

Full detail on each, including exact commands, in `docs/LLD.md`.

---

## 📈 Dashboard

**Live, publicly embeddable Looker Studio dashboard:**
[Machine Telemetry & AI Incident Monitoring](https://datastudio.google.com/embed/reporting/a6052335-bf85-4d05-a74e-df423a67dd15/page/xgk8F)

- Real-time trend charts (temperature, vibration) per machine
- Incident feed table with per-column conditional formatting — red text
  exactly where `avg_temperature > 80` or `avg_vibration > 5`, matching
  the pipeline's real threshold logic
- Connected via **Owner's Credentials**, so it loads for anyone with
  the link, no GCP account required

---

## 🔗 Repository

| Repository | Purpose |
|---|---|
| [`gcp-streaming-telemetry-pipeline`](https://github.com/bikram-singh/gcp-streaming-telemetry-pipeline) | Real-time telemetry pipeline · Dataflow · BigQuery · Gemini 2.5 Flash · Terraform · GCP |

---

<div align="center">

**Maintained by Bikram Singh**

*Built with Apache Beam · BigQuery · Vertex AI · Terraform · GitHub Actions*

</div>
