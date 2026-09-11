terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ------------------------------------------------------------------------------
# 1. NETWORKING (VPC, Subnet, Router, NAT, Internal Firewall)
# ------------------------------------------------------------------------------
resource "google_compute_network" "vpc" {
  name                    = "telemetry-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name                     = "telemetry-subnet"
  ip_cidr_range            = "10.10.0.0/24"
  region                   = var.region
  network                  = google_compute_network.vpc.id
  private_ip_google_access = true
}

resource "google_compute_router" "router" {
  name    = "telemetry-router"
  region  = var.region
  network = google_compute_network.vpc.id
}

resource "google_compute_router_nat" "nat" {
  name                               = "telemetry-nat"
  router                             = google_compute_router.router.name
  region                             = var.region
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
  nat_ip_allocate_option             = "AUTO_ONLY"
}

# Worker-to-Worker Firewall Rule for Private Dataflow Execution
resource "google_compute_firewall" "allow_internal" {
  name    = "telemetry-allow-internal"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
    ports    = ["12345-12346"]
  }
  allow {
    protocol = "udp"
    ports    = ["12345-12346"]
  }

  source_ranges = ["10.10.0.0/24"]
}

# ------------------------------------------------------------------------------
# 2. PUB/SUB TOPICS & SUBSCRIPTIONS
# ------------------------------------------------------------------------------
# Main Ingestion Topic
resource "google_pubsub_topic" "telemetry_input" {
  name = "telemetry-input-topic"
}

# Dataflow Pull Subscription for Ingestion
resource "google_pubsub_subscription" "telemetry_input_sub" {
  name  = "telemetry-input-sub"
  topic = google_pubsub_topic.telemetry_input.name

  ack_deadline_seconds = 30
}

# Dead Letter Queue (DLQ) Topic for Malformed Payloads
resource "google_pubsub_topic" "telemetry_dlq" {
  name = "telemetry-dlq-topic"
}

# Alert Topic triggered by Dataflow on metric breach
resource "google_pubsub_topic" "telemetry_alerts" {
  name = "telemetry-alerts-topic"
}

# ------------------------------------------------------------------------------
# 3. BIGQUERY DATASET & TABLES
# ------------------------------------------------------------------------------
resource "google_bigquery_dataset" "telemetry_ds" {
  dataset_id = "telemetry_analytics"
  location   = var.region
}

resource "google_bigquery_table" "telemetry_aggregates" {
  dataset_id          = google_bigquery_dataset.telemetry_ds.dataset_id
  table_id            = "telemetry_aggregates"
  deletion_protection = false

  schema = <<EOF
[
  {"name": "machine_id", "type": "STRING", "mode": "REQUIRED"},
  {"name": "avg_temperature", "type": "FLOAT", "mode": "REQUIRED"},
  {"name": "avg_vibration", "type": "FLOAT", "mode": "REQUIRED"},
  {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
  {"name": "calculated_at", "type": "TIMESTAMP", "mode": "REQUIRED"}
]
EOF
}

resource "google_bigquery_table" "incident_log" {
  dataset_id          = google_bigquery_dataset.telemetry_ds.dataset_id
  table_id            = "incident_log"
  deletion_protection = false

  schema = <<EOF
[
  {"name": "incident_id", "type": "STRING", "mode": "REQUIRED"},
  {"name": "machine_id", "type": "STRING", "mode": "REQUIRED"},
  {"name": "avg_temperature", "type": "FLOAT", "mode": "REQUIRED"},
  {"name": "avg_vibration", "type": "FLOAT", "mode": "REQUIRED"},
  {"name": "diagnostic", "type": "STRING", "mode": "NULLABLE"},
  {"name": "detected_at", "type": "TIMESTAMP", "mode": "REQUIRED"}
]
EOF
}

# ------------------------------------------------------------------------------
# 4. SERVICE ACCOUNTS & LEAST-PRIVILEGE IAM
# ------------------------------------------------------------------------------
# Dataflow Worker SA
resource "google_service_account" "dataflow_worker_sa" {
  account_id   = "sa-dataflow-worker"
  display_name = "Dataflow Worker Service Account"
}

resource "google_project_iam_member" "df_worker_role" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_worker_sa.email}"
}

resource "google_project_iam_member" "df_pubsub_sub" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.dataflow_worker_sa.email}"
}

resource "google_project_iam_member" "df_pubsub_pub" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.dataflow_worker_sa.email}"
}

resource "google_project_iam_member" "df_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataflow_worker_sa.email}"
}

# Gemini Diagnostics Cloud Function SA
resource "google_service_account" "gemini_function_sa" {
  account_id   = "sa-gemini-function"
  display_name = "Gemini Diagnostics Cloud Function SA"
}

resource "google_project_iam_member" "cf_ai_user" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.gemini_function_sa.email}"
}

resource "google_project_iam_member" "cf_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.gemini_function_sa.email}"
}