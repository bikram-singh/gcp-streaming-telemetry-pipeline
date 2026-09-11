# ------------------------------------------------------------------------------
# CI/CD PREREQUISITES — Workload Identity Federation, Artifact Registry, GCS staging
# ------------------------------------------------------------------------------
# Without this, .github/workflows/* have no way to authenticate to GCP and the
# Dataflow Flex Template build has nowhere to push the image or template spec.

# --- Required APIs, declared here so a missing one never surfaces as a
#     surprise mid-deploy again (eventarc was the one that bit us) ---
resource "google_project_service" "required_apis" {
  for_each = toset([
    "compute.googleapis.com",
    "dataflow.googleapis.com",
    "pubsub.googleapis.com",
    "bigquery.googleapis.com",
    "aiplatform.googleapis.com",
    "cloudfunctions.googleapis.com",
    "run.googleapis.com",
    "cloudbuild.googleapis.com",
    "eventarc.googleapis.com",
    "artifactregistry.googleapis.com",
    "iamcredentials.googleapis.com",
    "sts.googleapis.com",
  ])
  service            = each.key
  disable_on_destroy = false
}

# --- Cloud Build default service account permissions ---
# Newer GCP projects no longer auto-grant Editor to the default Cloud Build
# SA (PROJECT_NUMBER@cloudbuild.gserviceaccount.com). Cloud Functions gen2
# deploys go through Cloud Build under the hood, so without this the build
# step fails with "missing permission on the build service account."
data "google_project" "current" {
  project_id = var.project_id
}

resource "google_project_iam_member" "cloudbuild_default_sa_builder" {
  project = var.project_id
  role    = "roles/cloudbuild.builds.builder"
  member  = "serviceAccount:${data.google_project.current.number}@cloudbuild.gserviceaccount.com"
}

# --- Compute Engine default service account permissions ---
# Since mid-2024, Cloud Functions gen2 / Cloud Build v2 builds run as the
# Compute Engine default SA (PROJECT_NUMBER-compute@developer.gserviceaccount.com)
# rather than the legacy Cloud Build SA above, on projects created after that
# change. Granting the legacy SA the builder role (above) didn't resolve the
# build failure, confirming this project is on the newer path — so this SA
# needs the same permissions.
resource "google_project_iam_member" "compute_default_sa_builder" {
  project = var.project_id
  role    = "roles/cloudbuild.builds.builder"
  member  = "serviceAccount:${data.google_project.current.number}-compute@developer.gserviceaccount.com"
}

resource "google_project_iam_member" "compute_default_sa_artifactregistry" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${data.google_project.current.number}-compute@developer.gserviceaccount.com"
}

resource "google_project_iam_member" "compute_default_sa_logging" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${data.google_project.current.number}-compute@developer.gserviceaccount.com"
}

# --- Dataflow worker: read access to the Flex Template launcher image ---
# The launcher VM runs as sa-dataflow-worker and needs to pull the built
# container image from Artifact Registry to start at all — without this the
# launcher VM boots but every docker pull attempt is denied and the job dies
# after ~4 retries with no pipeline code ever executing.
resource "google_project_iam_member" "df_artifact_registry_reader" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.dataflow_worker_sa.email}"
}

# Pub/Sub subscription config inspection (ack deadline etc.) — separate from
# pubsub.subscriber, which only allows pulling/acking messages, not reading
# subscription metadata. Dataflow checks this at startup as a sanity check.
resource "google_project_iam_member" "df_pubsub_viewer" {
  project = var.project_id
  role    = "roles/pubsub.viewer"
  member  = "serviceAccount:${google_service_account.dataflow_worker_sa.email}"
}

# --- Artifact Registry: Docker images for the Dataflow Flex Template launcher ---
resource "google_artifact_registry_repository" "telemetry_images" {
  location      = var.region
  repository_id = "telemetry-images"
  format        = "DOCKER"
}

# --- GCS bucket: Dataflow staging + Flex Template spec JSON ---
resource "google_storage_bucket" "dataflow_staging" {
  name                        = "${var.project_id}-dataflow-staging"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true # lab convenience — remove for anything longer-lived
}

# --- Workload Identity Federation pool + provider for GitHub Actions ---
resource "google_iam_workload_identity_pool" "github_pool" {
  workload_identity_pool_id = "github-actions-pool"
  display_name              = "GitHub Actions pool"
}

resource "google_iam_workload_identity_pool_provider" "github_provider" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github_pool.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-provider"
  display_name                       = "GitHub OIDC provider"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
  }

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }

  # Restrict to this repo only — replace bikram-singh if the repo lives under a
  # different GitHub account/org than assumed here.
  attribute_condition = "assertion.repository == 'bikram-singh/gcp-streaming-telemetry-pipeline'"
}

# --- Service account GitHub Actions runs as ---
resource "google_service_account" "github_actions_sa" {
  account_id   = "sa-github-actions"
  display_name = "GitHub Actions CI/CD Service Account"
}

resource "google_service_account_iam_member" "wif_binding" {
  service_account_id = google_service_account.github_actions_sa.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_pool.name}/attribute.repository/bikram-singh/gcp-streaming-telemetry-pipeline"
}

# --- Permissions the CI/CD SA needs ---
# NOTE: this is broad (near-editor) for lab convenience — Terraform apply needs
# to create/modify VPC, Pub/Sub, BigQuery, IAM bindings, and service accounts,
# which collectively requires close to project-editor scope. Tighten this to
# per-resource custom roles before using this pattern on anything beyond a lab.
resource "google_project_iam_member" "cicd_roles" {
  for_each = toset([
    "roles/editor",
    "roles/resourcemanager.projectIamAdmin",
    "roles/iam.serviceAccountAdmin",
    "roles/iam.serviceAccountUser",
    "roles/artifactregistry.writer",
    "roles/dataflow.developer",
    "roles/cloudfunctions.developer",
    "roles/iam.workloadIdentityPoolAdmin",
  ])
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.github_actions_sa.email}"
}

output "wif_provider" {
  value = google_iam_workload_identity_pool_provider.github_provider.name
}

output "github_actions_sa_email" {
  value = google_service_account.github_actions_sa.email
}

output "artifact_registry_repo" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.telemetry_images.repository_id}"
}

output "dataflow_staging_bucket" {
  value = google_storage_bucket.dataflow_staging.name
}
