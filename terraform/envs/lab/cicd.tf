# ------------------------------------------------------------------------------
# CI/CD PREREQUISITES — Workload Identity Federation, Artifact Registry, GCS staging
# ------------------------------------------------------------------------------
# Without this, .github/workflows/* have no way to authenticate to GCP and the
# Dataflow Flex Template build has nowhere to push the image or template spec.

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
