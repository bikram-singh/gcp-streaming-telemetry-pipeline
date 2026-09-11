variable "project_id" {
  type        = string
  description = "The GCP Project ID"
  default     = "project-streaming-telemetry"
}

variable "region" {
  type        = string
  description = "The target GCP region"
  default     = "us-central1"
}