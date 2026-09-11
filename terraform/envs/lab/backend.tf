terraform {
  backend "gcs" {
    bucket = "project-streaming-telemetry-tfstate"
    prefix = "envs/lab"
  }
}
