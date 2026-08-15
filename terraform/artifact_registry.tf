resource "google_artifact_registry_repository" "app" {
  location      = var.region
  repository_id = var.repository_id
  description   = "Container images for BigQuery FinOps Optimizer"
  format        = "DOCKER"
  labels        = var.resource_labels

  depends_on = [google_project_service.required]
}