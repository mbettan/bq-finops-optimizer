output "project_id" {
  description = "Deployed GCP project ID."
  value       = var.project_id
}

output "iam_scope" {
  description = "IAM scope used for runtime BigQuery viewer roles."
  value       = var.iam_scope
}

output "org_id" {
  description = "GCP organization ID (set when iam_scope is organization)."
  value       = var.org_id
}

output "folder_id" {
  description = "GCP folder ID (set when iam_scope is folder)."
  value       = var.folder_id
}

output "runtime_service_account_email" {
  description = "Email of the Cloud Run runtime service account."
  value       = local.runtime_sa_email
}

output "artifact_registry_repository" {
  description = "Artifact Registry repository resource name."
  value       = google_artifact_registry_repository.app.name
}

output "container_image_url" {
  description = "Target container image URL for Cloud Build deployments."
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.app.repository_id}/app:latest"
}

output "cloud_run_service_name" {
  description = "Cloud Run service name."
  value       = google_cloud_run_v2_service.app.name
}

output "cloud_run_url" {
  description = "HTTPS URL of the Cloud Run service."
  value       = google_cloud_run_v2_service.app.uri
}

output "cloudbuild_service_account" {
  description = "Default Cloud Build service account email."
  value       = local.cloudbuild_sa
}
