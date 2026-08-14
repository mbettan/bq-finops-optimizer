locals {
  runtime_sa_member = "serviceAccount:${google_service_account.runtime.email}"

  project_sa_roles = [
    "roles/bigquery.jobUser",
    "roles/bigquery.metadataViewer",
    "roles/aiplatform.user",
  ]
}

resource "google_project_iam_member" "runtime" {
  for_each = toset(local.project_sa_roles)

  project = var.project_id
  role    = each.value
  member  = local.runtime_sa_member
}
