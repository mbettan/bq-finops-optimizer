locals {
  project_sa_roles = [
    "roles/bigquery.jobUser",
    "roles/bigquery.metadataViewer",
    "roles/aiplatform.user",
  ]
}

resource "google_project_iam_member" "runtime" {
  for_each = var.manage_iam ? toset(local.project_sa_roles) : toset([])

  project = var.project_id
  role    = each.value
  member  = local.runtime_sa_member
}
