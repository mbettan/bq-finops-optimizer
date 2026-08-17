locals {
  deployer_roles = [
    "roles/cloudbuild.builds.editor",
    "roles/storage.objectAdmin",
  ]
}

resource "google_project_iam_member" "deployer" {
  for_each = {
    for pair in setproduct(var.deployer_members, local.deployer_roles) :
    "${pair[0]}::${pair[1]}" => {
      member = pair[0]
      role   = pair[1]
    }
  }

  project = var.project_id
  role    = each.value.role
  member  = each.value.member
}

resource "google_project_iam_member" "compute_storage_admin" {
  count = var.manage_iam ? 1 : 0

  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${local.compute_sa}"
}

resource "google_project_iam_member" "compute_artifact_writer" {
  count = var.manage_iam ? 1 : 0

  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${local.compute_sa}"
}

resource "google_project_iam_member" "compute_log_writer" {
  count = var.manage_iam ? 1 : 0

  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${local.compute_sa}"
}

resource "google_project_iam_member" "compute_run_developer" {
  count = var.manage_iam ? 1 : 0

  project = var.project_id
  role    = "roles/run.developer"
  member  = "serviceAccount:${local.compute_sa}"
}

resource "google_service_account_iam_member" "compute_runtime_user" {
  count = var.grant_runtime_act_as ? 1 : 0

  service_account_id = local.runtime_sa_name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${local.compute_sa}"
}
