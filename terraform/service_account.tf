locals {
  existing_runtime_sa_email = var.runtime_service_account_email == null ? "" : trimspace(var.runtime_service_account_email)
  create_runtime_sa         = local.existing_runtime_sa_email == ""
  runtime_sa_account_id     = local.create_runtime_sa ? var.sa_account_id : split("@", local.existing_runtime_sa_email)[0]
}

resource "google_service_account" "runtime" {
  count = local.create_runtime_sa ? 1 : 0

  account_id   = var.sa_account_id
  display_name = "BigQuery FinOps Optimizer runtime"
  description  = "Runtime identity for Cloud Run BigQuery FinOps Optimizer."

  depends_on = [google_project_service.required]
}

data "google_service_account" "existing_runtime" {
  count = local.create_runtime_sa ? 0 : 1

  account_id = local.runtime_sa_account_id
  project    = var.project_id
}

locals {
  runtime_sa_email  = local.create_runtime_sa ? google_service_account.runtime[0].email : data.google_service_account.existing_runtime[0].email
  runtime_sa_name   = local.create_runtime_sa ? google_service_account.runtime[0].name : data.google_service_account.existing_runtime[0].name
  runtime_sa_member = "serviceAccount:${local.runtime_sa_email}"
}