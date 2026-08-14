resource "google_service_account" "runtime" {
  account_id   = var.sa_account_id
  display_name = "BigQuery FinOps Optimizer runtime"
  description  = "Runtime identity for Cloud Run BigQuery FinOps Optimizer."

  depends_on = [google_project_service.required]
}
