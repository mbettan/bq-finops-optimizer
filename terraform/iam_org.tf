locals {
  scope_sa_roles = concat(
    [
      "roles/bigquery.resourceViewer",
      "roles/bigquery.metadataViewer",
    ],
    var.enable_recommender_viewer ? ["roles/recommender.bigqueryPartitionClusterViewer"] : [],
  )
}

resource "terraform_data" "iam_scope_guard" {
  lifecycle {
    precondition {
      condition     = !var.manage_analysis_iam || var.iam_scope != "organization" || try(length(var.org_id) > 0, false)
      error_message = "org_id is required when iam_scope is \"organization\"."
    }

    precondition {
      condition     = !var.manage_analysis_iam || var.iam_scope != "folder" || try(length(var.folder_id) > 0, false)
      error_message = "folder_id is required when iam_scope is \"folder\"."
    }
  }
}

resource "google_organization_iam_member" "runtime" {
  for_each = var.manage_analysis_iam && var.iam_scope == "organization" ? toset(local.scope_sa_roles) : toset([])

  org_id = var.org_id
  role   = each.value
  member = local.runtime_sa_member

  depends_on = [terraform_data.iam_scope_guard]
}
