resource "google_folder_iam_member" "runtime" {
  for_each = var.iam_scope == "folder" ? toset(local.scope_sa_roles) : toset([])

  folder = var.folder_id
  role   = each.value
  member = local.runtime_sa_member

  depends_on = [terraform_data.iam_scope_guard]
}
