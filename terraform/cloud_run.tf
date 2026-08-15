resource "google_cloud_run_v2_service" "app" {
  name     = var.service_name
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"
  labels   = var.resource_labels

  template {
    service_account = local.runtime_sa_email
    timeout         = "${var.cloud_run_timeout_seconds}s"
    labels          = var.resource_labels

    scaling {
      min_instance_count = 0
      max_instance_count = var.cloud_run_max_instances
    }

    containers {
      image = var.placeholder_image

      dynamic "env" {
        for_each = var.cloud_run_env
        content {
          name  = env.key
          value = env.value
        }
      }

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = var.cloud_run_cpu
          memory = var.cloud_run_memory
        }
      }
    }
  }

  lifecycle {
    ignore_changes = [
      template[0].containers[0].image,
      client,
      client_version,
    ]
  }

  depends_on = [
    google_project_service.required,
    google_project_iam_member.runtime,
    google_organization_iam_member.runtime,
    google_folder_iam_member.runtime,
  ]
}