variable "project_id" {
  description = "GCP project ID for the FinOps Optimizer deployment."
  type        = string
}

variable "iam_scope" {
  description = "Where to bind runtime BigQuery viewer roles: organization or folder."
  type        = string
  default     = "organization"

  validation {
    condition     = contains(["organization", "folder"], var.iam_scope)
    error_message = "iam_scope must be \"organization\" or \"folder\"."
  }
}

variable "org_id" {
  description = "Numeric GCP organization ID. Required when iam_scope is organization."
  type        = string
  default     = null
  nullable    = true
}

variable "folder_id" {
  description = "Numeric folder ID or folders/{id}. Required when iam_scope is folder."
  type        = string
  default     = null
  nullable    = true
}

variable "region" {
  description = "Primary region for Cloud Run, Artifact Registry, and Cloud Build."
  type        = string
  default     = "us-central1"
}

variable "service_name" {
  description = "Cloud Run service name."
  type        = string
  default     = "bq-finops-optimizer"
}

variable "sa_account_id" {
  description = "Account ID (short name) for the runtime service account when creating one."
  type        = string
  default     = "bq-finops-optimizer-runtime"
}

variable "runtime_service_account_email" {
  description = "Existing runtime SA email. When set, Terraform does not create a service account."
  type        = string
  default     = null
  nullable    = true
}

variable "resource_labels" {
  description = "Labels applied to Cloud Run and Artifact Registry (org-policy compliant keys)."
  type        = map(string)
  default     = {}
}

variable "manage_analysis_iam" {
  description = "Bind org- or folder-level BigQuery viewer roles on the runtime SA."
  type        = bool
  default     = true
}

variable "manage_iam" {
  description = "Create project-level IAM bindings for the runtime SA and Cloud Build identities."
  type        = bool
  default     = true
}

variable "grant_runtime_act_as" {
  description = "Grant Cloud Build and Compute SAs roles/iam.serviceAccountUser on the runtime SA."
  type        = bool
  default     = true
}

variable "repository_id" {
  description = "Artifact Registry repository ID."
  type        = string
  default     = "bq-finops-optimizer"
}

variable "invoker_members" {
  description = "Principals granted roles/run.invoker on the Cloud Run service."
  type        = list(string)
  default     = []
}

variable "deployer_members" {
  description = "Principals allowed to submit Cloud Build jobs and upload deploy sources."
  type        = list(string)
  default     = []
}

variable "enable_recommender_viewer" {
  description = "Grant org- or folder-level recommender.bigqueryPartitionClusterViewer for Active Assist."
  type        = bool
  default     = false
}

variable "cloud_run_memory" {
  description = "Cloud Run container memory limit."
  type        = string
  default     = "2Gi"
}

variable "cloud_run_cpu" {
  description = "Cloud Run container CPU limit."
  type        = string
  default     = "2"
}

variable "cloud_run_timeout_seconds" {
  description = "Maximum request timeout for Cloud Run."
  type        = number
  default     = 3600
}

variable "cloud_run_max_instances" {
  description = "Maximum Cloud Run instances."
  type        = number
  default     = 3
}

variable "placeholder_image" {
  description = "Initial container image until Cloud Build deploys the app image."
  type        = string
  default     = "us-docker.pkg.dev/cloudrun/container/hello"
}

variable "cloud_run_env" {
  description = "Environment variables for the Cloud Run container."
  type        = map(string)
  default = {
    AUTH_ENFORCED_UPSTREAM = "true"
  }
}
