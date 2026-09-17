#!/usr/bin/env bash
#
# deploy_cloud_run.sh — FinOps Optimizer for BigQuery, Cloud Run deployment.
#
# Two modes, because the person who can bind organization-level IAM is almost
# never the person evaluating the tool:
#
#   --emit-iam   Prints the organization-level commands for review.
#                EXECUTES NOTHING. Safe to run anywhere, by anyone.
#   --deploy     Creates the runtime service account, binds PROJECT-level
#                roles, and deploys to Cloud Run with authentication required.
#
# Requires: bash 4+, gcloud. No Terraform, no plugins — runs in Cloud Shell.
#
# Read README "IAM Roles & Permissions" before running either mode.

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults (override with flags)
# ---------------------------------------------------------------------------
SERVICE_NAME="bq-finops-optimizer"
SA_NAME="bq-finops-sa"
REGION="us-central1"
PROJECT_ID=""
ORG_ID=""
CUSTOM_ROLE_ID="bqFinOpsReader"
MEMORY="2Gi"
CPU="1"
MIN_INSTANCES="0"
MAX_INSTANCES="3"
TIMEOUT="3600"
MODE=""
SKIP_PREFLIGHT="false"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROLE_FILE="${SCRIPT_DIR}/roles/bq_finops_reader.yaml"

# ---------------------------------------------------------------------------
# Output helpers (stderr for diagnostics, stdout stays parseable in --emit-iam)
# ---------------------------------------------------------------------------
if [[ -t 2 ]]; then
  C_RED=$'\033[31m'; C_YEL=$'\033[33m'; C_GRN=$'\033[32m'
  C_DIM=$'\033[2m';  C_OFF=$'\033[0m'
else
  C_RED=""; C_YEL=""; C_GRN=""; C_DIM=""; C_OFF=""
fi

info()  { printf '%s\n' "  $*" >&2; }
step()  { printf '\n%s\n' "${C_GRN}==>${C_OFF} $*" >&2; }
warn()  { printf '%s\n' "${C_YEL}WARNING:${C_OFF} $*" >&2; }
fail()  { printf '\n%s\n' "${C_RED}ERROR:${C_OFF} $*" >&2; exit 1; }
dim()   { printf '%s\n' "${C_DIM}$*${C_OFF}" >&2; }

usage() {
  cat >&2 <<'USAGE'
Usage:
  deploy/deploy_cloud_run.sh --emit-iam --org-id ORGANIZATION_ID [--project PROJECT_ID]
  deploy/deploy_cloud_run.sh --deploy   --project PROJECT_ID [options]

Modes (exactly one required):
  --emit-iam            Print the organization-level IAM commands. Mutates nothing.
  --deploy              Create the service account, bind project roles, deploy.

Options:
  --project PROJECT_ID  Execution project. Defaults to `gcloud config get-value project`.
  --org-id ORG_ID       Organization numeric ID (required for --emit-iam).
  --region REGION       Cloud Run region.            Default: us-central1
  --service NAME        Cloud Run service name.      Default: bq-finops-optimizer
  --sa-name NAME        Service account short name.  Default: bq-finops-sa
  --role-id ID          Custom role ID.              Default: bqFinOpsReader
  --memory SIZE         Default: 2Gi
  --cpu N               Default: 1
  --min-instances N     Default: 0
  --max-instances N     Default: 3
  --skip-preflight      Skip API/permission preflight (not recommended).
  -h, --help            This message.

Examples:
  # 1. Security team reviews and runs these (organization node):
  deploy/deploy_cloud_run.sh --emit-iam --org-id 123456789012 --project my-finops-proj

  # 2. Evaluator deploys (project node only, no org admin needed):
  deploy/deploy_cloud_run.sh --deploy --project my-finops-proj --region us-central1

  # 3. Prove the data-plane boundary afterwards:
  python3 deploy/check_permissions.py --project my-finops-proj \
      --impersonate-service-account bq-finops-sa@my-finops-proj.iam.gserviceaccount.com
USAGE
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --emit-iam)       MODE="emit-iam"; shift ;;
    --deploy)         MODE="deploy";   shift ;;
    --project)        PROJECT_ID="${2:?--project needs a value}"; shift 2 ;;
    --org-id)         ORG_ID="${2:?--org-id needs a value}"; shift 2 ;;
    --region)         REGION="${2:?--region needs a value}"; shift 2 ;;
    --service)        SERVICE_NAME="${2:?--service needs a value}"; shift 2 ;;
    --sa-name)        SA_NAME="${2:?--sa-name needs a value}"; shift 2 ;;
    --role-id)        CUSTOM_ROLE_ID="${2:?--role-id needs a value}"; shift 2 ;;
    --memory)         MEMORY="${2:?--memory needs a value}"; shift 2 ;;
    --cpu)            CPU="${2:?--cpu needs a value}"; shift 2 ;;
    --min-instances)  MIN_INSTANCES="${2:?--min-instances needs a value}"; shift 2 ;;
    --max-instances)  MAX_INSTANCES="${2:?--max-instances needs a value}"; shift 2 ;;
    --skip-preflight) SKIP_PREFLIGHT="true"; shift ;;
    -h|--help)        usage; exit 0 ;;
    *)                usage; fail "Unknown argument: $1" ;;
  esac
done

[[ -n "${MODE}" ]] || { usage; fail "Specify exactly one of --emit-iam or --deploy."; }

command -v gcloud >/dev/null 2>&1 \
  || fail "gcloud not found. Install the Google Cloud CLI or run this in Cloud Shell."

if [[ -z "${PROJECT_ID}" ]]; then
  PROJECT_ID="$(gcloud config get-value project 2>/dev/null || true)"
  [[ -n "${PROJECT_ID}" && "${PROJECT_ID}" != "(unset)" ]] \
    || fail "No project. Pass --project PROJECT_ID or run: gcloud config set project PROJECT_ID"
fi

SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# ---------------------------------------------------------------------------
# Preflight — fail with something a human can act on, not a gcloud stack trace
# ---------------------------------------------------------------------------
REQUIRED_APIS_DEPLOY=(
  "run.googleapis.com"
  "cloudbuild.googleapis.com"
  "artifactregistry.googleapis.com"
)
REQUIRED_APIS_RUNTIME=(
  "bigquery.googleapis.com"
)

preflight() {
  step "Preflight (project: ${PROJECT_ID})"

  local account
  account="$(gcloud config get-value account 2>/dev/null || true)"
  [[ -n "${account}" && "${account}" != "(unset)" ]] \
    || fail "No active gcloud credential. Run: gcloud auth login"
  info "Active account: ${account}"

  gcloud projects describe "${PROJECT_ID}" --format="value(projectId)" >/dev/null 2>&1 \
    || fail "Project '${PROJECT_ID}' not found, or the active account cannot see it.
  Check the ID and that you have at least roles/viewer on it."

  # --- enabled services ---
  local enabled
  if ! enabled="$(gcloud services list --enabled --project="${PROJECT_ID}" \
                    --format='value(config.name)' 2>/dev/null)"; then
    warn "Could not list enabled services (serviceusage.services.list denied?)."
    warn "Skipping the API check. If the deploy fails, enable these manually:"
    warn "  gcloud services enable ${REQUIRED_APIS_DEPLOY[*]} --project=${PROJECT_ID}"
    enabled=""
  fi

  if [[ -n "${enabled}" ]]; then
    local missing=()
    local api
    local wanted=("${REQUIRED_APIS_RUNTIME[@]}")
    if [[ "${MODE}" == "deploy" ]]; then
      wanted+=("${REQUIRED_APIS_DEPLOY[@]}")
    fi
    for api in "${wanted[@]}"; do
      grep -qx "${api}" <<<"${enabled}" || missing+=("${api}")
    done
    if (( ${#missing[@]} > 0 )); then
      fail "$(printf '%s\n' \
        "Required API(s) are NOT enabled on ${PROJECT_ID}:" \
        "$(printf '    - %s\n' "${missing[@]}")" \
        "" \
        "  This deployment builds the container with Cloud Build and stores the" \
        "  image in Artifact Registry, so both must be on." \
        "" \
        "  Fix:" \
        "    gcloud services enable ${missing[*]} --project=${PROJECT_ID}" \
        "" \
        "  If you cannot enable APIs yourself, send that one line to whoever" \
        "  holds roles/serviceusage.serviceUsageAdmin on the project.")"
    fi
    info "APIs enabled: ${wanted[*]}"
  fi

  # --- caller build permissions (advisory: group/inherited roles are invisible here) ---
  if [[ "${MODE}" == "deploy" ]]; then
    local roles=""
    roles="$(gcloud projects get-iam-policy "${PROJECT_ID}" \
               --flatten='bindings[].members' \
               --filter="bindings.members:${account}" \
               --format='value(bindings.role)' 2>/dev/null || true)"
    if [[ -n "${roles}" ]]; then
      if ! grep -qE 'roles/(owner|editor|cloudbuild\.builds\.editor|cloudbuild\.builds\.builder)' <<<"${roles}"; then
        warn "Your account has no direct roles/cloudbuild.builds.editor (or owner/editor)
  binding on ${PROJECT_ID}. 'gcloud run deploy --source .' submits a Cloud Build
  job and will 403 without it. Inherited or group-granted roles are not visible
  to this check, so continuing — if the build 403s, this is why."
      else
        info "Caller holds a Cloud Build capable role."
      fi
    else
      warn "Could not read the project IAM policy (getIamPolicy denied). Skipping
  the Cloud Build permission check."
    fi

    # --- what this will create, and what it costs ---
    cat >&2 <<COSTS

  ${C_DIM}This deployment will create billable resources:${C_OFF}
    - a Cloud Build job that builds the container from source (build minutes)
    - an Artifact Registry repository 'cloud-run-source-deploy' holding the image
      (storage, ~a few hundred MB per revision — prune old images periodically)
    - a Cloud Run service, min-instances=${MIN_INSTANCES} (scales to zero; no idle cost at 0)
  Analysis queries are billed to ${PROJECT_ID} as normal BigQuery usage.

COSTS
  fi

  if [[ "${MODE}" == "deploy" ]]; then
    [[ -f "${REPO_ROOT}/Dockerfile" && -f "${REPO_ROOT}/src/main.py" ]] \
      || fail "Expected the application source at ${REPO_ROOT} (Dockerfile + src/main.py).
  'gcloud run deploy --source .' builds from the repository root — run this
  script from a full checkout, not from a copy of deploy/ alone."
  fi

  info "Preflight OK."
}

# ---------------------------------------------------------------------------
# --emit-iam : print, never execute
# ---------------------------------------------------------------------------
emit_iam() {
  if [[ -z "${ORG_ID}" ]]; then
    fail "--emit-iam needs --org-id ORGANIZATION_ID.
  Find it with: gcloud organizations list"
  fi
  [[ -f "${ROLE_FILE}" ]] || fail "Custom role file not found: ${ROLE_FILE}"

  step "Organization-level IAM — REVIEW AND RUN MANUALLY (nothing was executed)"

  cat <<EOF
# ===========================================================================
# FinOps Optimizer for BigQuery — organization-level IAM
#
# Run by someone holding roles/resourcemanager.organizationAdmin (or an
# equivalent custom role) on organization ${ORG_ID}.
#
# NOTHING BELOW HAS BEEN EXECUTED. This is a reviewable script, by design:
# the evaluator running --deploy needs no organization privileges at all.
#
# Role definition (read it — the exclusion block is the point):
#   ${ROLE_FILE}
# ===========================================================================

# 1. Create the read-only custom role at the organization node.
#    Re-running this errors with ALREADY_EXISTS; use 'update' instead (step 1b).
gcloud iam roles create ${CUSTOM_ROLE_ID} \\
    --organization=${ORG_ID} \\
    --file=${ROLE_FILE}

# 1b. If the role already exists, update it in place:
# gcloud iam roles update ${CUSTOM_ROLE_ID} \\
#     --organization=${ORG_ID} \\
#     --file=${ROLE_FILE}

# 2. Prove what was granted. The output must NOT contain bigquery.tables.getData.
gcloud iam roles describe ${CUSTOM_ROLE_ID} --organization=${ORG_ID}

# 3. Bind the custom role to the runtime service account at the organization.
#    This is what enables the *_BY_ORGANIZATION views.
gcloud organizations add-iam-policy-binding ${ORG_ID} \\
    --member="serviceAccount:${SA_EMAIL}" \\
    --role="organizations/${ORG_ID}/roles/${CUSTOM_ROLE_ID}" \\
    --condition=None

# --- Folder-scoped alternative (if you are piloting on one folder) ----------
# Custom roles cannot be created at a folder; create it at the organization
# (step 1) and bind it to the folder:
# gcloud resource-manager folders add-iam-policy-binding FOLDER_ID \\
#     --member="serviceAccount:${SA_EMAIL}" \\
#     --role="organizations/${ORG_ID}/roles/${CUSTOM_ROLE_ID}" \\
#     --condition=None
# Org-scoped views then only return rows for projects inside that folder.

# --- Predefined-role equivalent (if custom roles are not permitted) --------
# Coarser: resourceViewer grants more than this app reads. Neither predefined
# role grants bigquery.tables.getData.
# gcloud organizations add-iam-policy-binding ${ORG_ID} \\
#     --member="serviceAccount:${SA_EMAIL}" \\
#     --role="roles/bigquery.resourceViewer" --condition=None
#
# OPTIONAL — fast path for Storage Analysis. Without it the app falls back to
# a slower project-by-project loop; the only cost is scan time (README.md:425).
# gcloud organizations add-iam-policy-binding ${ORG_ID} \\
#     --member="serviceAccount:${SA_EMAIL}" \\
#     --role="roles/bigquery.metadataViewer" --condition=None

# --- OPTIONAL — Active Assist recommendations module -----------------------
# Needed only for INFORMATION_SCHEMA.RECOMMENDATIONS_BY_ORGANIZATION.
# gcloud organizations add-iam-policy-binding ${ORG_ID} \\
#     --member="serviceAccount:${SA_EMAIL}" \\
#     --role="roles/recommender.bigqueryPartitionClusterViewer" --condition=None

# ===========================================================================
# NOT granted at the organization node, ever:
#   roles/bigquery.admin        — write access to every dataset in the org
#   roles/bigquery.dataViewer   — READS TABLE CONTENTS (bigquery.tables.getData)
#   roles/aiplatform.user       — project-level only, and only for AI Doctor
# ===========================================================================
EOF

  step "Nothing was executed. Review the above, then run it, then run --deploy."
}

# ---------------------------------------------------------------------------
# --deploy : idempotent
# ---------------------------------------------------------------------------
create_service_account() {
  step "Service account: ${SA_EMAIL}"
  if gcloud iam service-accounts describe "${SA_EMAIL}" \
       --project="${PROJECT_ID}" >/dev/null 2>&1; then
    info "Already exists — reusing it (idempotent, no changes made)."
  else
    gcloud iam service-accounts create "${SA_NAME}" \
      --project="${PROJECT_ID}" \
      --display-name="FinOps Optimizer for BigQuery runtime" \
      --description="Read-only INFORMATION_SCHEMA telemetry. No table data access." \
      >/dev/null
    info "Created."
    # IAM propagation is eventually consistent; a binding immediately after
    # creation can 400 with "does not exist".
    info "Waiting 10s for IAM propagation..."
    sleep 10
  fi
}

bind_project_roles() {
  step "Project-level role bindings on ${PROJECT_ID}"
  # add-iam-policy-binding is idempotent: re-adding an existing binding is a no-op.
  local role
  for role in "roles/bigquery.jobUser" "roles/bigquery.metadataViewer"; do
    info "Binding ${role}"
    gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
      --member="serviceAccount:${SA_EMAIL}" \
      --role="${role}" \
      --condition=None \
      --quiet >/dev/null
  done

  dim "  Not bound automatically (grant only if you use the module):"
  dim "    roles/aiplatform.user                               — AI Doctor"
  dim "    roles/recommender.bigqueryPartitionClusterViewer    — Active Assist"
  dim "  Organization-level bindings are NOT touched here. See --emit-iam."
}

deploy_service() {
  step "Deploying '${SERVICE_NAME}' to Cloud Run (${REGION})"
  info "Building from source at ${REPO_ROOT} — this takes a few minutes."

  # --no-allow-unauthenticated is the authentication boundary. The application
  # itself has no login screen.
  # No application environment variables are required.
  gcloud run deploy "${SERVICE_NAME}" \
    --source "${REPO_ROOT}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --platform=managed \
    --service-account="${SA_EMAIL}" \
    --no-allow-unauthenticated \
    --ingress=all \
    --memory="${MEMORY}" \
    --cpu="${CPU}" \
    --min-instances="${MIN_INSTANCES}" \
    --max-instances="${MAX_INSTANCES}" \
    --timeout="${TIMEOUT}" \
    --quiet
}

postflight() {
  local url
  url="$(gcloud run services describe "${SERVICE_NAME}" \
           --project="${PROJECT_ID}" --region="${REGION}" \
           --format='value(status.url)' 2>/dev/null || true)"

  step "Deployed"
  cat >&2 <<POST
  Service:          ${SERVICE_NAME}
  URL:              ${url:-<unavailable>}
  Runtime identity: ${SA_EMAIL}
  Public access:    DISABLED (--no-allow-unauthenticated)

  The URL returns 403 to anonymous callers. That is correct: this application
  has no built-in login, so Cloud Run IAM is the authentication boundary.

  --- Open it privately (recommended) ---------------------------------------
  Authenticated localhost tunnel — no public exposure, no IAP setup:

      gcloud run services proxy ${SERVICE_NAME} \\
          --project=${PROJECT_ID} \\
          --region=${REGION} \\
          --port=8080

  Then browse http://127.0.0.1:8080

  --- Grant a colleague access ----------------------------------------------
      gcloud run services add-iam-policy-binding ${SERVICE_NAME} \\
          --project=${PROJECT_ID} --region=${REGION} \\
          --member="user:person@example.com" \\
          --role="roles/run.invoker"

  --- Prove the data-plane boundary -----------------------------------------
      python3 deploy/check_permissions.py \\
          --project=${PROJECT_ID} \\
          --impersonate-service-account=${SA_EMAIL}

  Org-scoped modules (Compute, Slots, Governance, AI Doctor discovery) need the
  organization binding from --emit-iam. Without it they return an actionable
  403; the project-scoped modules still work.
POST
}

# ---------------------------------------------------------------------------
main() {
  case "${MODE}" in
    emit-iam)
      [[ "${SKIP_PREFLIGHT}" == "true" ]] || preflight
      emit_iam
      ;;
    deploy)
      [[ "${SKIP_PREFLIGHT}" == "true" ]] || preflight
      create_service_account
      bind_project_roles
      deploy_service
      postflight
      ;;
    *)
      usage; fail "Unreachable mode: ${MODE}" ;;
  esac
}

main
