#!/usr/bin/env bash
# ==============================================================================
# Deploy BigQuery FinOps Optimizer to Cloud Run with Server-Side GCS Result Cache
# ==============================================================================
set -euo pipefail

# ------------------------------------------------------------------------------
# Configuration & Defaults
# ------------------------------------------------------------------------------
ACTIVE_GCLOUD_PROJECT="$(gcloud config get-value project 2>/dev/null || true)"
PROJECT_ID="${PROJECT_ID:-${ACTIVE_GCLOUD_PROJECT}}"

if [[ -z "${PROJECT_ID}" ]]; then
    echo "❌ Error: PROJECT_ID is not set and no active gcloud project found."
    echo "Usage: PROJECT_ID=my-project-id ./deploy.sh"
    exit 1
fi

EXECUTION_PROJECT_ID="${EXECUTION_PROJECT_ID:-}"
ORG_ID="${ORG_ID:-}"
REGION="${REGION:-us-central1}"
SERVICE_NAME="${SERVICE_NAME:-finops-optimizer}"
CACHE_BUCKET="${CACHE_BUCKET:-${PROJECT_ID}-cache}"
SA_NAME="${SA_NAME:-svc-finops-optimizer}"
CACHE_TTL="${CACHE_TTL_DEFAULT:-900}" # 15 minutes default for initial staging
AR_REPO="${AR_REPO:-cloud-run-source-deploy}"
IMAGE_TAG="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/${SERVICE_NAME}:latest"

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${BLUE}ℹ️  $*${NC}"; }
log_success() { echo -e "${GREEN}✅ $*${NC}"; }
log_warn() { echo -e "${YELLOW}⚠️  $*${NC}"; }
log_error() { echo -e "${RED}❌ $*${NC}"; }

echo -e "${BLUE}"
echo "=================================================================="
echo "⚡ Deploying BigQuery FinOps Optimizer to Google Cloud Run"
echo "=================================================================="
echo -e "${NC}"
echo "Project ID       : ${PROJECT_ID}"
echo "Region           : ${REGION}"
echo "Service Name     : ${SERVICE_NAME}"
echo "Cache Bucket     : gs://${CACHE_BUCKET}"
echo "Service Account  : ${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
echo "Artifact Image   : ${IMAGE_TAG}"
echo "Default Cache TTL: ${CACHE_TTL}s"
echo ""

# ------------------------------------------------------------------------------
# 1. Enable Required GCP APIs
# ------------------------------------------------------------------------------
log_info "Step 1: Enabling required Google Cloud APIs on ${PROJECT_ID}..."
gcloud services enable \
    run.googleapis.com \
    storage.googleapis.com \
    cloudbuild.googleapis.com \
    artifactregistry.googleapis.com \
    bigquery.googleapis.com \
    bigquerystorage.googleapis.com \
    aiplatform.googleapis.com \
    iap.googleapis.com \
    --project="${PROJECT_ID}" \
    --quiet
log_success "APIs enabled."

# ------------------------------------------------------------------------------
# 2. Service Account Setup (Least-Privilege)
# ------------------------------------------------------------------------------
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
log_info "Step 2: Checking Service Account (${SA_EMAIL})..."

if ! gcloud iam service-accounts describe "${SA_EMAIL}" --project="${PROJECT_ID}" &>/dev/null; then
    log_info "Creating Service Account: ${SA_NAME}..."
    gcloud iam service-accounts create "${SA_NAME}" \
        --display-name="BigQuery FinOps Optimizer Service Account" \
        --project="${PROJECT_ID}"
    log_success "Created Service Account ${SA_EMAIL}."
else
    log_info "Service Account ${SA_EMAIL} already exists."
fi

log_info "Assigning project-level IAM roles..."
ROLES=(
    "roles/bigquery.jobUser"
    "roles/bigquery.metadataViewer"
    "roles/bigquery.resourceViewer"
    "roles/aiplatform.user"
)

for ROLE in "${ROLES[@]}"; do
    gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
        --member="serviceAccount:${SA_EMAIL}" \
        --role="${ROLE}" \
        --condition=None \
        --quiet &>/dev/null || true
done
log_success "Assigned project IAM roles on ${PROJECT_ID} to ${SA_EMAIL}."

# Assign roles on Execution Project if different from Host Project
if [[ -n "${EXECUTION_PROJECT_ID}" && "${EXECUTION_PROJECT_ID}" != "${PROJECT_ID}" ]]; then
    log_info "Assigning Execution Project roles on ${EXECUTION_PROJECT_ID}..."
    gcloud projects add-iam-policy-binding "${EXECUTION_PROJECT_ID}" \
        --member="serviceAccount:${SA_EMAIL}" \
        --role="roles/bigquery.jobUser" \
        --condition=None \
        --quiet &>/dev/null || true
    gcloud projects add-iam-policy-binding "${EXECUTION_PROJECT_ID}" \
        --member="serviceAccount:${SA_EMAIL}" \
        --role="roles/bigquery.dataViewer" \
        --condition=None \
        --quiet &>/dev/null || true
    log_success "Assigned bigquery.jobUser & dataViewer on ${EXECUTION_PROJECT_ID} to ${SA_EMAIL}."
fi

# Assign roles at Organization level if ORG_ID is supplied
if [[ -n "${ORG_ID}" ]]; then
    log_info "Assigning Organization-level roles on org ${ORG_ID}..."
    gcloud organizations add-iam-policy-binding "${ORG_ID}" \
        --member="serviceAccount:${SA_EMAIL}" \
        --role="roles/bigquery.resourceViewer" \
        --condition=None \
        --quiet &>/dev/null || true
    gcloud organizations add-iam-policy-binding "${ORG_ID}" \
        --member="serviceAccount:${SA_EMAIL}" \
        --role="roles/bigquery.metadataViewer" \
        --condition=None \
        --quiet &>/dev/null || true
    log_success "Assigned org-level BigQuery viewer roles on ${ORG_ID} to ${SA_EMAIL}."
fi

# Configure Cloud Build SA permissions for source-based builds
PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")
CB_ROLES=("roles/logging.logWriter" "roles/storage.objectViewer" "roles/artifactregistry.writer")
for ROLE in "${CB_ROLES[@]}"; do
    gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
        --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
        --role="${ROLE}" \
        --condition=None \
        --quiet &>/dev/null || true
    gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
        --member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
        --role="${ROLE}" \
        --condition=None \
        --quiet &>/dev/null || true
done

# ------------------------------------------------------------------------------
# 3. Cache Bucket Setup (GCS FUSE)
# ------------------------------------------------------------------------------
log_info "Step 3: Configuring GCS cache bucket gs://${CACHE_BUCKET}..."

if ! gcloud storage buckets describe "gs://${CACHE_BUCKET}" --project="${PROJECT_ID}" &>/dev/null; then
    log_info "Creating bucket gs://${CACHE_BUCKET} in ${REGION} (UBLA + PAP)..."
    gcloud storage buckets create "gs://${CACHE_BUCKET}" \
        --project="${PROJECT_ID}" \
        --location="${REGION}" \
        --uniform-bucket-level-access \
        --public-access-prevention \
        --quiet
    log_success "Created bucket gs://${CACHE_BUCKET}."
else
    log_info "Bucket gs://${CACHE_BUCKET} already exists."
fi

# Hard 7-day retention lifecycle rule to purge orphaned parameter variants
LIFECYCLE_TMP=$(mktemp /tmp/cache_lifecycle_XXXXXX.json)
cat <<EOF > "${LIFECYCLE_TMP}"
{"rule":[{"action":{"type":"Delete"},"condition":{"age":7}}]}
EOF
gcloud storage buckets update "gs://${CACHE_BUCKET}" --lifecycle-file="${LIFECYCLE_TMP}" --quiet
rm -f "${LIFECYCLE_TMP}"
log_success "Applied 7-day lifecycle retention policy."

# Grant objectUser (read/write objects) to the Cloud Run Service Account
log_info "Granting roles/storage.objectUser on gs://${CACHE_BUCKET} to ${SA_EMAIL}..."
gcloud storage buckets add-iam-policy-binding "gs://${CACHE_BUCKET}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/storage.objectUser" \
    --quiet &>/dev/null
log_success "Storage IAM binding granted."

# ------------------------------------------------------------------------------
# 4. Artifact Registry & Image Build
# ------------------------------------------------------------------------------
log_info "Step 4: Ensuring Artifact Registry repository '${AR_REPO}' exists..."
if ! gcloud artifacts repositories describe "${AR_REPO}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
    gcloud artifacts repositories create "${AR_REPO}" \
        --repository-format=docker \
        --location="${REGION}" \
        --description="Docker repository for Cloud Run deployments" \
        --project="${PROJECT_ID}" \
        --quiet
    log_success "Created Artifact Registry repository '${AR_REPO}'."
else
    log_info "Artifact Registry repository '${AR_REPO}' already exists."
fi

log_info "Building container image ${IMAGE_TAG} via Cloud Build..."
gcloud builds submit \
    --tag="${IMAGE_TAG}" \
    --project="${PROJECT_ID}" \
    --quiet
log_success "Container image built successfully."

# ------------------------------------------------------------------------------
# 5. Deploy to Cloud Run Gen 2 with GCS Volume Mount
# ------------------------------------------------------------------------------
# Authentication configuration
GOOGLE_CLIENT_ID="${GOOGLE_CLIENT_ID:-}"
GOOGLE_CLIENT_SECRET="${GOOGLE_CLIENT_SECRET:-}"
ALLOWED_DOMAINS="${ALLOWED_DOMAINS:-}"
ALLOWED_USERS="${ALLOWED_USERS:-}"
AUTH_FLAG="--no-allow-unauthenticated"
ENV_VARS="CACHE_BACKEND=file,CACHE_DIR=/cache,CACHE_TTL_DEFAULT=${CACHE_TTL},LOG_LEVEL=INFO"

if [[ -n "${GOOGLE_CLIENT_ID}" && -n "${GOOGLE_CLIENT_SECRET}" ]]; then
    log_info "Configuring Google OAuth authentication (direct browser login enabled)..."
    AUTH_FLAG="--allow-unauthenticated"
    ENV_VARS="${ENV_VARS},GOOGLE_CLIENT_ID=${GOOGLE_CLIENT_ID},GOOGLE_CLIENT_SECRET=${GOOGLE_CLIENT_SECRET}"
    if [[ -n "${ALLOWED_DOMAINS}" ]]; then
        ENV_VARS="${ENV_VARS},ALLOWED_DOMAINS=${ALLOWED_DOMAINS}"
    fi
    if [[ -n "${ALLOWED_USERS}" ]]; then
        ENV_VARS="${ENV_VARS},ALLOWED_USERS=${ALLOWED_USERS}"
    fi
else
    ENV_VARS="${ENV_VARS},AUTH_ENFORCED_UPSTREAM=true"
fi

gcloud run deploy "${SERVICE_NAME}" \
    --image="${IMAGE_TAG}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --service-account="${SA_EMAIL}" \
    --execution-environment="gen2" \
    --add-volume="name=cache,type=cloud-storage,bucket=${CACHE_BUCKET}" \
    --add-volume-mount="volume=cache,mount-path=/cache" \
    --set-env-vars="${ENV_VARS}" \
    --memory="2Gi" \
    --cpu="1" \
    --min-instances="0" \
    --max-instances="10" \
    ${AUTH_FLAG} \
    --quiet

# Automatically grant roles/run.invoker to the deploying user
ACTIVE_USER=$(gcloud config get-value account 2>/dev/null || true)
if [[ -n "${ACTIVE_USER}" ]]; then
    log_info "Granting roles/run.invoker to ${ACTIVE_USER}..."
    gcloud run services add-iam-policy-binding "${SERVICE_NAME}" \
        --project="${PROJECT_ID}" \
        --region="${REGION}" \
        --member="user:${ACTIVE_USER}" \
        --role="roles/run.invoker" \
        --quiet &>/dev/null || true
    
    log_info "Granting roles/iap.httpsResourceAccessor to ${ACTIVE_USER}..."
    gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
        --member="user:${ACTIVE_USER}" \
        --role="roles/iap.httpsResourceAccessor" \
        --condition=None \
        --quiet &>/dev/null || true
fi

# Configure IAP Service Agent Invoker permissions
IAP_SA="service-${PROJECT_NUMBER}@gcp-sa-iap.iam.gserviceaccount.com"
log_info "Granting Cloud Run Invoker role to IAP Service Agent (${IAP_SA})..."
gcloud beta services identity create --service=iap.googleapis.com --project="${PROJECT_ID}" --quiet &>/dev/null || true
gcloud run services add-iam-policy-binding "${SERVICE_NAME}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --member="serviceAccount:${IAP_SA}" \
    --role="roles/run.invoker" \
    --quiet &>/dev/null || true

# Grant IAP access to configured allowed domains / users if specified
if [[ -n "${ALLOWED_DOMAINS}" ]]; then
    IFS=',' read -ra DOMAINS <<< "${ALLOWED_DOMAINS}"
    for DOMAIN in "${DOMAINS[@]}"; do
        DOMAIN=$(echo "${DOMAIN}" | xargs)
        if [[ -n "${DOMAIN}" ]]; then
            log_info "Granting roles/iap.httpsResourceAccessor to domain:${DOMAIN}..."
            gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
                --member="domain:${DOMAIN}" \
                --role="roles/iap.httpsResourceAccessor" \
                --condition=None \
                --quiet &>/dev/null || true
        fi
    done
fi

if [[ -n "${ALLOWED_USERS}" ]]; then
    IFS=',' read -ra USERS <<< "${ALLOWED_USERS}"
    for USER_EMAIL in "${USERS[@]}"; do
        USER_EMAIL=$(echo "${USER_EMAIL}" | xargs)
        if [[ -n "${USER_EMAIL}" ]]; then
            log_info "Granting roles/iap.httpsResourceAccessor to user:${USER_EMAIL}..."
            gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
                --member="user:${USER_EMAIL}" \
                --role="roles/iap.httpsResourceAccessor" \
                --condition=None \
                --quiet &>/dev/null || true
        fi
    done
fi

SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" --project="${PROJECT_ID}" --region="${REGION}" --format="value(status.url)")

echo ""
echo -e "${GREEN}=================================================================="
echo "🎉 Deployment Complete!"
echo "=================================================================="
echo -e "${NC}"
echo "Service URL      : ${SERVICE_URL}"
echo "Cache Mount      : gs://${CACHE_BUCKET} -> /cache"
echo "Execution Env    : Cloud Run Gen 2 (gcsfuse enabled)"
echo "Authentication   : Secured (IAM / IAP enabled)"
echo ""
echo -e "${YELLOW}=================================================================="
echo "🔐 Step 1: Turn on Identity-Aware Proxy (IAP) (1 minute)"
echo "==================================================================${NC}"
echo "1. Open the Identity-Aware Proxy Console:"
echo "   👉 https://console.cloud.google.com/security/iap?project=${PROJECT_ID}"
echo ""
echo "2. If prompted for OAuth Consent Screen:"
echo "   - User type : Internal"
echo "   - App name  : BigQuery FinOps Optimizer"
echo "   - Save & Continue"
echo ""
echo "3. In the IAP resources table, locate '${SERVICE_NAME}'."
echo "4. Click the toggle switch under the IAP column to turn it ON (confirm in popup)."
echo ""
echo -e "${GREEN}Once enabled, open the application directly in Chrome:${NC}"
echo "👉 ${SERVICE_URL}"
echo ""
echo "Or test via authenticated CLI proxy:"
echo "  gcloud run services proxy ${SERVICE_NAME} --region ${REGION} --project ${PROJECT_ID}"
echo ""
