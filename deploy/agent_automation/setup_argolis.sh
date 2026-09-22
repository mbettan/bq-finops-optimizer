#!/usr/bin/env bash
# ==============================================================================
# Autonomous Issue-to-PR Agent Deployment for GCP Project: bq-finops-optimizer
# ==============================================================================
set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-bq-finops-optimizer}"
REGION="${GCP_REGION:-us-central1}"
VERTEX_REGION="${VERTEX_REGION:-us-east5}"
GITHUB_REPO="${GITHUB_REPO:-mbettan/bq-finops-optimizer-private}"
AR_REPO="agent-automation"
IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/bq-finops-agent:latest"
POLLER_JOB="bq-finops-agent-poller"
WORKER_JOB="bq-finops-agent-worker"
SCHEDULER_JOB="bq-finops-agent-poller-cron"
POLLER_SA="bq-finops-poller-sa"
WORKER_SA="bq-finops-worker-sa"
SECRET_NAME="github-agent-pat"

echo "=== [1/7] Enabling Required Google Cloud APIs in ${PROJECT_ID} ==="
gcloud services enable \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  aiplatform.googleapis.com \
  iam.googleapis.com \
  --project="${PROJECT_ID}"

echo "=== [2/7] Creating Least-Privilege Service Accounts ==="
if ! gcloud iam service-accounts describe "${POLLER_SA}@${PROJECT_ID}.iam.gserviceaccount.com" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud iam service-accounts create "${POLLER_SA}" \
    --display-name="FinOps Agent Pull Poller SA" \
    --project="${PROJECT_ID}"
fi

if ! gcloud iam service-accounts describe "${WORKER_SA}@${PROJECT_ID}.iam.gserviceaccount.com" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud iam service-accounts create "${WORKER_SA}" \
    --display-name="FinOps Agent Ephemeral Worker SA" \
    --project="${PROJECT_ID}"
fi

# Grant Worker SA Vertex AI User permission (for Claude 3.7 Sonnet on Vertex AI)
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${WORKER_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/aiplatform.user" \
  --condition=None \
  --quiet

# Grant Poller SA permission to trigger Cloud Run Jobs
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${POLLER_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/run.developer" \
  --condition=None \
  --quiet

echo "=== [3/7] Ensuring Secret Manager Secret (${SECRET_NAME}) Exists ==="
if ! gcloud secrets describe "${SECRET_NAME}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud secrets create "${SECRET_NAME}" \
    --replication-policy="automatic" \
    --project="${PROJECT_ID}"
  if [[ -n "${GITHUB_PAT:-}" ]]; then
    printf "%s" "${GITHUB_PAT}" | gcloud secrets versions add "${SECRET_NAME}" --data-file=- --project="${PROJECT_ID}"
  else
    echo "NOTE: Secret ${SECRET_NAME} created. Add your Fine-Grained GitHub PAT via:"
    echo "  printf '%s' 'github_pat_...' | gcloud secrets versions add ${SECRET_NAME} --data-file=- --project=${PROJECT_ID}"
  fi
fi

for SA in "${POLLER_SA}" "${WORKER_SA}"; do
  gcloud secrets add-iam-policy-binding "${SECRET_NAME}" \
    --member="serviceAccount:${SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor" \
    --project="${PROJECT_ID}" \
    --quiet
done

echo "=== [4/7] Creating Artifact Registry Repository (${AR_REPO}) ==="
if ! gcloud artifacts repositories describe "${AR_REPO}" --location="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud artifacts repositories create "${AR_REPO}" \
    --repository-format=docker \
    --location="${REGION}" \
    --description="Container repository for Autonomous Issue-to-PR Agent" \
    --project="${PROJECT_ID}"
fi

echo "=== [5/7] Building & Pushing Agent Image via Cloud Build ==="
gcloud builds submit . \
  --config=/dev/stdin \
  --project="${PROJECT_ID}" <<CLOUD_BUILD_EOF
steps:
- name: 'gcr.io/cloud-builders/docker'
  args: ['build', '-t', '${IMAGE_URI}', '-f', 'deploy/agent_automation/Dockerfile.agent', '.']
images:
- '${IMAGE_URI}'
CLOUD_BUILD_EOF

echo "=== [6/7] Deploying Cloud Run Jobs (${WORKER_JOB} & ${POLLER_JOB}) ==="
gcloud run jobs deploy "${WORKER_JOB}" \
  --image="${IMAGE_URI}" \
  --command="/app/worker_entrypoint.sh" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --service-account="${WORKER_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --set-secrets="GITHUB_PAT=${SECRET_NAME}:latest" \
  --set-env-vars="GCP_PROJECT_ID=${PROJECT_ID},GCP_REGION=${REGION},VERTEX_REGION=${VERTEX_REGION},GITHUB_REPO=${GITHUB_REPO}" \
  --max-retries=0 \
  --task-timeout=3600s \
  --cpu=2 \
  --memory=4Gi

gcloud run jobs deploy "${POLLER_JOB}" \
  --image="${IMAGE_URI}" \
  --command="/opt/venv/bin/python3" \
  --args="/app/poller.py" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --service-account="${POLLER_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --set-secrets="GITHUB_PAT=${SECRET_NAME}:latest" \
  --set-env-vars="GCP_PROJECT_ID=${PROJECT_ID},GCP_REGION=${REGION},WORKER_JOB_NAME=${WORKER_JOB},GITHUB_REPO=${GITHUB_REPO}" \
  --max-retries=0 \
  --task-timeout=300s \
  --cpu=1 \
  --memory=1Gi

echo "=== [7/7] Configuring 5-Minute Cloud Scheduler Trigger (${SCHEDULER_JOB}) ==="
POLLER_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${POLLER_JOB}:run"
if gcloud scheduler jobs describe "${SCHEDULER_JOB}" --location="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud scheduler jobs update http "${SCHEDULER_JOB}" \
    --location="${REGION}" \
    --schedule="*/5 * * * *" \
    --uri="${POLLER_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${POLLER_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --project="${PROJECT_ID}"
else
  gcloud scheduler jobs create http "${SCHEDULER_JOB}" \
    --location="${REGION}" \
    --schedule="*/5 * * * *" \
    --uri="${POLLER_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${POLLER_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --project="${PROJECT_ID}"
fi

echo "✅ Autonomous Issue-to-PR Agent deployed in ${PROJECT_ID} (${REGION})!"
