# BigQuery FinOps Optimizer

A tool for analyzing and optimizing BigQuery storage and compute costs by switching between logical and physical billing models.

---

## 🚀 Getting Started

### 1. Installation
1. Navigate to the project directory.
2. Activate your virtual environment:
   ```bash
   source venv/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### 2. Launching the App
Run the FastAPI server using `uvicorn`:
```bash
uvicorn main:app --reload
```
The application will be accessible at `http://127.0.0.1:8000`.

---

## 🔑 Authentication & Configuration

### 🔑 Complete Authentication Setup (Local Development)

If you are running the tool locally, follow these steps to ensure your environment is correctly configured for BigQuery access and billing.

#### Step 1: Login to gcloud CLI
This authenticates your primary user account for the gcloud CLI.

```bash
gcloud auth login
```

#### Step 2: Set your Active Project
Set the default project for the gcloud CLI context.

```bash
gcloud config set project <YOUR_PROJECT_ID>
```

#### Step 3: Generate Application Default Credentials (ADC)
This is what the Python BigQuery Client uses to authenticate your API calls.

```bash
gcloud auth application-default login
```

#### Step 4: Set the Quota Project for ADC (Crucial for API Billing)
If you encounter `Access Denied` or `jobs.create` errors (especially with `INFORMATION_SCHEMA` queries), it means the API needs a defined project for quota and billing. This command writes the quota project into your ADC file.

```bash
gcloud auth application-default set-quota-project <YOUR_PROJECT_ID>
```

#### 🔍 Verification: Check your setup

To verify your active configuration:

```bash
gcloud config list
```

To verify which account is used for ADC:

```bash
gcloud auth application-default print-access-token
```

2. **Option B: Using a Service Account Key**:
   Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
   ```

### Changing the Target Project ID
The application dynamically scopes BigQuery clients based on incoming API requests. You do not need to hardcode a project ID in the backend. 
- In the **Frontend UI**, you can specify the `Work Project` or `Org Project ID` in the respective input fields.
- In **API Calls**, include the `work_project` or `org_project_id` in the JSON payload (e.g., `{"work_project": "my-target-project"}`).

---

## 🔒 Permissions & Roles

This document outlines the required Identity and Access Management (IAM) roles and permissions to run the BigQuery FinOps Optimizer application successfully, particularly when querying organization-wide `INFORMATION_SCHEMA` views.

## Resolving 403 Access Denied for Organization Views

If you encounter an error like:
`Access Denied: Table ... INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION: User does not have permission to query table...`

This issue occurs because querying organization-wide views requires permissions at the **Organization level**, not just the project level.

### Required Permission
*   `bigquery.jobs.listAll` for the **Organization**.

### Recommended Roles (Granted at the Organization Level)
To resolve this, grant one of the following roles to the Service Account running the application at the **Google Cloud Organization level**:

*   **BigQuery Resource Admin** (`roles/bigquery.resourceAdmin`)
*   **Organization Admin** (`roles/resourcemanager.organizationAdmin`)
*   **Organization Owner**

---

## Complete Permission Requirements

For the application to function fully (Storage, Compute, and Editions features), the Service Account requires the following permissions across your environment:

### 1. Project-Level Permissions (Where the app runs/queries)
*   **BigQuery Job User** (`roles/bigquery.jobUser`): Required to execute queries.
*   **BigQuery Metadata Viewer** (`roles/bigquery.metadataViewer`): Required to read standard `INFORMATION_SCHEMA` views (like tables and schemas).

### 2. Organization-Level Permissions (For `*_BY_ORGANIZATION` views)
*   **BigQuery Resource Admin** (`roles/bigquery.resourceAdmin`): Required to read `JOBS_TIMELINE_BY_ORGANIZATION`, `JOBS_BY_ORGANIZATION`, and `TABLE_STORAGE_BY_ORGANIZATION`. It also allows viewing/modifying reservations.

### 3. Dataset-Level Permissions (For Storage Actions)
*   **BigQuery Data Owner** (`roles/bigquery.dataOwner`): Required if you want the tool to execute `ALTER SCHEMA` DDL statements to switch storage billing models for datasets.

> [!IMPORTANT]
> Ensure that any role required for `*_BY_ORGANIZATION` views is granted at the **Organization** resource level in the IAM console, not the project level.
