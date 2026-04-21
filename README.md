# BigQuery FinOps Optimizer

A tool for analyzing and optimizing BigQuery storage and compute costs by switching between logical and physical billing models.

## ✨ Key Features

- **Storage Optimization**: Analyze potential savings by switching between Logical and Physical storage billing models.
- **Compute Optimization**: Compare On-Demand vs. Editions pricing for your historical query workload.
- **Edition Matrix Simulation**: Advanced statistical simulation (Geh Bucket Method) to find the optimal baseline for Autoscale scenarios. Projects costs to a standard 730-hour billing month and provides bucket frequency and utilization metrics.
- **Profile Categorization**: Automatically categorizes queries into **Reservation Candidates**, **On-Demand Candidates**, and **Balanced / Uncertain** based on cost efficiency.
- **Interactive UI Filtering**: Filter top inefficient queries by their billing profile directly in the dashboard.
- **Scalable Analysis**: Displays the top **500** most inefficient queries to focus on high-impact optimization opportunities.
- **Workload Profiler**: Identifies reservations experiencing a "continuous trickle" of small, frequent queries. Identifies candidates for **Short Query Optimizations** in the Advanced Runtime.
- **Top Spenders**: Identifies the users consuming the most resources (data billed and slot hours) in your organization to help with cost attribution and optimization.

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
- In the **Frontend UI**, you can specify the `Org Project ID` in the respective input field.
- In **API Calls**, include the `org_project_id` in the JSON payload (e.g., `{"org_project_id": "my-target-project"}`).

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

---

## ⚠️ Security & Scale Considerations

When deploying and using this tool, please be mindful of the following security and scalability considerations:

1. **SQL Injection Risks**: The application currently uses direct string interpolation for some parameters (like region and project IDs) in SQL queries. Ensure strict input validation is applied if exposing this tool, and only grant the minimum necessary permissions to the executing identity.
2. **Scale Limits with Large Organizations**: In environments with thousands of projects, querying metadata across all projects simultaneously can exceed BigQuery's query size and complexity limits, or hit quota limits. Consider batching or scoped analysis for very large scales.
3. **No Built-in Authentication**: This tool lacks built-in authentication or authorization mechanisms. **Do not expose this application to the public internet or internally without adding an authentication layer.** It is strongly recommended to run this tool locally or within a secure internal network.

---

## 📊 Example Output: Top Offending Queries

Here is an example of the output from the **Workload Profiler** showing the most frequent small queries that can cause autoscaler waste:

| Query Text | Project | Example Job ID | Frequency | Avg Slot Hours | Avg Duration (s) | Avg Bytes | Recommendation |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `COMMIT TRANSACTION` | `identity-services-2` | `script_job_e38a440...` | 21,456 | 0.000138 | 0.01 | 0 MB | Candidate |
| `BEGIN TRANSACTION` | `identity-services-2` | `script_job_41a26b5...` | 24,188 | 0.000000 | 0.00 | 0 MB | Candidate |
| `SELECT * FROM \`analytics_project.ecommerce.products\`` | `ecommerce-web-5` | `job_xvB6HRmV8Tec6i...` | 5,259 | 0.000000 | 0.00 | 1.0 MB | Candidate |
| `INSERT INTO \`marketing_project.campaigns.metrics\`` | `marketing-analytics-5` | `job_uNhCQFFnhCdGax...` | 7,642 | 0.000000 | 1.22 | 0 MB | Candidate |

---

## ⚠️ Disclaimer

This tool is provided "as is" without warranty of any kind. It performs complex calculations and simulations based on BigQuery metadata. Calculations may contain mistakes or not reflect all aspects of your specific billing situation (such as custom discounts or blended rates). 

**Always verify the results and generated DDL statements manually before making any changes to your production environment.**
