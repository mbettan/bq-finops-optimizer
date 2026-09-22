# GAP-28.4 — Enterprise InfoSec & Deployment Package

| Field | Value |
| :--- | :--- |
| **Spec ID** | GAP-28.4 |
| **Epic** | [GAP-28 — Enterprise Field Readiness](README.md) |
| **Status** | `SHIPPED` |
| **Module** | Governance & Partition Guardrails |
| **Target Release** | v1.4.4 |
| **Depends On** | — |
| **Blocks** | — |
| **Estimated Effort** | L — 4-6 days |
| **Owner** | _unassigned_ |
| **Last Updated** | September 16, 2026 |

---

## 1. Problem

Every enterprise evaluation reaches a security review that asks the same three questions:

1. **"Can this tool read our data?"** — we say no, but we offer prose, not proof.
2. **"Exactly which IAM roles does it need, and why?"** — the README documents predefined roles, which are
   coarse; a reviewer cannot see what is *excluded*.
3. **"Give us a deployment we can run without granting broad privileges."** — today deployment is a
   copy-pasted `gcloud run deploy` with no service account guidance.

This stalls deals for weeks in a queue that has nothing to do with the product's value.

## 2. Evidence

| Claim | Evidence |
| :--- | :--- |
| Roles are documented but only as predefined bundles | `README.md:414-436` — `jobUser`, `metadataViewer`, `resourceViewer`, `aiplatform.user` |
| `metadataViewer` at org level is **optional** today | `README.md:425` — describes a `SCHEMATA_OPTIONS` fast path with a project-by-project fallback |
| Org-scoped views in use go beyond what a short role list implies | `README.md:424` — `JOBS_TIMELINE_BY_ORGANIZATION`, `JOBS_BY_ORGANIZATION`, `RESERVATIONS`, `CAPACITY_COMMITMENT_CHANGES`; plus `TABLE_STORAGE_BY_ORGANIZATION` at `src/bqrecommender.py:234` |
| The app self-attests to an upstream identity boundary | (Superseded) Replaced by a warning-only startup check (`src/main.py:203-244`). The real boundary is Cloud Run `--no-allow-unauthenticated` |
| 403s are already translated into actionable guidance in one path | `src/main.py:2170-2174` |
| Third-party assets are loaded from CDNs | `static/index.html:44` (Chart.js), `docs/simulator.html:347` (Font Awesome), `:362` (DataTables) |
| No deployment automation exists | No `deploy/` directory in the repository |

## 3. Goals / Non-Goals

**Goals**
* Make "we cannot read your data" **machine-verifiable**, not a claim.
* Ship a deployment script that works for an evaluator who is *not* an organization admin.
* Produce a whitepaper that a CISO can answer a questionnaire from, consistent with the README.

**Non-Goals**
* Terraform or any third-party IaC (bash + `gcloud` only — runs in Cloud Shell as-is).
* Adding authentication to the app itself (Cloud Run IAM / IAP remains the boundary).
* SOC2/ISO certification claims.

## 4. Design

### 4.1 Custom IAM role — proof instead of prose

A predefined-role list is an assertion. A custom role is evidence a reviewer can run
`gcloud iam roles describe` against.

`deploy/roles/bq_finops_reader.yaml`:

```yaml
title: "BigQuery FinOps Optimizer — Read-Only Telemetry"
description: "Organization-scoped INFORMATION_SCHEMA telemetry access. Cannot read table contents."
stage: GA
includedPermissions:
  - bigquery.jobs.listAll            # JOBS_BY_ORGANIZATION, JOBS_TIMELINE_BY_ORGANIZATION
  - bigquery.jobs.get
  - bigquery.jobs.create             # submit the diagnostic queries themselves
  - bigquery.tables.list             # TABLE_STORAGE_BY_ORGANIZATION, SCHEMATA_OPTIONS
  - bigquery.tables.get              # schema / partitioning / clustering metadata
  - bigquery.reservations.get
  - bigquery.reservations.list
  - bigquery.capacityCommitments.get
  - bigquery.capacityCommitments.list
# DELIBERATELY EXCLUDED — this is the data-plane boundary:
#   bigquery.tables.getData          ← cannot read a single row of customer data
#   bigquery.tables.update / delete
#   bigquery.datasets.*  (create/update/delete)
#   bigquery.reservations.update / delete
#   bigquery.capacityCommitments.create / update / delete
```

> **Least-privilege note:** never grant `roles/bigquery.admin` at the organization node. The omission of
> `bigquery.tables.getData` is the single line a CISO needs, and it is independently verifiable.

The Vertex AI path (AI Doctor) stays **separate and optional** — `roles/aiplatform.user` on the execution
project only, never bundled into the org-level role.

### 4.2 Deployment script — split privilege model

Most evaluators do not hold `roles/resourcemanager.organizationAdmin`, so a script that binds org-level
IAM fails at its first call and leaves a half-provisioned project.

`deploy/deploy_cloud_run.sh` therefore has two distinct modes:

| Mode | Who runs it | Behavior |
| :--- | :--- | :--- |
| `--emit-iam` | Platform / security team | **Prints** the org-level role creation + binding commands for review. Executes nothing. |
| `--deploy` | Evaluator | Creates `bq-finops-sa` (idempotent), binds project-level roles, runs `gcloud run deploy --source .` with `--no-allow-unauthenticated`, `--service-account`, ``--memory 2Gi --cpu 1 --min-instances 0 --max-instances 3` |

Both modes run a **preflight** that verifies the `run`, `cloudbuild` and `artifactregistry` APIs are
enabled and that the caller holds `roles/cloudbuild.builds.editor` — failing with an actionable message
instead of a raw `gcloud` stack trace. `--deploy` finishes with a **postflight** printing the exact
`gcloud run services proxy` command for private browser access.

Idempotency: re-running `--deploy` against an existing `bq-finops-sa` updates rather than errors.

### 4.3 Permission verifier

`deploy/check_permissions.py` uses `projects.testIamPermissions` (and the reservations equivalent) so it
needs **no privilege of its own**. It asserts in both directions:

* **Positive** — every permission the app actually needs is held.
* **Negative** — `bigquery.tables.getData` is **not** held. This is the headline output.

Exit code is non-zero if either assertion fails, so it can be dropped into a customer's own pipeline.

### 4.4 Whitepaper — `docs/SECURITY_AND_PERMISSIONS.md`

> [!IMPORTANT]
> The whitepaper must **reconcile with, not contradict**, `README.md:414-436`. Today's README states that
> org-level `metadataViewer` is *optional* with a documented slower fallback. Any statement that it is
> required is a regression in accuracy.

Contents:

1. **Data plane vs. control plane** — with the custom role YAML as the proof artifact.
   * *Data plane* (table contents) requires `bigquery.tables.getData`. Never requested, never used.
   * *Control plane* (job telemetry, bytes billed, slot-ms, storage sizes, reservations) — read strictly through `INFORMATION_SCHEMA`.
2. **Egress boundary** — no phone-home. Enumerate every external asset: `cdn.jsdelivr.net` (Chart.js),
   `cdnjs.cloudflare.com` (Font Awesome), `cdn.datatables.net`. Note the option to vendor them for
   air-gapped review (epic Q3).
3. **Data residency** — what is written where: `app.log` (rotating, 10 MB × 5), browser `localStorage`
   keys prefixed `bq_`, and the fact that query *results* are never persisted server-side.
4. **Role matrix** — every INFORMATION_SCHEMA view used, the permission it needs, the predefined role that
   grants it, and the custom-role equivalent.
5. **Authentication boundary** — the app has **no built-in authentication**. Document that the boundary
   is Cloud Run IAM (`--no-allow-unauthenticated`) or IAP, exactly what that does and does not cover,
   and that `_warn_if_publicly_bound()` (`src/main.py:204-248`) is a startup *warning*, not a control.
   (Superseded: this section previously documented the `AUTH_ENFORCED_UPSTREAM` attestation flag, which
   has since been removed.)
6. **Verification** — how to run `check_permissions.py` and read its output.

A doc test diffs the whitepaper's role list against the README's so the two cannot drift.

## 5. Implementation Tasks

- [ ] Enumerate every INFORMATION_SCHEMA view the codebase queries (grep `INFORMATION_SCHEMA`) and map each to its required permission.
- [ ] Author `deploy/roles/bq_finops_reader.yaml`; validate with `gcloud iam roles create --dry-run` equivalent.
- [ ] Write `deploy/deploy_cloud_run.sh` with `--emit-iam` / `--deploy`, preflight, postflight and idempotent SA handling.
- [ ] Write `deploy/check_permissions.py` with positive **and** negative assertions.
- [ ] Write `docs/SECURITY_AND_PERMISSIONS.md` (six sections above).
- [ ] Add the doc test diffing whitepaper ↔ README role lists.
- [ ] Link the whitepaper from `README.md` §IAM and from the Cloud Run deployment section.
- [ ] Verify the whole flow end-to-end in a scratch project, including the non-admin path.
- [ ] `RELEASE_NOTES.md` entry.

## 6. Acceptance Criteria

1. `gcloud iam roles describe` on the created custom role returns a permission set containing **no** `bigquery.tables.getData`.
2. A service account holding only the custom role can complete a full analysis run (all views, all modules) without a permission error.
3. `check_permissions.py` exits 0 on a correctly provisioned SA and prints the data-plane exclusion explicitly; exits non-zero if `bigquery.tables.getData` is present.
4. `deploy_cloud_run.sh --emit-iam` performs **zero** mutations (verified against Cloud Audit Logs in a scratch project).
5. `deploy_cloud_run.sh --deploy` succeeds for a caller who is *not* an organization admin, given the org bindings were applied beforehand.
6. Re-running `--deploy` against an existing deployment succeeds (idempotent).
7. Preflight fails with an actionable message when the Cloud Build API is disabled — not a raw gcloud error.
8. The whitepaper's role matrix and `README.md:414-436` agree, asserted by the doc test.
9. The whitepaper enumerates all three CDN origins.

## 7. Risks & Mitigations

| Risk | Mitigation |
| :--- | :--- |
| The custom role is missing a permission and analysis fails mid-run | AC 2 requires a full end-to-end run on the custom role alone before merge |
| Google adds a permission requirement to an INFORMATION_SCHEMA view later | `check_permissions.py` is the early-warning system; document it as a pre-upgrade check |
| A whitepaper with wrong roles is worse than none | Doc test against the README; every claim cites a file/line or a Google doc |
| `--source .` builds surprise customers with Cloud Build costs and Artifact Registry artifacts | Preflight states what will be created and roughly what it costs |
| Publishing a detailed security document invites scrutiny of gaps | That is the point — it is better found by us than in a review call |

## 8. Rollback

All artifacts are additive (`deploy/`, `docs/SECURITY_AND_PERMISSIONS.md`). Rollback = delete them. No
runtime code path changes.

## 9. Open Questions

| # | Question | Default |
| :--- | :--- | :--- |
| 1 | Vendor the three CDN assets for air-gapped review? *(epic Q3)* | Document only; no vendoring in v1.5.0. |
| 2 | Also publish a Terraform module? | No — bash + gcloud only, so it runs in Cloud Shell with zero prerequisites. |
| 3 | Should the custom role be org-level only, or also offered folder-scoped? | Document both bindings; the script emits org-level by default. |
