# Disclaimer & Legal Notices

> [!IMPORTANT]
> **This is not a Google product.** It is an independent, personal open-source
> project released under the [Apache 2.0 License](LICENSE). It is not developed,
> maintained, reviewed, endorsed, or supported by Google LLC, Alphabet Inc., or
> Google Cloud. No support, SLA, or guarantee of correctness is offered or implied.
>
> This project was built independently, on personal time, with personal resources.
> It reflects the personal views of its author alone and not those of any employer,
> and it incorporates no confidential or proprietary information belonging to any
> third party.
>
> *Google*, *Google Cloud*, *BigQuery*, *Vertex AI*, *Gemini*, *Active Assist*, and
> all related marks are trademarks of Google LLC, used here nominatively to describe
> the services this tool reads from. Their use does not imply any affiliation or
> endorsement.

---

## Use Google's native tooling as your source of truth

This tool is an **advisory diagnostic layer** over BigQuery `INFORMATION_SCHEMA`
metadata. It is not a billing system, not a capacity manager, and not an
authoritative record of anything.

Before acting on any output here, verify it against the official surfaces that
Google supports and stands behind:

| To verify… | Use |
| :--- | :--- |
| Cost, spend, and billing | [Cloud Billing Console](https://console.cloud.google.com/billing), Cloud Billing export to BigQuery, and your invoice — these are authoritative; this tool is not |
| Capacity and reservations | BigQuery Reservations / Capacity Manager |
| Partitioning and clustering advice | [Active Assist](https://cloud.google.com/active-assist) and the Recommender API |
| Query behaviour and performance | [BigQuery Studio](https://console.cloud.google.com/bigquery), `bq`, `gcloud`, [Cloud Monitoring](https://cloud.google.com/monitoring) |

Every dollar figure in this application is a **modelled estimate**, computed from
historical telemetry at **public list prices**. It cannot see your Private Pricing
Addendum, Committed Use Discounts, negotiated rates, credits, currency terms, or
internal chargeback model. Past usage is not a prediction of future usage, and real
costs will differ — potentially by a wide margin.

Nothing produced by this tool is financial, architectural, tax, or legal advice.

---

## You are responsible for everything you run

You run this software on **your** Google Cloud infrastructure, under **your**
credentials, at **your** expense. That carries real, concrete obligations:

- **This tool costs money to operate.** It executes BigQuery jobs against
  organization-scoped `INFORMATION_SCHEMA` views, and the AI Doctor module invokes
  Vertex AI. Those jobs are billed to your projects. Configure the **Max Bytes
  Billed** safety cap before your first scan, and understand what a
  full-organization run will consume.

- **Read the code before you run it.** It is open source specifically so that you
  can audit it. Doing so is your responsibility, not the author's.

- **Never apply generated SQL or DDL to production unreviewed.** `ALTER SCHEMA`
  statements, storage-billing-model changes, time-travel window reductions,
  reservation changes, and AI-suggested query rewrites must be independently
  reviewed and tested in a non-production environment first. Some of these
  operations are difficult or impossible to reverse, and some carry data-loss risk.

- **Understand where your data goes.** The AI Doctor sends SQL query text to a
  Vertex AI Gemini endpoint. Exported snapshots may contain user email addresses,
  project identifiers, and raw SQL. Confirm both are compatible with your
  organization's data-handling, residency, and privacy policies before enabling
  or sharing them.

- **Apply least privilege.** Grant only the IAM roles you need, and never expose
  this application to the internet without IAP or Cloud Run IAM in front of it —
  it has no built-in authentication and surfaces org-wide metadata.

Perform your own due diligence. If a finding here would drive a material decision,
independently confirm it first.

---

## No warranty, no liability

This section restates and supplements Sections 7 and 8 of the
[Apache 2.0 License](LICENSE).

THE SOFTWARE IS PROVIDED "AS IS" AND "AS AVAILABLE", WITHOUT WARRANTY OR CONDITION
OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY WARRANTY OF TITLE,
NON-INFRINGEMENT, MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, ACCURACY,
COMPLETENESS, OR UNINTERRUPTED AVAILABILITY.

IN NO EVENT AND UNDER NO LEGAL THEORY — WHETHER IN CONTRACT, TORT (INCLUDING
NEGLIGENCE), STRICT LIABILITY, OR OTHERWISE — SHALL THE AUTHORS, CONTRIBUTORS, OR
COPYRIGHT HOLDERS BE LIABLE TO YOU OR ANY THIRD PARTY FOR ANY DIRECT, INDIRECT,
SPECIAL, INCIDENTAL, CONSEQUENTIAL, PUNITIVE, OR EXEMPLARY DAMAGES ARISING FROM OR
RELATING TO THIS SOFTWARE OR ITS USE OR INABILITY TO BE USED. THIS INCLUDES, WITHOUT
LIMITATION, UNEXPECTED CLOUD CHARGES OR BILLING OVERAGES, COST INCREASES RESULTING
FROM ACTING ON ITS RECOMMENDATIONS, LOST OR CORRUPTED DATA, DEGRADED QUERY
PERFORMANCE, SERVICE INTERRUPTION, WORK STOPPAGE, OR LOSS OF GOODWILL — EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGES.

BY USING THIS SOFTWARE YOU ACCEPT FULL RESPONSIBILITY FOR ALL RESULTING COSTS,
CONFIGURATION CHANGES, AND OPERATIONAL OUTCOMES IN YOUR ENVIRONMENT.
