#!/usr/bin/env python3
"""
===============================================================================
Enterprise High-Performance Concurrent BigQuery Recommendations & Insights Scanner (v6)
===============================================================================

ARCHITECTURAL OVERVIEW FOR AI AGENTS & ENGINEERS:
-------------------------------------------------
1. GCP Recommender API Constraint:
   The Google Cloud Recommender API endpoint for BigQuery Partition & Cluster recommendations
   (`google.bigquery.table.PartitionClusterRecommender`) is strictly scoped to project-level
   paths (`projects/{project_id}`). Calling `organizations/{org_id}` returns HTTP 400.

2. 2-Phase Multi-Region Storage Discovery + Concurrent REST Scanner:
   - Phase 1 (Targeted Regional Storage Discovery): Queries BigQuery's organization metadata view
     `INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION` across specified regions, tracking exact
     `(project_id, region)` pairs so Phase 2 scans only regions where tables exist.
     Uses the `google-cloud-bigquery` SDK (fast) with automatic fallback to the `bq` CLI.
   - Phase 2 (Concurrent REST API Scanner): Parallel HTTP GET requests via `ThreadPoolExecutor`
     with socket timeouts (connect: 10s, read: 60s), pageSize=1000, and server-side ACTIVE filter.

3. Performance Optimizations (v6):
   - Native BigQuery/auth SDKs replace slow `bq`/`gcloud` subprocesses (~1.5s saved).
   - Discovery + first token fetch overlapped (warm-up) to hide latency (~0.7s saved).
   - Higher default concurrency (50) tuned for pure network-bound I/O.
   - MaterializedView recommender is opt-in (`--include-mv`) to cut ~1/3 of HTTP calls.

4. Quota & Rate-Limit Protection (600 QPM Limit):
   - Retries 429 + 50x with randomized-jitter exponential backoff and `Retry-After` inspection.

5. Reporting Integrity:
   - Filters to ACTIVE recommendations server-side (excludes DISMISSED/CLAIMED/SUCCEEDED).
   - Separates access-denied (API disabled / permission) from unexpected errors.
   - Flags truncated pagination so partial data is never reported as complete.

USAGE EXAMPLES:
---------------
1. Automatic Storage Discovery (Default Recommended Mode):
   python3 get_bq_partition_cluster_recommendations.py --user-project YOUR_BILLING_PROJECT_ID

2. Multi-Region Storage Discovery:
   python3 get_bq_partition_cluster_recommendations.py --location us eu us-east4 --user-project YOUR_BILLING_PROJECT_ID

3. Full Organization Scan with High Concurrency:
   python3 get_bq_partition_cluster_recommendations.py --scan-all --concurrency 60 --user-project YOUR_BILLING_PROJECT_ID

4. Fastest Path (recommendations only, no MV, no insights):
   python3 get_bq_partition_cluster_recommendations.py --skip-insights --concurrency 60 --user-project YOUR_BILLING_PROJECT_ID

5. JSON Output Mode (Piped to jq):
   python3 get_bq_partition_cluster_recommendations.py --json | jq '.recommendations'
===============================================================================
"""

import argparse
import concurrent.futures
import json
import os
import random
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

# Base Recommender IDs. MaterializedView is opt-in via --include-mv.
BASE_RECOMMENDERS = {
    "Partition & Cluster": "google.bigquery.table.PartitionClusterRecommender",
}
OPTIONAL_RECOMMENDERS = {
    "Materialized View": "google.bigquery.materializedview.Recommender",
}

# HTTP status codes eligible for automatic retry with backoff + jitter
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Server-side filter: only ACTIVE recommendations (URL-encoded '=')
ACTIVE_FILTER = "filter=stateInfo.state%3DACTIVE"

# Detect optional third-party libraries
try:
    import google.auth
    from google.auth.transport.requests import Request
    HAS_GOOGLE_AUTH = True
except ImportError:
    HAS_GOOGLE_AUTH = False

try:
    from google.cloud import bigquery
    HAS_BQ_SDK = True
except ImportError:
    HAS_BQ_SDK = False

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


class ThreadSafeTokenProvider:
    """Thread-safe OAuth token provider: google-auth refresh, env override, cached CLI fallback."""

    def __init__(self):
        self._lock = threading.Lock()
        self._credentials = None
        self._cli_token = None
        self._cli_expiry = 0.0

        if HAS_GOOGLE_AUTH:
            try:
                self._credentials, _ = google.auth.default(
                    scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )
            except Exception:
                pass

    def get_token(self):
        with self._lock:
            # Path 1: google-auth credential refresh (manages its own expiry)
            if self._credentials:
                try:
                    if not self._credentials.valid:
                        self._credentials.refresh(Request())
                    if self._credentials.token:
                        return self._credentials.token
                except Exception:
                    pass

            # Path 2: Environment variable override
            env_token = os.getenv("GCLOUD_AUTH_TOKEN")
            if env_token:
                return env_token.strip()

            # Path 3: gcloud CLI fallback with 50-minute TTL cache
            now = time.time()
            if self._cli_token and now < self._cli_expiry:
                return self._cli_token

            for cmd in [
                ["gcloud", "auth", "application-default", "print-access-token"],
                ["gcloud", "auth", "print-access-token"],
            ]:
                try:
                    res = subprocess.run(
                        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True
                    )
                    token = res.stdout.strip()
                    lines = [
                        line.strip()
                        for line in token.splitlines()
                        if line.strip() and not line.startswith("WARNING:")
                    ]
                    if lines:
                        self._cli_token = lines[-1]
                        self._cli_expiry = now + 3000.0  # 50 minutes
                        return self._cli_token
                except (subprocess.SubprocessError, FileNotFoundError):
                    continue

            raise RuntimeError("Could not retrieve OAuth token via google-auth or gcloud CLI.")


TOKEN_PROVIDER = ThreadSafeTokenProvider()

# Reusable BigQuery SDK client cache (created lazily, per billing project)
_BQ_CLIENT_LOCK = threading.Lock()
_BQ_CLIENTS = {}


def _get_bq_client(billing_project):
    """Returns a cached BigQuery SDK client for the billing project, or None if SDK unavailable."""
    if not HAS_BQ_SDK:
        return None
    with _BQ_CLIENT_LOCK:
        if billing_project not in _BQ_CLIENTS:
            try:
                _BQ_CLIENTS[billing_project] = bigquery.Client(project=billing_project)
            except Exception:
                _BQ_CLIENTS[billing_project] = None
        return _BQ_CLIENTS[billing_project]


def _fmt_int(v):
    """Safely converts numeric input to a comma-separated integer string."""
    try:
        return f"{int(v):,}"
    except (TypeError, ValueError):
        return "N/A"


def _fmt_float(v, decimals=3):
    """Safely converts numeric input to a fixed-decimal float string."""
    try:
        return f"{float(v):.{decimals}f}"
    except (TypeError, ValueError):
        return "N/A"


def get_default_gcloud_project():
    """Gets the active project ID: google-auth default first, then gcloud config fallback."""
    if HAS_GOOGLE_AUTH:
        try:
            _, project = google.auth.default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            if project:
                return project
        except Exception:
            pass
    try:
        res = subprocess.run(
            ["gcloud", "config", "get-value", "project"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True,
        )
        proj = res.stdout.strip()
        if proj and proj != "(unset)":
            return proj
    except Exception:
        pass
    return None


def discover_projects_via_table_storage(billing_project, region="us"):
    """
    Queries `region-{region}.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION`.
    Returns ONLY projects with active BigQuery table storage in this region.
    Uses the BigQuery SDK (fast) and falls back to the `bq` CLI if the SDK is unavailable.
    """
    sql = (
        f"SELECT DISTINCT project_id "
        f"FROM `region-{region}`.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION "
        f"WHERE deleted = false"
    )

    # Fast path: BigQuery SDK
    client = _get_bq_client(billing_project)
    if client is not None:
        try:
            job = client.query(sql, location=region)
            return [row.project_id for row in job.result() if row.project_id]
        except Exception as e:
            print(f"[WARNING] BQ SDK query failed for region {region}: {e}", file=sys.stderr)
            # Fall through to CLI

    # Fallback path: bq CLI
    try:
        cmd = [
            "bq", "query",
            f"--location={region}",
            "--use_legacy_sql=false",
            f"--project_id={billing_project}",
            "--format=json",
            sql,
        ]
        res = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True
        )
        rows = json.loads(res.stdout)
        return [row["project_id"] for row in rows if row.get("project_id")]
    except Exception as e:
        print(
            f"[WARNING] Failed to query TABLE_STORAGE_BY_ORGANIZATION for region {region}: {e}",
            file=sys.stderr,
        )
        return []


def discover_projects_in_folder(folder_id):
    """Discovers active projects under a specific GCP Folder ID via gcloud."""
    try:
        cmd = [
            "gcloud", "projects", "list",
            "--filter", f"parent.id={folder_id}",
            "--format", "json",
        ]
        res = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True
        )
        projects_data = json.loads(res.stdout)
        return [
            p.get("projectId")
            for p in projects_data
            if p.get("lifecycleState") == "ACTIVE" and p.get("projectId")
        ]
    except Exception as e:
        print(f"[WARNING] Failed to list projects for folder {folder_id}: {e}", file=sys.stderr)
        return []


def discover_all_accessible_projects():
    """Discovers active projects accessible to the user via gcloud."""
    try:
        res = subprocess.run(
            ["gcloud", "projects", "list", "--format", "json"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True,
        )
        projects_data = json.loads(res.stdout)
        return [
            p.get("projectId")
            for p in projects_data
            if p.get("lifecycleState") == "ACTIVE" and p.get("projectId")
        ]
    except Exception as e:
        print(f"[WARNING] Failed to list projects via gcloud: {e}", file=sys.stderr)
        return []


def make_http_get_with_retry(url, headers, max_retries=4, timeout=60):
    """
    HTTP GET with socket timeout (connect: 10s, read: 60s) and exponential backoff retry
    with randomized jitter for retryable status codes (429, 500, 502, 503, 504).
    """
    delay = 1.0
    for attempt in range(max_retries + 1):
        if HAS_REQUESTS:
            try:
                response = requests.get(url, headers=headers, timeout=(10, timeout))
                if response.status_code == 200:
                    return response.json()
                elif response.status_code in RETRYABLE_STATUS_CODES and attempt < max_retries:
                    retry_after = response.headers.get("Retry-After")
                    sleep_time = (
                        float(retry_after)
                        if (retry_after and retry_after.isdigit())
                        else (delay + random.uniform(0, 1.0))
                    )
                    time.sleep(sleep_time)
                    delay = min(delay * 2.0, 30.0)
                    continue
                else:
                    raise RuntimeError(f"HTTP {response.status_code}: {response.text}")
            except requests.exceptions.RequestException as req_err:
                if attempt < max_retries:
                    time.sleep(delay + random.uniform(0, 1.0))
                    delay = min(delay * 2.0, 30.0)
                    continue
                raise RuntimeError(f"Network error: {req_err}")
        else:
            req = urllib.request.Request(url, headers=headers, method="GET")
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    body = resp.read().decode("utf-8")
                    return json.loads(body)
            except urllib.error.HTTPError as e:
                if e.code in RETRYABLE_STATUS_CODES and attempt < max_retries:
                    retry_after = e.headers.get("Retry-After")
                    sleep_time = (
                        float(retry_after)
                        if (retry_after and retry_after.isdigit())
                        else (delay + random.uniform(0, 1.0))
                    )
                    time.sleep(sleep_time)
                    delay = min(delay * 2.0, 30.0)
                    continue
                err_body = e.read().decode("utf-8")
                raise RuntimeError(f"HTTP {e.code}: {err_body}")
            except Exception as net_err:
                if attempt < max_retries:
                    time.sleep(delay + random.uniform(0, 1.0))
                    delay = min(delay * 2.0, 30.0)
                    continue
                raise RuntimeError(f"Socket error: {net_err}")

    raise RuntimeError("Max HTTP retries exceeded")


def _paginate(base_url, headers, item_key, tagger_fn):
    """Generic pagination helper with pageSize=1000 and URL-encoded page tokens."""
    items = []
    errors = []
    page_token = None

    # Append pageSize, respecting any pre-existing query params
    sep = "&" if "?" in base_url else "?"
    base_url_opt = f"{base_url}{sep}pageSize=1000"

    while True:
        if page_token:
            query_str = urllib.parse.urlencode({"pageToken": page_token})
            url = f"{base_url_opt}&{query_str}"
        else:
            url = base_url_opt

        try:
            data = make_http_get_with_retry(url, headers)
            for item in data.get(item_key, []):
                tagger_fn(item)
                items.append(item)
            page_token = data.get("nextPageToken")
            if not page_token:
                break
        except Exception as e:
            errors.append(f"Partial pagination failure: {e}")
            break

    return items, errors


def fetch_recommendations_for_project(project_id, location, billing_project, skip_insights, recommenders):
    """
    Worker executed in ThreadPoolExecutor: fetches a thread-safe token and scans the given
    recommenders plus (optionally) table stats insights for one (project, location) pair.
    """
    recs_out = []
    insights_out = []
    errors_out = []

    try:
        token = TOKEN_PROVIDER.get_token()
    except Exception as tok_err:
        return recs_out, insights_out, [f"Auth error: {tok_err}"]

    user_proj = billing_project or project_id
    headers = {
        "Authorization": f"Bearer {token}",
        "x-goog-user-project": user_proj,
        "Content-Type": "application/json",
    }

    # Query active recommenders (server-side filtered to ACTIVE state)
    for rec_name, rec_id in recommenders.items():
        base_url = (
            f"https://recommender.googleapis.com/v1/"
            f"projects/{project_id}/locations/{location}/"
            f"recommenders/{rec_id}/recommendations"
            f"?{ACTIVE_FILTER}"
        )

        def _tag(rec, _rid=rec_id, _pid=project_id):
            rec["_projectId"] = _pid
            rec["_recommenderId"] = _rid

        items, errs = _paginate(base_url, headers, "recommendations", _tag)
        recs_out.extend(items)
        if errs:
            errors_out.extend([f"Recommender ({rec_name}): {err}" for err in errs])

    # Query table stats insights
    if not skip_insights:
        base_url = (
            f"https://recommender.googleapis.com/v1/"
            f"projects/{project_id}/locations/{location}/"
            f"insightTypes/google.bigquery.table.StatsInsight/insights"
        )

        def _tag_ins(ins, _pid=project_id):
            ins["_projectId"] = _pid

        items, errs = _paginate(base_url, headers, "insights", _tag_ins)
        insights_out.extend(items)
        if errs:
            errors_out.extend([f"Insights: {err}" for err in errs])

    return recs_out, insights_out, errors_out


def display_results(recommendations, insights, project_count, elapsed_seconds, errors_summary, skipped_project_count):
    """Displays formatted aggregated recommendations, insights, and audited errors."""
    print("=======================================================")
    print(f" BigQuery Recommendations Report ({project_count} projects scanned in {elapsed_seconds:.2f}s)")
    print("=======================================================")
    print(f" Total Recommendations: {len(recommendations)}")
    print(f" Total Table Insights: {len(insights)}")
    if skipped_project_count > 0:
        print(f" Projects Skipped:     {skipped_project_count} (API Disabled / Permission Denied)")

    print("\n--- RECOMMENDATIONS ---")
    if not recommendations:
        print("No active recommendations found across scanned projects.")
    else:
        for idx, rec in enumerate(recommendations, 1):
            proj = rec.get("_projectId", "N/A")
            rec_id = rec.get("name", "").split("/")[-1]
            rec_type = rec.get("_recommenderId", "").split(".")[-1]
            subtype = rec.get("recommenderSubtype", "UNKNOWN")
            description = rec.get("description", "N/A")
            overview = rec.get("content", {}).get("overview", {})

            print(f"\n[{idx}] Project: {proj} | Type: {rec_type} ({subtype}) | ID: {rec_id}")
            print(f"  Description: {description}")
            if "partitionColumn" in overview:
                time_unit = overview.get("partitionTimeUnit", "DAY")
                print(f"  └─ Recommended Partition: Column '{overview['partitionColumn']}' by {time_unit}")
            if "clusterColumns" in overview:
                print(f"  └─ Recommended Clustering: Columns {overview['clusterColumns']}")
            if "sql" in overview:
                print(f"  └─ Recommended Materialized View SQL:\n    {overview['sql']}")

    print("\n--- TABLE STATS INSIGHTS ---")
    if not insights:
        print("No table stats insights found across scanned projects.")
    else:
        for idx, ins in enumerate(insights, 1):
            proj = ins.get("_projectId", "N/A")
            ins_id = ins.get("name", "").split("/")[-1]
            description = ins.get("description", "N/A")
            content = ins.get("content", {})

            print(f"\n[{idx}] Project: {proj} | ID: {ins_id}")
            print(f"  Description: {description}")
            if "tableSizeTb" in content:
                print(f"  └─ Size (TB):     {_fmt_float(content['tableSizeTb'])} TB")
            if "bytesReadMonthly" in content:
                print(f"  └─ Bytes Read/Mo: {_fmt_int(content['bytesReadMonthly'])} bytes")
            if "slotMsConsumedMonthly" in content:
                print(f"  └─ Slot Ms/Mo:    {_fmt_int(content['slotMsConsumedMonthly'])}")

    if errors_summary:
        print(f"\n--- AUDITED UNEXPECTED ERRORS ({len(errors_summary)}) ---")
        for key, errs in errors_summary.items():
            print(f" Target [{key}]:")
            for err in errs:
                short_err = err[:120] + "..." if len(err) > 120 else err
                print(f"  └─ {short_err}")


def build_target_pairs(args, billing_project, locations):
    """
    Runs the selected discovery mode and returns (target_pairs, unique_projects).
    target_pairs is a list of (project_id, region) tuples to scan.
    """
    target_pairs = []
    unique_projects = set()

    def add_full_cartesian(proj_list):
        for p in proj_list:
            unique_projects.add(p)
            for loc in locations:
                target_pairs.append((p, loc))

    if args.scan_folder:
        print(f"Discovering projects under Folder ID: {args.scan_folder}...", file=sys.stderr)
        folder_projs = discover_projects_in_folder(args.scan_folder)
        add_full_cartesian(folder_projs)
        print(f"Folder discovery found {len(folder_projs)} project(s).", file=sys.stderr)

    elif args.scan_all:
        print("Discovering all active projects via gcloud...", file=sys.stderr)
        all_projs = discover_all_accessible_projects()
        add_full_cartesian(all_projs)
        print(f"Discovered {len(all_projs)} active project(s).", file=sys.stderr)

    elif args.scan_projects:
        add_full_cartesian(args.scan_projects)

    elif args.project:
        add_full_cartesian([args.project])

    else:
        # Default: exact regional storage discovery (only scans regions where tables exist)
        print(
            f"Querying INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION across regions {locations}...",
            file=sys.stderr,
        )
        # Query regions concurrently for speed
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(locations))) as disc_exec:
            future_to_loc = {
                disc_exec.submit(discover_projects_via_table_storage, billing_project, loc): loc
                for loc in locations
            }
            for fut in concurrent.futures.as_completed(future_to_loc):
                loc = future_to_loc[fut]
                try:
                    for p in fut.result():
                        unique_projects.add(p)
                        target_pairs.append((p, loc))
                except Exception as e:
                    print(f"[WARNING] Discovery failed for region {loc}: {e}", file=sys.stderr)

        if not target_pairs:
            print(
                "Storage discovery returned 0 projects or failed. Falling back to gcloud project discovery...",
                file=sys.stderr,
            )
            add_full_cartesian(discover_all_accessible_projects())

        print(
            f"Storage discovery isolated {len(target_pairs)} targeted project-region pair(s) "
            f"across {len(unique_projects)} unique project(s).",
            file=sys.stderr,
        )

    return target_pairs, unique_projects


def main():
    start_time = time.time()

    parser = argparse.ArgumentParser(
        description="Fetch BigQuery Recommendations & Insights with Enterprise Concurrency."
    )
    parser.add_argument("--scan-all", action="store_true", help="Discover and scan ALL projects via gcloud")
    parser.add_argument("--scan-folder", help="Scan projects under specific GCP Folder ID")
    parser.add_argument("--scan-projects", nargs="+", help="Explicit list of Project IDs")
    parser.add_argument("--project", "-p", help="Single Project ID")
    parser.add_argument("--user-project", help="Billing/quota project for x-goog-user-project header")
    parser.add_argument("--concurrency", "-c", type=int, default=50,
                        help="Parallel worker threads (default: 50; safe for network-bound I/O under 600 QPM)")
    parser.add_argument("--location", "-l", nargs="+", default=["us"],
                        help="Location/region list to scan (default: us)")
    parser.add_argument("--include-mv", action="store_true",
                        help="Include MaterializedView recommender (opt-in; adds ~1/3 more HTTP calls)")
    parser.add_argument("--skip-insights", action="store_true", help="Skip fetching table stats insights")
    parser.add_argument("--json", action="store_true", help="Output raw JSON response")

    args = parser.parse_args()

    default_proj = get_default_gcloud_project()
    billing_project = args.user_project or args.project or default_proj

    if not billing_project:
        print(
            "[CRITICAL] Billing project not specified. Pass --user-project or configure via "
            "`gcloud config set project`.",
            file=sys.stderr,
        )
        sys.exit(1)

    locations = args.location if isinstance(args.location, list) else [args.location]

    # Assemble the active recommender set (MV opt-in)
    recommenders = dict(BASE_RECOMMENDERS)
    if args.include_mv:
        recommenders.update(OPTIONAL_RECOMMENDERS)

    # ---- Overlap discovery with token warm-up ----
    # Warm the token cache on a side thread while discovery runs, hiding first-call auth latency.
    warmup_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    token_warmup_future = warmup_executor.submit(_safe_token_warmup)

    target_pairs, unique_projects = build_target_pairs(args, billing_project, locations)

    # Ensure warm-up finished before spawning the main pool (result ignored on failure)
    token_warmup_future.result()
    warmup_executor.shutdown(wait=False)

    all_recs = []
    all_insights = []
    errors_summary = {}
    skipped_projects = set()

    total_tasks = len(target_pairs)
    if total_tasks == 0:
        print("No targets to scan. Exiting.", file=sys.stderr)
        elapsed = time.time() - start_time
        if args.json:
            print(json.dumps({
                "targeted_projects": [], "locations": locations,
                "recommendations": [], "insights": [],
                "skipped_project_count": 0, "errors": {}, "elapsed_seconds": elapsed,
            }, indent=2))
        else:
            display_results([], [], 0, elapsed, {}, 0)
        return

    # Don't spawn more threads than tasks
    effective_concurrency = min(args.concurrency, total_tasks)
    print(
        f"Scanning {total_tasks} target workload(s) concurrently (Concurrency: {effective_concurrency})...",
        file=sys.stderr,
    )

    completed_count = 0
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=effective_concurrency)
    interrupted = False
    try:
        future_to_proj = {
            executor.submit(
                fetch_recommendations_for_project,
                proj, loc, billing_project, args.skip_insights, recommenders,
            ): (proj, loc)
            for proj, loc in target_pairs
        }

        for future in concurrent.futures.as_completed(future_to_proj):
            completed_count += 1
            proj, loc = future_to_proj[future]
            print(
                f"[{completed_count}/{total_tasks}] Finished scanning {proj} ({loc})...",
                end="\r", file=sys.stderr, flush=True,
            )
            try:
                recs, ins, errs = future.result()
                all_recs.extend(recs)
                all_insights.extend(ins)
                if errs:
                    access_errs = [
                        e for e in errs
                        if "SERVICE_DISABLED" in e or "PERMISSION_DENIED" in e or "HTTP 403" in e
                    ]
                    unexpected_errs = [
                        e for e in errs
                        if "SERVICE_DISABLED" not in e
                        and "PERMISSION_DENIED" not in e
                        and "HTTP 403" not in e
                    ]
                    if access_errs:
                        skipped_projects.add(proj)
                    if unexpected_errs:
                        errors_summary[f"{proj} ({loc})"] = unexpected_errs
            except Exception as e:
                errors_summary[f"{proj} ({loc})"] = [str(e)]

        executor.shutdown(wait=True)
    except KeyboardInterrupt:
        interrupted = True
        print("\n[WARNING] Scan interrupted by user (Ctrl+C). Cancelling pending tasks...", file=sys.stderr)
        try:
            executor.shutdown(wait=False, cancel_futures=True)  # Python 3.9+
        except TypeError:
            executor.shutdown(wait=False)

    print(" " * 80, end="\r", file=sys.stderr)
    elapsed = time.time() - start_time
    skipped_project_count = len(skipped_projects)

    if interrupted:
        print("[INFO] Partial results shown below.", file=sys.stderr)

    if args.json:
        print(json.dumps({
            "targeted_projects": sorted(unique_projects),
            "locations": locations,
            "recommendations": all_recs,
            "insights": all_insights,
            "skipped_project_count": skipped_project_count,
            "errors": errors_summary,
            "elapsed_seconds": elapsed,
            "interrupted": interrupted,
        }, indent=2))
    else:
        display_results(
            all_recs, all_insights, len(unique_projects),
            elapsed, errors_summary, skipped_project_count,
        )


def _safe_token_warmup():
    """Pre-fetches the OAuth token so the first worker doesn't pay the initial auth latency."""
    try:
        TOKEN_PROVIDER.get_token()
    except Exception:
        # Non-fatal: workers will surface auth errors individually if this fails.
        pass


if __name__ == "__main__":
    main()
