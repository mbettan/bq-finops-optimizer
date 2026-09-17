#!/usr/bin/env python3
"""Prove what the FinOps Optimizer for BigQuery's identity can and cannot do.

Two assertions, both of which must hold:

  POSITIVE  every permission the application actually needs is held.
  NEGATIVE  ``bigquery.tables.getData`` is NOT held at the project/organization
            node — the identity cannot read table data via those bindings.

The negative assertion is the headline. It is the one a security reviewer is
being asked to take on trust in a questionnaire; this script turns it into a
command with an exit code (0 if verified PASS, 2 if FAIL, 3 if UNVERIFIED).

Mechanism
---------
``cloudresourcemanager.projects.testIamPermissions`` and
``organizations.testIamPermissions``. Those APIs answer "does the *caller*
hold these permissions on this resource" and require no privilege of their
own, so this is safe to hand to a customer to run in their own pipeline.

Only the Python standard library is used: no new dependency is introduced.
Authentication is delegated to ``gcloud auth print-access-token``.

Usage
-----
    # As yourself
    python3 deploy/check_permissions.py --project PROJECT_ID

    # As the deployed runtime identity (requires
    # roles/iam.serviceAccountTokenCreator on the SA)
    python3 deploy/check_permissions.py \
        --project PROJECT_ID \
        --impersonate-service-account bq-finops-sa@PROJECT_ID.iam.gserviceaccount.com

    # Include the organization node
    python3 deploy/check_permissions.py --project PROJECT_ID --org-id 123456789012

Exit codes
----------
    0  all required permissions held, no data-plane permission held
    1  a required permission is missing
    2  a data-plane permission IS held  (worst case; wins over 1)
    3  the check could not be completed (auth, network, bad arguments)
"""

from __future__ import annotations

import argparse
import json
import shutil
import ssl
import subprocess
import sys
import urllib.error
import urllib.request
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

CRM = "https://cloudresourcemanager.googleapis.com/v1"

EXIT_OK = 0
EXIT_MISSING_REQUIRED = 1
EXIT_DATA_PLANE_PRESENT = 2
EXIT_UNVERIFIED = 3
EXIT_ERROR = 3

# ---------------------------------------------------------------------------
# What the application needs, and why. Keep in step with
# deploy/roles/bq_finops_reader.yaml.
# ---------------------------------------------------------------------------

# Execution project. Without these nothing runs at all.
PROJECT_REQUIRED: Dict[str, str] = {
    "bigquery.jobs.create": "submit the diagnostic queries (roles/bigquery.jobUser)",
    "bigquery.config.get": "INFORMATION_SCHEMA.PROJECT_OPTIONS (roles/bigquery.jobUser)",
    "bigquery.tables.list": "INFORMATION_SCHEMA.TABLES / TABLE_STORAGE",
    "bigquery.tables.get": "INFORMATION_SCHEMA.TABLES / PARTITIONS / COLUMNS",
    "bigquery.datasets.get": "INFORMATION_SCHEMA.SCHEMATA / SCHEMATA_OPTIONS",
}

# Organization node. Required for the *_BY_ORGANIZATION views.
ORG_REQUIRED: Dict[str, str] = {
    "bigquery.jobs.listAll": "JOBS_BY_ORGANIZATION, JOBS_TIMELINE_BY_ORGANIZATION",
    "bigquery.reservations.list": "RESERVATIONS, RESERVATION_CHANGES",
    "bigquery.capacityCommitments.list": "CAPACITY_COMMITMENT_CHANGES",
}

# Organization node, OPTIONAL. Absence degrades speed, never correctness:
# the app falls back to a project-by-project loop (README.md:425).
ORG_OPTIONAL: Dict[str, str] = {
    "bigquery.tables.list": "TABLE_STORAGE_BY_ORGANIZATION + SCHEMATA_OPTIONS fast path",
    "bigquery.tables.get": "TABLE_STORAGE_BY_ORGANIZATION metadata",
    "bigquery.datasets.get": "batched SCHEMATA_OPTIONS fast path",
}

# The boundary. Holding this at any tested scope is a hard failure.
DATA_PLANE = "bigquery.tables.getData"

# Held-but-should-not-be. Reported always; fatal only under --strict.
UNEXPECTED_WRITE: Dict[str, str] = {
    "bigquery.tables.create": "can create tables",
    "bigquery.tables.update": "can alter table definitions",
    "bigquery.tables.updateData": "can write table contents",
    "bigquery.tables.delete": "can delete tables",
    "bigquery.tables.export": "can export table contents out of BigQuery",
    "bigquery.datasets.create": "can create datasets",
    "bigquery.datasets.update": "can alter dataset definitions",
    "bigquery.datasets.delete": "can delete datasets",
    "bigquery.config.update": "can change project-level BigQuery defaults",
    "bigquery.reservations.update": "can resize reservations (cost impact)",
    "bigquery.reservations.delete": "can delete reservations (availability impact)",
    "bigquery.capacityCommitments.create": "can buy commitments (cost impact)",
    "bigquery.capacityCommitments.delete": "can delete commitments",
}


class CheckError(RuntimeError):
    """Something prevented the check from completing."""


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def _color(enabled: bool) -> Dict[str, str]:
    if not enabled:
        return {k: "" for k in ("red", "green", "yellow", "bold", "dim", "off")}
    return {
        "red": "\033[31m",
        "green": "\033[32m",
        "yellow": "\033[33m",
        "bold": "\033[1m",
        "dim": "\033[2m",
        "off": "\033[0m",
    }


C = _color(sys.stdout.isatty())


def header(text: str) -> None:
    print(f"\n{C['bold']}{text}{C['off']}")
    print("-" * len(text))


# ---------------------------------------------------------------------------
# Auth + API
# ---------------------------------------------------------------------------

def access_token(impersonate: str | None) -> str:
    """Mint an OAuth token via gcloud (no extra Python dependency)."""
    if shutil.which("gcloud") is None:
        raise CheckError(
            "gcloud not found on PATH. Install the Google Cloud CLI, or run "
            "this in Cloud Shell."
        )
    cmd = ["gcloud", "auth", "print-access-token"]
    if impersonate:
        cmd.append(f"--impersonate-service-account={impersonate}")
    try:
        out = subprocess.run(
            cmd, check=True, capture_output=True, text=True, timeout=120
        )
    except subprocess.TimeoutExpired as exc:
        raise CheckError("gcloud auth print-access-token timed out.") from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or "").strip()
        hint = ""
        if impersonate and "iam.serviceAccounts.getAccessToken" in detail:
            hint = (
                "\n\n  You need roles/iam.serviceAccountTokenCreator on "
                f"{impersonate}:\n"
                "    gcloud iam service-accounts add-iam-policy-binding "
                f"{impersonate} \\\n"
                '      --member="user:YOU@example.com" \\\n'
                '      --role="roles/iam.serviceAccountTokenCreator"'
            )
        raise CheckError(f"Could not mint an access token.\n  {detail}{hint}") from exc
    return out.stdout.strip()


def _get_ssl_context() -> Optional[ssl.SSLContext]:
    """Create an SSLContext backed by certifi when installed, or system defaults."""
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        return None


def _post(url: str, token: str, payload: dict) -> dict:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "bq-finops-verify-permissions/1.0",
        },
    )
    ctx = _get_ssl_context()
    with urllib.request.urlopen(req, timeout=60, context=ctx) as resp:  # noqa: S310 (fixed host)
        return json.loads(resp.read().decode("utf-8"))


def test_permissions(
    resource_url: str, token: str, permissions: Sequence[str]
) -> Tuple[List[str], List[str]]:
    """Return (held, untestable) for ``permissions`` on ``resource_url``.

    ``testIamPermissions`` rejects the whole batch with HTTP 400 when any one
    permission is not applicable to the resource type. When that happens the
    batch is re-tried one permission at a time so a single odd permission
    cannot silently invalidate the entire report.
    """
    if not permissions:
        return [], []
    url = f"{resource_url}:testIamPermissions"
    try:
        data = _post(url, token, {"permissions": list(permissions)})
        return list(data.get("permissions", [])), []
    except urllib.error.HTTPError as exc:
        if exc.code != 400 or len(permissions) == 1:
            _raise_http(exc, resource_url, permissions)
        # Isolate the offender(s).
        held: List[str] = []
        untestable: List[str] = []
        for perm in permissions:
            try:
                data = _post(url, token, {"permissions": [perm]})
                held.extend(data.get("permissions", []))
            except urllib.error.HTTPError as inner:
                if inner.code == 400:
                    untestable.append(perm)
                else:
                    _raise_http(inner, resource_url, [perm])
        return held, untestable
    except urllib.error.URLError as exc:
        reason = exc.reason
        if isinstance(reason, ssl.SSLCertVerificationError) or "CERTIFICATE_VERIFY_FAILED" in str(reason):
            raise CheckError(
                f"SSL certificate verification failed calling {url}: {reason}\n"
                f"Actionable fix: run `pip install certifi` or on macOS run "
                f"'/Applications/Python 3.x/Install Certificates.command'."
            ) from exc
        raise CheckError(f"Network error calling {url}: {exc.reason}") from exc


def _raise_http(
    exc: urllib.error.HTTPError, resource_url: str, permissions: Iterable[str]
) -> None:
    try:
        detail = json.loads(exc.read().decode("utf-8"))
        message = detail.get("error", {}).get("message", str(exc))
    except Exception:  # noqa: BLE001 - diagnostics only
        message = str(exc)
    hint = ""
    if exc.code == 403:
        hint = (
            "\n  The caller cannot even call testIamPermissions on this "
            "resource. For an organization this usually means no binding of "
            "any kind at the org node — which, for the data-plane check, is "
            "itself a good sign, but the positive assertions cannot be run."
        )
    elif exc.code == 404:
        hint = "\n  Resource not found. Check the project ID / organization ID."
    raise CheckError(
        f"HTTP {exc.code} from {resource_url}:testIamPermissions "
        f"({', '.join(permissions)})\n  {message}{hint}"
    )


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def report_scope(
    label: str,
    resource_url: str,
    token: str,
    required: Dict[str, str],
    optional: Dict[str, str],
    strict: bool,
) -> Tuple[List[str], Optional[bool], List[str]]:
    """Check one resource. Returns (missing_required, data_plane_held (True/False/None for unverified), notes)."""
    header(f"{label}")

    to_test = (
        list(required)
        + list(optional)
        + [DATA_PLANE]
        + list(UNEXPECTED_WRITE)
    )
    # De-duplicate, preserve order.
    seen = set()
    ordered = [p for p in to_test if not (p in seen or seen.add(p))]

    held, untestable = test_permissions(resource_url, token, ordered)
    held_set = set(held)

    missing_required: List[str] = []
    notes: List[str] = []

    print(f"{C['bold']}  Required{C['off']}")
    if not required:
        print(f"    {C['dim']}(none at this scope){C['off']}")
    for perm, why in required.items():
        if perm in untestable:
            notes.append(f"{perm} could not be tested at {label.lower()}")
            print(f"    {C['yellow']}?{C['off']} {perm:<36} not testable here — {why}")
        elif perm in held_set:
            print(f"    {C['green']}+{C['off']} {perm:<36} {why}")
        else:
            missing_required.append(perm)
            print(f"    {C['red']}-{C['off']} {perm:<36} MISSING — needed for {why}")

    if optional:
        print(f"\n{C['bold']}  Optional (absence slows the scan, nothing breaks){C['off']}")
        for perm, why in optional.items():
            if perm in untestable:
                print(f"    {C['yellow']}?{C['off']} {perm:<36} not testable here")
            elif perm in held_set:
                print(f"    {C['green']}+{C['off']} {perm:<36} {why}")
            else:
                print(
                    f"    {C['dim']}o {perm:<36} not held — fallback path will "
                    f"be used{C['off']}"
                )

    print(f"\n{C['bold']}  Data-plane boundary{C['off']}")
    if DATA_PLANE in untestable:
        data_plane_held = None
        notes.append(
            f"{DATA_PLANE} was not testable at {label.lower()} — treat as UNVERIFIED"
        )
        print(f"    {C['yellow']}?{C['off']} {DATA_PLANE:<36} NOT TESTABLE at this scope")
    else:
        data_plane_held = DATA_PLANE in held_set
        
    if data_plane_held is True:
        print(
            f"    {C['red']}!{C['off']} {DATA_PLANE:<36} "
            f"{C['red']}HELD — this identity CAN read table contents{C['off']}"
        )
    else:
        print(
            f"    {C['green']}+{C['off']} {DATA_PLANE:<36} "
            f"{C['green']}NOT held — cannot read any table data{C['off']}"
        )

    unexpected = [p for p in UNEXPECTED_WRITE if p in held_set]
    if unexpected:
        tone = C["red"] if strict else C["yellow"]
        print(f"\n{C['bold']}  Unexpected write / mutate permissions{C['off']}")
        for perm in unexpected:
            print(f"    {tone}!{C['off']} {perm:<36} {UNEXPECTED_WRITE[perm]}")
        print(
            f"    {C['dim']}The identity holds more than this application uses. "
            f"That is over-provisioning, not a product requirement.{C['off']}"
        )
        if strict:
            missing_required.append("__strict_unexpected__")

    return missing_required, data_plane_held, notes


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Verify the FinOps Optimizer for BigQuery's identity holds the "
            "telemetry permissions it needs and NOT the data-plane permission "
            "bigquery.tables.getData at the project/organization node."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--project", required=True, help="Execution project ID.")
    parser.add_argument(
        "--org-id",
        default=None,
        help="Organization numeric ID. Omit to check the project only.",
    )
    parser.add_argument(
        "--impersonate-service-account",
        default=None,
        metavar="SA_EMAIL",
        help="Check this service account's permissions instead of your own.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Also fail when unexpected write permissions are held.",
    )
    args = parser.parse_args(argv)

    identity = args.impersonate_service_account or "your active gcloud credential"

    print(f"{C['bold']}FinOps Optimizer for BigQuery — permission verification{C['off']}")
    print(f"  Identity under test : {identity}")
    print(f"  Project             : {args.project}")
    print(f"  Organization        : {args.org_id or '(not checked)'}")

    try:
        token = access_token(args.impersonate_service_account)

        missing: List[str] = []
        data_plane_states: List[Optional[bool]] = []
        notes: List[str] = []

        m, d, n = report_scope(
            f"Project: {args.project}",
            f"{CRM}/projects/{args.project}",
            token,
            PROJECT_REQUIRED,
            {},
            args.strict,
        )
        missing += m
        data_plane_states.append(d)
        notes += n

        if args.org_id:
            m, d, n = report_scope(
                f"Organization: {args.org_id}",
                f"{CRM}/organizations/{args.org_id}",
                token,
                ORG_REQUIRED,
                ORG_OPTIONAL,
                args.strict,
            )
            missing += m
            data_plane_states.append(d)
            notes += n
    except CheckError as exc:
        print(f"\n{C['red']}Could not complete the check:{C['off']} {exc}", file=sys.stderr)
        return EXIT_ERROR

    header("Scope of this check")
    print(
        "  testIamPermissions was evaluated at the project"
        + (" and organization" if args.org_id else "")
        + " node only.\n"
        "  BigQuery also supports IAM at the dataset, table and row/column\n"
        "  level. A dataset-level grant of bigquery.tables.getData would NOT\n"
        "  appear above. To be exhaustive, also review dataset-level policies\n"
        "  for this identity. The custom role in deploy/roles/ never grants it."
    )
    for note in notes:
        print(f"  {C['yellow']}note:{C['off']} {note}")

    header("Result")
    # `report_scope` returns True / False / None per scope, where None means the
    # probe could not be completed. Only a definite True is a failure — an
    # unverified scope must not be reported as "held".
    data_plane_anywhere = any(state is True for state in data_plane_states)
    if data_plane_anywhere:
        print(
            f"  {C['red']}FAIL — {DATA_PLANE} is held.{C['off']}\n"
            "  This identity can read customer table contents. It is NOT the\n"
            "  least-privilege identity this application is designed for.\n"
            "  Remove roles/bigquery.dataViewer (or dataEditor / dataOwner /\n"
            "  admin) and grant deploy/roles/bq_finops_reader.yaml instead."
        )
        return EXIT_DATA_PLANE_PRESENT

    real_missing = [m for m in missing if m != "__strict_unexpected__"]
    if real_missing:
        print(
            f"  {C['red']}FAIL — {len(real_missing)} required permission(s) "
            f"missing:{C['off']}"
        )
        for perm in real_missing:
            print(f"    - {perm}")
        print(
            "\n  The data-plane boundary holds, but the application will 403\n"
            "  partway through an analysis run. See README §Troubleshooting\n"
            "  Permissions for which module each permission unblocks."
        )
        return EXIT_MISSING_REQUIRED

    if "__strict_unexpected__" in missing:
        print(
            f"  {C['red']}FAIL (--strict) — unexpected write permissions are held."
            f"{C['off']}\n"
            "  The data-plane boundary holds and all required permissions are\n"
            "  present, but the identity is over-provisioned."
        )
        return EXIT_MISSING_REQUIRED

    if any(s is None for s in data_plane_states):
        print(
            f"  {C['yellow']}UNVERIFIED — {DATA_PLANE} probe could not be completed.{C['off']}\n"
            "  All required telemetry permissions are held, but the data-plane\n"
            "  boundary could not be verified on at least one scope. Verify manually\n"
            "  that bigquery.tables.getData is not granted."
        )
        return EXIT_UNVERIFIED

    print(
        f"  {C['green']}PASS{C['off']}\n"
        "    - every required telemetry permission is held\n"
        f"    - {DATA_PLANE} is NOT held: this identity cannot read\n"
        "      a single row of customer data, whatever SQL the application sends"
    )
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
