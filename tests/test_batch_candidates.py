"""
Interactive vs. Batch priority engine regression guards.

Locks in the invariants of the batch-candidates workload engine — the
workload-centric rewrite of /api/antipatterns/batch_candidates. These tests are
deliberately source-level in places: the SQL is the product here, and a silent
regression in the slot math or the label-provenance grouping would produce
plausible-but-wrong recommendations that no schema check would catch.
"""

import inspect
import pathlib
import re

import pytest

import src.main as _main


class TestBatchCandidateSchema:
    """BatchCandidateResult must carry all required fields for the UNDER_BATCHED / OVER_BATCHED UX."""

    def test_model_has_all_required_fields(self):
        field_names = set(_main.BatchCandidateResult.model_fields.keys())
        required = {
            "workload_name", "workload_type", "project_id",
            "total_job_runs", "total_slot_hours", "avg_duration_minutes",
            "pct_interactive", "pct_batch", "pct_on_demand",
            "total_human_wait_seconds", "p95_queue_delay_seconds",
            "sample_job_id", "finding_category", "recommended_priority",
            "confidence", "has_remediation", "detection_reasons",
            "impact_score",
        }
        missing = required - field_names
        assert not missing, f"BatchCandidateResult missing fields: {missing}"

    def test_finding_category_accepts_valid_values(self):
        base = dict(
            workload_name="test", workload_type="dbt Pipeline",
            project_id="proj", total_job_runs=100, total_slot_hours=5.0,
            pct_interactive=90.0, pct_batch=10.0, pct_on_demand=50.0,
            total_human_wait_seconds=0, sample_job_id="job123",
            recommended_priority="BATCH", confidence="HIGH",
            has_remediation=True, detection_reasons=["test"],
            impact_score=4.5,
        )
        for cat in ("UNDER_BATCHED", "OVER_BATCHED", "OPTIMAL"):
            r = _main.BatchCandidateResult(**{**base, "finding_category": cat})
            assert r.finding_category == cat

    def test_optional_metrics_default_when_absent(self):
        """Workloads with no BATCH runs return NULL p95 — the model must tolerate it."""
        r = _main.BatchCandidateResult(
            workload_name="w", workload_type="Human Ad-hoc", project_id="proj",
            total_job_runs=1, total_slot_hours=0.0, pct_interactive=100.0,
            pct_batch=0.0, pct_on_demand=100.0, total_human_wait_seconds=0.0,
            sample_job_id="j", finding_category="OPTIMAL",
            recommended_priority="INTERACTIVE", confidence="LOW",
            has_remediation=True, detection_reasons=[], impact_score=0.0,
        )
        assert r.p95_queue_delay_seconds == 0
        assert r.avg_duration_minutes == 0.0


class TestBatchCandidateSQLEngine:
    """Verify the SQL engine uses correct patterns and avoids known pitfalls."""

    def test_sql_uses_correct_slot_math(self):
        src = inspect.getsource(_main.analyze_batch_candidates)
        assert "SAFE_DIVIDE(total_slot_ms, NULLIF(" in src
        assert "* 1000.0" not in src

    def test_sql_does_not_reference_query_text(self):
        src = inspect.getsource(_main.analyze_batch_candidates)
        assert "REGEXP_CONTAINS(query," not in src

    def test_sql_extracts_labels_via_unnest(self):
        src = inspect.getsource(_main.analyze_batch_candidates)
        assert "UNNEST(labels)" in src

    def test_sql_isolates_human_wait_from_sa(self):
        src = inspect.getsource(_main.analyze_batch_candidates)
        assert "NOT LIKE '%.gserviceaccount.com'" in src

    def test_sql_has_flags(self):
        src = inspect.getsource(_main.analyze_batch_candidates)
        assert "flag_pipeline_interactive" in src
        assert "flag_heavy_dml_interactive" in src
        assert "flag_sa_interactive" in src
        assert "flag_human_batch_queued" in src

    def test_endpoint_has_permission_fallback(self):
        src = inspect.getsource(_main.analyze_batch_candidates)
        assert "JOBS_BY_ORGANIZATION" in src
        assert "JOBS_BY_PROJECT" in src
        assert "dry_run=True" in src

    def test_sql_excludes_cache_hits_and_child_jobs(self):
        """Cached and script-child jobs would double-count a workload's runs."""
        src = inspect.getsource(_main.analyze_batch_candidates)
        assert "cache_hit IS FALSE OR cache_hit IS NULL" in src
        assert "parent_job_id IS NULL" in src

    def test_sql_time_window_is_parameterized(self):
        src = inspect.getsource(_main.analyze_batch_candidates)
        assert "@start_time_period" in src
        assert "@end_time_period" in src
        assert "time_period_query_params(params)" in src

    def test_non_nullable_response_fields_are_null_guarded(self):
        """total_slot_ms is NULL for jobs that never reserved slots (metadata-only
        queries, script parents). A workload made entirely of those would emit NULL
        into `total_slot_hours`/`impact_score`, both non-Optional floats, turning
        the whole endpoint into a 500 from Pydantic validation."""
        src = inspect.getsource(_main.analyze_batch_candidates)
        assert "ROUND(IFNULL(SUM(total_slot_ms), 0) / 3600000.0, 2) AS total_slot_hours" in src
        assert "END, 0.0) AS impact_score" in src
        # user_email is NULL for some system-issued jobs; workload_name is required.
        assert "'Unattributed'" in src

    def test_detection_reasons_contain_no_html_metacharacters(self):
        """API strings are HTML-escaped by the global fetch proxy and again at the
        render sink, so a literal '>' would reach the user as '&gt;'."""
        src = inspect.getsource(_main.analyze_batch_candidates)
        reasons = re.findall(r"IF\(flag_\w+[^)]*?CONCAT\((.*?)\), NULL\)", src)
        assert reasons, "detection_reasons CONCAT literals not found"
        for literal in re.findall(r"'([^']*)'", " ".join(reasons)):
            assert not set(literal) & set("<>&"), f"HTML metachar in reason: {literal!r}"


class TestSnapshotRedactionCoverage:
    """The workload engine reports operators under `workload_name`, not `user_email`.

    The snapshot redactor used to match on key names only, so moving the address
    out of a key called *email* silently dropped it from redaction. Guard the
    value-level pass that closed that hole, in both shipped bundles.
    """

    BUNDLES = ["static/app.js", "docs/static/app.js"]

    @pytest.mark.parametrize("bundle", BUNDLES)
    def test_redactor_scrubs_emails_by_value_not_just_by_key(self, bundle):
        src = pathlib.Path(bundle).read_text(encoding="utf-8")
        assert "const EMAIL_RE = " in src, f"{bundle}: value-level email pass missing"
        assert "const scrubString = " in src
        assert "o[k] = scrubString(val);" in src, f"{bundle}: object strings not scrubbed"
        assert "o[i] = scrubString(item);" in src, f"{bundle}: array strings not scrubbed"

    def test_payload_carries_identities_under_a_non_email_key(self):
        """Establishes why the value pass is load-bearing: the engine folds the
        operator's address into `workload_name`, and no field name in the response
        matches the redactor's /email/i key rule any more."""
        fields = list(_main.BatchCandidateResult.model_fields)
        assert "workload_name" in fields
        assert not [f for f in fields if re.search("email", f, re.I)]
        src = inspect.getsource(_main.analyze_batch_candidates)
        assert re.search(r"user_email,\s*'Unattributed'\s*\)\s*AS workload_name", src), \
            "workload_name no longer falls back to user_email — re-check redaction"

    def test_fallback_log_does_not_embed_the_bigquery_exception_body(self):
        """A BigQuery permission error names the caller's service account and the
        fully-qualified table; the org-view fallback is expected, so only the
        exception *type* is logged."""
        src = inspect.getsource(_main.analyze_batch_candidates)
        assert "type(e).__name__" in src
        assert "({e})" not in src

    def test_email_regex_matches_the_identities_the_engine_emits(self):
        src = pathlib.Path("static/app.js").read_text(encoding="utf-8")
        pattern = re.search(r"const EMAIL_RE = /(.+?)/g;", src).group(1)
        rx = re.compile(pattern)
        for addr in ("amanda@demo-company.com",
                     "etl-loader@demo-company.iam.gserviceaccount.com"):
            assert rx.fullmatch(addr), f"EMAIL_RE fails to match {addr}"
        # Workload names that are not identities must survive untouched.
        for name in ("analytics.fct_orders_daily", "Scheduled Query Pipeline"):
            assert not rx.search(name), f"EMAIL_RE over-matches {name}"


class TestBatchCandidateEndpoint:
    """End-to-end behaviour with a mocked BigQuery layer."""

    def test_returns_200_on_empty_results(self, test_client, mock_bq_all):
        response = test_client.post(
            "/api/antipatterns/batch_candidates",
            json={"org_project_id": "valid-proj", "region": "region-us", "lookback_days": 7},
        )
        assert response.status_code == 200
        assert response.json() == []

    def test_falls_back_to_project_view_when_org_view_denied(self, test_client, mock_bq_all):
        """A denied org-level dry run must degrade to JOBS_BY_PROJECT, not 500."""
        executed = []
        real_query = mock_bq_all.query
        job = real_query.return_value

        def _query(sql, job_config=None, **kwargs):
            if job_config is not None and getattr(job_config, "dry_run", False):
                raise PermissionError("bigquery.jobs.listAll denied on organization")
            executed.append(sql)
            return job

        mock_bq_all.query = _query
        try:
            response = test_client.post(
                "/api/antipatterns/batch_candidates",
                json={"org_project_id": "valid-proj", "region": "region-us", "lookback_days": 7},
            )
        finally:
            mock_bq_all.query = real_query

        assert response.status_code == 200
        assert executed, "analysis query was never submitted"
        assert "INFORMATION_SCHEMA.JOBS_BY_PROJECT" in executed[0]
        assert "JOBS_BY_ORGANIZATION" not in executed[0]

    def test_time_period_params_are_bound(self, test_client, mock_bq_all):
        """The lookback window must reach BigQuery as bound params, not literals."""
        seen = {}
        real_query = mock_bq_all.query
        job = real_query.return_value

        def _query(sql, job_config=None, **kwargs):
            if job_config is not None and getattr(job_config, "dry_run", False):
                return job
            seen["names"] = [p.name for p in (job_config.query_parameters or [])]
            return job

        mock_bq_all.query = _query
        try:
            response = test_client.post(
                "/api/antipatterns/batch_candidates",
                json={"org_project_id": "valid-proj", "region": "region-us", "lookback_days": 14},
            )
        finally:
            mock_bq_all.query = real_query

        assert response.status_code == 200
        assert "start_time_period" in seen.get("names", [])
        assert "end_time_period" in seen.get("names", [])


class TestTimePeriodQueryParams:
    """The shared lookback-window helper backing the parameterized time filter."""

    def test_window_matches_lookback_days(self):
        from src.utils import time_period_query_params

        class _P:
            lookback_days = 30

        start, end = time_period_query_params(_P())
        assert start.name == "start_time_period"
        assert end.name == "end_time_period"
        assert start.type_ == "TIMESTAMP"
        assert (end.value - start.value).days == 30

    def test_defaults_when_lookback_missing(self):
        from src.utils import time_period_query_params

        start, end = time_period_query_params(object())
        assert (end.value - start.value).days == 7
