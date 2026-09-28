"""Tests for the governance auditor's scan scope (#78A, #79B).

- The dataset expiration audit reads SCHEMATA/SCHEMATA_OPTIONS through each
  audited project's own `project`.`region` qualifier.
- The require_partition_filter audit checks the GOVERNANCE_MAX_DATASETS largest
  datasets, and the response reports how many were checked out of how many.
"""

from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

from src.main import GOVERNANCE_MAX_DATASETS, GOVERNANCE_MAX_PROJECTS, app

client = TestClient(app)

_ENDPOINT = "/api/governance/analyze"
_BASE = {"org_project_id": "valid-proj", "region": "region-us"}


def _ds(project, dataset, size):
    return SimpleNamespace(project_id=project, dataset_id=dataset, total_bytes=size)


def _exp(project, dataset):
    return SimpleNamespace(project_id=project, dataset_id=dataset, default_table_expiration=None)


class _FakeBQ:
    """Routes run_query_and_log calls by job label."""

    def __init__(self, discovery=(), expiration=None, fail_union=False, fail_projects=()):
        self.discovery = list(discovery)
        self.expiration = expiration or {}
        self.fail_union = fail_union
        self.fail_projects = set(fail_projects)
        self.calls = []

    def __call__(self, client, sql, label, params=None, query_parameters=None, **kw):
        self.calls.append((label, sql, query_parameters))
        if label == "Governance Dataset Discovery":
            return iter(self.discovery)
        if label == "Expiration Audit":
            if self.fail_union:
                raise RuntimeError("Access Denied")
            return iter(r for p in self.expiration for r in self.expiration[p]
                        if f"`{p}`.`region-us`.INFORMATION_SCHEMA.SCHEMATA " in sql)
        if label.startswith("Expiration Audit ("):
            p = label[len("Expiration Audit ("):-1]
            if p in self.fail_projects:
                raise RuntimeError("Access Denied")
            return iter(self.expiration.get(p, []))
        return iter([])

    def sqls(self, label):
        return [sql for lbl, sql, _ in self.calls if lbl == label]


def _post(fake, **payload):
    with patch("src.main.run_query_and_log", side_effect=fake):
        response = client.post(_ENDPOINT, json={**_BASE, **payload})
    assert response.status_code == 200, response.text
    return response.json()


class TestExpirationAuditScope:
    def test_each_focus_project_is_queried_in_its_own_project(self):
        fake = _FakeBQ(expiration={
            "proj-a": [_exp("proj-a", "tmp_a")],
            "proj-b": [_exp("proj-b", "staging_b")],
        })
        data = _post(fake, audit_type="expiration", focus_projects=["proj-a", "proj-b"])

        sql = fake.sqls("Expiration Audit")[0]
        for p in ("proj-a", "proj-b"):
            assert f"`{p}`.`region-us`.INFORMATION_SCHEMA.SCHEMATA s" in sql
            assert f"`{p}`.`region-us`.INFORMATION_SCHEMA.SCHEMATA_OPTIONS o" in sql
        # The org project is not one of the focus projects, so it is not read.
        assert "`valid-proj`.`region-us`.INFORMATION_SCHEMA.SCHEMATA" not in sql
        assert {(r["project_id"], r["dataset_id"]) for r in data["expiration_issues"]} == {
            ("proj-a", "tmp_a"), ("proj-b", "staging_b"),
        }
        assert data["expiration_projects_checked"] == 2
        assert data["expiration_projects_total"] == 2
        assert data["expiration_projects_failed"] == []

    def test_focus_projects_skip_discovery_and_stay_bound(self):
        fake = _FakeBQ()
        _post(fake, audit_type="expiration", focus_projects=["proj-a"])
        assert fake.sqls("Governance Dataset Discovery") == []
        (_, _, qp), = [c for c in fake.calls if c[0] == "Expiration Audit"]
        assert any(getattr(p, "name", "") == "focus_projects" for p in qp)

    def test_org_wide_audits_discovered_projects_largest_first(self):
        fake = _FakeBQ(discovery=[
            _ds("big-proj", "d1", 900), _ds("small-proj", "d2", 10), _ds("big-proj", "d3", 50),
        ])
        data = _post(fake, audit_type="expiration")
        sql = fake.sqls("Expiration Audit")[0]
        assert sql.index("`big-proj`") < sql.index("`small-proj`")
        assert data["expiration_projects_total"] == 2
        assert data["expiration_projects_checked"] == 2

    def test_org_wide_project_cap(self):
        n = GOVERNANCE_MAX_PROJECTS + 7
        fake = _FakeBQ(discovery=[_ds(f"proj-{i:03d}", "d", n - i) for i in range(n)])
        data = _post(fake, audit_type="expiration")
        sql = fake.sqls("Expiration Audit")[0]
        assert sql.count("INFORMATION_SCHEMA.SCHEMATA s") == GOVERNANCE_MAX_PROJECTS
        assert f"`proj-{n - 1:03d}`" not in sql  # smallest project skipped
        assert data["expiration_projects_checked"] == GOVERNANCE_MAX_PROJECTS
        assert data["expiration_projects_total"] == n

    def test_inaccessible_project_does_not_hide_the_others(self):
        fake = _FakeBQ(
            expiration={"proj-a": [_exp("proj-a", "tmp_a")]},
            fail_union=True, fail_projects={"proj-b"},
        )
        data = _post(fake, audit_type="expiration", focus_projects=["proj-a", "proj-b"])
        assert [r["dataset_id"] for r in data["expiration_issues"]] == ["tmp_a"]
        assert data["expiration_projects_failed"] == ["proj-b"]
        assert data["expiration_projects_checked"] == 1
        assert data["expiration_projects_total"] == 2


class TestFilterAuditScope:
    def test_discovery_has_no_hardcoded_limit(self):
        fake = _FakeBQ()
        _post(fake, audit_type="filter")
        sql = fake.sqls("Governance Dataset Discovery")[0]
        assert "LIMIT" not in sql
        assert "ORDER BY total_bytes DESC" in sql

    def test_checks_up_to_the_cap_and_reports_coverage(self):
        n = GOVERNANCE_MAX_DATASETS + 12
        fake = _FakeBQ(discovery=[_ds("proj-a", f"ds_{i:03d}", n - i) for i in range(n)])
        data = _post(fake, audit_type="filter")
        (audit_sql,) = fake.sqls("Missing Partition Filters Audit")
        assert audit_sql.count("INFORMATION_SCHEMA.TABLE_OPTIONS") == GOVERNANCE_MAX_DATASETS
        assert "`proj-a`.`ds_000`" in audit_sql
        assert f"`proj-a`.`ds_{n - 1:03d}`" not in audit_sql
        assert data["filter_datasets_checked"] == GOVERNANCE_MAX_DATASETS
        assert data["filter_datasets_total"] == n

    def test_small_org_checks_every_dataset(self):
        fake = _FakeBQ(discovery=[_ds("proj-a", f"ds_{i}", 100 - i) for i in range(8)])
        data = _post(fake, audit_type="filter")
        (audit_sql,) = fake.sqls("Missing Partition Filters Audit")
        assert audit_sql.count("INFORMATION_SCHEMA.TABLE_OPTIONS") == 8
        assert data["filter_datasets_checked"] == 8
        assert data["filter_datasets_total"] == 8

    def test_expiration_only_leaves_filter_coverage_empty(self):
        data = _post(_FakeBQ(), audit_type="expiration", focus_projects=["proj-a"])
        assert data["filter_datasets_checked"] is None
        assert data["filter_datasets_total"] is None


class TestAllAuditsShareDiscovery:
    def test_one_discovery_query_for_both_audits(self):
        fake = _FakeBQ(discovery=[_ds("proj-a", "ds_1", 10)])
        data = _post(fake)
        assert len(fake.sqls("Governance Dataset Discovery")) == 1
        assert data["expiration_projects_total"] == 1
        assert data["filter_datasets_total"] == 1


class TestOfflineSnapshotGovernance:
    """The offline simulator sample must include the coverage fields."""

    def test_sample_governance_has_coverage_fields(self):
        import json
        from pathlib import Path

        from src.main import GovernanceResponse

        snapshot_path = Path(__file__).resolve().parents[1] / "docs" / "finops-snapshot_dummy.json"
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        payload = json.loads(snapshot["data"]["bq_gov_results"])
        GovernanceResponse(**payload)
        for key in (
            "expiration_projects_checked",
            "expiration_projects_total",
            "expiration_projects_failed",
            "filter_datasets_checked",
            "filter_datasets_total",
        ):
            assert key in payload, f"sample governance payload is missing {key}"
