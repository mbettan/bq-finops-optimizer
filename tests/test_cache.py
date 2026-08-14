"""Unit tests for the server-side result cache.

Env vars are read at import time in src.cache, so they must be set before the
first import — same pattern as AUTH_ENFORCED_UPSTREAM in conftest.py.
"""

import os
import tempfile

os.environ.setdefault("AUTH_ENFORCED_UPSTREAM", "true")
os.environ.setdefault("CACHE_BACKEND", "file")
os.environ.setdefault("CACHE_DIR", tempfile.mkdtemp(prefix="bqfinops_cache_test_"))

import time
import threading
import pytest
from pydantic import BaseModel
from typing import Optional, List

from src import cache as C


class P(BaseModel):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = 30
    focus_projects: Optional[List[str]] = None
    max_bytes_billed_gb: Optional[int] = None


class TestKeyDerivation:
    def test_defaults_normalise_to_the_same_key(self):
        """{} and an explicit-defaults body must hit the same entry — otherwise
        the hit rate roughly halves for no reason."""
        a = C.cache_key("jobs", P(org_project_id="proj"))
        b = C.cache_key("jobs", P(org_project_id="proj", region="region-us", lookback_days=30))
        assert a[1] == b[1]

    def test_lookback_changes_the_key(self):
        """The regression this whole design exists for: a 7-day scan must never
        be served to a 90-day request."""
        a = C.cache_key("jobs", P(org_project_id="proj", lookback_days=7))
        b = C.cache_key("jobs", P(org_project_id="proj", lookback_days=90))
        assert a[1] != b[1]

    def test_focus_projects_order_and_dupes_are_irrelevant(self):
        a = C.cache_key("jobs", P(org_project_id="p", focus_projects=["a", "b"]))
        b = C.cache_key("jobs", P(org_project_id="p", focus_projects=["b", " a", "a"]))
        assert a[1] == b[1]

    def test_focus_scope_differs_from_org_scope(self):
        a = C.cache_key("jobs", P(org_project_id="p"))
        b = C.cache_key("jobs", P(org_project_id="p", focus_projects=["a"]))
        assert a[1] != b[1]

    def test_byte_cap_excluded(self):
        a = C.cache_key("jobs", P(org_project_id="p", max_bytes_billed_gb=100))
        b = C.cache_key("jobs", P(org_project_id="p", max_bytes_billed_gb=9000))
        assert a[1] == b[1]

    def test_scopes_are_isolated(self):
        a = C.cache_key("jobs", P(org_project_id="org-a"))
        b = C.cache_key("jobs", P(org_project_id="org-b"))
        assert a[0] != b[0]

    def test_unscoped_request_is_not_cacheable(self):
        assert C.cache_key("jobs", P()) is None


class TestScopeComponentSafety:
    @pytest.mark.parametrize("bad", [
        "../../etc", "..", ".", "", "   ", "a/b", "a\\b", "a\x00b", "-leading",
        "x" * 200,
    ])
    def test_rejects_unsafe(self, bad):
        assert C.scope_component(bad) is None

    def test_domain_scoped_project_is_hashed_not_mangled(self):
        """Legal GCP ID, illegal Windows path segment, contains ':' — hash it."""
        out = C.scope_component("example.com:legacy-project")
        assert out.startswith("h_")
        assert ":" not in out and "." not in out and "/" not in out

    def test_distinct_domain_projects_do_not_collide(self):
        assert C.scope_component("a.com:p") != C.scope_component("b.com:p")

    def test_backend_rejects_an_escaping_key(self):
        b = C.FileCacheBackend(C._CACHE_DIR)
        with pytest.raises(ValueError):
            b._resolve("v1/../../../../etc/passwd")


class TestRoundTrip:
    def test_store_then_load(self):
        p = P(org_project_id="rt-proj")
        C.store("jobs", p, {"rows": [1, 2, 3]})
        hit = C.load("jobs", p)
        assert hit is not None and hit.data == {"rows": [1, 2, 3]}

    def test_empty_result_is_cached(self):
        """A clean org must not pay full price on every load."""
        p = P(org_project_id="empty-proj")
        C.store("ap_linter", p, [])
        hit = C.load("ap_linter", p)
        assert hit is not None and hit.data == []

    def test_expired_entry_is_a_miss(self):
        p = P(org_project_id="exp-proj")
        C.store("jobs", p, {"x": 1}, ttl_s=-1)
        assert C.load("jobs", p) is None

    def test_corrupt_entry_is_a_miss_not_a_crash(self):
        p = P(org_project_id="corrupt-proj")
        C.store("jobs", p, {"x": 1})
        prefix, h, _ = C.cache_key("jobs", p)
        C._backend.put(f"{prefix}/{h}.json", b"{not json")
        assert C.load("jobs", p) is None

    def test_large_entry_is_skipped(self, monkeypatch):
        monkeypatch.setattr(C, "_MAX_ENTRY_BYTES", 100)
        monkeypatch.setattr(C, "_COMPRESS", "never")
        p = P(org_project_id="big-proj")
        C.store("jobs", p, {"blob": "x" * 10_000})
        assert C.load("jobs", p) is None

    def test_gzip_round_trip(self, monkeypatch):
        monkeypatch.setattr(C, "_COMPRESS", "always")
        p = P(org_project_id="gz-proj")
        C.store("jobs", p, {"blob": "y" * 5000})
        assert C.load("jobs", p).data == {"blob": "y" * 5000}

    def test_invalidate_drops_the_entry(self):
        p = P(org_project_id="inv-proj")
        C.store("jobs", p, {"x": 1})
        assert C.invalidate(module="jobs", org="inv-proj", region="region-us") >= 1
        assert C.load("jobs", p) is None

    def test_invalidate_rejects_unknown_module(self):
        from fastapi import HTTPException
        with pytest.raises(HTTPException):
            C.invalidate(module="../../etc", org="p", region="region-us")

    def test_latest_pointer_tracks_the_newest_write(self):
        C.store("jobs", P(org_project_id="ptr-proj", lookback_days=7), {"d": 7})
        C.store("jobs", P(org_project_id="ptr-proj", lookback_days=90), {"d": 90})
        assert C.load_latest_data("jobs", "ptr-proj", "region-us") == {"d": 90}


class TestSingleFlight:
    def test_concurrent_misses_compute_once(self):
        """Mirrors tests/test_bq_client_pool.py's approach."""
        calls = []
        gate = threading.Event()

        def slow():
            calls.append(1)
            gate.wait(timeout=2)
            return {"v": 1}

        p = P(org_project_id="sf-proj")
        C.invalidate(module="jobs", org="sf-proj", region="region-us")
        key = C.cache_key("jobs", p)
        flight = f"{key[0]}/{key[1]}"

        def worker():
            with C._single_flight(flight):
                if C.load("jobs", p) is None:
                    C.store("jobs", p, slow())

        threads = [threading.Thread(target=worker) for _ in range(5)]
        [t.start() for t in threads]
        time.sleep(0.1)
        gate.set()
        [t.join(timeout=5) for t in threads]

        assert len(calls) == 1
        assert not C._flights, "flight registry leaked an entry"
