"""Server-side shared result cache.

Replaces browser localStorage as the authoritative store for analysis results
so that they survive Cloud Run cold starts and are shared across users and
instances.  See BUILD_GUIDE_server-side-cache.md for the reasoning behind
every decision in here.

Backends
--------
file  : plain file I/O.  Serves both the Cloud Run GCS-FUSE mount (/cache) and
        local development (./.cache) with identical code.
gcs   : google-cloud-storage client.  Fallback if FUSE overhead becomes a
        problem.  Imported lazily -- the package is NOT in requirements.txt.
off   : disabled.  The default, so an unconfigured deploy behaves exactly as
        it did before this module existed.

Nothing in here may raise into the request path.  A cache that is broken must
degrade to "always miss", never to a 500.
"""

from __future__ import annotations

import functools
import gzip
import hashlib
import inspect
import json
import logging
import os
import re
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Tuple

from fastapi import HTTPException, Query, Response
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel

from .constants import __version__
from .utils import _IDENT_RE

logger = logging.getLogger(__name__)

# Bump to invalidate every entry at once (e.g. after a response-shape change).
CACHE_SCHEMA_VERSION = 1
_NS = "v1"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_BACKEND_NAME = os.environ.get("CACHE_BACKEND", "off").strip().lower()
_CACHE_DIR = os.environ.get("CACHE_DIR", "/cache").strip()
_CACHE_BUCKET = os.environ.get("CACHE_BUCKET", "").strip()
_TTL_DEFAULT = int(os.environ.get("CACHE_TTL_DEFAULT", "3600"))
_MAX_ENTRY_BYTES = int(os.environ.get("CACHE_MAX_ENTRY_MB", "32")) * 1024 * 1024
_COMPRESS = os.environ.get("CACHE_COMPRESS", "auto").strip().lower()  # auto|always|never
_COMPRESS_OVER = int(os.environ.get("CACHE_COMPRESS_OVER_KB", "512")) * 1024

# endpoint path -> (module name, default TTL seconds).
#
# Keyed by endpoint because REPORT_MODULES in report_generator.py is already
# keyed that way -- report_generator can look modules up here instead of
# duplicating the mapping.  This table also covers the 8 endpoints that are not
# report inputs (profiler, utilization, peak, HBO sub-endpoints), which is why
# it is a separate table rather than extra columns on REPORT_MODULES.
#
# TTL rationale: storage/schema state moves slowly (6h); job history is the
# common case (1h); AI Doctor is the only endpoint that costs real money per
# call, via Vertex AI, so it gets 24h.
CACHE_MODULES: Dict[str, Tuple[str, int]] = {
    "/api/storage/analyze":                 ("storage",            6 * 3600),
    "/api/storage/static_audit":            ("static_audit",       6 * 3600),
    "/api/storage/hygiene":                 ("hygiene",            6 * 3600),
    "/api/storage/active_assist":           ("active_assist",     12 * 3600),
    "/api/governance/analyze":              ("governance",         6 * 3600),
    "/api/jobs/analyze":                    ("jobs",                   3600),
    "/api/slots/analyze":                   ("slots",                  3600),
    "/api/slots/simulate":                  ("slots_sim",              3600),
    "/api/slots/tiered_recommendations":    ("slots_tiered",           3600),
    "/api/slots/utilization":               ("slots_util",             3600),
    "/api/slots/actual_provisioning":       ("slots_actual",           3600),
    "/api/slots/peak":                      ("slots_peak",             3600),
    "/api/slots/fluid_simulation":          ("fluid_sim",              3600),
    "/api/slots/profiler":                  ("profiler",               3600),
    "/api/slots/profiler/queries":          ("profiler_queries",       3600),
    "/api/users/top_spenders":              ("top_spenders",           3600),
    "/api/cost-attribution/calculate":      ("cost_attribution",       3600),
    "/api/antipatterns/dml":                ("ap_dml",                 3600),
    "/api/antipatterns/mv":                 ("ap_mv",                  3600),
    "/api/antipatterns/linter":             ("ap_linter",              3600),
    "/api/antipatterns/skew":               ("ap_skew",                3600),
    "/api/antipatterns/batch_candidates":   ("ap_batch",               3600),
    "/api/resource_warnings/analyze":       ("resource_warnings",      3600),
    "/api/mv/analyze":                      ("mv_rejections",          3600),
    "/api/bi/analyze":                      ("bi",                     3600),
    "/api/hbo/analyze":                     ("hbo",                    3600),
    "/api/hbo/optimizations":               ("hbo_optimizations",      3600),
    "/api/hbo/summary":                     ("hbo_summary",            3600),
    "/api/hbo/performance_insights":        ("hbo_performance",        3600),
    "/api/hbo/status":                      ("hbo_status",             3600),
    "/api/fluid-scaling/estimate":          ("fluid_estimate",         3600),
    "/api/fluid-scaling/status":            ("fluid_status",           3600),
    "/api/ai/analyze":                      ("ai_doctor",         24 * 3600),
}

_MODULE_NAMES = frozenset(m for m, _ in CACHE_MODULES.values())
_MODULE_TTL = {m: t for m, t in CACHE_MODULES.values()}


def ttl_for(module: str) -> int:
    """Per-module TTL.  CACHE_TTL_<MODULE> overrides the table; CACHE_TTL_DEFAULT
    overrides the fallback."""
    override = os.environ.get(f"CACHE_TTL_{module.upper()}")
    if override:
        try:
            return int(override)
        except ValueError:
            logger.warning("Ignoring non-integer CACHE_TTL_%s=%r", module.upper(), override)
    return _MODULE_TTL.get(module, _TTL_DEFAULT)


# ---------------------------------------------------------------------------
# Scope path components
# ---------------------------------------------------------------------------

# Portable subset: safe as a path segment on POSIX *and* Windows, and free of
# any traversal character.  Anything outside it gets hashed -- see 1.5.
_GCP_IDENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_\-\.\:]{0,99}$")
_PORTABLE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,62}$")


def scope_component(value: Optional[str]) -> Optional[str]:
    """Turn a user-supplied project ID or region into a safe path segment.

    Returns None when the value is absent or is not a valid GCP identifier --
    callers treat None as "not cacheable" and skip the cache entirely rather
    than guessing a scope.
    """
    v = (value or "").strip()
    if not v or v in (".", ".."):
        return None
    if not _GCP_IDENT_RE.match(v):
        return None
    if _PORTABLE.match(v):
        return v
    # Domain-scoped IDs ("example.com:legacy-project") are legal GCP
    # identifiers but contain ':' and '.', which are not portable path
    # segments.  Hash rather than mangle, so two such projects never collide.
    return "h_" + hashlib.sha256(v.encode("utf-8")).hexdigest()[:20]


# ---------------------------------------------------------------------------
# Key derivation
# ---------------------------------------------------------------------------

# Excluded from the key: a spend guardrail on the query and the billing project
# for the probe.  Neither changes the result.  See 1.1.
_KEY_EXCLUDE = frozenset({"max_bytes_billed_gb", "admin_project_id"})


def cache_key(module: str, params: BaseModel) -> Optional[Tuple[str, str, Dict[str, Any]]]:
    """Return (prefix, param_hash, digest) or None when the request is not cacheable.

    prefix : "v1/{org}/{region}/{module}"
    hash   : sha256[:16] of the canonicalised, default-filled parameters
    digest : small human-readable subset, stored for UI/debug only
    """
    try:
        d = params.model_dump(mode="json", exclude_none=True)
    except Exception:
        return None

    org = scope_component(d.pop("org_project_id", None))
    region = scope_component(d.pop("region", None) or "region-us")
    if not org or not region:
        # No explicit org scope -> the handler would fall back to the ADC
        # default project.  Caching under a guessed scope risks serving one
        # organisation's data to another.  Skip the cache instead.
        return None

    for k in _KEY_EXCLUDE:
        d.pop(k, None)

    # Match validate_focus_projects() (utils.py:107) -- trim, dedupe -- then
    # sort, because ["a","b"] and ["b","a"] are the same scan.
    fp = d.get("focus_projects")
    if isinstance(fp, list):
        d["focus_projects"] = sorted({str(x).strip() for x in fp if str(x).strip()})

    canon = json.dumps(d, sort_keys=True, separators=(",", ":"), default=str)
    param_hash = hashlib.sha256(canon.encode("utf-8")).hexdigest()[:16]

    digest = {
        k: d[k] for k in ("lookback_days", "focus_projects", "percentile", "model")
        if k in d
    }
    return f"{_NS}/{org}/{region}/{module}", param_hash, digest


# ---------------------------------------------------------------------------
# Serialisation
# ---------------------------------------------------------------------------

def _encode(obj: Any) -> bytes:
    jsonable = jsonable_encoder(obj)
    raw = json.dumps(jsonable, separators=(",", ":"), default=str).encode("utf-8")
    if _COMPRESS == "always" or (_COMPRESS == "auto" and len(raw) > _COMPRESS_OVER):
        return gzip.compress(raw, compresslevel=6)
    return raw


def _decode(blob: bytes) -> Any:
    # Gzip magic -- one filename, self-describing content, so a read never
    # needs a second round-trip to guess the encoding.
    if blob[:2] == b"\x1f\x8b":
        blob = gzip.decompress(blob)
    return json.loads(blob.decode("utf-8"))


# ---------------------------------------------------------------------------
# Backends
# ---------------------------------------------------------------------------

class CacheBackend(Protocol):
    enabled: bool
    def get(self, key: str) -> Optional[bytes]: ...
    def put(self, key: str, blob: bytes) -> None: ...
    def delete_glob(self, pattern: str) -> int: ...


class NullBackend:
    enabled = False
    def get(self, key: str) -> Optional[bytes]: return None
    def put(self, key: str, blob: bytes) -> None: return None
    def delete_glob(self, pattern: str) -> int: return 0


class FileCacheBackend:
    """Plain file I/O.  Used for both the FUSE mount and local development."""

    enabled = True

    def __init__(self, root: str):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _resolve(self, key: str) -> Path:
        p = (self.root / key).resolve()
        root = self.root.resolve()
        if p != root and root not in p.parents:
            # Defence in depth: scope_component() should already have made this
            # impossible.  If it ever fires, that is a bug worth a loud log.
            raise ValueError(f"cache key escapes root: {key!r}")
        return p

    def get(self, key: str) -> Optional[bytes]:
        p = self._resolve(key)
        try:
            return p.read_bytes()
        except FileNotFoundError:
            return None

    def put(self, key: str, blob: bytes) -> None:
        p = self._resolve(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(f"{p.name}.tmp.{uuid.uuid4().hex}")
        try:
            tmp.write_bytes(blob)
            os.replace(tmp, p)
        except Exception:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
            raise
        # NOTE on gcsfuse: os.replace is copy+delete, not atomic.  We do not
        # rely on rename atomicity -- gcsfuse uploads a whole object on close,
        # so a concurrent reader sees the old object or the new one, never a
        # torn one.  The temp file exists only to keep local dev honest.

    def delete_glob(self, pattern: str) -> int:
        n = 0
        for p in self.root.glob(pattern):
            try:
                if p.is_file():
                    p.unlink()
                    n += 1
            except Exception:
                logger.debug("cache: failed to unlink %s", p, exc_info=True)
        return n


class GcsApiBackend:
    """Option 3 fallback.  google-cloud-storage is imported lazily because it is
    NOT in requirements.txt -- add it only if you select this backend."""

    enabled = True

    def __init__(self, bucket_name: str):
        from google.cloud import storage  # noqa: PLC0415 -- intentionally lazy
        self._bucket = storage.Client().bucket(bucket_name)

    def get(self, key: str) -> Optional[bytes]:
        from google.cloud.exceptions import NotFound
        try:
            return self._bucket.blob(key).download_as_bytes()
        except NotFound:
            return None

    def put(self, key: str, blob: bytes) -> None:
        self._bucket.blob(key).upload_from_string(blob, content_type="application/json")

    def delete_glob(self, pattern: str) -> int:
        import fnmatch
        prefix = pattern.split("*", 1)[0]
        n = 0
        for b in self._bucket.list_blobs(prefix=prefix):
            if fnmatch.fnmatch(b.name, pattern):
                b.delete()
                n += 1
        return n


def _build_backend() -> CacheBackend:
    if _BACKEND_NAME == "file":
        try:
            return FileCacheBackend(_CACHE_DIR)
        except Exception as e:
            logger.error("cache: FileCacheBackend(%s) failed (%s) -- caching DISABLED", _CACHE_DIR, e)
            return NullBackend()
    if _BACKEND_NAME == "gcs":
        if not _CACHE_BUCKET:
            logger.error("cache: CACHE_BACKEND=gcs but CACHE_BUCKET is unset -- caching DISABLED")
            return NullBackend()
        try:
            return GcsApiBackend(_CACHE_BUCKET)
        except Exception as e:
            logger.error("cache: GcsApiBackend(%s) failed (%s) -- caching DISABLED", _CACHE_BUCKET, e)
            return NullBackend()
    return NullBackend()


_backend: CacheBackend = _build_backend()


def enabled() -> bool:
    return _backend.enabled


def describe() -> str:
    if not _backend.enabled:
        return "disabled (CACHE_BACKEND=off)"
    target = _CACHE_DIR if _BACKEND_NAME == "file" else f"gs://{_CACHE_BUCKET}"
    return f"{_BACKEND_NAME} at {target}, default TTL {_TTL_DEFAULT}s"


# ---------------------------------------------------------------------------
# Envelope read / write
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CacheHit:
    data: Any
    created_at: float
    expires_at: float
    param_hash: str


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def load(module: str, params: BaseModel) -> Optional[CacheHit]:
    """Return a fresh entry, or None.  Never raises."""
    if not _backend.enabled:
        return None
    key = cache_key(module, params)
    if key is None:
        return None
    prefix, param_hash, _ = key
    try:
        blob = _backend.get(f"{prefix}/{param_hash}.json")
        if blob is None:
            return None
        env = _decode(blob)
        if env.get("schema_version") != CACHE_SCHEMA_VERSION:
            return None
        if env.get("app_version") != __version__:
            logger.info("cache: %s written by app %s, current %s -- recomputing",
                        module, env.get("app_version"), __version__)
            return None
        expires_at = float(env["expires_at_epoch"])
        if expires_at <= time.time():
            return None
        return CacheHit(
            data=env["data"],
            created_at=float(env["created_at_epoch"]),
            expires_at=expires_at,
            param_hash=param_hash,
        )
    except Exception:
        logger.warning("cache: unreadable entry for %s -- treating as miss", module, exc_info=True)
        return None


def store(module: str, params: BaseModel, data: Any, ttl_s: Optional[int] = None) -> None:
    """Write an entry plus its latest-pointer.  Never raises."""
    if not _backend.enabled:
        return
    key = cache_key(module, params)
    if key is None:
        return
    prefix, param_hash, digest = key
    ttl = ttl_s if ttl_s is not None else ttl_for(module)
    now = time.time()
    env = {
        "schema_version": CACHE_SCHEMA_VERSION,
        "app_version": __version__,
        "module": module,
        "param_hash": param_hash,
        "params_digest": digest,
        "created_at": _iso(now),
        "expires_at": _iso(now + ttl),
        "created_at_epoch": now,
        "expires_at_epoch": now + ttl,
        "data": data,
    }
    try:
        blob = _encode(env)
        if len(blob) > _MAX_ENTRY_BYTES:
            logger.warning("cache: %s entry is %.1f MB (> CACHE_MAX_ENTRY_MB) -- not cached",
                           module, len(blob) / 1024 / 1024)
            return
        _backend.put(f"{prefix}/{param_hash}.json", blob)
        # Pointer written AFTER the payload: a partial write leaves a pointer to
        # an entry that exists, never the reverse.
        _backend.put(f"{prefix}/latest.json", _encode({
            "param_hash": param_hash,
            "params_digest": digest,
            "created_at": env["created_at"],
            "expires_at": env["expires_at"],
            "created_at_epoch": now,
            "expires_at_epoch": now + ttl,
            "size_bytes": len(blob),
        }))
    except Exception:
        logger.warning("cache: failed to store %s -- continuing", module, exc_info=True)


def latest(module: str, org: str, region: str) -> Optional[Dict[str, Any]]:
    """Read the latest-pointer for a module in a scope.  Used by /api/cache/status
    and (phase 5) by the report aggregator."""
    if not _backend.enabled:
        return None
    o, r = scope_component(org), scope_component(region)
    if not o or not r or module not in _MODULE_NAMES:
        return None
    try:
        blob = _backend.get(f"{_NS}/{o}/{r}/{module}/latest.json")
        return _decode(blob) if blob else None
    except Exception:
        return None


def load_latest_data(module: str, org: str, region: str) -> Optional[Any]:
    """Resolve the latest pointer and return that entry's payload if still fresh."""
    ptr = latest(module, org, region)
    if not ptr:
        return None
    o, r = scope_component(org), scope_component(region)
    try:
        blob = _backend.get(f"{_NS}/{o}/{r}/{module}/{ptr['param_hash']}.json")
        if not blob:
            return None
        env = _decode(blob)
        if env.get("schema_version") != CACHE_SCHEMA_VERSION:
            return None
        if float(env["expires_at_epoch"]) <= time.time():
            return None
        return env["data"]
    except Exception:
        return None


def invalidate(module: Optional[str] = None,
               org: Optional[str] = None,
               region: Optional[str] = None) -> int:
    """Delete entries.  None means 'any' for that component.

    `module` is checked against the registry, so it can never reach the
    filesystem as arbitrary caller-supplied text.
    """
    if not _backend.enabled:
        return 0
    if module is not None and module not in _MODULE_NAMES:
        raise HTTPException(status_code=404, detail=f"Unknown cache module: {module}")
    o = scope_component(org) if org else "*"
    r = scope_component(region) if region else "*"
    if not o or not r:
        raise HTTPException(status_code=400, detail="Invalid cache scope.")
    m = module or "*"
    try:
        return _backend.delete_glob(f"{_NS}/{o}/{r}/{m}/*.json")
    except Exception:
        logger.warning("cache: invalidate failed", exc_info=True)
        return 0


# ---------------------------------------------------------------------------
# Single-flight (per process)
# ---------------------------------------------------------------------------

_flight_registry_lock = threading.Lock()
_flights: Dict[str, List[Any]] = {}   # key -> [Lock, waiter_count]


@contextmanager
def _single_flight(key: str):
    with _flight_registry_lock:
        entry = _flights.get(key)
        if entry is None:
            entry = _flights[key] = [threading.Lock(), 0]
        entry[1] += 1
    lock = entry[0]
    lock.acquire()
    try:
        yield
    finally:
        lock.release()
        with _flight_registry_lock:
            entry[1] -= 1
            if entry[1] <= 0:
                _flights.pop(key, None)


# ---------------------------------------------------------------------------
# The decorator
# ---------------------------------------------------------------------------

def _hit_headers(hit: CacheHit) -> Dict[str, str]:
    return {
        "X-Cache": "HIT",
        "X-Cache-Age": str(int(time.time() - hit.created_at)),
        "X-Cache-Expires": _iso(hit.expires_at),
        "X-Cache-Key": hit.param_hash,
    }


def cached_analysis(module: str, ttl_s: Optional[int] = None):
    """Cache an analysis endpoint's result, keyed by (org, region, module, params).

    Adds two FastAPI-visible parameters to the wrapped endpoint:
      response : injected, used to set X-Cache* headers
      refresh  : ?refresh=true recomputes and overwrites

    Both are appended KEYWORD_ONLY at the end, so `params` remains the first
    parameter -- /api/meta/scope-map depends on that ordering.
    """
    if module not in _MODULE_NAMES:
        raise RuntimeError(f"cached_analysis: '{module}' is not in CACHE_MODULES")

    def decorator(fn):
        sig = inspect.signature(fn)

        @functools.wraps(fn)
        def wrapper(*args, response: Response = None, refresh: bool = False, **kwargs):
            params = kwargs.get("params") or (args[0] if args else None)

            if not _backend.enabled or not isinstance(params, BaseModel):
                if response is not None:
                    response.headers["X-Cache"] = "OFF"
                return fn(*args, **kwargs)

            if refresh:
                result = fn(*args, **kwargs)
                store(module, params, result, ttl_s)
                if response is not None:
                    response.headers["X-Cache"] = "BYPASS"
                return result

            hit = load(module, params)
            if hit is not None:
                if response is not None:
                    response.headers.update(_hit_headers(hit))
                return hit.data

            key = cache_key(module, params)
            if key is None:
                if response is not None:
                    response.headers["X-Cache"] = "UNSCOPED"
                return fn(*args, **kwargs)
            flight_key = f"{key[0]}/{key[1]}"

            with _single_flight(flight_key):
                # Double-check: another thread may have populated it while we
                # waited on the lock.
                hit = load(module, params)
                if hit is not None:
                    if response is not None:
                        response.headers.update(_hit_headers(hit))
                        response.headers["X-Cache"] = "HIT"
                    return hit.data
                result = fn(*args, **kwargs)
                store(module, params, result, ttl_s)

            if response is not None:
                response.headers["X-Cache"] = "MISS"
            return result

        # functools.wraps set __wrapped__, which would make inspect.signature
        # follow through to `fn`.  An explicit __signature__ takes precedence
        # and is what FastAPI reads to build the route.
        wrapper.__signature__ = sig.replace(parameters=[
            *sig.parameters.values(),
            inspect.Parameter("response", inspect.Parameter.KEYWORD_ONLY,
                              annotation=Response),
            inspect.Parameter("refresh", inspect.Parameter.KEYWORD_ONLY,
                              annotation=bool,
                              default=Query(False, description="Recompute and overwrite the cached result.")),
        ])
        return wrapper

    return decorator
