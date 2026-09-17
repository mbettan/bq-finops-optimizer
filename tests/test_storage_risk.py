import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

def test_storage_risk_thresholds_match_backend_filter():
    """Verify that JS risk constants stay consistent with the backend pre-filter.
    The backend pre-filters COALESCE(s.size_bytes, 0) > 1073741824 (1 GiB) in src/main.py.
    The frontend JS risk thresholds (largeGiB, minGiB) must match.
    """
    app_js_path = REPO_ROOT / "static" / "app.js"
    app_js = app_js_path.read_text(encoding="utf-8")
    
    match = re.search(r'largeGiB:\s*(\d+)', app_js)
    assert match is not None, "Could not find largeGiB in static/app.js"
    large_gib = int(match.group(1))
    
    match = re.search(r'minGiB:\s*(\d+)', app_js)
    assert match is not None, "Could not find minGiB in static/app.js"
    min_gib = int(match.group(1))
    
    main_py_path = REPO_ROOT / "src" / "main.py"
    main_py = main_py_path.read_text(encoding="utf-8")
    
    # We expect `COALESCE(s.size_bytes, 0) > 1073741824`
    match = re.search(r'COALESCE\(s\.size_bytes,\s*0\)\s*>\s*(\d+)', main_py)
    assert match is not None, "Could not find the storage prefilter in src/main.py"
    backend_filter_bytes = int(match.group(1))
    
    # 1073741824 bytes = 1 GiB
    backend_filter_gib = backend_filter_bytes / (1024 ** 3)
    
    assert min_gib == backend_filter_gib, f"Frontend minGiB {min_gib} != backend {backend_filter_gib} GiB"
    assert min_gib == 1
    assert large_gib == 1024
