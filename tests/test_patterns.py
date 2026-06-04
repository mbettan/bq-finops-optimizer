from main import _extract_pattern, _rank_and_cap

def test_extract_pattern_uuid():
    # UUID should be masked to <uuid>
    assert _extract_pattern("scheduled_query_8f294711-2ba8-4c12-8e12-421731671234") == "scheduled_query_<uuid>"

def test_extract_pattern_long_hex():
    # Hex blobs (>=8 chars) should be masked to #
    assert _extract_pattern("my_job_18f9e1a12a3b4c5d6e") == "my_job_#"

def test_extract_pattern_bquxjob():
    # Console job bquxjob_<hex>_<hex> should be masked to bquxjob_#
    assert _extract_pattern("bquxjob_5c2184f_18df32a8190") == "bquxjob_#"
    assert _extract_pattern("bquxjob_2ba847c1_18f9e1a12a3") == "bquxjob_#"

def test_extract_pattern_long_digits():
    # Digit runs >= 6 should be masked to #
    assert _extract_pattern("job_20260604123456_done") == "job_#_done"

def test_extract_pattern_short_tokens():
    # Short structures like step12 or q3 should be preserved
    assert _extract_pattern("pipeline_q3_step12") == "pipeline_q3_step12"

def test_bquxjob_variants_collapse_to_single_pattern():
    import random
    patterns = set()
    for _ in range(10_000):  # Reduced from 400k in unit test for speed, 10k is plenty to verify regex
        hex1 = ''.join(random.choices('0123456789abcdef', k=7))
        hex2 = ''.join(random.choices('0123456789abcdef', k=11))
        jid = f"mock-project-test:US.bquxjob_{hex1}_{hex2}"
        patterns.add(_extract_pattern(jid))
    assert len(patterns) == 1, f"Expected 1 pattern, got {len(patterns)}"
    assert patterns == {"bquxjob_#"}

def test_uuid_pipeline_collapses():
    import random
    patterns = set()
    for _ in range(1_000):  # 1k is plenty
        u = "%08x-%04x-%04x-%04x-%012x" % tuple(
            random.getrandbits(b) for b in (32, 16, 16, 16, 48)
        )
        patterns.add(_extract_pattern(f"airflow_dag_{u}_run"))
    assert len(patterns) == 1
    assert patterns == {"airflow_dag_<uuid>_run"}

def test_underscore_uuid_jobs_collapse_to_single_pattern():
    """Real BigQuery job IDs use underscore-delimited UUIDs: job_<8>_<4>_<4>_<4>_<12>.
    This is the format that produced 305k patterns / 189MB in production."""
    import random
    patterns = set()
    for _ in range(10_000):  # 10k is plenty to verify regex
        parts = [random.getrandbits(b) for b in (32, 16, 16, 16, 48)]
        uid = "%08x_%04x_%04x_%04x_%012x" % tuple(parts)
        jid = f"mock-project-test:US.job_{uid}"
        patterns.add(_extract_pattern(jid))
    assert len(patterns) == 1, f"Expected 1 pattern, got {len(patterns)}"
    assert patterns == {"job_<uuid>"}

def test_distinct_pipelines_not_over_merged():
    """Structural tokens (step numbers, versions) must NOT collapse, so
    genuinely different pipelines stay separate."""
    a = _extract_pattern("etl_step3_v2_load")
    b = _extract_pattern("etl_step4_v2_load")
    assert a != b, "Short structural tokens were wrongly masked"

def test_qualifier_stripped():
    assert _extract_pattern("proj:US.bquxjob_aaaaaaa_bbbbbbbbbbb") == "bquxjob_#"

def test_script_child_chains_collapse():
    """SCRIPT child jobs (script_job_<hex>_0_0_0...) must collapse regardless
    of nesting depth — NOT fragment into one pattern per depth."""
    patterns = set()
    for depth in range(1, 12):
        suffix = "_0" * depth
        patterns.add(_extract_pattern(f"proj:US.script_job_663d274e{suffix}"))
    assert len(patterns) == 1, f"Expected 1 pattern, got {len(patterns)}: {patterns}"
    assert patterns == {"script_job_#"}

def test_script_child_single_index_collapses():
    """script_job_<hex>_2, _0, _1 (single trailing child index) must collapse
    to one pattern — reproduces the script_job_#_2 vs script_job_#_0 split
    seen in production output."""
    patterns = {
        _extract_pattern(f"proj:US.script_job_d386b185d36b4017fd1031e3ee7dc45f_{i}")
        for i in range(10)
    }
    assert len(patterns) == 1, f"Single-index children fragmented: {patterns}"
    assert patterns == {"script_job_#"}


def test_all_job_id_formats_collapse():
    # 1. Standard UUID
    assert _extract_pattern("b8e94cf9-8b18-4365-ac7d-a94ffee175ac") == "<uuid>"
    
    # 2. Underscore UUID prefixed with job_
    assert _extract_pattern("job_0eb0d3ec_06a4_42b4_954f_7ebe552e9d17") == "job_<uuid>"
    
    # 3. Base64 random job ID
    assert _extract_pattern("job_tA22_C8qQ52s7gT0F8F1C3d2A1s3") == "job_#"
    assert _extract_pattern("job_0exmojCTJWI1uH1WS59bB6O-Sb7c") == "job_#"
    
    # 4. script_job shapes
    assert _extract_pattern("script_job_5f05a8e250659e8c1dcea49d5aaf6bcf_2") == "script_job_#"
    
    # 5. Airflow shapes
    assert _extract_pattern("airflow_EDP_PRODUCTION_API_sql_batch_group_execute_sql_merge_collateral_production_2026_06_03T12_15_00_00_00_52d95074cfd5c84e62a787397caa9aa8") == "airflow_EDP_PRODUCTION_API_sql_batch_group_execute_sql_merge_collateral_production_#"

def test_rank_and_cap_orders_by_savings_and_limits():
    from types import SimpleNamespace
    fake_patterns = [
        SimpleNamespace(indicative_savings_usd=10),
        SimpleNamespace(indicative_savings_usd=999),
        SimpleNamespace(indicative_savings_usd=1),
        SimpleNamespace(indicative_savings_usd=500),
        SimpleNamespace(indicative_savings_usd=50),
    ]
    out = _rank_and_cap(fake_patterns, limit=3)
    assert [p.indicative_savings_usd for p in out] == [999, 500, 50]

def test_script_chains_collapse_to_single_pattern_real_format():
    """The EXACT regression seen in API output: same parent hex at varying
    depths must collapse to ONE pattern."""
    inputs = [
        "proj:US.script_job_43d2c8ea_0_0_0_0",
        "proj:US.script_job_43d2c8ea_0_0_0_0_0",
        "proj:US.script_job_43d2c8ea_0_0_0_0_0_0",
        "proj:US.script_job_663d274e_0",
        "proj:US.script_job_0ad01062_0_0",
    ]
    patterns = {_extract_pattern(i) for i in inputs}
    assert len(patterns) == 1, f"Script depths fragmented: {sorted(patterns)}"
    assert patterns == {"script_job_#"}

def test_airflow_timestamped_runs_collapse():
    """All Airflow DAG runs differing only by timestamp+hash collapse to ONE pattern."""
    a = _extract_pattern("airflow_EDP_API_merge_2026_06_03T12_15_00_00_00_52d95074cfd5c84e62a787397caa9aa8")
    b = _extract_pattern("airflow_EDP_API_merge_2026_06_04T09_30_00_00_00_af01bb22cc33dd44ee55ff66aa77bb88")
    assert a == b, f"Airflow runs fragmented: {a!r} vs {b!r}"
    assert a == "airflow_EDP_API_merge_#"

def test_bqjob_console_format_collapses():
    a = _extract_pattern("bqjob_r5a3f2901_1")
    b = _extract_pattern("bqjob_r9c2e8801_2_ff00aa")
    assert a == b == "bqjob_#"


def test_airflow_subsecond_timestamps_collapse():
    a = _extract_pattern("airflow_X_records_2026_05_30T00_15_00_00_00_b4559d9861c39578")
    b = _extract_pattern("airflow_X_records_2026_05_28T13_45_14_472588_00_00_e8c5ab5b835bdc")
    assert a == b, f"Sub-second timestamp fragmented: {a!r} vs {b!r}"


