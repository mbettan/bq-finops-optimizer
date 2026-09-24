# CLAUDE.md — FinOps Optimizer for BigQuery Agent Guidelines

## 1. Non-Negotiable Security & Architectural Invariants
1. **Zero Data-Plane Access:** Never write SQL or Python that queries user tables directly or requires `bigquery.tables.getData`. Every BigQuery query must exclusively target `INFORMATION_SCHEMA` metadata views or `INFORMATION_SCHEMA.JOBS*`.
2. **Offline Test Isolation:** All unit tests in `tests/` execute under `tests/conftest.py`, which enforces a strict socket-level network blocker (`socket.socket.connect` raises `RuntimeError`). Every BigQuery client or HTTP call MUST be mocked in unit tests.
3. **Static Bundle & CSP SHA-256 Synchronization:** Whenever you modify `static/app.js`, `static/style.css`, `static/index.html`, or `RELEASE_NOTES.md`, you MUST run:
   ```bash
   ./scripts/sync_docs_bundle.sh
   ```
   This mirrors assets to `docs/static/` and recomputes the inline script SHA-256 Content-Security-Policy hash in `docs/simulator.html`.
4. **Pricing Parity:** If touching pricing constants or calculator logic, verify parity with:
   ```bash
   node scripts/sync_pricing.js --check
   node tests/test_calculator_engine.js
   ```
5. **Protected Files (NEVER MODIFY):** You are strictly forbidden from modifying `.github/`, `deploy/`, `Dockerfile`, `.gitattributes`, `.gitignore`, `tests/conftest.py`, or `CLAUDE.md`.

## 2. Mandatory Pre-Completion Verification
Run verification appropriate for your execution context:
- **Autonomous Agent Mode (diff-scoped verification):**
  1. `./.venv/bin/ruff check --config /opt/pinned/ruff.pinned.toml <modified_py_files>`
  2. `./.venv/bin/pytest <targeted_tests>`
  3. `node tests/test_calculator_engine.js` (if touching pricing or calculator logic)
  4. `./scripts/sync_docs_bundle.sh && git status -s` (if touching `static/`, `docs/`, or `RELEASE_NOTES.md`)
- **Full Repository CI Verification:**
  1. `./.venv/bin/ruff check .`
  2. `./.venv/bin/pytest`
  3. `node tests/test_calculator_engine.js`
  4. `./scripts/sync_docs_bundle.sh && git status -s`

