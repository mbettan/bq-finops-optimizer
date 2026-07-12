# Phase 2 — Frontend Code Review: Security & Architecture Audit

Following the backend code review and verification, a deep audit of the frontend JavaScript (`static/app.js`) was performed. The frontend was found to contain **Critical Security Vulnerabilities** related to Cross-Site Scripting (XSS), specifically stemming from structural flaws in the global sanitizer and local storage hydration logic.

> **Remediation Status:** Both critical XSS vulnerabilities were **fixed in v1.1.3** (2026-07-11). See the fix details below each finding.

---

## 🚨 Top Frontend Security Findings

### 1. [Critical] DOM-Based Stored XSS via Whitelisted Keys (`query`, `sql`) — ✅ FIXED v1.1.3

**Location:** `static/app.js` (lines 59-61, 2068)
**Issue:** The application uses a global `window.fetch` interceptor (lines 5-70) that automatically proxies JSON responses and runs them through a `sanitizeData` HTML escaping function. However, the sanitizer contains a fatal logic flaw: it explicitly **whitelists and skips escaping** for specific keys:
```javascript
if (key === 'query' || key === 'sql' || key === 'gemini_optimization_advice' || key === 'ddl' || key === 'referenced_schemas') {
    sanitized[key] = data[key]; // PASSED THROUGH RAW
} else {
    sanitized[key] = sanitizeData(data[key]);
}
```
In `renderSlotsProfiler` (line 2068), `row.query` is injected directly into the DOM using `.innerHTML` inside DataTables:
```javascript
`<div title="${row.query}">${row.query}</div>`
```
**Impact:** If a malicious actor executes a BigQuery job containing an XSS payload in the SQL string (e.g., `SELECT "<script>alert('XSS')</script>" AS foo`), the payload is stored by BigQuery, retrieved by the Python backend, ignored by the JavaScript sanitizer, and executed when an admin loads the Slots Profiler page.

**Verified Exploit Surface (narrower than initially claimed):**

| Rendering Path | Via Fetch API | Via Snapshot Import | Why |
|---|---|---|---|
| **Profiler `row.query`** ([L2068](static/app.js)) | 🔴 **XSS** | 🔴 **XSS** | No local escaping; whitelist bypass feeds directly to innerHTML |
| **Profiler `title` attribute** ([L2068](static/app.js)) | 🟠 **Attribute injection** | 🟠 **Attribute injection** | Unescaped `"` in query breaks out of `title="..."` |
| **Linter `row.query_snippet`** ([L3192](static/app.js)) | ✅ Safe | 🔴 **XSS** | `query_snippet` is NOT on the whitelist (sanitized by proxy), but snapshot import bypasses it |
| **AI Doctor `row.query`** ([L3934](static/app.js)) | ✅ Safe | ✅ Safe | Local `escapeHtml()` at line 3934 neutralizes the whitelist bypass |
| **AI Doctor advice** ([L3943](static/app.js)) | ✅ Safe | ✅ Safe | `renderMarkdown()` at L3893-3896 HTML-escapes first (`.replace(/</g, '&lt;')`) |
| **DDL rendering** ([L2774](static/app.js)) | ✅ Safe | ✅ Safe | Uses `.textContent` (not `.innerHTML`) |

> **Note:** The initial review incorrectly claimed the AI Doctor renderer was exploitable. It was not — it applies its own local `escapeHtml()` that neutralizes the whitelist bypass. The actual exploitable sink was the Profiler renderer at line 2068.

**Remediation Applied (v1.1.3):**

```diff
 // Inside renderProfilerQueries():
+// XSS-safe helper: escapes HTML entities for use in innerHTML / title attributes
+const esc = (s) => s == null ? '' : String(s).replaceAll('&','&amp;').replaceAll('<','&lt;')
+    .replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'", '&#39;');
+
 table.row.add([
-    `<div ... title="${row.query}">${row.query}</div>`,
+    `<div ... title="${esc(row.query)}">${esc(row.query)}</div>`,
```

**Fix location:** [`static/app.js` lines 2057-2071](static/app.js) — `esc()` helper added inside `renderProfilerQueries`, applied to both `title` attribute and innerHTML content. The whitelist remains in place (removing it would double-escape fields that have local escaping), but the vulnerable sink is now protected.

---

### 2. [Critical] Local Storage Hydration XSS via Snapshot Import — ✅ FIXED v1.1.3

**Location:** `static/app.js` (`importSnapshot` function, lines 247-300)
**Issue:** The Snapshot Import feature allows users to upload a `.json` cache file. The `importSnapshot` function parses this JSON and writes it directly to `localStorage` without passing it through the global `sanitizeData` proxy.
When the application reloads, it hydrates the UI by pulling data directly from `localStorage` (`JSON.parse(localStorage.getItem(...))`). Since the `window.fetch` proxy only intercepts network responses, **all data loaded from an imported snapshot completely bypasses HTML sanitization.**

**Complete exploit chain verified:**
1. Attacker crafts a malicious snapshot JSON with `bq_profiler_queries` containing `{"query": "<img src=x onerror=alert(document.cookie)>"}`
2. Attacker sets `_meta.app = "bq-finops-optimizer"` and `_meta.schema_version = 1` (trivially bypasses validation at [L258](static/app.js))
3. Admin imports the file → `importSnapshot()` writes raw data to localStorage via `safeSetLocalStorage()` at [L288](static/app.js)
4. Page reloads → `renderProfilerQueries(JSON.parse(cachedQueries))` called at [L4231](static/app.js) — no sanitization
5. `row.query` injected into innerHTML at [L2068](static/app.js) → **XSS fires**

**Impact:** An attacker can achieve persistent XSS across **all dashboard views** that render data from localStorage. The attack requires social engineering (tricking an admin into importing a file), but the technical chain is fully verified. The snapshot export redaction at [L168-169](static/app.js) only sanitizes the **export** side — an attacker crafting from scratch bypasses it entirely.

**Remediation Applied (v1.1.3):**

```diff
+// Sanitize imported data to prevent XSS via snapshot hydration.
+// The global fetch-proxy sanitizer only covers network responses;
+// imported snapshots bypass it entirely, so we must sanitize here.
+const escHtml = (s) => s == null ? '' : String(s).replaceAll('&','&amp;')
+    .replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'", '&#39;');
+function sanitizeImport(data) {
+    if (data === null || data === undefined) return data;
+    if (typeof data === 'string') return escHtml(data);
+    if (Array.isArray(data)) return data.map(sanitizeImport);
+    if (typeof data === 'object') {
+        const out = {};
+        for (const k2 in data) {
+            if (Object.prototype.hasOwnProperty.call(data, k2)) {
+                out[k2] = sanitizeImport(data[k2]);
+            }
+        }
+        return out;
+    }
+    return data;
+}
+
 keys.forEach(k => {
     if (k.startsWith(KEY_PREFIX)) {
-        const ok = safeSetLocalStorage(k, parsed.data[k]);
+        // Parse JSON values, sanitize recursively, re-stringify
+        let raw = parsed.data[k];
+        try {
+            const parsed2 = typeof raw === 'string' ? JSON.parse(raw) : raw;
+            raw = JSON.stringify(sanitizeImport(parsed2));
+        } catch (_e) {
+            if (typeof raw === 'string') raw = escHtml(raw);
+        }
+        const ok = safeSetLocalStorage(k, raw);
```

**Fix location:** [`static/app.js` lines 285-320](static/app.js) — `sanitizeImport()` function added. All imported values are recursively HTML-escaped before being written to localStorage. JSON values are parsed, sanitized, and re-stringified. Plain strings are escaped directly.

---

## ⚠️ Additional Frontend Architecture Findings

### 3. [Medium] Potential Data Loss on Snapshot Import — ⏳ Open

**Location:** `static/app.js` (line 283)
**Issue:** When a user imports a snapshot, the system runs:
```javascript
keysToClear.forEach(k => localStorage.removeItem(k));
```
This wipes out all existing cached results *before* attempting to parse and validate the internal structure of the new `parsed.data` object. If the write loop (line 286-291) fails midway due to a quota/storage size limit, the user's previous data is gone and the new data is only partially imported.
**Remediation:** Implement atomic replacements. Store the existing keys in memory, attempt to write the new data to temporary keys, and only perform the swap if the writes succeed without hitting `QuotaExceededError`.

### 4. [Medium] Missing Chart.js Instance Management in Certain Views — ⏳ Open

**Location:** `static/app.js`
**Issue:** While `jobsScatterChart` and `actualProvisioningChart` explicitly call `.destroy()` before reassignment (preventing memory leaks and overlay glitches), there is a lack of uniform garbage collection for other UI components (e.g., DataTables instances are cleared via `.clear()`, but memory footprint can grow if the underlying DOM elements are completely detached without `table.destroy()`).
**Remediation:** Audit all Chart.js and DataTables instances. Ensure `.destroy()` is called before re-initialization on every rendering cycle.

---

## Fix Summary

| # | Finding | Severity | Status | Fix Version |
|---|---------|----------|--------|-------------|
| 1 | DOM-Based Stored XSS via whitelist bypass (Profiler `row.query`) | 🔴 Critical | ✅ **FIXED** | v1.1.3 |
| 2 | Full Application XSS via snapshot hydration bypass | 🔴 Critical | ✅ **FIXED** | v1.1.3 |
| 3 | Potential data loss on snapshot import (non-atomic replace) | 🟡 Medium | ⏳ Open | — |
| 4 | Missing Chart.js/DataTables `.destroy()` calls | 🟡 Medium | ⏳ Open | — |

### Remaining Architectural Considerations

The following items were **not vulnerabilities** but are worth noting for future development:

- **Whitelist still exists in `sanitizeData`:** The 5-key whitelist (`query`, `sql`, `gemini_optimization_advice`, `ddl`, `referenced_schemas`) was intentionally preserved. Removing it would double-escape fields where local renderers already apply `escapeHtml()` (AI Doctor at [L3934](static/app.js)) or `renderMarkdown()` (at [L3893](static/app.js)). The correct pattern going forward is: keep the whitelist but ensure **every `.innerHTML` sink** that renders whitelisted fields applies local `escapeHtml()` first. New rendering paths for these 5 keys must always include escaping.
- **`localStorage` as a cache layer:** The application uses `localStorage` as both a session cache and a snapshot persistence layer. While convenient, it means the XSS attack surface extends to any code path that writes to `bq_*` keys. The snapshot sanitization fix closes the import vector, but any future direct writes to localStorage from untrusted sources must also be sanitized.
