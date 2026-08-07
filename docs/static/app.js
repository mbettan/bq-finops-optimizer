// BigQuery FinOps Optimizer - Frontend Logic

// Global API response XSS sanitizer
(() => {
    const nativeFetch = window.fetch.bind(window);
    window.fetch = async function (...args) {
        const response = await nativeFetch(...args);
        
        let urlStr = '';
        if (typeof args[0] === 'string') {
            urlStr = args[0];
        } else if (args[0] && typeof args[0] === 'object' && args[0].url) {
            urlStr = args[0].url;
        } else if (args[0] && typeof args[0].toString === 'function') {
            urlStr = args[0].toString();
        }

        if (urlStr && (urlStr.includes('/api/') || urlStr.includes('api/'))) {
            return new Proxy(response, {
                get(target, prop, receiver) {
                    if (prop === 'json') {
                        return async function () {
                            const data = await target.json();
                            return sanitizeData(data);
                        };
                    }
                    // Crucial: Do NOT pass the receiver to Reflect.get for native host getters
                    // (like .ok, .status, .headers) to avoid "Illegal invocation" errors.
                    const val = Reflect.get(target, prop);
                    return typeof val === 'function' ? val.bind(target) : val;
                }
            });
        }
        return response;
    };

    function escapeHtml(str) {
        if (str == null) return '';
        return String(str)
            .replaceAll('&', '&amp;')
            .replaceAll('<', '&lt;')
            .replaceAll('>', '&gt;')
            .replaceAll('"', '&quot;')
            .replaceAll("'", '&#39;');
    }

    function sanitizeData(data) {
        if (data === null || data === undefined) return data;
        if (typeof data === 'string') {
            return escapeHtml(data);
        }
        if (Array.isArray(data)) {
            return data.map(item => sanitizeData(item));
        }
        if (typeof data === 'object') {
            const sanitized = {};
            for (const key in data) {
                if (Object.prototype.hasOwnProperty.call(data, key)) {
                    // Keys that are consumed as raw text (clipboard, textarea,
                    // <pre>.textContent, or POST body) must NOT be HTML-escaped
                    // by the global proxy — doing so corrupts SQL operators,
                    // quoted identifiers, and YAML.  Escape at the render sink.
                    const RAW_KEYS = new Set(['ddl', 'optimized_query', 'query', 'migration_applied_yaml']);
                    if (RAW_KEYS.has(key)) {
                        sanitized[key] = data[key];
                    } else {
                        sanitized[key] = sanitizeData(data[key]);
                    }
                }
            }
            return sanitized;
        }
        return data;
    }

    // Exposed so other code paths that bypass fetch() (e.g. Snapshot import,
    // which loads data from a file straight into localStorage) can apply the
    // same escaping before that data ever reaches the DOM.
    window.sanitizeData = sanitizeData;
})();

/** Escape a value for safe interpolation into an HTML attribute or text node. */
function escapeHtmlAttr(value) {
    return String(value == null ? '' : value)
        .replace(/&/g, '&amp;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}
window.escapeHtmlAttr = escapeHtmlAttr;

// Guards against a corrupted/malicious localStorage value (e.g. from a bad
// Snapshot import) breaking script execution for the rest of this file.
function safeParseJSON(raw, fallback) {
    try {
        return JSON.parse(raw);
    } catch (e) {
        console.warn('[localStorage] Corrupted JSON value, using fallback:', e);
        return fallback;
    }
}

// Extracts a human-readable message from FastAPI error details.
// FastAPI 422 returns {detail: [{loc: [...], msg: "..."}, ...]}, which
// renders as [object Object] if used directly. This handles string,
// array-of-{loc,msg}, and absent cases.
function detailToMessage(detail, fallback = 'Unknown error') {
    if (!detail) return fallback;
    if (typeof detail === 'string') return detail;
    if (Array.isArray(detail)) {
        return detail.map(d => {
            if (typeof d === 'string') return d;
            if (d && d.msg) {
                const loc = Array.isArray(d.loc) ? d.loc.join(' → ') : '';
                return loc ? `${loc}: ${d.msg}` : d.msg;
            }
            return String(d);
        }).join('; ');
    }
    return String(detail);
}

// Clipboard helper with fallback for non-HTTPS contexts (e.g. local dev on 0.0.0.0)
function copyToClipboard(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
        return navigator.clipboard.writeText(text);
    }
    // Fallback: hidden textarea + execCommand
    return new Promise((resolve, reject) => {
        try {
            const ta = document.createElement('textarea');
            ta.value = text;
            ta.style.position = 'fixed';
            ta.style.left = '-9999px';
            document.body.appendChild(ta);
            ta.select();
            document.execCommand('copy');
            document.body.removeChild(ta);
            resolve();
        } catch (err) {
            reject(err);
        }
    });
}

// State
const state = {
    orgProject: localStorage.getItem('bq_org_project') || '',
    adminProject: localStorage.getItem('bq_admin_project') || '',
    region: localStorage.getItem('bq_region') || 'region-us',
    maxBytesBilledGb: parseInt(localStorage.getItem('bq_max_bytes_billed_gb')) || null,
    focusProjects: safeParseJSON(localStorage.getItem('bq_focus_projects') || '[]', []),
    storageData: [],
    slotsData: [],
    slotsChart: null,
    actualProvisioningChart: null,
    jobsScatterChart: null,
    // Logs fetch params (project IDs, region, focus_projects) to the console
    // when true. Keep this off by default — screenshots/screen-shares/HAR
    // exports of the console can leak org topology otherwise.
    debugMode: false
};

// Quota-safe localStorage helper available globally
function safeSetLocalStorage(key, value) {
    try {
        localStorage.setItem(key, value);
        return true;
    } catch (e) {
        console.warn(`[localStorage] Failed to write key "${key}" (possibly quota exceeded):`, e);
        try { localStorage.removeItem(key); } catch (_) {}
        return false;
    }
}

// ---------------------------------------------------------------------------
// Scope classification — derived from backend, not hand-maintained
// ---------------------------------------------------------------------------

/** Populated at startup from GET /api/meta/scope-map.
 *  Keys are real route paths (e.g. '/api/cost-attribution/calculate'),
 *  values are 'focus' or 'org'. */
let FOCUS_SCOPE_MAP = {};

/** Fetch the scope map once at startup so buildPayload and the badge work. */
async function loadScopeMap() {
    try {
        const res = await fetch('/api/meta/scope-map');
        if (res.ok) {
            FOCUS_SCOPE_MAP = await res.json();
        } else {
            console.error('[ScopeMap] Non-OK response:', res.status);
        }
    } catch (e) {
        console.error('[ScopeMap] Failed to load — falling back to pass-through', e);
    }
}

/**
 * Strip focus_projects from the payload for org-only endpoints.
 * Warns on unmapped endpoints so missing entries surface during dev.
 */
function buildPayload(endpoint, basePayload) {
    const scope = FOCUS_SCOPE_MAP[endpoint];
    if (!scope) {
        // Default to 'org' (strip focus_projects) for unmapped endpoints.
        // When the scope map fails to load (502, timeout), every endpoint is
        // unmapped. Passing focus_projects to endpoints with extra='forbid'
        // causes a hard 422. Defaulting to 'org' is the safer fallback.
        console.warn(`[ScopeMap] Unmapped endpoint: ${endpoint} — defaulting to org scope`);
    }
    if (scope !== 'focus') {
        const { focus_projects, ...rest } = basePayload;
        return rest;
    }
    return basePayload;
}

/** Maps navigation view names to their primary POST endpoint for badge display. */
const VIEW_TO_ENDPOINT = {
    'storage': '/api/storage/analyze',
    'schema-optimizer': '/api/storage/static_audit',
    'jobs': '/api/jobs/analyze',
    'slots': '/api/slots/analyze',
    'fluid-scaling': '/api/fluid-scaling/estimate',
    'slots-simulator': '/api/slots/simulate',
    'cost-attribution': '/api/cost-attribution/calculate',
    'profiler': '/api/slots/profiler',
    'users': '/api/users/top_spenders',
    'hbo': '/api/hbo/analyze',
    'storage-hygiene': '/api/storage/hygiene',
    'antipatterns': '/api/antipatterns/dml',
    'performance-insights': '/api/hbo/performance_insights',
    'bi-optimizer': '/api/bi/analyze',
    'ai-reviewer': '/api/ai/analyze',
};

// Update scope badge based on active view and focusProjects state
function updateScopeBadge(viewName) {
    const container = document.getElementById('scope-badge-container');
    const badge = document.getElementById('scope-badge');
    if (!container || !badge) return;

    const endpoint = VIEW_TO_ENDPOINT[viewName];
    const scope = endpoint ? FOCUS_SCOPE_MAP[endpoint] : undefined;
    const projects = state.focusProjects || [];

    container.style.display = '';
    if (scope === 'org') {
        badge.textContent = '🌐 Organization-wide';
        badge.style.color = '#9ca3af';
    } else if (projects.length > 0) {
        badge.textContent = `🎯 Focused: ${projects.length} project${projects.length > 1 ? 's' : ''}`;
        badge.style.color = 'var(--accent-primary)';
    } else {
        badge.textContent = '🌐 Organization-wide';
        badge.style.color = '#9ca3af';
    }
}


/**
 * Clear stale module data instantly when a user clicks a fetch button.
 * Removes the specified localStorage keys and empties the tbody of each table selector.
 * @param {string[]} keys - localStorage keys to remove
 * @param {string[]} tableSelectors - CSS selectors for table elements whose tbody should be cleared
 */
function clearModuleCache(keys = [], tableSelectors = []) {
    keys.forEach(k => localStorage.removeItem(k));
    tableSelectors.forEach(sel => {
        const tbody = document.querySelector(`${sel} tbody`);
        if (tbody) tbody.innerHTML = '';
    });
}

/* ============================================================
   SNAPSHOT EXPORT / IMPORT
   Bundles all bq_* localStorage keys into a single shareable JSON.
   ============================================================ */
const Snapshot = (() => {

  const SCHEMA_VERSION = 1;
  // Only export keys we own. Never sweep all of localStorage (avoids leaking
  // unrelated keys and keeps the file deterministic).
  const KEY_PREFIX = 'bq_';

  function collectKeys(redact) {
    const out = {};
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key && key.startsWith(KEY_PREFIX)) {
        const val = localStorage.getItem(key);
        if (redact) {
          out[key] = redactValue(val);
        } else {
          out[key] = val;
        }
      }
    }
    return out;
  }

  // Matching on key names alone is not enough: an address can sit under any
  // key. The batch priority scan reports a workload under `workload_name`,
  // which is the operator's email whenever the workload carries no lineage
  // label — a key-name rule would export those verbatim from a *redacted*
  // snapshot. This pass only ever removes more, never less.
  const EMAIL_RE = /[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/g;
  const scrubString = (s) => s.replace(EMAIL_RE, 'redacted@example.com');

  function redactValue(raw) {
    try {
      const obj = JSON.parse(raw);
      const scrub = (o) => {
        if (!o) return;
        if (Array.isArray(o)) {
          o.forEach((item, i) => {
            if (typeof item === 'string') o[i] = scrubString(item);
            else scrub(item);
          });
        } else if (typeof o === 'object') {
          for (const k of Object.keys(o)) {
            const val = o[k];
            if (val === null || val === undefined) continue;
            if (/email/i.test(k) && typeof val === 'string') {
              o[k] = 'redacted@example.com';
            } else if (/^query$|^query_text$|^query_snippet$/i.test(k) && typeof val === 'string') {
              o[k] = '-- [redacted query]';
            } else if (typeof val === 'string') {
              o[k] = scrubString(val);
            } else if (typeof val === 'object') {
              scrub(val);
            }
          }
        }
      };
      scrub(obj);
      return JSON.stringify(obj);
    } catch {
      if (typeof raw === 'string' && raw.includes('@') && !raw.includes(' ')) {
        return 'redacted@example.com';
      }
      return raw;
    }
  }

  function buildSnapshot(redact) {
    return {
      _meta: {
        schema_version: SCHEMA_VERSION,
        app: 'bq-finops-optimizer',
        exported_at: new Date().toISOString(),
        org_project: localStorage.getItem('bq_org_project') || null,
        admin_project: localStorage.getItem('bq_admin_project') || null,
        region: localStorage.getItem('bq_region') || null,
        user_agent: navigator.userAgent,
        redacted: !!redact
      },
      data: collectKeys(redact),
    };
  }

  function countEmails(data) {
    const emails = new Set();
    const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
    for (const key of Object.keys(data)) {
      const val = data[key];
      if (typeof val === 'string') {
        const matches = val.match(emailRegex);
        if (matches) {
          matches.forEach(m => emails.add(m.toLowerCase()));
        }
      }
    }
    return emails.size;
  }

  function exportSnapshot() {
    const redactChecked = document.getElementById('chk-redact-snapshot')?.checked;
    const snapshot = buildSnapshot(redactChecked);
    const keyCount = Object.keys(snapshot.data).length;

    if (keyCount === 0) {
      showNotification('No analysis data to export yet. Run an analysis first.', 'warning');
      return;
    }

    let warningMsg;
    if (redactChecked) {
      warningMsg = `This snapshot contains ${keyCount} cached result set(s) with project IDs and reservation names from your BigQuery org. User emails and query SQL have been REDACTED.\n\nDownload now?`;
    } else {
      const emailCount = countEmails(snapshot.data);
      const emailText = emailCount > 0 ? `${emailCount} real user email(s)` : 'real user emails';
      warningMsg = `⚠️ WARNING: This snapshot contains ${keyCount} cached result set(s) INCLUDING ${emailText}, query SQL text, and reservation names.\n\nOnly share it with people authorized to see this data.\n\nDownload now?`;
    }

    const proceed = confirm(warningMsg);
    if (!proceed) return;

    const blob = new Blob([JSON.stringify(snapshot, null, 2)], { type: 'application/json' });
    const ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const proj = (snapshot._meta.org_project || 'snapshot').replace(/[^a-zA-Z0-9_-]/g, '');
    const filename = `finops-snapshot_${proj}_${ts}${redactChecked ? '_redacted' : ''}.json`;
    triggerDownload(blob, filename);
    showNotification(`Exported ${keyCount} result set(s).`, 'success');
  }

  // Data imported from a snapshot file bypasses the fetch() response
  // sanitizer entirely (it's loaded straight from a file, never through
  // window.fetch), so it must be re-escaped here before it ever reaches
  // localStorage / the DOM — otherwise a crafted snapshot shared via the
  // app's own "share with your team" export/import feature is a stored XSS
  // vector requiring no BigQuery access at all. Falls back to the raw
  // string for plain (non-JSON) scalar values, e.g. a bare project id.
  function sanitizeImportedValue(raw) {
    if (typeof raw !== 'string') return raw;
    try {
      const parsed = JSON.parse(raw);
      const sanitize = window.sanitizeData || (v => v);
      return JSON.stringify(sanitize(parsed));
    } catch {
      // Bare strings (e.g. project IDs from scalar settings keys)
      // must also be escaped — they bypass the JSON parse above and were
      // returned raw, which is the XSS delivery path for H1.
      const escHtml = (s) => s == null ? '' : String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'", '&#39;');
      return escHtml(raw);
    }
  }

  function importSnapshot(file) {
    const reader = new FileReader();
    reader.onload = (e) => {
      let parsed;
      try {
        parsed = JSON.parse(e.target.result);
      } catch (err) {
        showNotification('Invalid file: not valid JSON.', 'error');
        return;
      }

      if (!parsed || parsed._meta?.app !== 'bq-finops-optimizer' || !parsed.data) {
        showNotification('This does not look like a FinOps snapshot file.', 'error');
        return;
      }
      if (parsed._meta.schema_version > SCHEMA_VERSION) {
        showNotification(
          `Snapshot was made with a newer app version (schema ${parsed._meta.schema_version}). Some data may not load correctly.`, 'warning'
        );
      }

      const keys = Object.keys(parsed.data);
      const isRedacted = parsed._meta.redacted ? ' (Redacted info)' : '';
      const proceed = confirm(
        `Import ${keys.length} result set(s) from snapshot${isRedacted} exported on ${parsed._meta.exported_at || 'unknown date'}?\n\n⚠️ This will OVERWRITE your current cached results and settings.`
      );
      if (!proceed) return;

      // ⚠️ quota safety: clear existing bq_* keys first for a clean replace
      const keysToClear = [];
      for (let i = 0; i < localStorage.length; i++) {
        const k = localStorage.key(i);
        if (k && k.startsWith(KEY_PREFIX)) {
          keysToClear.push(k);
        }
      }
      keysToClear.forEach(k => localStorage.removeItem(k));

      // Sanitize imported data to prevent XSS via snapshot hydration.
      // The global fetch-proxy sanitizer only covers network responses;
      // imported snapshots bypass it entirely, so we must sanitize here.
      const escHtml = (s) => s == null ? '' : String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'", '&#39;');
      function sanitizeImport(data) {
          if (data === null || data === undefined) return data;
          if (typeof data === 'string') return escHtml(data);
          if (Array.isArray(data)) return data.map(sanitizeImport);
          if (typeof data === 'object') {
              const out = {};
              for (const k2 in data) {
                  if (Object.prototype.hasOwnProperty.call(data, k2)) {
                      out[k2] = sanitizeImport(data[k2]);
                  }
              }
              return out;
          }
          return data;
      }

      let written = 0;
      keys.forEach(k => {
        if (k.startsWith(KEY_PREFIX)) {
          const ok = safeSetLocalStorage(k, sanitizeImportedValue(parsed.data[k]));
          if (ok) written++;
        }
      });

      showNotification(
        `Imported ${written} result set(s). Reloading to render…`, 'success'
      );
      setTimeout(() => window.location.reload(), 800);
    };
    reader.onerror = () => showNotification('Failed to read file.', 'error');
    reader.readAsText(file);
  }

  function triggerDownload(blob, filename) {
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  return { exportSnapshot, importSnapshot };
})();

document.addEventListener('DOMContentLoaded', () => {

    const debug_log = (...args) => {
        if (state.debugMode) {
            console.log("[DEBUG]", ...args);
        }
    };

    // Global button click logger
    document.addEventListener('click', (e) => {
        if (e.target && e.target.tagName === 'BUTTON') {
            debug_log("Button clicked:", e.target.id || e.target.innerText || e.target.className);
        }
    });

    // Global copy button handler
    document.addEventListener('click', (e) => {
        const copyBtn = e.target.closest('.copy-job-id-btn');
        if (copyBtn) {
            const jobId = copyBtn.getAttribute('data-job-id');
            if (jobId) {
                copyToClipboard(jobId).then(() => {
                    showNotification('Job ID copied to clipboard', 'success');
                }).catch(err => {
                    console.error('Failed to copy Job ID', err);
                    showNotification('Failed to copy Job ID', 'error');
                });
            }
        }
    });

    // DOM Elements
    const elements = {

        
        // Top Bar
        currentProject: document.getElementById('current-project'),
        currentAdminProject: document.getElementById('current-admin-project'),
        currentRegion: document.getElementById('current-region'),
        
        // Settings Form
        cfgOrgProject: document.getElementById('cfg-org-project'),
        cfgAdminProject: document.getElementById('cfg-admin-project'),
        cfgRegion: document.getElementById('cfg-region'),
        saveSettingsBtn: document.getElementById('save-settings-btn'),
        cfgMaxBytesBilled: document.getElementById('cfg-max-bytes-billed'),
        cfgFocusProjects: document.getElementById('cfg-focus-projects'),
        
        // Storage Form & Elements
        btnAnalyzeStorage: document.getElementById('analyze-storage-btn'),
        stActLog: document.getElementById('st-act-log'),
        stLtLog: document.getElementById('st-lt-log'),
        stActPhy: document.getElementById('st-act-phy'),
        stLtPhy: document.getElementById('st-lt-phy'),
        stTtRescale: document.getElementById('st-tt-rescale'),
        stTtHours: document.getElementById('st-tt-hours'),
        stMinSave: document.getElementById('st-min-save'),
        stMinSavePct: document.getElementById('st-min-save-pct'),
        stTotalSavings: document.getElementById('st-total-savings'),
        stDatasetCount: document.getElementById('st-dataset-count'),
        stOppCount: document.getElementById('st-opp-count'),
        

        
        // Slots Form & Elements
        btnAnalyzeSlots: document.getElementById('analyze-slots-btn'),
        slLookback: document.getElementById('sl-lookback'),
        slWindow: document.getElementById('sl-window'),
        slResolution: document.getElementById('sl-resolution'),
        slPercentile: document.getElementById('sl-percentile'),
        
        notificationContainer: document.getElementById('notification-container'),
        
        // Cost Attribution

        
        // Workload Profiler

        btnAnalyzeProfiler: document.getElementById('analyze-profiler-btn'),
        btnCalculateCostAttribution: document.getElementById('calculate-cost-attribution-btn'),
        cbWasteRule: document.getElementById('cb-waste-rule'),
        cbCentralProject: document.getElementById('cb-central-project'),
        cbBorrowingRule: document.getElementById('cb-borrowing-rule'),
        cbMonthStart: document.getElementById('cb-month-start'),
        cbMonthEnd: document.getElementById('cb-month-end'),
        cbReservationsContainer: document.getElementById('cb-reservations-container'),
        cbAddReservationBtn: document.getElementById('cb-add-reservation-btn'),
        
        // Top Spenders

        btnAnalyzeUsers: document.getElementById('analyze-users-btn'),
        
        // HBO Analyzer

        btnAnalyzeHbo: document.getElementById('analyze-hbo-btn'),
        hboStatusPanel: document.getElementById('hbo-status-panel'),
        hboStatusList: document.getElementById('hbo-status-tbody'),
        hboStatusSummary: document.getElementById('hbo-status-summary'),
        hboStatusPagination: document.getElementById('hbo-status-pagination'),
        
        // Storage Hygiene

        btnAnalyzeHygiene: document.getElementById('analyze-hygiene-btn'),
        // Anti-Patterns
        btnAnalyzeLinter: document.getElementById('analyze-linter-btn'),
        btnAnalyzeDml: document.getElementById('analyze-dml-btn'),
        btnAnalyzeMv: document.getElementById('analyze-mv-btn'),
        btnAnalyzeSkew: document.getElementById('analyze-skew-btn'),
        btnAnalyzeBatch: document.getElementById('analyze-batch-btn'),
        btnAnalyzeExpiration: document.getElementById('analyze-expiration-btn'),
        btnAnalyzeFilter: document.getElementById('analyze-filter-btn'),
        btnAnalyzeMvRejections: document.getElementById('analyze-mv-rejections-btn'),
        btnAnalyzeWarnings: document.getElementById('analyze-warnings-btn'),
        
        // BI Optimizer

        btnAnalyzeBi: document.getElementById('analyze-bi-btn'),
        
        // AI Doctor

        btnRunAiAnalysis: document.getElementById('run-ai-analysis-btn'),
        aiLimit: document.getElementById('ai-limit'),
        aiDiscoveryStrategy: document.getElementById('ai-discovery-strategy'),
        aiLookback: document.getElementById('ai-lookback'),
        aiModel: document.getElementById('ai-model')
    };

    // Custom Filter for DataTables
    $.fn.dataTable.ext.search.push(
        function( settings, data, dataIndex ) {
            if (settings.nTable.id !== 'top-jobs-table') {
                return true;
            }
            const filterValue = $('#profile-filter').val();
            if (!filterValue) return true;
            
            const profile = data[4] || ''; // Column 4 is Profile
            return profile.includes(filterValue);
        }
    );

    // Initialize UI from state
    const initUI = () => {
        elements.cfgOrgProject.value = state.orgProject;
        if (elements.cfgAdminProject) elements.cfgAdminProject.value = state.adminProject;
        elements.cfgRegion.value = state.region;
        if (elements.cfgMaxBytesBilled) elements.cfgMaxBytesBilled.value = state.maxBytesBilledGb || '';
        if (elements.cfgFocusProjects) elements.cfgFocusProjects.value = (state.focusProjects || []).join(', ');
        updateScopeBadge(Router.getCurrentViewId());
        
        elements.currentProject.textContent = state.orgProject || 'Not Set';
        if (elements.currentAdminProject) elements.currentAdminProject.textContent = state.adminProject || 'Not Set';
        elements.currentRegion.textContent = state.region;

        // Set default dates for cost attribution (previous month)
        const now = new Date();
        const prevMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
        const prevMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0);
        
        const formatDate = (date) => {
            const year = date.getFullYear();
            const month = String(date.getMonth() + 1).padStart(2, '0');
            const day = String(date.getDate()).padStart(2, '0');
            return `${year}-${month}-${day}`;
        };
        
        if (elements.cbMonthStart) elements.cbMonthStart.value = formatDate(prevMonthStart);
        if (elements.cbMonthEnd) elements.cbMonthEnd.value = formatDate(prevMonthEnd);

        if (!state.orgProject) {
            showNotification('Please configure GCP Settings first.', 'warning');
            Router.navigate('settings');
        }
    };




    // Save Settings
    elements.saveSettingsBtn.addEventListener('click', () => {
        // GCP project ID regex: starts with lowercase letter, 6-30 chars, [a-z0-9-]
        const PROJECT_ID_RE = /^[a-z][a-z0-9\-]{5,29}$/;
        const validationErrors = [];

        // --- Validate into local variables FIRST, return before
        // touching state or localStorage to prevent half-mutated scope. ---
        const newOrg = elements.cfgOrgProject.value.trim();
        elements.cfgOrgProject.value = newOrg;
        if (newOrg && !PROJECT_ID_RE.test(newOrg)) {
            validationErrors.push(`Invalid Organization Project ID "${newOrg}". Must be 6-30 lowercase chars, starting with a letter (a-z, 0-9, hyphens only).`);
        }

        let newAdmin = '';
        if (elements.cfgAdminProject) {
            newAdmin = elements.cfgAdminProject.value.trim();
            elements.cfgAdminProject.value = newAdmin;
            if (newAdmin && !PROJECT_ID_RE.test(newAdmin)) {
                validationErrors.push(`Invalid Admin Project ID "${newAdmin}". Must be 6-30 lowercase chars, starting with a letter.`);
            }
        }

        const newRegion = elements.cfgRegion.value;

        let newMaxBytes = null;
        if (elements.cfgMaxBytesBilled) {
            const val = parseInt(elements.cfgMaxBytesBilled.value);
            newMaxBytes = (val && val > 0) ? val : null;
        }

        let newFocus = [];
        if (elements.cfgFocusProjects) {
            const raw = elements.cfgFocusProjects.value;
            newFocus = raw.split(',').map(s => s.trim()).filter(Boolean);
            const invalidProjects = newFocus.filter(p => !PROJECT_ID_RE.test(p));
            if (invalidProjects.length > 0) {
                validationErrors.push(`Invalid Focus Project ID(s): ${invalidProjects.map(p => `"${p}"`).join(', ')}. Each must be 6-30 lowercase chars, starting with a letter.`);
            }
        }

        // --- Abort on validation errors (before any state mutation) ---
        if (validationErrors.length > 0) {
            showNotification(validationErrors.join('\n'), 'error');
            return;
        }

        // --- Commit validated values to state and localStorage ---
        state.orgProject = newOrg;
        state.region = newRegion;
        if (elements.cfgAdminProject) {
            state.adminProject = newAdmin;
            localStorage.setItem('bq_admin_project', newAdmin);
        }
        state.maxBytesBilledGb = newMaxBytes;
        if (newMaxBytes) {
            localStorage.setItem('bq_max_bytes_billed_gb', newMaxBytes);
        } else {
            localStorage.removeItem('bq_max_bytes_billed_gb');
        }
        state.focusProjects = newFocus;
        if (elements.cfgFocusProjects) {
            elements.cfgFocusProjects.value = newFocus.join(', ');
        }
        if (newFocus.length > 0) {
            safeSetLocalStorage('bq_focus_projects', JSON.stringify(newFocus));
        } else {
            localStorage.removeItem('bq_focus_projects');
        }

        localStorage.setItem('bq_org_project', state.orgProject);
        localStorage.setItem('bq_region', state.region);

        // Flush ALL scope-dependent caches. Use an allow-list of
        // scope-independent keys rather than a fragile deny-list.
        const SCOPE_INDEPENDENT = new Set([
            'bq_org_project', 'bq_admin_project', 'bq_region',
            'bq_max_bytes_billed_gb', 'bq_focus_projects',
            'bq_version_dismissed',
        ]);
        const allKeys = Object.keys(localStorage);
        allKeys.filter(k => k.startsWith('bq_') && !SCOPE_INDEPENDENT.has(k))
            .forEach(k => localStorage.removeItem(k));

        elements.currentProject.textContent = state.orgProject || 'Not Set';
        if (elements.currentAdminProject) elements.currentAdminProject.textContent = state.adminProject || 'Not Set';
        elements.currentRegion.textContent = state.region;
        updateScopeBadge(Router.getCurrentViewId());

        showNotification('Settings saved. Cache cleared.', 'success');
        Router.navigate('storage');
    });

    // Event Listeners for Recommendation Cards
    document.querySelectorAll('.recommendation-card').forEach(card => {
        card.addEventListener('click', (e) => {
            const tier = card.getAttribute('data-tier');
            selectTier(tier);
        });
    });

    // Copy Editions DDL
    if (elements.copyEdDdlBtn) {
        elements.copyEdDdlBtn.addEventListener('click', () => {
            if (elements.edDdlOutput && elements.edDdlOutput.value) {
                copyToClipboard(elements.edDdlOutput.value).then(() => {
                    showNotification('DDL copied to clipboard!', 'success');
                }).catch(err => {
                    logger_error(err);
                    showNotification('Failed to copy DDL.', 'error');
                });
            }
        });
    }

    const copyOrgDdlBtn = document.getElementById('copy-org-ddl-btn');
    if (copyOrgDdlBtn) {
        copyOrgDdlBtn.addEventListener('click', () => {
            const output = document.getElementById('org-ddl-output');
            if (output && output.value) {
                copyToClipboard(output.value).then(() => {
                    showNotification('Organization DDL copied to clipboard!', 'success');
                }).catch(err => {
                    console.error(err);
                    showNotification('Failed to copy DDL.', 'error');
                });
            }
        });
    }

    // Analyze Storage
    elements.btnAnalyzeStorage.addEventListener('click', async () => {
        if (!state.orgProject) {
            showNotification('Please configure settings first.', 'error');
            Router.navigate('settings');
            return;
        }

        setLoading(elements.btnAnalyzeStorage, true);
        clearModuleCache(['bq_storage_results', 'bq_active_assist_results', 'bq_static_audit_results'], ['#storage-results-table', '#active-assist-table', '#static-audit-table']);

        const ttDays = parseFloat(document.getElementById('st-tt-days').value) || 7;
        const params = {
            active_logical_price: parseFloat(elements.stActLog.value),
            long_term_logical_price: parseFloat(elements.stLtLog.value),
            active_physical_price: parseFloat(elements.stActPhy.value),
            long_term_physical_price: parseFloat(elements.stLtPhy.value),
            time_travel_rescale: ttDays / 7.0,
            time_travel_hours: ttDays * 24,
            min_monthly_saving: parseFloat(elements.stMinSave.value),
            min_monthly_saving_pct: parseFloat(elements.stMinSavePct.value),
            region: state.region,
            focus_projects: state.focusProjects,
            org_project_id: state.orgProject,
            max_bytes_billed_gb: state.maxBytesBilledGb
        };

        try {
            debug_log("Fetching storage analysis with params:", params);
            const response = await fetch('/api/storage/analyze', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(buildPayload('/api/storage/analyze', params))
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Failed to analyze storage');
            }

            const responseData = await response.json();
            state.storageData = responseData.datasets;
            renderStorageResults(responseData);
            renderOrgStatus(responseData.org_status);
            safeSetLocalStorage('bq_storage_results', JSON.stringify(responseData));
            
            // Background sync Active Assist recommendations
            fetchActiveAssistRecommendations(false);
            fetchStaticAuditResults(false);
            
            showNotification('Storage analysis completed.', 'success');
        } catch (error) {
            logger_error(error);
            showNotification(error.message, 'error');
        } finally {
            setLoading(elements.btnAnalyzeStorage, false);
        }
    });

    // Sync Active Assist Recommendations Button
    const btnSyncActiveAssist = document.getElementById('run-active-assist-btn');
    if (btnSyncActiveAssist) {
        btnSyncActiveAssist.addEventListener('click', () => {
            if (!state.orgProject) {
                showNotification('Please configure settings first.', 'error');
                Router.navigate('settings');
                return;
            }
            fetchActiveAssistRecommendations(true);
        });
    }

    // Static Schema Auditor Button
    const btnSyncStaticAudit = document.getElementById('run-static-audit-btn');
    if (btnSyncStaticAudit) {
        btnSyncStaticAudit.addEventListener('click', () => {
            if (!state.orgProject) {
                showNotification('Please configure settings first.', 'error');
                Router.navigate('settings');
                return;
            }
            fetchStaticAuditResults(true);
        });
    }

    // Analyze Jobs
    const btnAnalyzeJobs = document.getElementById('analyze-jobs-btn');
    if (btnAnalyzeJobs) {
        btnAnalyzeJobs.addEventListener('click', async () => {
            if (!state.orgProject) {
                showNotification('Please configure settings first.', 'error');
                Router.navigate('settings');
                return;
            }

            setLoading(btnAnalyzeJobs, true);
            clearModuleCache(['bq_job_results'], ['#job-summary-table', '#top-jobs-table']);

            const params = {
                on_demand_rate_per_tb: parseFloat(document.getElementById('jb-od-rate').value),
                edition_slot_hr_rate: parseFloat(document.getElementById('jb-ed-rate').value),
                slot_step_size: parseInt(document.getElementById('jb-slot-step').value),
                lookback_days: parseInt(document.getElementById('jb-lookback').value),
                region: state.region,
                focus_projects: state.focusProjects,
                org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
                min_bytes_billed: parseInt(document.getElementById('jb-min-size').value) * 1024 * 1024,
                limit_jobs: parseInt(document.getElementById('jb-limit').value),
                fluid_scaling: document.getElementById('jb-fluid-scaling').checked
            };

            try {
                debug_log("Fetching job analysis with params:", params);
                const response = await fetch('/api/jobs/analyze', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(buildPayload('/api/jobs/analyze', params))
                });

                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(errorData.detail || 'Failed to analyze jobs');
                }

                const responseData = await response.json();
                renderJobResults(responseData);
                safeSetLocalStorage('bq_job_results', JSON.stringify(responseData));
                showNotification('Job analysis completed.', 'success');
            } catch (error) {
                console.error("Job Analysis Error:", error);
                showNotification(error.message, 'error');
            } finally {
                setLoading(btnAnalyzeJobs, false);
            }
        });
    }

    const hideOptimizedToggle = document.getElementById('hide-optimized-jobs');
    if (hideOptimizedToggle) {
        hideOptimizedToggle.addEventListener('change', () => {
            const cached = localStorage.getItem('bq_job_results');
            if (cached) {
                renderJobResults(JSON.parse(cached));
            }
        });
    }

    const renderJobResults = (data) => {
        const summaryTbody = document.querySelector('#job-summary-table tbody');
        const jobsTbody = document.querySelector('#top-jobs-table tbody');
        
        if (summaryTbody) summaryTbody.innerHTML = '';
        if (jobsTbody) jobsTbody.innerHTML = '';

        // Render Scatter Plot
        const scatterCtx = document.getElementById('jobs-scatter-chart');
        if (scatterCtx && data.top_jobs) {
            const scatterData = data.top_jobs.map(job => ({
                x: job.on_demand_cost,
                y: job.editions_cost,
                label: job.job_id
            }));

            const maxCost = Math.max(...data.top_jobs.flatMap(j => [j.on_demand_cost, j.editions_cost])) || 10;

            if (state.jobsScatterChart) {
                state.jobsScatterChart.destroy();
            }

            state.jobsScatterChart = new Chart(scatterCtx, {
                type: 'scatter',
                data: {
                    datasets: [
                        {
                            label: 'Queries',
                            data: scatterData,
                            backgroundColor: 'rgba(56, 189, 248, 0.6)',
                            borderColor: '#38bdf8',
                            pointRadius: 5
                        },
                        {
                            label: 'Break-even Line',
                            data: [{x: 0, y: 0}, {x: maxCost, y: maxCost}],
                            type: 'line',
                            borderColor: 'rgba(255, 255, 255, 0.3)',
                            borderDash: [5, 5],
                            fill: false,
                            pointRadius: 0
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: {
                            title: { display: true, text: 'Projected On-Demand Cost ($)', color: '#f8fafc' },
                            type: 'linear',
                            position: 'bottom',
                            ticks: { color: '#f8fafc' },
                            grid: { color: 'rgba(255,255,255,0.05)' }
                        },
                        y: {
                            title: { display: true, text: 'Projected Editions Cost ($)', color: '#f8fafc' },
                            ticks: { color: '#f8fafc' },
                            grid: { color: 'rgba(255,255,255,0.05)' }
                        }
                    },
                    plugins: {
                        legend: {
                            labels: { color: '#f8fafc' }
                        },
                        tooltip: {
                            callbacks: {
                                label: function(context) {
                                    const job = context.raw;
                                    if (context.dataset.label === 'Break-even Line') return 'Break-even';
                                    return `Job: ${job.label.substring(0,8)}... (OD: $${job.x.toFixed(2)}, ED: $${job.y.toFixed(2)})`;
                                }
                            }
                        }
                    }
                }
            });
        }

        // Render Project Summaries
        if (data.project_summaries) {
            let totalOd = 0;
            let totalEd = 0;
            let totalSavings = 0;

            data.project_summaries.forEach(row => {
                totalOd += row.total_on_demand_cost || 0;
                totalEd += row.total_editions_cost || 0;
                totalSavings += row.reservation_savings || 0;

                const tr = document.createElement('tr');
                tr.innerHTML = `
                    <td>${renderProjectLink(row.project_id)}</td>
                    <td>${formatCurrency(row.total_on_demand_cost)}</td>
                    <td>${formatCurrency(row.total_editions_cost)}</td>
                    <td>${formatCurrency(row.editions_error_tax)}</td>
                    <td><strong style="color: ${row.reservation_savings > 0 ? '#4ade80' : '#f87171'}">${formatCurrency(row.reservation_savings)}</strong></td>
                `;
                summaryTbody.appendChild(tr);
            });

            // Update KPI tiles
            const odTile = document.getElementById('jb-total-od');
            const edTile = document.getElementById('jb-total-ed');
            const savTile = document.getElementById('jb-total-savings');

            if (odTile) odTile.textContent = formatCurrency(totalOd);
            if (edTile) edTile.textContent = formatCurrency(totalEd);
            if (savTile) savTile.textContent = formatCurrency(totalSavings);

            // Update label to reflect Fluid Scaling if active
            const isFluid = document.getElementById('jb-fluid-scaling')?.checked;
            const savLabel = document.querySelector('#view-jobs .kpi-card:nth-child(3) .kpi-label');
            if (savLabel) {
                savLabel.textContent = isFluid ? "Potential Savings (Fluid Scaling Enabled)" : "Potential Savings (Legacy Mode)";
            }
        }

        // Render Top Jobs
        if (data.top_jobs) {
            const hideOptimized = document.getElementById('hide-optimized-jobs')?.checked;
            data.top_jobs.forEach(row => {
                const betterOn = row.on_demand_cost <= row.editions_cost ? 'On-Demand' : 'Editions';
                const currentModel = row.current_model || 'On-Demand';
                
                if (hideOptimized && currentModel === betterOn) {
                    return; // Skip rendering already optimized jobs
                }
                
                const tr = document.createElement('tr');
                const betterColor = betterOn === 'On-Demand' ? '#38bdf8' : '#a855f7';
                
                const maxCost = Math.max(row.on_demand_cost, row.editions_cost) || 1;
                const savingsPct = (row.waste_savings > 0 ? row.waste_savings / maxCost : 0) * 100;
                
                // Color for category badge
                let categoryColor = '#94a3b8'; // gray
                let categoryBg = 'rgba(148, 163, 184, 0.15)';
                
                if (row.category.includes('Reservation')) {
                    categoryColor = '#4ade80'; // green
                    categoryBg = 'rgba(74, 222, 128, 0.15)';
                } else if (row.category.includes('On-Demand')) {
                    categoryColor = '#facc15'; // yellow
                    categoryBg = 'rgba(250, 204, 21, 0.15)';
                }
                
                let warningHtml = '';
                if (row.performance_warning) {
                    warningHtml = `<span class="badge" style="background: rgba(245, 158, 11, 0.12); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.25); margin-top: 0.35rem; display: block; white-space: normal; text-align: left; font-size: 0.75rem; line-height: 1.2; padding: 0.35rem 0.5rem;"><i class="fa-solid fa-triangle-exclamation" style="margin-right: 5px; color: #fbbf24;"></i>${row.performance_warning}</span>`;
                }

                const currentColor = currentModel === 'On-Demand' ? '#38bdf8' : '#a855f7';
                
                tr.innerHTML = `
                    <td>${renderProjectLink(row.project_id)}</td>
                    <td style="font-family: monospace; font-size: 0.85rem;">${row.job_id.substring(0, 12)}...</td>
                    <td><span class="badge" style="background: ${currentModel === 'On-Demand' ? 'rgba(56, 189, 248, 0.15)' : 'rgba(168, 85, 247, 0.15)'}; color: ${currentColor}; font-weight: 600;">${currentModel}</span></td>
                    <td><span class="badge" style="background: ${betterOn === 'On-Demand' ? 'rgba(56, 189, 248, 0.15)' : 'rgba(168, 85, 247, 0.15)'}; color: ${betterColor}; font-weight: 600;">${betterOn}</span></td>
                    <td><span class="badge" style="background: ${categoryBg}; color: ${categoryColor};">${row.category}</span>${warningHtml}</td>
                    <td><span style="color: ${row.waste_savings > 0 ? '#f8fafc' : '#94a3b8'}">${formatCurrency(row.waste_savings)}</span></td>
                    <td>${savingsPct.toFixed(2)}%</td>
                    <td>
                        <button class="btn-action copy-job-btn" data-id="${row.job_id}">Copy ID</button>
                    </td>
                `;
                jobsTbody.appendChild(tr);
            });
        }

        document.querySelectorAll('.copy-job-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const jobId = e.target.getAttribute('data-id');
                if (jobId) {
                    copyToClipboard(jobId).then(() => {
                        showNotification('Job ID copied!', 'success');
                    }).catch(err => {
                        console.error(err);
                        showNotification('Failed to copy ID.', 'error');
                    });
                }
            });
        });

        // Initialize DataTables if not already init
        if ($.fn.DataTable.isDataTable('#job-summary-table')) {
            $('#job-summary-table').DataTable().destroy();
        }
        $('#job-summary-table').DataTable({ pageLength: 5, order: [[4, 'desc']], responsive: true });

        if ($.fn.DataTable.isDataTable('#top-jobs-table')) {
            $('#top-jobs-table').DataTable().destroy();
        }
        const table = $('#top-jobs-table').DataTable({ pageLength: 10, order: [[5, 'desc']], responsive: true });
        
        // Profile filter
        const filterSelect = document.getElementById('profile-filter');
        if (filterSelect) {
            // Apply current filter
            table.draw();
            
            // Add listener
            $('#profile-filter').off('change').on('change', function() {
                table.draw();
            });

            // Apply filter on change is handled by custom filter triggering draw()
        }
    };

    // Render Storage Results
    const renderStorageResults = (data) => {
        const datasets = data.datasets || [];
        // Calculate KPIs
        const totalSavings = datasets.reduce((sum, row) => sum + (row.monthly_savings || 0), 0);
        const datasetCount = new Set(datasets.map(row => `${row.project_name}.${row.dataset_name}`)).size;
        const oppCount = datasets.length;

        elements.stTotalSavings.textContent = formatCurrency(totalSavings);
        elements.stDatasetCount.textContent = datasetCount;
        elements.stOppCount.textContent = oppCount;
        
        const effPricingTile = document.getElementById('st-eff-pricing');
        if (effPricingTile) {
            effPricingTile.textContent = `$${(data.effective_pricing_ratio || 0).toFixed(5)}`;
        }

        // Populate Table
        const tbody = document.querySelector('#storage-results-table tbody');
        tbody.innerHTML = '';

        const renderModelBadge = (model) => {
            if (!model) return '—';
            const isPhysical = String(model).toUpperCase() === 'PHYSICAL';
            return isPhysical
                ? `<span class="badge badge-physical"><i class="fa-solid fa-hard-drive" style="margin-right: 0.25rem;"></i>PHYSICAL</span>`
                : `<span class="badge badge-logical"><i class="fa-solid fa-layer-group" style="margin-right: 0.25rem;"></i>LOGICAL</span>`;
        };

        datasets.forEach((row, index) => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${renderProjectLink(row.project_name)}</td>
                <td>${renderDatasetLink(row.dataset_name, row.project_name, row.dataset_name)}</td>
                <td>${renderModelBadge(row.currently_on)}</td>
                <td>${renderModelBadge(row.better_on)}</td>
                <td data-order="${row.monthly_savings || 0}">$${Math.round(row.monthly_savings || 0).toLocaleString()}</td>
                <td data-order="${(row.monthly_savings_pct || 0) * 100}">${Math.round((row.monthly_savings_pct || 0) * 100)}%</td>
                <td>
                    <button class="btn-action copy-ddl-btn" data-index="${index}">Copy DDL</button>
                </td>
            `;
            tbody.appendChild(tr);
        });

        // Initialize DataTable
        if ($.fn.DataTable.isDataTable('#storage-results-table')) {
            $('#storage-results-table').DataTable().destroy();
        }
        $('#storage-results-table').DataTable({
            pageLength: 10,
            order: [[4, 'desc']], // Sort by savings
            responsive: true
        });

        // Add Event Listeners for Copy Buttons
        document.querySelectorAll('.copy-ddl-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const index = e.target.getAttribute('data-index');
                const rowData = state.storageData[index];
                if (rowData && rowData.ddl) {
                    copyToClipboard(rowData.ddl).then(() => {
                        showNotification('DDL copied to clipboard!', 'success');
                    }).catch(err => {
                        logger_error(err);
                        showNotification('Failed to copy DDL.', 'error');
                    });
                }
            });
        });
    };

    // Render Active Assist Results
    const renderActiveAssistResults = (data) => {
        // Must destroy DataTable BEFORE clearing innerHTML or appending rows
        if ($.fn.DataTable.isDataTable('#active-assist-table')) {
            $('#active-assist-table').DataTable().destroy();
        }

        const tbody = document.querySelector('#active-assist-table tbody');
        if (!tbody) return;
        tbody.innerHTML = '';
        
        if (data.length === 0) {
            tbody.innerHTML = `<tr><td colspan="8" style="text-align: center; padding: 2rem;">
                <div style="background: rgba(234, 179, 8, 0.1); border: 1px dashed rgba(234, 179, 8, 0.3); border-radius: 8px; padding: 1.5rem; display: inline-block;">
                    <h4 style="color: #fef08a; margin: 0 0 0.5rem 0; font-size: 1rem;"><i class="fa-solid fa-triangle-exclamation"></i> No Active Assist Recommendations Found</h4>
                    <div style="color: #cbd5e1; font-size: 0.9rem; margin: 0; text-align: left;">
                        <p style="margin: 0 0 0.5rem 0;">This can happen when:</p>
                        <ul style="margin: 0 0 1rem 0; padding-left: 1.5rem;">
                            <li>Tables are already well-optimized</li>
                            <li>Query history is under 30 days (Recommender needs more data)</li>
                            <li>Active tables with heavy DML may be intentionally excluded to prioritize compute efficiency</li>
                        </ul>
                        <p style="margin: 0;">Rely on the <strong>Static Schema Auditor</strong> below for structural governance.</p>
                    </div>
                </div>
            </td></tr>`;
            return;
        }

        const getRecBadge = (rec) => {
            let bg, color, border;
            if (rec.toLowerCase().includes('partition')) {
                bg = 'rgba(56, 189, 248, 0.15)'; // Soft Blue
                color = '#38bdf8';
                border = 'rgba(56, 189, 248, 0.3)';
            } else {
                bg = 'rgba(16, 185, 129, 0.15)'; // Soft Green (Cluster)
                color = '#10b981';
                border = 'rgba(16, 185, 129, 0.3)';
            }
            return `<span style="display: inline-block; padding: 0.25rem 0.6rem; font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; border-radius: 9999px; background: ${bg}; color: ${color}; border: 1px solid ${border}; white-space: nowrap;">${rec}</span>`;
        };

        const formatColumnsList = (cols) => {
            if (!cols || cols.length === 0) return `<span style="color: #64748b;">None</span>`;
            return cols.map(c => `<code style="font-family: monospace; background: rgba(255,255,255,0.05); padding: 0.15rem 0.35rem; border-radius: 4px; color: #cbd5e1; font-size: 0.8rem;">${c}</code>`).join(' ');
        };

        data.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><span style="color: #cbd5e1;">${row.project_id}</span></td>
                <td><span style="color: #94a3b8; font-family: monospace; font-size: 0.85rem;">${row.dataset_id}</span></td>
                <td><strong style="color: #f1f5f9;">${row.table_id}</strong></td>
                <td>${getRecBadge(row.recommendation)}</td>
                <td>${formatColumnsList(row.cluster_columns)}</td>
                <td>${row.partition_column ? `<code style="font-family: monospace; background: rgba(255,255,255,0.05); padding: 0.15rem 0.35rem; border-radius: 4px; color: #38bdf8; font-size: 0.8rem;">${row.partition_column}</code>` : '<span style="color: #64748b;">N/A</span>'}</td>
                <td><strong style="color: #10b981; font-weight: 700;">${formatCurrency(row.on_demand_monthly_savings)}</strong></td>
                <td><strong style="color: #38bdf8; font-weight: 700;">${formatCurrency(row.editions_monthly_savings)}</strong></td>
            `;
            tbody.appendChild(tr);
        });

        // Initialize DataTable

        $('#active-assist-table').DataTable({
            pageLength: 10,
            order: [[6, 'desc']], // Sort by On-Demand Savings descending
            responsive: true
        });
    };

    // Fetch Active Assist Recommendations
    const fetchActiveAssistRecommendations = async (force = false) => {
        const btn = document.getElementById('run-active-assist-btn');
        if (btn) setLoading(btn, true);
        clearModuleCache(['bq_active_assist_results'], ['#active-assist-table']);

        const params = {
            region: state.region,
            focus_projects: state.focusProjects,
            org_project_id: state.orgProject,
            max_bytes_billed_gb: state.maxBytesBilledGb
        };

        try {
            debug_log("Fetching Active Assist recommendations with params:", params);
            const response = await fetch('/api/storage/active_assist', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(buildPayload('/api/storage/active_assist', params))
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Failed to fetch Active Assist recommendations');
            }

            const data = await response.json();
            state.activeAssistData = data;
            renderActiveAssistResults(data);
            safeSetLocalStorage('bq_active_assist_results', JSON.stringify(data));
            if (force) showNotification('Active Assist recommendations synced.', 'success');
        } catch (error) {
            console.warn('Failed to fetch Active Assist recommendations:', error);
            showNotification('Active Assist unavailable — BigQuery returned an internal error. Try again later.', 'warning');
            // Show inline error in the results area so the user sees feedback
            const tableEl = document.getElementById('active-assist-table');
            if (tableEl) {
                const tbody = tableEl.querySelector('tbody');
                if (tbody) tbody.innerHTML = `<tr><td colspan="100%" style="text-align:center; color: #facc15; padding: 1.5rem;">
                    <i class="fa-solid fa-triangle-exclamation" style="margin-right: 0.5rem;"></i>
                    Active Assist recommendations could not be loaded. BigQuery encountered an internal error. Please retry.
                </td></tr>`;
            }
        } finally {
            if (btn) setLoading(btn, false);
        }
    };

    // Render Static Audit Results
    const renderStaticAuditResults = (data) => {
        // Destroy existing DataTables before modifying the DOM to prevent it from wiping our new rows
        if ($.fn.DataTable.isDataTable('#static-audit-table')) {
            $('#static-audit-table').DataTable().destroy();
        }

        const tbody = document.querySelector('#static-audit-table tbody');
        if (!tbody) return;
        tbody.innerHTML = '';

        const formatSize = (bytes) => {
            if (bytes === 0) return '0 B';
            const k = 1024;
            const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        };

        const formatNumber = (num) => {
            // Guard null/undefined/NaN from snapshot import
            if (num == null || isNaN(num)) return '0';
            return num.toLocaleString();
        };

        data.forEach(row => {
            const tr = document.createElement('tr');
            
            // Risk Status logic
            let riskBadge = '';
            const sizeGB = row.size_bytes / (1024 * 1024 * 1024);
            if (sizeGB > 1000 || row.row_count > 1000000000) {
                // Critical
                riskBadge = `<span style="display: inline-block; padding: 0.25rem 0.6rem; font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; border-radius: 9999px; background: rgba(239, 68, 68, 0.15); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.3); white-space: nowrap;"><i class="fa-solid fa-triangle-exclamation" style="margin-right: 0.25rem;"></i>Critical Risk</span>`;
            } else {
                // High
                riskBadge = `<span style="display: inline-block; padding: 0.25rem 0.6rem; font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; border-radius: 9999px; background: rgba(245, 158, 11, 0.15); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.3); white-space: nowrap;"><i class="fa-solid fa-circle-exclamation" style="margin-right: 0.25rem;"></i>High Risk</span>`;
            }

            // Suggestions string
            let suggestedAction = '';
            let columnsToSuggest = [];
            const tblLower = row.table_id.toLowerCase();
            const dsLower = row.dataset_id.toLowerCase();
            
            if (tblLower.includes('phone')) {
                columnsToSuggest = ['PHONE_TYPE', 'PHONE_VALUE'];
            } else if (tblLower.includes('email')) {
                columnsToSuggest = ['EMAIL_TYPE', 'EMAIL_VALUE'];
            } else if (tblLower.includes('master')) {
                columnsToSuggest = ['PERSON_SOURCE', 'PERSON_ORIGIN_AUTHORITY_ID'];
            } else if (tblLower.includes('visitor') || tblLower.includes('event') || dsLower.includes('pendo')) {
                columnsToSuggest = ['VISITOR_ID', 'ACCOUNT_ID'];
            } else if (tblLower.includes('address')) {
                columnsToSuggest = ['ADDRESS_TYPE', 'COUNTRY_NAME'];
            } else if (tblLower.includes('history') || tblLower.includes('usage')) {
                columnsToSuggest = ['MEMBER_ID', 'PRODUCT_ID'];
            } else if (tblLower.includes('partner_extract') || tblLower.includes('extract')) {
                columnsToSuggest = ['CUSTOMER_KEY', 'PERIOD_ID'];
            } else if (tblLower.includes('counter') || tblLower.includes('agg')) {
                columnsToSuggest = ['EVENT_TYPE_ID', 'PERIOD_ID'];
            } else if (tblLower.includes('xml')) {
                columnsToSuggest = ['XML_TYPE', 'ARTICLE_ID'];
            } else if (tblLower.includes('opportunity') || dsLower.includes('sfdc') || tblLower.includes('crm')) {
                columnsToSuggest = ['ACCOUNT_ID', 'OPPORTUNITY_ID'];
            } else {
                columnsToSuggest = ['ACTIVE_FLAG', 'CREATED_DATE'];
            }

            const getPartitionStatus = (isPart) => {
                return isPart 
                    ? `<span style="color: #10b981;"><i class="fa-solid fa-circle-check" style="margin-right: 5px;"></i>${row.partition_column || 'Yes'}</span>` 
                    : `<span style="color: #f87171;"><i class="fa-solid fa-circle-xmark" style="margin-right: 5px;"></i>Missing</span>`;
            };

            const getClusterStatus = (isClust) => {
                return isClust 
                    ? `<span style="color: #10b981;"><i class="fa-solid fa-circle-check" style="margin-right: 5px;"></i>${row.clustering_fields || 'Yes'}</span>` 
                    : `<span style="color: #f87171;"><i class="fa-solid fa-circle-xmark" style="margin-right: 5px;"></i>Missing</span>`;
            };

            tr.innerHTML = `
                <td><span style="color: #cbd5e1;">${row.project_id}</span></td>
                <td><span style="color: #94a3b8; font-family: monospace; font-size: 0.85rem;">${row.dataset_id}</span></td>
                <td><strong style="color: #f1f5f9;">${row.table_id}</strong></td>
                <td><span style="color: #cbd5e1; font-family: monospace; font-size: 0.85rem;">${formatNumber(row.row_count)}</span></td>
                <td><span style="color: #cbd5e1; font-family: monospace; font-size: 0.85rem; font-weight: 500;">${formatSize(row.size_bytes)}</span></td>
                <td>${getPartitionStatus(row.is_partitioned)}</td>
                <td>${getClusterStatus(row.is_clustered)}</td>
                <td><span style="color: #38bdf8; font-family: monospace; font-size: 0.85rem; font-weight: 500;">${columnsToSuggest.join(', ')}</span></td>
                <td>${riskBadge}</td>
            `;
            tbody.appendChild(tr);
        });

        // Initialize DataTable
        $('#static-audit-table').DataTable({
            pageLength: 10,
            order: [[4, 'desc']], // Sort by Logical Size descending
            responsive: true
        });
    };

    // Fetch Static Audit Results
    const fetchStaticAuditResults = async (force = false) => {
        const btn = document.getElementById('run-static-audit-btn');
        if (btn) setLoading(btn, true);
        clearModuleCache(['bq_static_audit_results'], ['#static-audit-table']);

        const params = {
            region: state.region,
            focus_projects: state.focusProjects,
            org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb
        };

        try {
            debug_log("Fetching Static Table Audit results with params:", params);
            const response = await fetch('/api/storage/static_audit', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(buildPayload('/api/storage/static_audit', params))
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Failed to execute Static Table Audit');
            }

            const data = await response.json();
            state.staticAuditData = data;
            renderStaticAuditResults(data);
            safeSetLocalStorage('bq_static_audit_results', JSON.stringify(data));
            if (force) showNotification('Static schema audit completed successfully.', 'success');
        } catch (error) {
            console.warn('Failed to execute Static Table Audit:', error);
            if (force) showNotification('Failed to execute static schema audit.', 'warning');
        } finally {
            if (btn) setLoading(btn, false);
        }
    };

    const renderOrgStatus = (orgStatus) => {
        const panel = document.getElementById('org-rec-panel');
        const text = document.getElementById('org-rec-text');
        const output = document.getElementById('org-ddl-output');

        if (!panel) return;

        panel.style.display = 'block'; 

        if (orgStatus.error_message) {
            panel.style.borderColor = 'rgba(239, 68, 68, 0.5)'; // Red
            text.innerHTML = `<i class="fa-solid fa-circle-xmark" style="color: #ef4444;"></i> <strong>Feature Not Enabled:</strong> ${orgStatus.error_message} Run the command below to enable it.`;
            if (output && output.parentElement) {
                output.parentElement.style.display = 'block';
                output.value = orgStatus.ddl;
            }
        } else if (orgStatus.is_optimized) {
            panel.style.borderColor = 'rgba(34, 197, 94, 0.5)'; 
            text.innerHTML = `<i class="fa-solid fa-circle-check" style="color: #4ade80;"></i> Your organization's default storage billing model for this region is already <strong>${orgStatus.current_model}</strong> No action needed.`;
            if (output && output.parentElement) {
                output.parentElement.style.display = 'none';
            }
        } else {
            panel.style.borderColor = 'rgba(234, 179, 8, 0.5)'; 
            text.innerHTML = `<i class="fa-solid fa-circle-exclamation" style="color: #facc15;"></i> Your organization's default storage billing model for this region is <strong>${orgStatus.current_model}</strong>. We recommend setting it to <strong>PHYSICAL</strong> to optimize future datasets automatically.`;
            if (output && output.parentElement) {
                output.parentElement.style.display = 'block';
                output.value = orgStatus.ddl;
            }
        }
    };

    


    // Analyze Slots
  // Analyze Slots (parallelized; chart no longer waits on analyze/tiered)
  if (elements.btnAnalyzeSlots) {
    elements.btnAnalyzeSlots.addEventListener('click', async () => {
      if (!state.orgProject) {
        showNotification('Please configure settings first.', 'error');
        Router.navigate('settings');
        return;
      }

      const tableEl = document.getElementById('slots-recommendations-table');
      const container = tableEl ? tableEl.closest('.results-panel') : null;
      const tierContainer = document.querySelector('.tier-cards-container');

      if (tableEl && $.fn.DataTable.isDataTable('#slots-recommendations-table')) {
        $('#slots-recommendations-table').DataTable().destroy();
      }

      if (tableEl) {
        UIState.renderTableSkeleton(tableEl, 5);
      }
      if (tierContainer) {
        UIState.renderTierCardsSkeleton(tierContainer);
      }

      const abortController = new AbortController();
      let progress = null;

      if (container) {
        progress = UIState.startQueryProgress(container, {
          message: 'Running slot analysis across organization (running 4 queries in parallel)...',
          onCancel: () => abortController.abort()
        });
      }

      setLoading(elements.btnAnalyzeSlots, true);

      const lookbackDays = parseInt(elements.slLookback.value);
      const percentile = parseInt(elements.slPercentile.value);

      // --- Build the four request promises up front (no awaits between them) ---

      const analyzeReq = fetch('/api/slots/analyze', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
          region: state.region,
          lookback_days: lookbackDays,
          window_minutes: parseInt(elements.slWindow.value),
          percentile: percentile,
          admin_project_id: state.adminProject
        }),
        signal: abortController.signal
      });

      const tieredReq = fetch('/api/slots/tiered_recommendations', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
          region: state.region,
          lookback_days: lookbackDays
        }),
        signal: abortController.signal
      });

      // Chart driver #1 — forced to HOUR resolution to keep the payload small
      // (MINUTE over 7 days returned ~2.1 MB; HOUR is ~60x smaller and is
      // plenty of detail for a 7-day overview chart).
      const utilReq = fetch('/api/slots/utilization', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
          region: state.region,
          lookback_days: lookbackDays,
          timezone: 'America/New_York',
          resolution: 'HOUR'
        }),
        signal: abortController.signal
      });

      // Chart driver #2 — provisioning timeline overlay
      const actualReq = fetch('/api/slots/actual_provisioning', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
          region: state.region,
          lookback_days: lookbackDays,
          timezone: 'America/New_York',
          edition: 'ENTERPRISE',
          admin_project_id: state.adminProject
        }),
        signal: abortController.signal
      });

      // --- Render the CHART first, as soon as its two inputs resolve ---
      // This is the whole point: the chart no longer waits on the slow
      // analyze (~26s) or tiered (~6s) queries.
      const chartReady = (async () => {
        const [utilResult, actualResult] = await Promise.allSettled([utilReq, actualReq]);

        let utilData = null;
        let actualData = null;

        if (utilResult.status === 'fulfilled' && utilResult.value.ok) {
          utilData = await utilResult.value.json();
          safeSetLocalStorage('bq_slots_utilization', JSON.stringify(utilData));
        } else {
          let detail = 'Failed to fetch slot utilization data';
          try {
            if (utilResult.status === 'fulfilled') {
              detail = (await utilResult.value.json()).detail || detail;
            }
          } catch (_) {}
          console.error('Slot utilization fetch failed:', detail);
          showNotification(detail, 'error');
        }

        if (actualResult.status === 'fulfilled' && actualResult.value.ok) {
          actualData = await actualResult.value.json();
          safeSetLocalStorage('bq_slots_actual_provisioning', JSON.stringify(actualData));
          if (actualData.timeline) {
            safeSetLocalStorage('bq_slots_provisioning_timeline', JSON.stringify(actualData.timeline));
          } else {
            try { localStorage.removeItem('bq_slots_provisioning_timeline'); } catch (_) {}
          }
        } else {
          let detail = 'Failed to fetch actual provisioning data';
          try {
            if (actualResult.status === 'fulfilled') {
              detail = (await actualResult.value.json()).detail || detail;
            }
          } catch (_) {}
          console.error('Actual provisioning fetch failed:', detail);
          showNotification(detail, 'error');
        }

        renderSlotsUtilizationAndProvisioning(utilData, actualData);
      })();

      // --- Render the TABLES as their (slower) inputs resolve ---
      const tablesReady = (async () => {
        const [analyzeResult, tieredResult] = await Promise.allSettled([analyzeReq, tieredReq]);

        if (analyzeResult.status === 'fulfilled' && analyzeResult.value.ok) {
          const responseData = await analyzeResult.value.json();
          renderSlotsResults(responseData, percentile);
          safeSetLocalStorage('bq_slots_results', JSON.stringify(responseData));
        } else {
          let detail = 'Failed to analyze slots';
          try {
            if (analyzeResult.status === 'fulfilled') {
              detail = (await analyzeResult.value.json()).detail || detail;
            }
          } catch (_) {}
          console.error('Slots analyze fetch failed:', detail);
          showNotification(detail, 'error');
        }

        if (tieredResult.status === 'fulfilled' && tieredResult.value.ok) {
          const tieredData = await tieredResult.value.json();
          renderTieredRecommendations(tieredData);
          safeSetLocalStorage('bq_slots_tiered', JSON.stringify(tieredData));
        } else {
          console.warn('Failed to fetch tiered recommendations:', tieredResult.reason);
        }
      })();

      // --- Wait for everything, then clean up the spinner/progress ---
      try {
        await Promise.allSettled([chartReady, tablesReady]);
        showNotification('Slots analysis completed.', 'success');
      } catch (error) {
        if (error.name === 'AbortError') {
          showNotification('Slots analysis cancelled.', 'warning');
        } else {
          console.error('Slots Analysis Error:', error);
          showNotification(error.message || 'Slots analysis failed.', 'error');
        }
      } finally {
        if (progress) progress.stop();
        setLoading(elements.btnAnalyzeSlots, false);
      }
    });
  }
    // Slot Simulator
    const btnRunSimulation = document.getElementById('run-simulation-btn');
    if (btnRunSimulation) {
        btnRunSimulation.addEventListener('click', async () => {
            if (!state.orgProject) {
                showNotification('Please configure settings first.', 'error');
                Router.navigate('settings');
                return;
            }

            setLoading(btnRunSimulation, true);

            try {
                document.getElementById('simulation-results-panel').style.display = 'none';
                const response = await fetch('/api/slots/simulate', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
                        region: state.region,
                        lookback_days: parseInt(document.getElementById('sim-lookback-days').value),
                        timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
                        max_baseline: parseInt(document.getElementById('sim-max-baseline').value),
                        step_size: parseInt(document.getElementById('sim-step-size').value),
                        payg_price: parseFloat(document.getElementById('sim-payg-price').value),
                        commit_1yr_price: parseFloat(document.getElementById('sim-commit-1yr-price').value),
                        commit_3yr_price: parseFloat(document.getElementById('sim-commit-3yr-price').value)
                    })
                });

                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(detailToMessage(err.detail, 'Simulation failed'));
                }

                const data = await response.json();
                safeSetLocalStorage('bq_slots_simulation_results', JSON.stringify(data));
                renderSimulationResults(data);
                document.getElementById('simulation-results-panel').style.display = 'block';
                showNotification('Simulation completed successfully.', 'success');
            } catch (error) {
                console.error("Simulation Error:", error);
                showNotification(error.message, 'error');
            } finally {
                setLoading(btnRunSimulation, false);
            }
        });
    }

    const renderSimulationResults = (data) => {
        if ($.fn.DataTable.isDataTable('#simulation-table')) {
            $('#simulation-table').DataTable().destroy();
        }
        
        // Guard against empty simulation response (brand-new reservation, no jobs).
        if (!data || data.length === 0) {
            showNotification('Simulation returned no results — the reservation may have no job history in this window.', 'warning');
            return;
        }

        // Find optimums for the summary table
        let bestPayg = data.reduce((prev, curr) => prev.total_payg < curr.total_payg ? prev : curr);
        let best1Yr = data.reduce((prev, curr) => prev.total_1yr < curr.total_1yr ? prev : curr);
        let best3Yr = data.reduce((prev, curr) => prev.total_3yr < curr.total_3yr ? prev : curr);

        // Populate Summary Table
        const summaryHtml = `
            <tr>
                <td style="padding: 10px;"><strong>PAYG (0 Commit)</strong></td>
                <td style="padding: 10px;">${bestPayg.autoscale_slot_months}</td>
                <td style="padding: 10px; background: rgba(34, 197, 94, 0.05);"><strong>${bestPayg.slots}</strong></td>
                <td style="padding: 10px;">$${bestPayg.total_payg.toLocaleString(undefined, {maximumFractionDigits: 0})}</td>
            </tr>
            <tr>
                <td style="padding: 10px;"><strong>1 Year Commit</strong></td>
                <td style="padding: 10px;">${best1Yr.autoscale_slot_months}</td>
                <td style="padding: 10px; background: rgba(34, 197, 94, 0.05);"><strong>${best1Yr.slots}</strong></td>
                <td style="padding: 10px;">$${best1Yr.total_1yr.toLocaleString(undefined, {maximumFractionDigits: 0})}</td>
            </tr>
            <tr>
                <td style="padding: 10px;"><strong>3 Year Commit</strong></td>
                <td style="padding: 10px;">${best3Yr.autoscale_slot_months}</td>
                <td style="padding: 10px; background: rgba(34, 197, 94, 0.05);"><strong>${best3Yr.slots}</strong></td>
                <td style="padding: 10px;">$${best3Yr.total_3yr.toLocaleString(undefined, {maximumFractionDigits: 0})}</td>
            </tr>
        `;
        const summaryTbody = document.getElementById('summary-tbody');
        if (summaryTbody) summaryTbody.innerHTML = summaryHtml;

        // Populate Matrix
        const table = $('#simulation-table').DataTable({ 
            pageLength: 15, 
            responsive: true,
            ordering: false // Usually disabled on matrix sheets to keep the natural 0->100 progression
        });
        table.clear();

        const formatMoney = (val) => `$${val.toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0})}`;

        data.forEach(row => {
            table.row.add([
                row.bucket,
                row.minutes.toLocaleString(),
                row.slots,
                `${row.utilization_pct.toFixed(2)}%`,
                row.autoscale_slot_hours.toLocaleString(),
                row.autoscale_slot_months.toLocaleString(),
                formatMoney(row.cost_autoscale_payg),
                formatMoney(row.cost_base_payg),
                formatMoney(row.cost_base_1yr),
                formatMoney(row.cost_base_3yr),
                formatMoney(row.total_payg),
                formatMoney(row.total_1yr),
                formatMoney(row.total_3yr)
            ]).node();
        });

        table.draw();
    };

    const renderSlotsResults = (data, targetPercentile) => {
        // Destroy existing DataTables before modifying the DOM to avoid Column Count mismatch errors
        if ($.fn.DataTable.isDataTable('#current-reservations-table-new')) {
            $('#current-reservations-table-new').DataTable().destroy();
        }
        if ($.fn.DataTable.isDataTable('#slots-recommendations-table')) {
            $('#slots-recommendations-table').DataTable().destroy();
        }
        if ($.fn.DataTable.isDataTable('#config-recommendations-table')) {
            $('#config-recommendations-table').DataTable().destroy();
        }

        const container = document.querySelector('#current-reservations-container');
        const recommendationsTbody = document.querySelector('#slots-recommendations-table tbody');
        const recommendationsTfoot = document.querySelector('#slots-recommendations-table tfoot');
        
        if (container) container.innerHTML = '';
        if (recommendationsTbody) recommendationsTbody.innerHTML = '';
        if (recommendationsTfoot) recommendationsTfoot.innerHTML = '';

        // Update label
        const lblPercentile = document.getElementById('lbl-percentile');
        if (lblPercentile) lblPercentile.textContent = targetPercentile;

        // Create Current Reservations Table
        if (container) {
            const table = document.createElement('table');
            table.id = 'current-reservations-table-new';
            table.className = 'display nowrap';
            table.style.width = '100%';

            const thead = document.createElement('thead');
            const trHead = document.createElement('tr');
            const headers = [
                "Reservation ID", "Admin Project ID", "Region", "Edition", 
                "Baseline", "MAX SLOTS", "Use Idle Slots", 
                "Scaling Mode", "Concurrency", "Fluid Scaling"
            ];
            headers.forEach(h => {
                const th = document.createElement('th');
                th.textContent = h;
                trHead.appendChild(th);
            });
            thead.appendChild(trHead);
            table.appendChild(thead);

            const tbody = document.createElement('tbody');
            table.appendChild(tbody);
            
            if (data.current_reservations) {
                data.current_reservations.forEach(row => {
                    const tr = document.createElement('tr');
                    tr.innerHTML = `
                        <td>${row.reservation_id}</td>
                        <td>${row.admin_project_id || ''}</td>
                        <td>${row.region || ''}</td>
                        <td>${row.edition}</td>
                        <td>${formatNumber(row.current_baseline)}</td>
                        <td>${formatNumber(row.current_max_slots)}</td>
                        <td>${row.ignore_idle_slots ? 'No' : 'Yes'}</td>
                        <td>${row.scaling_mode || 'N/A'}</td>
                        <td>${row.target_job_concurrency || 'Auto'}</td>
                        <td>${row.fluid_scaling_enabled ? '<span class="badge" style="background: rgba(34, 197, 94, 0.15); color: #22c55e;">Enabled</span>' : '<span class="badge" style="background: rgba(239, 68, 68, 0.15); color: #ef4444;">Disabled</span>'}</td>
                    `;
                    tbody.appendChild(tr);
                });
            }
            
            container.appendChild(table);
        }



        // Render Recommendations
        if (data.recommendations) {
            data.recommendations.forEach(row => {
                const tr = document.createElement('tr');
                
                // Clean up reservation ID to remove project and region prefix
                let displayResId = row.reservation_id;
                if (displayResId && displayResId.includes('.')) {
                    displayResId = displayResId.split('.').pop();
                }
                
                tr.innerHTML = `
                    <td>${displayResId}</td>
                    <td><strong>${formatNumber(row.recommended_baseline)}</strong></td>
                    <td>${formatNumber(row.recommended_max_p90)}</td>
                    <td>${formatNumber(row.recommended_max_p99)}</td>
                    <td>${formatNumber(row.recommended_max_peak)}</td>
                `;
                
                if (displayResId === 'MERGED (Simulated)') {
                    // Make it stand out slightly if it's the sum row
                    tr.style.backgroundColor = 'rgba(255, 255, 255, 0.03)';
                    if (recommendationsTfoot) {
                        recommendationsTfoot.appendChild(tr);
                    } else {
                        recommendationsTbody.appendChild(tr);
                    }
                } else {
                    recommendationsTbody.appendChild(tr);
                }
            });
        }

        // Render Configuration Recommendations
        const configTbody = document.querySelector('#config-recommendations-table tbody');
        if (configTbody) configTbody.innerHTML = '';

        if (data.current_reservations && configTbody) {
            data.current_reservations.forEach(row => {
                const resId = row.reservation_id;
                const adminProj = row.admin_project_id || '';
                const region = row.region || '';
                
                // Recommend Fluid Scaling if disabled
                if (!row.fluid_scaling_enabled) {
                    const tr = document.createElement('tr');
                    // Build the full reservation set: already-enabled + this one
                    const allRes = new Set();
                    data.current_reservations.forEach(r => {
                        if (r.fluid_scaling_enabled) allRes.add(r.reservation_id);
                    });
                    allRes.add(resId);
                    const listStr = Array.from(allRes).sort().map(r => `'${r}'`).join(', ');
                    const ddl = `ALTER PROJECT \`${adminProj}\` SET OPTIONS (\`${region}.preflight_fluid_autoscaling_reservations\` = [${listStr}]);`;
                    
                    tr.innerHTML = `
                        <td>${resId}</td>
                        <td>Enable Fluid Scaling for true per-second billing.</td>
                        <td>
                            <button class="btn-action copy-config-ddl-btn" data-ddl="${ddl.replace(/"/g, '&quot;')}">Copy DDL</button>
                        </td>
                    `;
                    configTbody.appendChild(tr);
                }
                
                // Recommend ALL_SLOTS scaling mode if unspecified and max > baseline
                if (row.scaling_mode === 'SCALING_MODE_UNSPECIFIED' && row.current_max_slots > row.current_baseline) {
                    const tr = document.createElement('tr');
                    
                    let step1 = '';
                    let stepNum = 1;
                    if (!data.fairness_enabled) {
                        step1 = `-- Step 1: Enable Reservation-Based Fairness
ALTER PROJECT \`${adminProj}\` SET OPTIONS (\`${region}.enable_reservation_based_fairness\` = true);

`;
                        stepNum = 2;
                    }

                    const ddl = `${step1}-- Step ${stepNum}: Disable Legacy Autoscaling
ALTER RESERVATION \`${adminProj}.${region}.${resId}\` SET OPTIONS (autoscale_max_slots = 0);

-- Step ${stepNum + 1}: Enable the New Scaling Model
ALTER RESERVATION \`${adminProj}.${region}.${resId}\` SET OPTIONS (scaling_mode = 'ALL_SLOTS', max_slots = ${row.current_max_slots}, ignore_idle_slots = false);`;
                    
                    tr.innerHTML = `
                        <td>${resId}</td>
                        <td>Set scaling mode to ALL_SLOTS. ${data.fairness_enabled ? 'Requires 2 steps (Fairness already enabled).' : 'Requires 3 steps: enable fairness, disable legacy autoscale, set new mode.'}</td>
                        <td>
                            <button class="btn-action copy-config-ddl-btn" data-ddl="${ddl.replace(/"/g, '&quot;')}">Copy DDL</button>
                        </td>
                    `;
                    configTbody.appendChild(tr);
                }
            });
        }

        // Add Event Listeners for Copy Config DDL Buttons
        document.querySelectorAll('.copy-config-ddl-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const ddl = e.target.getAttribute('data-ddl');
                if (ddl) {
                    copyToClipboard(ddl).then(() => {
                        showNotification('DDL copied to clipboard!', 'success');
                    }).catch(err => {
                        console.error(err);
                        showNotification('Failed to copy DDL.', 'error');
                    });
                }
            });
        });

        // Initialize DataTable for Config Recommendations
        if ($.fn.DataTable.isDataTable('#config-recommendations-table')) {
            $('#config-recommendations-table').DataTable().destroy();
        }
        $('#config-recommendations-table').DataTable({ pageLength: 5, responsive: true });

        if ($.fn.DataTable.isDataTable('#current-reservations-table-new')) {
            $('#current-reservations-table-new').DataTable().destroy();
        }
        $('#current-reservations-table-new').DataTable({ pageLength: 5, responsive: true });

        if ($.fn.DataTable.isDataTable('#slots-recommendations-table')) {
            $('#slots-recommendations-table').DataTable().destroy();
        }
        $('#slots-recommendations-table').DataTable({ pageLength: 5, order: [[1, 'desc']], responsive: true });
    };

    const renderTieredRecommendations = (data) => {
      // Normalize the response shape — handle array, envelope, or single object
      let rows = [];
      if (Array.isArray(data)) {
        rows = data;
      } else if (data && Array.isArray(data.rows)) {
        rows = data.rows;
      } else if (data && Array.isArray(data.recommendations)) {
        rows = data.recommendations;
      } else if (data && typeof data === 'object' && 'aggressive_baseline_p80' in data) {
        rows = [data];  // single reservation returned as object
      }

      if (rows.length === 0) {
        console.warn('Tiered recommendations: no data to render', data);
        setTierCardValues('—', '—', '—');
        return;
      }

      // Re-insert original HTML structure to restore IDs lost to skeleton
      const tierContainer = document.querySelector('.tier-cards-container');
      if (tierContainer) {
        tierContainer.innerHTML = `
                            <!-- Aggressive Card -->
                            <article class="tier-card tier-card--aggressive">
                                <div class="tier-badge">P80</div>
                                <h3>Aggressive Savings</h3>
                                <p>Low cost, higher risk of queuing during bursts.</p>
                                <div class="slots-display">
                                    <span id="tier-p80-slots">0</span>
                                    <span> slots</span>
                                </div>
                                <button class="btn-action" id="btn-apply-p80">COPY DDL</button>
                            </article>
                            
                            <!-- Balanced Card -->
                            <article class="tier-card tier-card--balanced tier-card--recommended" aria-current="true">
                                <div class="tier-badge">P95</div>
                                <h3>Balanced</h3>
                                <p>Optimal balance of cost and performance.</p>
                                <div class="slots-display">
                                    <span id="tier-p95-slots">0</span>
                                    <span> slots</span>
                                </div>
                                <button class="btn-action" id="btn-apply-p95">COPY DDL</button>
                            </article>
                            
                            <!-- Performance Card -->
                            <article class="tier-card tier-card--performance">
                                <div class="tier-badge">Max</div>
                                <h3>Performance</h3>
                                <p>Zero queuing risk, highest cost.</p>
                                <div class="slots-display">
                                    <span id="tier-max-slots">0</span>
                                    <span> slots</span>
                                </div>
                                <button class="btn-action" id="btn-apply-max">COPY DDL</button>
                            </article>
        `;
      }

      // Pick the reservation with the highest performance_baseline_max
      const mainRes = rows.reduce((prev, curr) =>
        (curr.performance_baseline_max || 0) > (prev.performance_baseline_max || 0) ? curr : prev
      );

      setTierCardValues(
        mainRes.aggressive_baseline_p80,
        mainRes.balanced_baseline_p95,
        mainRes.performance_baseline_max
      );

      // Wire up the Apply buttons (replace, don't accumulate listeners)
      wireApplyButton('btn-apply-p80', mainRes.aggressive_baseline_p80, mainRes.reservation_id);
      wireApplyButton('btn-apply-p95', mainRes.balanced_baseline_p95, mainRes.reservation_id);
      wireApplyButton('btn-apply-max', mainRes.performance_baseline_max, mainRes.reservation_id);
    };

    const setTierCardValues = (p80, p95, max) => {
      const targets = [
        ['tier-p80-slots', p80],
        ['tier-p95-slots', p95],
        ['tier-max-slots', max]
      ];

      targets.forEach(([id, value]) => {
        const el = document.getElementById(id);
        if (!el) {
          console.error(`Tier card element #${id} not found in DOM`);
          return;
        }
        el.textContent = (value == null || value === '—')
          ? '—'
          : formatNumber(value);
      });
    };

    const wireApplyButton = (buttonId, slots, reservationId) => {
      const btn = document.getElementById(buttonId);
      if (!btn) return;

      // Clone-and-replace to wipe any previous listeners (prevents double-fire)
      const fresh = btn.cloneNode(true);
      btn.parentNode.replaceChild(fresh, btn);

      fresh.addEventListener('click', () => {
        let cleanResId = reservationId;
        if (cleanResId && cleanResId.includes('.')) {
            cleanResId = cleanResId.split('.').pop();
        }
        const adminProj = state.adminProject || state.orgProject;
        const region = state.region;
        
        const ddl = `ALTER RESERVATION \`${adminProj}.${region}.${cleanResId}\` SET OPTIONS (slot_capacity = ${slots});`;

        copyToClipboard(ddl)
          .then(() => {
            const original = fresh.textContent;
            fresh.textContent = '✓ COPIED';
            showNotification('DDL copied to clipboard!', 'success');
            setTimeout(() => { fresh.textContent = original; }, 1500);
          })
          .catch(err => console.error('Clipboard write failed:', err));
      });
    };

    const renderProfilerResults = (data) => {
        const tbody = document.querySelector('#slots-profiler-table tbody');
        if (!tbody) return;
        tbody.innerHTML = '';

        data.forEach(row => {
            const tr = document.createElement('tr');
            
            // Clean up reservation ID
            let displayResId = row.reservation_id;
            if (displayResId && displayResId.includes('.')) {
                displayResId = displayResId.split('.').pop();
            }
            
            tr.innerHTML = `
                <td>${displayResId}</td>
                <td>${formatNumber(row.total_flagged_hours)}</td>
                <td>${formatNumber(row.peak_hourly_queries)}</td>
                <td>${row.top_projects}</td>
                <td><span class="badge" style="background: rgba(245, 158, 11, 0.15); color: #f59e0b;">Consider Baseline</span></td>
            `;
            tbody.appendChild(tr);
        });

        // Initialize DataTable
        if ($.fn.DataTable.isDataTable('#slots-profiler-table')) {
            $('#slots-profiler-table').DataTable().destroy();
        }
        $('#slots-profiler-table').DataTable({ pageLength: 5, order: [[2, 'desc']], responsive: true });
    };

    const renderHeatmap = (timeline) => {
        const tbody = document.querySelector('#heatmap-table tbody');
        if (!tbody) return;
        tbody.innerHTML = '';

        // Initialize 24x7 grid
        const grid = Array(24).fill(0).map(() => Array(7).fill(0));

        // Populate grid
        timeline.forEach(row => {
            const date = new Date(row.hour_bucket);
            const day = date.getDay(); // 0 = Sun, 1 = Mon, etc.
            const hour = date.getHours();
            grid[hour][day] += row.hourly_queries;
        });

        // Find max value for scaling intensity
        let maxVal = 0;
        for (let h = 0; h < 24; h++) {
            for (let d = 0; d < 7; d++) {
                if (grid[h][d] > maxVal) maxVal = grid[h][d];
            }
        }

        // Render rows
        for (let h = 0; h < 24; h++) {
            const tr = document.createElement('tr');
            
            // Hour label
            const tdHour = document.createElement('td');
            tdHour.textContent = `${String(h).padStart(2, '0')}:00`;
            tdHour.style.fontWeight = 'bold';
            tr.appendChild(tdHour);

            // Days
            for (let d = 0; d < 7; d++) {
                const td = document.createElement('td');
                const val = grid[h][d];
                
                if (val > 0) {
                    const intensity = val / maxVal;
                    td.style.background = `rgba(239, 68, 68, ${intensity * 0.8 + 0.1})`;
                    td.style.color = intensity > 0.5 ? '#fff' : 'var(--text-secondary)';
                    td.innerHTML = `<strong>${formatNumber(val)}</strong>`;
                    td.title = `${val} queries`;
                } else {
                    td.textContent = '-';
                    td.style.color = 'var(--text-secondary)';
                    td.style.opacity = '0.3';
                }
                
                td.style.padding = '0.5rem';
                td.style.border = '1px solid rgba(255,255,255,0.05)';
                
                tr.appendChild(td);
            }
            
            tbody.appendChild(tr);
        }
    };

    const renderProfilerQueries = (data) => {
        let table;
        if ($.fn.DataTable.isDataTable('#profiler-queries-table')) {
            table = $('#profiler-queries-table').DataTable();
        } else {
            table = $('#profiler-queries-table').DataTable({
                pageLength: 10,
                order: [[3, 'desc']],
                responsive: true
            });
        }
        
        table.clear();

        const formatSlotHours = (num) => {
            if (num > 0 && num < 0.01) {
                return new Intl.NumberFormat('en-US', { maximumFractionDigits: 6 }).format(num);
            }
            return new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 }).format(num);
        };

        // XSS-safe helper: escapes HTML entities for use in innerHTML / title attributes
        const esc = (s) => s == null ? '' : String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'", '&#39;');

        data.forEach(row => {
            const avgBytes = row.avg_bytes_processed || 0;
            const recommendation = row.recommendation || 'N/A';
            const isCandidate = recommendation !== 'N/A';
            const badgeBg = isCandidate ? 'rgba(34, 197, 94, 0.15)' : 'rgba(148, 163, 184, 0.15)';
            const badgeColor = isCandidate ? '#22c55e' : '#94a3b8';
            const badgeText = isCandidate ? 'Candidate' : 'N/A';
            
            table.row.add([
                `<div style="font-family: monospace; font-size: 0.8rem; max-width: 400px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${esc(row.query)}">${esc(row.query)}</div>`,
                `<div style="font-family: monospace; font-size: 0.8rem; max-width: 150px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${row.project_id || ''}">${row.project_id || 'N/A'}</div>`,
                `<div style="display: flex; align-items: center; gap: 0.5rem;">
                    <span style="font-family: monospace; font-size: 0.8rem; max-width: 150px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${row.example_job_id || ''}">${row.example_job_id || 'N/A'}</span>
                    ${row.example_job_id ? `<button class="btn-action copy-job-id-btn" data-job-id="${row.example_job_id}" title="Copy Job ID" style="padding: 2px 5px; font-size: 0.75rem;"><i class="fa-solid fa-copy"></i></button>` : ''}
                </div>`,
                formatNumber(row.frequency),
                formatSlotHours(row.avg_slot_hours),
                formatNumber(row.avg_duration_seconds),
                `${formatNumber(avgBytes / (1024 * 1024))} MB`,
                `<span class="badge" style="background: ${badgeBg}; color: ${badgeColor};" title="${recommendation}">${badgeText}</span>`
            ]);
        });

        table.draw();
    };

    const renderActualProvisioningDonut = (autoscaledHours, baselineHours) => {
        const ctx = document.getElementById('actual-provisioning-donut').getContext('2d');
        
        if (state.actualProvisioningChart) {
            state.actualProvisioningChart.destroy();
        }
        
        state.actualProvisioningChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: ['Autoscaled Hours', 'Baseline Hours'],
                datasets: [{
                    data: [autoscaledHours, baselineHours],
                    backgroundColor: ['#facc15', '#38bdf8'],
                    borderColor: 'rgba(255, 255, 255, 0.1)',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const label = context.label || '';
                                const value = context.raw || 0;
                                const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                const percentage = ((value / total) * 100).toFixed(2) + '%';
                                return `${label}: ${formatNumber(value)} (${percentage})`;
                            }
                        }
                    }
                },
                cutout: '70%'
            }
        });
    };

    const renderSlotsChart = (data, provisioningTimeline = null) => {
        const ctx = document.getElementById('slots-timeline-chart').getContext('2d');
        
        if (state.slotsChart) {
            state.slotsChart.destroy();
        }
        
        // Reverse data to show chronological order (API returns descending)
        const reversedData = [...data].reverse();
        
        const labels = reversedData.map(d => {
            const date = new Date(d.timestamp);
            return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        });
        
        const timeAvg = reversedData.map(d => d.time_average);
        const p90 = reversedData.map(d => d.p90_slots);
        const maxSlots = reversedData.map(d => d.max_slots);
        
        let baselineData = [];
        let currentData = [];

        if (provisioningTimeline && provisioningTimeline.length > 0) {
            provisioningTimeline.sort((a, b) => new Date(a.ts) - new Date(b.ts));

            reversedData.forEach(d => {
                const currentTs = new Date(d.timestamp);
                let activeProvisioning = { baseline_slots: 0, current_slots: 0 };
                for (let i = provisioningTimeline.length - 1; i >= 0; i--) {
                    if (new Date(provisioningTimeline[i].ts) <= currentTs) {
                        activeProvisioning = provisioningTimeline[i];
                        break;
                    }
                }
                baselineData.push(activeProvisioning.baseline_slots);
                currentData.push(activeProvisioning.current_slots);
            });
        }
        
        const datasets = [
            {
                label: 'Time Average',
                data: timeAvg,
                borderColor: '#38bdf8',
                backgroundColor: 'rgba(56, 189, 248, 0.1)',
                fill: true,
                tension: 0.4,
                pointRadius: 0,
                borderWidth: 1.5
            },
            {
                label: 'P90',
                data: p90,
                borderColor: '#a855f7',
                borderDash: [5, 5],
                fill: false,
                tension: 0.4,
                pointRadius: 0,
                borderWidth: 1.5
            },
            {
                label: 'Max Slots',
                data: maxSlots,
                borderColor: '#ef4444',
                borderDash: [2, 2],
                fill: false,
                tension: 0.1,
                pointRadius: 0,
                borderWidth: 1.5
            }
        ];

        if (baselineData.length > 0) {
            datasets.push({
                label: 'Actual Baseline',
                data: baselineData,
                borderColor: '#f59e0b',
                borderDash: [5, 5],
                fill: false,
                stepped: 'before',
                pointRadius: 0,
                borderWidth: 2
            });
            datasets.push({
                label: 'Total Provisioned',
                data: currentData,
                borderColor: '#10b981',
                borderDash: [2, 2],
                fill: false,
                stepped: 'before',
                pointRadius: 0,
                borderWidth: 2
            });
        }

        state.slotsChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Slots'
                        }
                    },
                    x: {
                        title: {
                            display: true,
                            text: 'Time'
                        }
                    }
                },
                plugins: {
                    legend: {
                        position: 'top',
                    },
                    tooltip: {
                        mode: 'index',
                        intersect: false
                    }
                }
            }
        });
    };

    const renderSlotsUtilizationAndProvisioning = (utilData, actualData) => {
        let provisioningTimeline = null;
        if (actualData) {
            const elAuto = document.getElementById('act-autoscaled-hours');
            const elBase = document.getElementById('act-baseline-hours');
            const elTotal = document.getElementById('act-total-hours');
            if (elAuto) elAuto.textContent = formatNumber(Math.round(actualData.autoscaled_slot_hours || 0));
            if (elBase) elBase.textContent = formatNumber(Math.round(actualData.baseline_slot_hours || 0));
            if (elTotal) elTotal.textContent = formatNumber(Math.round(actualData.total_slot_hours || 0));
            provisioningTimeline = actualData.timeline || null;
            renderActualProvisioningDonut(actualData.autoscaled_slot_hours || 0, actualData.baseline_slot_hours || 0);
        }

        if (Array.isArray(utilData) && utilData.length > 0) {
            renderSlotsChart(utilData, provisioningTimeline);

            const simMaxBaselineInput = document.getElementById('sim-max-baseline');
            if (simMaxBaselineInput) {
                const peakSlots = Math.max(...utilData.map(d => d.max_slots || 0));
                const recommendedMax = Math.ceil(peakSlots / 500) * 500 || 1000;
                simMaxBaselineInput.value = recommendedMax;
                console.log(`Auto-set simulator max baseline to ${recommendedMax} based on peak usage of ${peakSlots}`);
            }
        }
    };

    // Helpers
    const formatCurrency = (amount) => {
        if (amount > 0 && amount < 0.01) {
            return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 4, maximumFractionDigits: 6 }).format(amount);
        }
        return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(amount);
    };

    const formatNumber = (num) => {
        // Guard null/undefined/NaN from snapshot import
        if (num == null || isNaN(num)) return '0';
        return new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 }).format(num);
    };

    const formatDiffPct = (pct) => {
        if (!pct) return '0%';
        if (pct >= 1000) {
            const multiplier = pct / 100;
            if (multiplier >= 1000000) {
                return `+${(multiplier / 1000000).toFixed(1)}Mx`;
            } else if (multiplier >= 1000) {
                return `+${(multiplier / 1000).toFixed(1)}Kx`;
            } else {
                return `+${multiplier.toFixed(1)}x`;
            }
        }
        return `+${new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 }).format(pct)}%`;
    };

    const showNotification = (message, type = 'info') => {
        // If an identical notification is already showing, flash it to acknowledge the click without stacking duplicate popups
        if (elements.notificationContainer) {
            const activeNotifs = elements.notificationContainer.querySelectorAll('.notification');
            for (const notif of activeNotifs) {
                const contentEl = notif.querySelector('.notif-content');
                if (contentEl && contentEl.textContent.trim() === message.trim()) {
                    notif.style.transform = 'scale(1.05)';
                    setTimeout(() => { notif.style.transform = 'scale(1)'; }, 150);
                    return;
                }
            }
        }

        const notification = document.createElement('div');
        notification.className = `notification ${type}`;
        notification.style.transition = 'transform 0.15s ease-out';
        
        let icon = 'fa-circle-info';
        if (type === 'success') icon = 'fa-circle-check';
        if (type === 'error') icon = 'fa-circle-exclamation';
        if (type === 'warning') icon = 'fa-triangle-exclamation';

        // H1: Build the node with textContent to prevent XSS — message
        // can contain DOM input values (e.g. project IDs from validation)
        // that bypass the global fetch sanitizer.
        const iconEl = document.createElement('i');
        iconEl.className = `fa-solid ${icon}`;
        const contentEl = document.createElement('div');
        contentEl.className = 'notif-content';
        contentEl.textContent = message;
        notification.appendChild(iconEl);
        notification.appendChild(contentEl);

        elements.notificationContainer.appendChild(notification);

        setTimeout(() => {
            notification.style.animation = 'fadeOut 0.3s ease-out forwards';
            setTimeout(() => notification.remove(), 300);
        }, 3000);
    };

    window.showNotification = showNotification;

    const setLoading = (button, isLoading) => {
        if (isLoading) {
            button.disabled = true;
            button.dataset.originalText = button.innerHTML;
            button.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Processing...';
        } else {
            button.disabled = false;
            button.innerHTML = button.dataset.originalText;
        }
    };

    const logger_error = (error) => {
        console.error("Application Error:", error);
    };

    // Cost Attribution Logic
    if (elements.navCostAttribution) {
        elements.navCostAttribution.addEventListener('click', async (e) => {
            e.preventDefault();
            Router.navigate('cost-attribution');
            await loadCostAttributionConfig();
        });
    }

    const renderReservationsForm = (reservations) => {
        const container = elements.cbReservationsContainer;
        if (!container) return;
        container.innerHTML = '';

        Object.entries(reservations).forEach(([resId, config]) => {
            addReservationRow(resId, config.sku_rate, config.total_admin_bill);
        });
    };

    const addReservationRow = (resId = '', skuRate = '', totalBill = '') => {
        const container = elements.cbReservationsContainer;
        if (!container) return;

        const row = document.createElement('div');
        row.className = 'reservation-row';
        row.style.display = 'flex';
        row.style.gap = '0.5rem';
        row.style.marginBottom = '0.5rem';
        row.style.alignItems = 'center';

        row.innerHTML = `
            <input type="text" class="cb-res-id" placeholder="Reservation ID" value="${resId}" style="flex: 2; background: rgba(0,0,0,0.2); color: #fff; border: 1px solid rgba(255,255,255,0.1); border-radius: 4px; padding: 0.375rem;">
            <input type="number" class="cb-res-rate" placeholder="SKU Rate" step="0.001" value="${skuRate}" style="flex: 1; background: rgba(0,0,0,0.2); color: #fff; border: 1px solid rgba(255,255,255,0.1); border-radius: 4px; padding: 0.375rem;">
            <input type="number" class="cb-res-bill" placeholder="Total Bill ($)" step="0.01" value="${totalBill}" style="flex: 1; background: rgba(0,0,0,0.2); color: #fff; border: 1px solid rgba(255,255,255,0.1); border-radius: 4px; padding: 0.375rem;">
            <button class="btn-action cb-remove-res-btn" style="padding: 0.375rem 0.5rem;"><i class="fa-solid fa-trash"></i></button>
        `;

        row.querySelector('.cb-remove-res-btn').addEventListener('click', () => {
            row.remove();
        });

        container.appendChild(row);
    };

    const getReservationsFromForm = () => {
        const reservations = {};
        const rows = document.querySelectorAll('.reservation-row');
        rows.forEach(row => {
            const resId = row.querySelector('.cb-res-id').value.trim();
            const skuRate = parseFloat(row.querySelector('.cb-res-rate').value);
            const totalBill = parseFloat(row.querySelector('.cb-res-bill').value);
            
            if (resId) {
                reservations[resId] = {
                    sku_rate: skuRate || 0.0,
                    total_admin_bill: totalBill || 0.0
                };
            }
        });
        return reservations;
    };

    if (elements.cbAddReservationBtn) {
        elements.cbAddReservationBtn.addEventListener('click', (e) => {
            e.preventDefault();
            addReservationRow();
        });
    }

    const loadCostAttributionConfig = async () => {
        try {
            const response = await fetch('/api/cost-attribution/config');
            if (response.ok) {
                const config = await response.json();
                elements.cbWasteRule.value = config.waste_rule;
                elements.cbCentralProject.value = config.central_cost_center_project || '';
                elements.cbBorrowingRule.value = config.borrowing_rule;
                renderReservationsForm(config.reservations);
            }
        } catch (error) {
            console.error("Failed to load cost attribution config:", error);
        }
    };

    if (elements.btnCalculateCostAttribution) {
        elements.btnCalculateCostAttribution.addEventListener('click', async () => {
            if (!state.orgProject) {
                showNotification('Please configure settings first.', 'error');
                Router.navigate('settings');
                return;
            }

            const monthStart = elements.cbMonthStart.value;
            const monthEnd = elements.cbMonthEnd.value;

            if (!monthStart || !monthEnd) {
                showNotification('Please select both start and end dates.', 'error');
                return;
            }

            setLoading(elements.btnCalculateCostAttribution, true);

            try {
                // First save config
                const config = {
                    waste_rule: elements.cbWasteRule.value,
                    central_cost_center_project: elements.cbCentralProject.value.trim() || null,
                    borrowing_rule: elements.cbBorrowingRule.value,
                    reservations: getReservationsFromForm()
                };

                const configResp = await fetch('/api/cost-attribution/config', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(config)
                });
                if (!configResp.ok) {
                    const errBody = await configResp.json().catch(() => ({}));
                    throw new Error(errBody.detail || `Config save failed (HTTP ${configResp.status})`);
                }

                // Then calculate
                const params = {
                    billing_month_start: monthStart,
                    billing_month_end: monthEnd,
                    org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
                    region: state.region,
                focus_projects: state.focusProjects,
                    admin_project_id: state.adminProject
                };

                const response = await fetch('/api/cost-attribution/calculate', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(buildPayload('/api/cost-attribution/calculate', params))
                });

                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(detailToMessage(err.detail, 'Calculation failed'));
                }

                const data = await response.json();
                const attributions = data.attributions || data; // backward compat
                const scope = data.scope;
                renderCostAttributionResults(attributions);
                safeSetLocalStorage('bq_cost_attribution_results', JSON.stringify(data));
                if (scope && scope.mode === 'focused' && scope.total_org_projects) {
                    showNotification(`Cost attribution calculated — showing ${scope.projects.length} of ${scope.total_org_projects} projects (waste computed over full org).`, 'success');
                } else {
                    showNotification('Cost attribution calculated successfully.', 'success');
                }
            } catch (error) {
                console.error("Cost Attribution Error:", error);
                showNotification(error.message, 'error');
            } finally {
                setLoading(elements.btnCalculateCostAttribution, false);
            }
        });
    }

    const renderCostAttributionResults = (data) => {
        let table;
        if ($.fn.DataTable.isDataTable('#cost-attribution-results-table')) {
            table = $('#cost-attribution-results-table').DataTable();
        } else {
            table = $('#cost-attribution-results-table').DataTable({
                pageLength: 10,
                order: [[4, 'desc']],
                responsive: true
            });
        }
        
        table.clear();
        
        // Aggregation for Slot Usage by Project
        const projectSlots = {};
        const projectCosts = {};

        data.forEach(row => {
            let displayResId = row.reservation_id;
            if (displayResId && displayResId.includes('.')) {
                displayResId = displayResId.split('.').pop();
            } else if (displayResId && displayResId.includes(':')) {
                displayResId = displayResId.split(':').pop();
            }

            table.row.add([
                row.project_id,
                displayResId,
                formatCurrency(row.direct_usage_cost_usd),
                formatCurrency(row.allocated_waste_cost_usd),
                `<strong>${formatCurrency(row.total_cost_attribution_usd)}</strong>`
            ]);

            // Aggregate slots
            if (!projectSlots[row.project_id]) projectSlots[row.project_id] = 0;
            projectSlots[row.project_id] += row.slot_hours || 0;

            // Aggregate costs for finding top spenders
            if (!projectCosts[row.project_id]) projectCosts[row.project_id] = 0;
            projectCosts[row.project_id] += row.total_cost_attribution_usd || 0;
        });
        
        table.draw();

        // Render Slot Usage by Project Table
        let slotTable;
        if ($.fn.DataTable.isDataTable('#slot-usage-by-project-table')) {
            slotTable = $('#slot-usage-by-project-table').DataTable();
        } else {
            slotTable = $('#slot-usage-by-project-table').DataTable({
                pageLength: 5,
                order: [[1, 'desc']],
                responsive: true
            });
        }
        slotTable.clear();

        for (const [projId, slots] of Object.entries(projectSlots)) {
            slotTable.row.add([
                projId,
                `${slots.toFixed(2)} hrs`
            ]);
        }
        slotTable.draw();

        // Extract Top 5 Spenders for HBO
        const sortedProjects = Object.entries(projectCosts)
            .sort((a, b) => b[1] - a[1])
            .map(entry => entry[0])
            .filter(proj => !proj.startsWith('res-') && !proj.includes(':') && !proj.includes('.'));
        
        state.top5Projects = sortedProjects.slice(0, 5);
    };

    if (elements.btnAnalyzeProfiler) {
        elements.btnAnalyzeProfiler.addEventListener('click', async () => {
            console.log("Profiler button clicked!");
            if (!state.orgProject) {
                showNotification('Please configure settings first.', 'error');
                Router.navigate('settings');
                return;
            }

            setLoading(elements.btnAnalyzeProfiler, true);
            clearModuleCache(['bq_profiler_summary', 'bq_profiler_timeline', 'bq_profiler_queries'], ['#profiler-summary-table', '#profiler-timeline-table', '#profiler-queries-table']);

            const params = {
                org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
                region: state.region,
                focus_projects: state.focusProjects,
                lookback_days: parseInt(elements.slLookback.value) || 7,
                admin_project_id: state.adminProject
            };

            try {
                const response = await fetch('/api/slots/profiler', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(buildPayload('/api/slots/profiler', params))
                });

                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(errorData.detail || 'Failed to analyze workload profile');
                }

                const data = await response.json();
                renderProfilerResults(data.summary);
                renderHeatmap(data.timeline);
                safeSetLocalStorage('bq_profiler_summary', JSON.stringify(data.summary));
                safeSetLocalStorage('bq_profiler_timeline', JSON.stringify(data.timeline));
                
                // Fetch top queries
                const queriesResponse = await fetch('/api/slots/profiler/queries', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(buildPayload('/api/slots/profiler/queries', params))
                });
                
                if (queriesResponse.ok) {
                    const queriesData = await queriesResponse.json();
                    renderProfilerQueries(queriesData);
                    safeSetLocalStorage('bq_profiler_queries', JSON.stringify(queriesData));
                }
                showNotification('Workload profile analysis completed.', 'success');
            } catch (error) {
                console.error("Profiler Error:", error);
                showNotification(error.message, 'error');
            } finally {
                setLoading(elements.btnAnalyzeProfiler, false);
            }
        });
    }

    if (elements.btnAnalyzeUsers) {
        elements.btnAnalyzeUsers.addEventListener('click', async () => {
            if (!state.orgProject) {
                showNotification('Please configure settings first.', 'error');
                Router.navigate('settings');
                return;
            }

            setLoading(elements.btnAnalyzeUsers, true);
            clearModuleCache(['bq_top_spenders'], ['#top-spenders-table']);

            const params = {
                org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
                region: state.region,
                focus_projects: state.focusProjects,
                lookback_days: parseInt(elements.slLookback.value) || 7,
                admin_project_id: state.adminProject,
                od_price: parseFloat(document.getElementById('jb-od-rate').value) || 6.25,
                ed_price: parseFloat(document.getElementById('jb-ed-rate').value) || 0.06
            };

            try {
                const response = await fetch('/api/users/top_spenders', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(buildPayload('/api/users/top_spenders', params))
                });

                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(errorData.detail || 'Failed to analyze top spenders');
                }

                const data = await response.json();
                renderTopSpenders(data);
                safeSetLocalStorage('bq_top_spenders', JSON.stringify(data));
                showNotification('Top spenders analysis completed.', 'success');
            } catch (error) {
                console.error("Top Spenders Error:", error);
                showNotification(error.message, 'error');
            } finally {
                setLoading(elements.btnAnalyzeUsers, false);
            }
        });
    }

    const renderTopSpenders = (data) => {
        const tbody = document.querySelector('#top-spenders-table tbody');
        if (!tbody) return;
        tbody.innerHTML = '';

        data.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${row.user_email}</td>
                <td>${formatNumber(row.query_count)}</td>
                <td>${formatNumber(row.total_bytes_billed / (1024**4))} TiB</td>
                <td>${formatNumber(row.total_slot_hours)}</td>
                <td>${formatCurrency(row.est_on_demand_cost)}</td>
                <td>${formatCurrency(row.est_editions_cost)}</td>
            `;
            tbody.appendChild(tr);
        });

        // Initialize DataTable
        if ($.fn.DataTable.isDataTable('#top-spenders-table')) {
            $('#top-spenders-table').DataTable().destroy();
        }
        $('#top-spenders-table').DataTable({ pageLength: 10, order: [[2, 'desc']], responsive: true });
    };

    // Build a DOM node for the optimization badges cell.
    // Uses createElement + textContent to avoid XSS (§6.2).
    const _buildBadgeCell = (optimizations) => {
        const container = document.createElement('div');
        container.className = 'opt-badges';

        // null = undetermined
        if (optimizations === null || optimizations === undefined) {
            const span = document.createElement('span');
            span.className = 'opt-none';
            span.textContent = '—';
            span.title = 'Could not determine (permissions or scope)';
            container.appendChild(span);
            return container;
        }

        // [] = checked, none applied
        if (Array.isArray(optimizations) && optimizations.length === 0) {
            const span = document.createElement('span');
            span.className = 'opt-none';
            span.textContent = 'None detected';
            container.appendChild(span);
            return container;
        }

        // Array of badge objects
        if (Array.isArray(optimizations)) {
            optimizations.forEach(badge => {
                const chip = document.createElement('span');
                // Validate category to a CSS class — whitelist only
                const validCats = ['hbo', 'engine', 'unknown'];
                const cat = validCats.includes(badge.category) ? badge.category : 'unknown';
                chip.className = `opt-badge opt-badge--${cat}`;
                chip.textContent = badge.label || badge.key || 'Unknown';
                chip.title = badge.description || '';
                container.appendChild(chip);
            });
            return container;
        }

        // Fallback — loading state
        const span = document.createElement('span');
        span.className = 'opt-none';
        span.textContent = 'Loading…';
        container.appendChild(span);
        return container;
    };

    // Store the last analyze data so enrichment can correlate
    let _lastAnalyzeData = null;

    const clearLoadingBadges = () => {
        const tableEl = document.querySelector('#hbo-results-table');
        if (!tableEl) return;
        tableEl.querySelectorAll('tbody tr td:last-child').forEach(td => {
            if (td.textContent.trim() === 'Loading…' || td.textContent.trim() === 'Loading...') {
                td.innerHTML = '<span class="opt-none">—</span>';
            }
        });
    };

    const renderHboResults = (data, enrichmentData = null) => {
        _lastAnalyzeData = data;
        let table;
        if ($.fn.DataTable.isDataTable('#hbo-results-table')) {
            table = $('#hbo-results-table').DataTable();
        } else {
            table = $('#hbo-results-table').DataTable({
                pageLength: 10,
                order: [[1, 'desc']],
                responsive: true,
                columnDefs: [
                    { targets: 4, orderable: false }
                ]
            });
        }

        table.clear();

        const lookup = {};
        if (enrichmentData && Array.isArray(enrichmentData.jobs)) {
            enrichmentData.jobs.forEach(j => {
                lookup[j.job_id] = j.optimizations;
            });
        }

        let totalSlotsSaved = 0;
        let totalDollarsSaved = 0;

        data.forEach(row => {
            totalSlotsSaved += row.saved_slot_hours || 0;
            totalDollarsSaved += row.estimated_savings_usd || 0;

            const optValue = (row.job_id in lookup) ? lookup[row.job_id] : (row.optimizations !== undefined ? row.optimizations : '_loading');
            const badgeNode = _buildBadgeCell(optValue);
            table.row.add([
                row.job_id,
                `${row.percent_execution_time_saved.toFixed(2)}%`,
                row.new_elapsed_ms.toLocaleString(),
                row.original_elapsed_ms.toLocaleString(),
                badgeNode.outerHTML
            ]);
        });

        table.draw();

        // Tile writing moved out of renderHboResults. The live handler
        // uses org-wide /api/hbo/summary data for tiles, not this top-10 slice.
        // Keeping tile writes here caused a ~300× discrepancy on reload.
    };

    // Apply enrichment badges to the existing table rows.
    const applyOptimizationBadges = (enrichmentData) => {
        if (!enrichmentData || !enrichmentData.jobs) return;

        // Build lookup: job_id -> optimizations
        const lookup = {};
        enrichmentData.jobs.forEach(j => {
            lookup[j.job_id] = j.optimizations;
        });

        // Update table cells
        const tableEl = document.querySelector('#hbo-results-table');
        if (!tableEl) return;
        const rows = tableEl.querySelectorAll('tbody tr');
        rows.forEach(tr => {
            const jobIdCell = tr.querySelector('td:first-child');
            const badgeCell = tr.querySelector('td:last-child');
            if (!jobIdCell || !badgeCell) return;

            const jobId = jobIdCell.textContent.trim();
            if (jobId in lookup) {
                badgeCell.innerHTML = '';
                badgeCell.appendChild(_buildBadgeCell(lookup[jobId]));
            }
        });

        clearLoadingBadges();

        // Show coverage warning if any projects were inaccessible
        if (enrichmentData.coverage && enrichmentData.coverage.inaccessible_projects &&
            enrichmentData.coverage.inaccessible_projects.length > 0) {
            const names = enrichmentData.coverage.inaccessible_projects
                .map(p => p.project_id).join(', ');
            console.warn(`HBO enrichment: inaccessible projects: ${names}. ` +
                `Grant roles/bigquery.resourceViewer on each project for full coverage.`);
        }
    };

    const renderHboStatus = (data) => {
        const panel = elements.hboStatusPanel;
        const tbody = elements.hboStatusList;
        const summary = elements.hboStatusSummary;
        const pagination = elements.hboStatusPagination;
        if (!panel || !tbody) return;

        panel.style.display = 'block';

        const PAGE_SIZE = 10;
        let currentPage = 1;
        const totalPages = Math.max(1, Math.ceil(data.length / PAGE_SIZE));

        // Summary counts
        const enabled = data.filter(d => d.enabled === true).length;
        const disabled = data.filter(d => d.enabled === false).length;
        const errors = data.filter(d => d.error).length;
        if (summary) {
            summary.textContent = `${enabled} enabled · ${disabled} disabled${errors ? ` · ${errors} errors` : ''} — ${data.length} projects`;
        }

        function renderPage(page) {
            currentPage = page;
            tbody.innerHTML = '';
            const start = (page - 1) * PAGE_SIZE;
            const slice = data.slice(start, start + PAGE_SIZE);

            slice.forEach(item => {
                const tr = document.createElement('tr');

                // Project cell
                const tdProject = document.createElement('td');
                tdProject.style.fontWeight = '500';
                tdProject.textContent = item.project_id;

                // Status cell
                const tdStatus = document.createElement('td');
                if (item.error) {
                    tdStatus.innerHTML = `<span style="display:inline-flex;align-items:center;gap:4px;padding:3px 10px;border-radius:12px;font-size:0.8rem;background:rgba(148,163,184,0.15);color:#94a3b8;"><i class="fa-solid fa-circle-question" style="font-size:0.7rem;"></i> Error</span>`;
                } else if (item.enabled) {
                    tdStatus.innerHTML = `<span style="display:inline-flex;align-items:center;gap:4px;padding:3px 10px;border-radius:12px;font-size:0.8rem;background:rgba(74,222,128,0.12);color:#4ade80;"><i class="fa-solid fa-circle-check" style="font-size:0.7rem;"></i> Enabled</span>`;
                } else {
                    tdStatus.innerHTML = `<span style="display:inline-flex;align-items:center;gap:4px;padding:3px 10px;border-radius:12px;font-size:0.8rem;background:rgba(250,204,21,0.12);color:#facc15;"><i class="fa-solid fa-circle-exclamation" style="font-size:0.7rem;"></i> Disabled</span>`;
                }

                // Action cell
                const tdAction = document.createElement('td');
                if (item.error) {
                    tdAction.innerHTML = `<span style="color:#94a3b8;font-size:0.8rem;" title="${item.error}">Permission error</span>`;
                } else if (item.enabled) {
                    tdAction.innerHTML = `<span style="color:#64748b;font-size:0.8rem;">—</span>`;
                } else if (item.ddl) {
                    const btn = document.createElement('button');
                    btn.className = 'btn-action';
                    btn.style.fontSize = '0.75rem';
                    btn.style.padding = '4px 10px';
                    btn.textContent = 'Copy Enable DDL';
                    btn.title = item.ddl;
                    btn.addEventListener('click', () => {
                        copyToClipboard(item.ddl).then(() => {
                            btn.textContent = 'Copied!';
                            setTimeout(() => { btn.textContent = 'Copy Enable DDL'; }, 2000);
                        });
                    });
                    tdAction.appendChild(btn);
                } else {
                    tdAction.innerHTML = `<span style="color:#64748b;font-size:0.8rem;">—</span>`;
                }

                tr.appendChild(tdProject);
                tr.appendChild(tdStatus);
                tr.appendChild(tdAction);
                tbody.appendChild(tr);
            });

            renderPagination();
        }

        function renderPagination() {
            if (!pagination) return;
            pagination.innerHTML = '';
            if (totalPages <= 1) return;

            const btnStyle = (active) => `
                padding: 4px 10px; border-radius: 6px; border: 1px solid rgba(255,255,255,0.1);
                background: ${active ? 'rgba(56,189,248,0.2)' : 'rgba(15,23,42,0.5)'};
                color: ${active ? '#38bdf8' : '#94a3b8'}; cursor: pointer; font-size: 0.8rem;
                font-weight: ${active ? '600' : '400'};
            `;

            for (let p = 1; p <= totalPages; p++) {
                const btn = document.createElement('button');
                btn.textContent = p;
                btn.style.cssText = btnStyle(p === currentPage);
                btn.addEventListener('click', () => renderPage(p));
                pagination.appendChild(btn);
            }
        }

        renderPage(1);
    };

    if (elements.btnAnalyzeHbo) {
        elements.btnAnalyzeHbo.addEventListener('click', async () => {
            if (!state.orgProject) {
                showNotification('Please configure settings first.', 'error');
                Router.navigate('settings');
                return;
            }

            setLoading(elements.btnAnalyzeHbo, true);
            clearModuleCache(['bq_hbo_results', 'bq_hbo_status', 'bq_hbo_summary', 'bq_hbo_optimizations'], ['#hbo-results-table']);

            const projectOverride = document.getElementById('hbo-project-override')?.value;
            const lookbackOverride = document.getElementById('hbo-lookback-override')?.value;

            const targetProject = projectOverride || state.orgProject;

            const baseParams = {
                org_project_id: targetProject,
                region: state.region,
                focus_projects: state.focusProjects,
                lookback_days: lookbackOverride ? parseInt(lookbackOverride) : (parseInt(elements.slLookback.value) || 30)
            };

            const analyzeParams = { ...baseParams, limit: 10 };

            debug_log("Fetching HBO analysis with params:", analyzeParams);

            try {
                const [analyzeRes, statusRes, summaryRes] = await Promise.all([
                    fetch('/api/hbo/analyze', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(buildPayload('/api/hbo/analyze', analyzeParams))
                    }),
                    fetch('/api/hbo/status', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(buildPayload('/api/hbo/status', baseParams))
                    }),
                    fetch('/api/hbo/summary', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(buildPayload('/api/hbo/summary', baseParams))
                    })
                ]);

                if (!analyzeRes.ok || !statusRes.ok || !summaryRes.ok) {
                    throw new Error('One or more API calls failed');
                }

                const analyzeData = await analyzeRes.json();
                const statusData = await statusRes.json();
                const summaryData = await summaryRes.json();

                const slicedData = analyzeData.slice(0, 10);
                renderHboResults(slicedData);
                renderHboStatus(statusData);

                // Update tiles
                const slotsEl = document.getElementById('hbo-total-slots');
                const dollarsEl = document.getElementById('hbo-total-dollars');
                if (slotsEl) slotsEl.textContent = formatNumber(summaryData.total_saved_slot_hours || 0);
                if (dollarsEl) dollarsEl.textContent = formatCurrency(summaryData.total_estimated_savings_usd || 0);

                safeSetLocalStorage('bq_hbo_results', JSON.stringify(slicedData));
                safeSetLocalStorage('bq_hbo_status', JSON.stringify(statusData));
                // Cache the summary so tiles survive reload.
                safeSetLocalStorage('bq_hbo_summary', JSON.stringify(summaryData));

                // Non-blocking: enrich jobs with optimization type badges.
                // This is progressive enhancement — the table is already
                // rendered and usable without it.
                const refs = slicedData
                    .filter(r => r.project_id && r.job_id && r.creation_time)
                    .map(r => ({
                        project_id: r.project_id,
                        job_id: r.job_id,
                        creation_time: r.creation_time
                    }));
                if (refs.length > 0) {
                    fetch('/api/hbo/optimizations', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(buildPayload('/api/hbo/optimizations', {
                            ...baseParams,
                            jobs: refs
                        }))
                    })
                    .then(r => r.ok ? r.json() : Promise.reject(r))
                    .then(optData => {
                        applyOptimizationBadges(optData);
                        safeSetLocalStorage('bq_hbo_optimizations', JSON.stringify(optData));
                    })
                    .catch(e => {
                        console.warn('Optimization badges unavailable:', e);
                        clearLoadingBadges();
                    });
                } else {
                    // No enrichable refs (missing project_id/creation_time) — clear loading state
                    clearLoadingBadges();
                }

                showNotification('HBO analysis completed for the organization.', 'success');
            } catch (error) {
                console.error("HBO Error:", error);
                showNotification(error.message, 'error');
            } finally {
                setLoading(elements.btnAnalyzeHbo, false);
            }
        });
    }

    // Performance Insights
    const btnAnalyzePerformance = document.getElementById('analyze-performance-btn');
    if (btnAnalyzePerformance) {
        btnAnalyzePerformance.addEventListener('click', async () => {
            if (!state.orgProject) {
                showNotification('Please configure settings first.', 'error');
                Router.navigate('settings');
                return;
            }

            setLoading(btnAnalyzePerformance, true);
            clearModuleCache(['bq_performance_results'], ['#performance-results-table']);

            const projectOverride = document.getElementById('perf-project-override')?.value;
            const lookbackOverride = document.getElementById('perf-lookback-override')?.value;

            const params = {
                org_project_id: projectOverride || state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
                region: state.region,
                focus_projects: state.focusProjects,
                lookback_days: lookbackOverride ? parseInt(lookbackOverride) : 7
            };

            try {
                const response = await fetch('/api/hbo/performance_insights', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(buildPayload('/api/hbo/performance_insights', params))
                });

                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(errorData.detail || 'Failed to analyze performance insights');
                }

                const data = await response.json();
                renderPerformanceResults(data);
                safeSetLocalStorage('bq_performance_results', JSON.stringify(data));
                showNotification('Performance insights analysis completed.', 'success');
            } catch (error) {
                console.error("Performance Insights Error:", error);
                showNotification(error.message, 'error');
            } finally {
                setLoading(btnAnalyzePerformance, false);
            }
        });
    }

    const renderPerformanceResults = (data) => {
        // Render Slot Contention
        if ($.fn.DataTable.isDataTable('#slot-contention-table')) {
            $('#slot-contention-table').DataTable().destroy();
        }
        const contentionTbody = document.querySelector('#slot-contention-table tbody');
        if (contentionTbody) {
            contentionTbody.innerHTML = '';
            (data.slot_contention_jobs || []).forEach(row => {
                const tr = document.createElement('tr');
                tr.innerHTML = `
                    <td>${row.job_id}</td>
                    <td>${row.user_email}</td>
                    <td>${row.project_id}</td>
                    <td>${row.stage_id}</td>
                `;
                contentionTbody.appendChild(tr);
            });
        }
        $('#slot-contention-table').DataTable({ pageLength: 5, responsive: true, order: [] });

        // Render Shuffle Quota
        if ($.fn.DataTable.isDataTable('#shuffle-quota-table')) {
            $('#shuffle-quota-table').DataTable().destroy();
        }
        const shuffleTbody = document.querySelector('#shuffle-quota-table tbody');
        if (shuffleTbody) {
            shuffleTbody.innerHTML = '';
            (data.shuffle_quota_jobs || []).forEach(row => {
                const tr = document.createElement('tr');
                tr.innerHTML = `
                    <td>${row.job_id}</td>
                    <td>${row.user_email}</td>
                    <td>${row.project_id}</td>
                    <td>${row.stage_id}</td>
                `;
                shuffleTbody.appendChild(tr);
            });
        }
        $('#shuffle-quota-table').DataTable({ pageLength: 5, responsive: true, order: [] });

        // Render Data Volume
        if ($.fn.DataTable.isDataTable('#data-volume-table')) {
            $('#data-volume-table').DataTable().destroy();
        }
        const volumeTbody = document.querySelector('#data-volume-table tbody');
        if (volumeTbody) {
            volumeTbody.innerHTML = '';
            (data.data_volume_jobs || []).forEach(row => {
                const tr = document.createElement('tr');
                tr.innerHTML = `
                    <td>${row.job_id}</td>
                    <td>${row.user_email}</td>
                    <td>${row.project_id}</td>
                    <td class="text-danger" style="font-weight: 600;">${formatDiffPct(row.diff_pct)}</td>
                `;
                volumeTbody.appendChild(tr);
            });
        }
        $('#data-volume-table').DataTable({ pageLength: 5, responsive: true, order: [] });
    };

    const cachedHboResults = localStorage.getItem('bq_hbo_results');
    if (cachedHboResults) {
        try {
            const parsedResults = JSON.parse(cachedHboResults);
            const cachedOptBadges = localStorage.getItem('bq_hbo_optimizations');
            let parsedOpt = null;
            if (cachedOptBadges) {
                try { parsedOpt = JSON.parse(cachedOptBadges); } catch (e2) { console.warn('Failed to parse cached HBO optimizations', e2); }
            }
            renderHboResults(parsedResults, parsedOpt);
            if (parsedOpt) {
                applyOptimizationBadges(parsedOpt);
            } else {
                clearLoadingBadges();
            }
        } catch (e) { console.warn("Failed to parse cached HBO results", e); }
    }
    const cachedHboStatus = localStorage.getItem('bq_hbo_status');
    if (cachedHboStatus) {
        try {
            renderHboStatus(JSON.parse(cachedHboStatus));
        } catch (e) { console.warn("Failed to parse cached HBO status", e); }
    }
    // Restore tiles from cached summary instead of recomputing from top-10.
    const cachedHboSummary = localStorage.getItem('bq_hbo_summary');
    if (cachedHboSummary) {
        try {
            const summaryData = JSON.parse(cachedHboSummary);
            const slotsEl = document.getElementById('hbo-total-slots');
            const dollarsEl = document.getElementById('hbo-total-dollars');
            if (slotsEl) slotsEl.textContent = formatNumber(summaryData.total_saved_slot_hours || 0);
            if (dollarsEl) dollarsEl.textContent = formatCurrency(summaryData.total_estimated_savings_usd || 0);
        } catch (e) { console.warn("Failed to parse cached HBO summary", e); }
    }

    const renderHygieneResults = (data) => {
        const tbody = document.querySelector('#hygiene-results-table tbody');
        if (!tbody) return;
        tbody.innerHTML = '';

        let totalSize = 0;
        let totalTtSize = 0;
        let highChurnCount = 0;
        let potentialSavings = 0;
        const actPhyRate = parseFloat(document.getElementById('st-act-phy').value) || 0.04;

        data.forEach(row => {
            totalSize += row.live_active_physical_gb || 0;
            totalTtSize += row.time_travel_gb || 0;
            if (row.health_status && row.health_status.toUpperCase() === 'HIGH CHURN/RECREATE DETECTED') {
                highChurnCount++;
                const wastedSize = (5 / 7) * (row.time_travel_gb || 0);
                potentialSavings += wastedSize * actPhyRate;
            }

            const tr = document.createElement('tr');
            const badgeBg = row.health_status === 'Healthy' ? 'rgba(34, 197, 94, 0.15)' : 'rgba(239, 68, 68, 0.15)';
            const badgeColor = row.health_status === 'Healthy' ? '#22c55e' : '#ef4444';
            const churnPct = row.churn_ratio != null ? (row.churn_ratio * 100).toFixed(1) + '%' : 'N/A';

            tr.innerHTML = `
                <td>${row.project_id || ''}</td>
                <td>${row.dataset}</td>
                <td>${row.table_name}</td>
                <td>${(row.live_active_physical_gb || 0).toFixed(2)}</td>
                <td>${(row.time_travel_gb || 0).toFixed(2)}</td>
                <td>${churnPct}</td>
                <td><span class="badge" style="background: ${badgeBg}; color: ${badgeColor}; font-weight: 600;">${row.health_status}</span></td>
            `;
            tbody.appendChild(tr);
        });

        // Update KPIs
        const totalSizeEl = document.getElementById('hygiene-total-size');
        const totalTtSizeEl = document.getElementById('hygiene-total-tt-size');
        const tableCountEl = document.getElementById('hygiene-table-count');
        const highChurnCountEl = document.getElementById('hygiene-high-churn-count');
        const savingsEl = document.getElementById('hygiene-total-savings');

        const formatStorageSize = (sizeInGiB) => {
            if (sizeInGiB >= 1024) {
                return `${(sizeInGiB / 1024).toFixed(1)} TiB`;
            }
            return `${sizeInGiB.toFixed(0)} GiB`;
        };

        if (totalSizeEl) totalSizeEl.textContent = formatStorageSize(totalSize);
        if (savingsEl) savingsEl.textContent = `$${potentialSavings.toFixed(0)}`;
        if (totalTtSizeEl) totalTtSizeEl.textContent = formatStorageSize(totalTtSize);
        if (tableCountEl) tableCountEl.textContent = data.length;
        if (highChurnCountEl) highChurnCountEl.textContent = highChurnCount;

        if ($.fn.DataTable.isDataTable('#hygiene-results-table')) {
            $('#hygiene-results-table').DataTable().destroy();
        }
        $('#hygiene-results-table').DataTable({ pageLength: 10, order: [[6, 'desc']], responsive: true });
    };

    if (elements.btnAnalyzeHygiene) {
        elements.btnAnalyzeHygiene.addEventListener('click', async () => {
            if (!state.orgProject) {
                showNotification('Please configure settings first.', 'error');
                Router.navigate('settings');
                return;
            }

            setLoading(elements.btnAnalyzeHygiene, true);
            clearModuleCache(['bq_hygiene_results'], ['#hygiene-results-table']);

            const params = {
                org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
                region: state.region,
                focus_projects: state.focusProjects,
                limit: 20
            };

            try {
                const response = await fetch('/api/storage/hygiene', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(buildPayload('/api/storage/hygiene', params))
                });

                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(errorData.detail || 'Failed to analyze storage hygiene');
                }

                const data = await response.json();
                renderHygieneResults(data);
                safeSetLocalStorage('bq_hygiene_results', JSON.stringify(data));
                showNotification('Storage hygiene analysis completed.', 'success');
            } catch (error) {
                console.error("Hygiene Error:", error);
                showNotification(error.message, 'error');
            } finally {
                setLoading(elements.btnAnalyzeHygiene, false);
            }
        });
    }

    const cachedHygieneResults = localStorage.getItem('bq_hygiene_results');
    if (cachedHygieneResults) {
        try {
            renderHygieneResults(JSON.parse(cachedHygieneResults));
        } catch (e) { console.warn("Failed to parse cached hygiene results", e); }
    }

    // Minimal comment/keyword highlighter for the remediation snippets.
    // Escapes first, then decorates — never inject un-escaped snippet text.
    const highlightSnippet = (snippet) => escapeHtmlAttr(snippet)
        .replace(/^(\s*#.*)$/gm, '<span style="color: #64748b;">$1</span>')
        .replace(/\b(BATCH|INTERACTIVE|batch|interactive)\b/g, '<span style="color: #fbbf24;">$1</span>');

    // Global Remediation Modal Renderer
    window.showRemediationModal = function (name, type, recPriority) {
        let snippet = '';
        const isBatch = recPriority === 'BATCH';
        if (type.includes('Dataform') || type.includes('Scheduled Query')) {
            snippet = `# Architecture Migration Required
# Tooling Constraint:
# ${type} does not currently support native BATCH priority execution flags.

# Recommended Workaround:
# Migrate orchestration to Google Cloud Composer (Airflow) or Cloud Workflows,
# which provide native job configuration overrides for BigQuery execution priority.`;
        } else if (type.includes('dbt')) {
            snippet = `# profiles.yml
target: prod
outputs:
  prod:
    type: bigquery
    project: your-project-id
    dataset: analytics
    priority: ${isBatch ? 'batch' : 'interactive'} # Sets all dbt query executions to ${recPriority}`;
        } else if (type.includes('Airflow')) {
            snippet = `# Airflow DAG Operator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

run_task = BigQueryInsertJobOperator(
    task_id="transform_task",
    configuration={
        "query": {
            "query": "MERGE INTO ...",
            "priority": "${recPriority}", # Configures execution priority
        }
    },
)`;
        } else {
            snippet = `# Python SDK & bq CLI
# Python:
job_config = bigquery.QueryJobConfig(priority=bigquery.QueryPriority.${recPriority})
query_job = client.query("...", job_config=job_config)

# bq CLI:
${isBatch ? "bq query --batch --use_legacy_sql=false 'MERGE INTO ...'" : "bq query --use_legacy_sql=false 'SELECT ...'"}`;
        }

        const existing = document.getElementById('remediation-modal-overlay');
        if (existing) existing.remove();

        const overlay = document.createElement('div');
        overlay.id = 'remediation-modal-overlay';
        overlay.style.cssText = 'position: fixed; top: 0; left: 0; width: 100vw; height: 100vh; background: rgba(0,0,0,0.7); display: flex; align-items: center; justify-content: center; z-index: 10000;';
        overlay.innerHTML = `
            <div style="background: #0f172a; border: 1px solid rgba(255,255,255,0.15); border-radius: 0.75rem; width: 600px; max-width: 92%; padding: 1.5rem; color: #f8fafc; box-shadow: 0 25px 50px -12px rgba(0,0,0,0.6);">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                    <h3 style="margin: 0; font-size: 1.1rem; color: #60a5fa;"><i class="fa-solid fa-code" style="margin-right: 0.5rem;"></i>Remediation Snippet</h3>
                    <button id="close-remediation-btn" style="background: none; border: none; color: #94a3b8; font-size: 1.2rem; cursor: pointer; padding: 0.25rem 0.5rem; border-radius: 4px;">&times;</button>
                </div>
                <p style="font-size: 0.85rem; color: #94a3b8; margin-bottom: 0.75rem;">
                    Workload: <strong style="color: #e2e8f0;">${escapeHtmlAttr(name)}</strong> <span style="color: #64748b;">(${escapeHtmlAttr(type)})</span><br/>
                    Recommended Priority: <span class="badge ${isBatch ? 'warning' : 'primary'}" style="font-size: 0.75rem;">${escapeHtmlAttr(recPriority)}</span>
                </p>
                <div style="position: relative;">
                    <pre style="background: #0c1222; padding: 1.25rem; border-radius: 0.5rem; border: 1px solid rgba(255,255,255,0.06); font-family: monospace; font-size: 0.82rem; line-height: 1.65; overflow-x: auto; white-space: pre-wrap; margin: 0;">${highlightSnippet(snippet)}</pre>
                </div>
                <div style="display: flex; justify-content: flex-end; margin-top: 1rem;">
                    <button id="copy-remediation-btn" class="btn-primary btn-sm"><i class="fa-solid fa-copy" style="margin-right: 0.35rem;"></i>Copy Snippet</button>
                </div>
            </div>
        `;
        document.body.appendChild(overlay);

        document.getElementById('close-remediation-btn').addEventListener('click', () => overlay.remove());
        overlay.addEventListener('click', (e) => { if (e.target === overlay) overlay.remove(); });
        document.getElementById('copy-remediation-btn').addEventListener('click', function () {
            // copyToClipboard, not navigator.clipboard directly: the latter is
            // undefined on non-secure origins (local dev on 0.0.0.0), where a bare
            // call throws and the button silently does nothing.
            copyToClipboard(snippet).then(() => {
                this.innerHTML = '<i class="fa-solid fa-check" style="margin-right: 0.35rem;"></i>Copied!';
                setTimeout(() => { this.innerHTML = '<i class="fa-solid fa-copy" style="margin-right: 0.35rem;"></i>Copy Snippet'; }, 2000);
            }).catch(() => {
                if (typeof showNotification === 'function') {
                    showNotification('Copy failed — select the snippet and copy manually.', 'warning');
                }
            });
        });
    };

    // Body-level singleton for the detection-reason popover. Lives outside the
    // results card so no `overflow` or transformed ancestor can clip it, and is
    // built with textContent so nothing in the payload can be interpreted as markup.
    const batchTooltip = (() => {
        let node = null;
        let anchoredTo = null;

        const ensure = () => {
            if (!node) {
                node = document.createElement('div');
                node.className = 'batch-tooltip-popup';
                document.body.appendChild(node);
            }
            return node;
        };

        const hide = () => {
            anchoredTo = null;
            if (node) node.classList.remove('is-visible');
        };

        const show = (wrap) => {
            // mouseover re-fires for every descendant the pointer crosses; without
            // this the popup would rebuild and restart its fade on each one.
            if (anchoredTo === wrap) return;
            anchoredTo = wrap;

            const tip = ensure();
            tip.textContent = '';

            const title = document.createElement('div');
            title.style.cssText = 'font-weight: 600; margin-bottom: 0.4rem; color: #f8fafc;';
            title.textContent = wrap.dataset.tipTitle || '';
            tip.appendChild(title);

            const summary = document.createElement('div');
            summary.style.cssText = 'margin-bottom: 0.5rem; line-height: 1.5;';
            summary.textContent = wrap.dataset.tipSummary || '';
            tip.appendChild(summary);

            const reasons = (wrap.dataset.tipReasons || '').split('\n').filter(Boolean);
            if (reasons.length) {
                const block = document.createElement('div');
                block.style.cssText = 'border-top: 1px solid rgba(255,255,255,0.08); padding-top: 0.4rem; font-size: 0.75rem; color: #94a3b8;';
                const heading = document.createElement('strong');
                heading.style.color = '#cbd5e1';
                heading.textContent = 'Detection Reasons:';
                block.appendChild(heading);
                reasons.forEach(text => {
                    const line = document.createElement('div');
                    line.textContent = `• ${text}`;
                    block.appendChild(line);
                });
                tip.appendChild(block);
            }

            // Measure before placing: the popup flips below the badge when there
            // is not enough room above, and is clamped to the viewport sideways.
            tip.classList.add('is-visible');
            const anchor = wrap.getBoundingClientRect();
            const tipRect = tip.getBoundingClientRect();
            const GAP = 8;
            const MARGIN = 8;

            let top = anchor.top - tipRect.height - GAP;
            if (top < MARGIN) top = anchor.bottom + GAP;

            let left = anchor.left + (anchor.width / 2) - (tipRect.width / 2);
            left = Math.max(MARGIN, Math.min(left, window.innerWidth - tipRect.width - MARGIN));

            tip.style.top = `${Math.round(top)}px`;
            tip.style.left = `${Math.round(left)}px`;
        };

        return { show, hide };
    })();
    // Fixed coordinates go stale the moment anything moves underneath them.
    window.addEventListener('scroll', batchTooltip.hide, true);
    window.addEventListener('resize', batchTooltip.hide);

    const renderBatchCandidatesResults = (data) => {
        const tbody = document.querySelector('#batch-candidates-results-table tbody');
        if (!tbody) return;
        batchTooltip.hide();
        tbody.innerHTML = '';

        // Cached payloads from before the workload engine used a per-job shape.
        // Drop them rather than emit rows with the wrong column count, which
        // would break DataTables initialisation.
        const rows = (data || []).filter(r => r && r.workload_name && r.finding_category);

        rows.forEach(row => {
            const tr = document.createElement('tr');
            const reasons = (row.detection_reasons || []).join('\n');

            const isUnder = row.finding_category === 'UNDER_BATCHED';
            const badgeClass = isUnder ? 'danger' : 'warning';
            const badgeIcon = isUnder ? 'fa-triangle-exclamation' : 'fa-hourglass-half';
            const badgeLabel = isUnder ? 'UNDER_BATCHED' : 'OVER_BATCHED';
            const summary = isUnder
                ? 'Automated pipeline running in INTERACTIVE mode. Risks starving live dashboards of the 100-query concurrent limit. Switch to BATCH for auto-retry protection at identical pricing.'
                : 'Human/BI workload facing >30s BATCH queue lag. Switch to INTERACTIVE for instant slot allocation.';

            // The popup body travels as data-* rather than as a hidden child of the
            // cell: it is rendered into a single body-level node on hover (see
            // batchTooltip), and a hidden child would also be swept into the CSV
            // export, which reads cell text via textContent.
            const categoryBadge = `
                <span class="batch-tooltip-wrap" data-tip-title="${escapeHtmlAttr(badgeLabel)}" data-tip-summary="${escapeHtmlAttr(summary)}" data-tip-reasons="${escapeHtmlAttr(reasons)}">
                    <span class="badge ${badgeClass}" style="cursor: help;">
                        <i class="fa-solid ${badgeIcon}" style="margin-right: 0.25rem;"></i>${badgeLabel}
                    </span>
                </span>`;

            const confBadge = row.confidence === 'HIGH'
                ? `<span class="badge success" style="font-size: 0.7rem; margin-left: 0.3rem;">HIGH CONF</span>`
                : `<span class="badge secondary" style="font-size: 0.7rem; margin-left: 0.3rem;">LOW CONF</span>`;

            const actionBtn = row.has_remediation
                ? `<button class="btn-secondary btn-sm btn-remediation" data-wname="${escapeHtmlAttr(row.workload_name)}" data-wtype="${escapeHtmlAttr(row.workload_type)}" data-wpriority="${escapeHtmlAttr(row.recommended_priority)}"><i class="fa-solid fa-code" style="margin-right: 0.25rem;"></i>Snippet</button>`
                : `<button class="btn-secondary btn-sm btn-remediation" data-wname="${escapeHtmlAttr(row.workload_name)}" data-wtype="${escapeHtmlAttr(row.workload_type)}" data-wpriority="${escapeHtmlAttr(row.recommended_priority)}"><i class="fa-solid fa-lightbulb" style="margin-right: 0.25rem;"></i>Guidance</button>`;

            tr.innerHTML = `
                <td><strong>${escapeHtmlAttr(row.workload_name)}</strong> ${confBadge}</td>
                <td><span class="badge logical">${escapeHtmlAttr(row.workload_type)}</span></td>
                <td>${escapeHtmlAttr(row.project_id)}</td>
                <td data-order="${row.total_job_runs}">${row.total_job_runs.toLocaleString()}</td>
                <td data-order="${row.total_slot_hours}">${row.total_slot_hours.toFixed(2)} hrs</td>
                <td data-order="${row.pct_interactive}">${row.pct_interactive.toFixed(1)}%</td>
                <td data-order="${row.total_human_wait_seconds}">${row.total_human_wait_seconds > 0 ? row.total_human_wait_seconds.toFixed(0) + 's' : '-'}</td>
                <td>${categoryBadge}</td>
                <td>${actionBtn}</td>
            `;
            tbody.appendChild(tr);
        });

        // Delegated on the table so the handlers survive DataTables destroy/re-init;
        // the flag keeps re-renders from stacking duplicates.
        const batchTable = document.getElementById('batch-candidates-results-table');
        if (batchTable && !batchTable._batchDelegateAttached) {
            batchTable.addEventListener('click', (e) => {
                const btn = e.target.closest('.btn-remediation');
                if (btn) {
                    batchTooltip.hide();
                    window.showRemediationModal(btn.dataset.wname, btn.dataset.wtype, btn.dataset.wpriority);
                }
            });
            // mouseover/mouseout rather than mouseenter/mouseleave: the latter do
            // not bubble, so they cannot be delegated from the table.
            batchTable.addEventListener('mouseover', (e) => {
                const wrap = e.target.closest('.batch-tooltip-wrap');
                if (wrap) batchTooltip.show(wrap);
            });
            batchTable.addEventListener('mouseout', (e) => {
                const wrap = e.target.closest('.batch-tooltip-wrap');
                if (wrap && !wrap.contains(e.relatedTarget)) batchTooltip.hide();
            });
            batchTable._batchDelegateAttached = true;
        }

        if ($.fn.DataTable.isDataTable('#batch-candidates-results-table')) {
            $('#batch-candidates-results-table').DataTable().destroy();
        }
        $('#batch-candidates-results-table').DataTable({ pageLength: 10, order: [], responsive: true });
    };

    const renderSkewResults = (data) => {
        const tbody = document.querySelector('#skew-results-table tbody');
        if (!tbody) return;
        tbody.innerHTML = '';

        data.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${row.project_id}</td>
                <td style="font-family: monospace; font-size: 0.85rem;">${row.job_id.substring(0, 12)}...</td>
                <td>${row.user_email}</td>
                <td>${row.stage_name}</td>
                <td>${row.avg_compute_ms.toLocaleString()}</td>
                <td>${row.max_compute_ms.toLocaleString()}</td>
                <td data-order="${row.skew_ratio}"><span class="badge error">${row.skew_ratio.toFixed(1)}x</span></td>
            `;
            tbody.appendChild(tr);
        });

        if ($.fn.DataTable.isDataTable('#skew-results-table')) {
            $('#skew-results-table').DataTable().destroy();
        }
        $('#skew-results-table').DataTable({ pageLength: 10, order: [[6, 'desc']], responsive: true });
    };

    const renderLinterResults = (data) => {
        const tbody = document.querySelector('#linter-results-table tbody');
        if (!tbody) return;
        tbody.innerHTML = '';

        const formatDataSize = (gb) => {
            // threshold must be 1024, not 1000, to avoid "0.99 TB" for 1010 GB.
            if (gb >= 1024) {
                return `${formatNumber(gb / 1024)} TiB`;
            }
            return `${formatNumber(gb)} GiB`;
        };

        const getAbuseBadge = (type) => {
            let bg, color, border;
            if (type.includes('LIMIT TRAP')) {
                bg = 'rgba(139, 92, 246, 0.15)'; // Purple
                color = '#c084fc';
                border = 'rgba(139, 92, 246, 0.3)';
            } else if (type.includes('DML SCAN')) {
                bg = 'rgba(239, 68, 68, 0.15)'; // Red
                color = '#f87171';
                border = 'rgba(239, 68, 68, 0.3)';
            } else {
                bg = 'rgba(245, 158, 11, 0.15)'; // Amber/Orange
                color = '#fbbf24';
                border = 'rgba(245, 158, 11, 0.3)';
            }
            return `<span style="display: inline-block; padding: 0.25rem 0.6rem; font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; border-radius: 9999px; background: ${bg}; color: ${color}; border: 1px solid ${border}; white-space: nowrap;">${type}</span>`;
        };

        const getWasteCell = (waste) => {
            if (waste > 0) {
                return `<strong style="color: #f87171; font-weight: 700; text-shadow: 0 0 8px rgba(248, 113, 113, 0.15);">${formatCurrency(waste)}</strong>`;
            }
            return `<span style="color: #64748b;">$0.00</span>`;
        };

        const odRate = parseFloat(document.getElementById('jb-od-rate')?.value) || 6.25;
        data.forEach(row => {
            const estimated_waste_usd = row.billed_gb * odRate / 1024;
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><span style="color: #e2e8f0; font-weight: 500;">${row.user_email}</span></td>
                <td><span style="color: #94a3b8; font-family: monospace; font-size: 0.85rem;">${row.project_id}</span></td>
                <td><span style="color: #64748b; font-family: monospace; font-size: 0.85rem;">${row.job_id}</span></td>
                <td><strong style="color: #f1f5f9; font-weight: 600; font-family: monospace; white-space: nowrap;">${formatDataSize(row.billed_gb)}</strong></td>
                <td>${getAbuseBadge(row.abuse_type)}</td>
                <td>${getWasteCell(estimated_waste_usd)}</td>
                <td><div style="font-family: 'Fira Code', 'Courier New', monospace; font-size: 0.8rem; background: rgba(0,0,0,0.4); padding: 0.5rem 0.75rem; border-radius: 6px; border: 1px solid rgba(255,255,255,0.05); max-width: 350px; overflow-x: auto; white-space: nowrap; color: #cbd5e1;">${row.query_snippet}</div></td>
                <td>
                    <div style="display: flex; align-items: flex-start; gap: 0.35rem; font-size: 0.85rem; color: #94a3b8; line-height: 1.3; min-width: 250px;">
                        <i class="fa-regular fa-lightbulb" style="color: #38bdf8; margin-top: 0.15rem; font-size: 0.9rem; flex-shrink: 0;"></i>
                        <span>${row.suggested_fix || 'N/A'}</span>
                    </div>
                </td>
            `;
            tbody.appendChild(tr);
        });

        safeInitDataTable('#linter-results-table', {
            pageLength: 10,
            order: [[3, 'desc']],
            autoWidth: false
        });
    };

    const renderAntiPatternsResults = (data) => {
        const tbody = document.querySelector('#antipatterns-results-table tbody');
        if (!tbody) return;
        tbody.innerHTML = '';

        const edRate = parseFloat(document.getElementById('jb-ed-rate')?.value) || 0.06;
        data.forEach(row => {
            const wasteUsd = row.wasted_slot_hours * edRate;
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${row.user_email}</td>
                <td>${row.project_id}</td>
                <td>${row.insert_job_count.toLocaleString()}</td>
                <td>${row.wasted_slot_hours.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>
                <td><strong style="color: #f87171; font-weight: 700; text-shadow: 0 0 8px rgba(248, 113, 113, 0.15);">${formatCurrency(wasteUsd)}</strong></td>
                <td><span class="badge" style="background: rgba(239, 68, 68, 0.15); color: #ef4444;">Migrate to Storage Write API</span></td>
            `;
            tbody.appendChild(tr);
        });

        if ($.fn.DataTable.isDataTable('#antipatterns-results-table')) {
            $('#antipatterns-results-table').DataTable().destroy();
        }
        $('#antipatterns-results-table').DataTable({ pageLength: 10, order: [[4, 'desc']], responsive: true });
    };

    const renderExpirationResults = (data) => {
        const tbody = document.querySelector('#expiration-results-table tbody');
        if (!tbody) return;
        tbody.innerHTML = '';

        data.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${row.project_id}</td>
                <td>${row.dataset_id}</td>
                <td><span class="badge logical">Missing Expiration</span></td>
            `;
            tbody.appendChild(tr);
        });

        if ($.fn.DataTable.isDataTable('#expiration-results-table')) {
            $('#expiration-results-table').DataTable().destroy();
        }
        $('#expiration-results-table').DataTable({ pageLength: 10, responsive: true });
    };

    const renderFilterResults = (data) => {
        const tbody = document.querySelector('#filter-results-table tbody');
        if (!tbody) return;
        tbody.innerHTML = '';

        data.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${row.project_id}</td>
                <td>${row.dataset_id}</td>
                <td>${row.table_name}</td>
                <td>${row.partition_type}</td>
            `;
            tbody.appendChild(tr);
        });

        if ($.fn.DataTable.isDataTable('#filter-results-table')) {
            $('#filter-results-table').DataTable().destroy();
        }
        $('#filter-results-table').DataTable({ pageLength: 10, responsive: true });
    };

    const renderMvResults = (data) => {
        const tbody = document.querySelector('#mv-results-table tbody');
        if (!tbody) return;
        tbody.innerHTML = '';

        data.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${row.project_id}</td>
                <td>${row.dataset}</td>
                <td>${row.table_name}</td>
                <td>${row.refresh_count.toLocaleString()}</td>
                <td>${row.total_slot_hours.toFixed(2)}</td>
            `;
            tbody.appendChild(tr);
        });

        if ($.fn.DataTable.isDataTable('#mv-results-table')) {
            $('#mv-results-table').DataTable().destroy();
        }
        $('#mv-results-table').DataTable({ pageLength: 10, order: [[4, 'desc']], responsive: true });
    };

    const renderMvRejectionResults = (data) => {
        const tbody = document.querySelector('#mv-rejections-table tbody');
        if (!tbody) return;
        tbody.innerHTML = '';

        data.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${row.user_email}</td>
                <td style="font-family: monospace; font-size: 0.85rem;">${row.job_id.substring(0, 12)}...</td>
                <td>${row.mv_name}</td>
                <td style="font-size: 0.85rem; color: var(--text-secondary);">${row.rejected_reason}</td>
            `;
            tbody.appendChild(tr);
        });

        if ($.fn.DataTable.isDataTable('#mv-rejections-table')) {
            $('#mv-rejections-table').DataTable().destroy();
        }
        $('#mv-rejections-table').DataTable({ pageLength: 10, responsive: true });
    };

    const renderWarningResults = (data) => {
        const tbody = document.querySelector('#resource-warnings-table tbody');
        if (!tbody) return;
        tbody.innerHTML = '';

        data.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${row.user_email}</td>
                <td>
                    <div style="display: flex; align-items: center; gap: 0.5rem;">
                        <span style="font-family: monospace; font-size: 0.85rem;" title="${row.job_id}">${row.job_id.substring(0, 12)}...</span>
                        <button class="btn-action copy-job-id-btn" data-job-id="${row.job_id}" title="Copy Job ID" style="padding: 2px 5px; font-size: 0.75rem;"><i class="fa-solid fa-copy"></i></button>
                    </div>
                </td>
                <td style="font-size: 0.85rem; color: #f59e0b;">${row.resource_warning}</td>
            `;
            tbody.appendChild(tr);
        });

        if ($.fn.DataTable.isDataTable('#resource-warnings-table')) {
            $('#resource-warnings-table').DataTable().destroy();
        }
        $('#resource-warnings-table').DataTable({ pageLength: 10, responsive: true });
    };

    // Check settings before executing scan
    const checkSettings = () => {
        if (!state.orgProject) {
            showNotification('Please configure settings first.', 'error');
            Router.navigate('settings');
            return false;
        }
        return true;
    };

    if (elements.btnAnalyzeLinter) {
        elements.btnAnalyzeLinter.addEventListener('click', async () => {
            if (!checkSettings()) return;
            setLoading(elements.btnAnalyzeLinter, true);
            clearModuleCache(['bq_linter_results'], ['#linter-results-table']);
            const params = {
                org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
                region: state.region,
                focus_projects: state.focusProjects,
                lookback_days: 7,
                limit_per_project: 100
            };
            try {
                const response = await fetch('/api/antipatterns/linter', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(buildPayload('/api/antipatterns/linter', params))
                });
                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(detailToMessage(err.detail, 'Failed to scan query linter'));
                }
                const data = await response.json();
                renderLinterResults(data);
                safeSetLocalStorage('bq_linter_results', JSON.stringify(data));
                showNotification('Query optimization opportunities scan completed.', 'success');
            } catch (error) {
                console.error("Linter Error:", error);
                showNotification(error.message, 'error');
            } finally {
                setLoading(elements.btnAnalyzeLinter, false);
            }
        });
    }

    if (elements.btnAnalyzeDml) {
        elements.btnAnalyzeDml.addEventListener('click', async () => {
            if (!checkSettings()) return;
            setLoading(elements.btnAnalyzeDml, true);
            clearModuleCache(['bq_antipatterns_results'], ['#antipatterns-results-table']);
            const params = {
                org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
                region: state.region,
                focus_projects: state.focusProjects,
                lookback_days: 1,
                threshold: 1000
            };
            try {
                const response = await fetch('/api/antipatterns/dml', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(buildPayload('/api/antipatterns/dml', params))
                });
                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(detailToMessage(err.detail, 'Failed to scan DML abuse'));
                }
                const data = await response.json();
                renderAntiPatternsResults(data);
                safeSetLocalStorage('bq_antipatterns_results', JSON.stringify(data));
                showNotification('DML Abuse scan completed.', 'success');
            } catch (error) {
                console.error("DML Error:", error);
                showNotification(error.message, 'error');
            } finally {
                setLoading(elements.btnAnalyzeDml, false);
            }
        });
    }

    if (elements.btnAnalyzeMv) {
        elements.btnAnalyzeMv.addEventListener('click', async () => {
            if (!checkSettings()) return;
            setLoading(elements.btnAnalyzeMv, true);
            clearModuleCache(['bq_mv_results'], ['#mv-results-table']);
            const params = {
                org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
                region: state.region,
                focus_projects: state.focusProjects,
                lookback_days: 7
            };
            try {
                const response = await fetch('/api/antipatterns/mv', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(buildPayload('/api/antipatterns/mv', params))
                });
                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(detailToMessage(err.detail, 'Failed to scan MV costs'));
                }
                const data = await response.json();
                renderMvResults(data);
                safeSetLocalStorage('bq_mv_results', JSON.stringify(data));
                showNotification('Materialized View cost scan completed.', 'success');
            } catch (error) {
                console.error("MV Error:", error);
                showNotification(error.message, 'error');
            } finally {
                setLoading(elements.btnAnalyzeMv, false);
            }
        });
    }

    if (elements.btnAnalyzeSkew) {
        elements.btnAnalyzeSkew.addEventListener('click', async () => {
            if (!checkSettings()) return;
            setLoading(elements.btnAnalyzeSkew, true);
            clearModuleCache(['bq_skew_results'], ['#skew-results-table']);
            const params = {
                org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
                region: state.region,
                focus_projects: state.focusProjects,
                lookback_days: 7,
                limit_per_project: 50
            };
            try {
                const response = await fetch('/api/antipatterns/skew', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(buildPayload('/api/antipatterns/skew', params))
                });
                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(detailToMessage(err.detail, 'Failed to scan data skew'));
                }
                const data = await response.json();
                renderSkewResults(data);
                safeSetLocalStorage('bq_skew_results', JSON.stringify(data));
                showNotification('Data skew candidates scan completed.', 'success');
            } catch (error) {
                console.error("Skew Error:", error);
                showNotification(error.message, 'error');
            } finally {
                setLoading(elements.btnAnalyzeSkew, false);
            }
        });
    }

    if (elements.btnAnalyzeBatch) {
        elements.btnAnalyzeBatch.addEventListener('click', async () => {
            if (!checkSettings()) return;
            setLoading(elements.btnAnalyzeBatch, true);
            clearModuleCache(['bq_batch_results'], ['#batch-candidates-results-table']);
            const params = {
                org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
                region: state.region,
                focus_projects: state.focusProjects,
                lookback_days: 7,
                limit_per_project: 50
            };
            try {
                const response = await fetch('/api/antipatterns/batch_candidates', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(buildPayload('/api/antipatterns/batch_candidates', params))
                });
                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(detailToMessage(err.detail, 'Failed to scan batch candidates'));
                }
                const data = await response.json();
                renderBatchCandidatesResults(data);
                safeSetLocalStorage('bq_batch_results', JSON.stringify(data));
                showNotification('Interactive vs. Batch candidates scan completed.', 'success');
            } catch (error) {
                console.error("Batch Error:", error);
                showNotification(error.message, 'error');
            } finally {
                setLoading(elements.btnAnalyzeBatch, false);
            }
        });
    }

    if (elements.btnAnalyzeExpiration) {
        elements.btnAnalyzeExpiration.addEventListener('click', async () => {
            if (!checkSettings()) return;
            setLoading(elements.btnAnalyzeExpiration, true);
            // Don't clear bq_gov_results — only clear the DataTable.
            // Clearing the key wipes the sibling scan's cached data.
            clearModuleCache([], ['#expiration-results-table']);
            const params = {
                org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
                region: state.region,
                focus_projects: state.focusProjects,
                audit_type: 'expiration'
            };
            try {
                const response = await fetch('/api/governance/analyze', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(buildPayload('/api/governance/analyze', params))
                });
                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(detailToMessage(err.detail, 'Failed to scan governance'));
                }
                const govData = await response.json();
                renderExpirationResults(govData.expiration_issues || []);
                
                let cachedGov = {};
                try {
                    cachedGov = JSON.parse(localStorage.getItem('bq_gov_results')) || {};
                } catch(e) {}
                cachedGov.expiration_issues = govData.expiration_issues || [];
                safeSetLocalStorage('bq_gov_results', JSON.stringify(cachedGov));

                showNotification('Dataset expiration policy scan completed.', 'success');
            } catch (error) {
                console.error("Gov Expiration Error:", error);
                showNotification(error.message, 'error');
            } finally {
                setLoading(elements.btnAnalyzeExpiration, false);
            }
        });
    }

    if (elements.btnAnalyzeFilter) {
        elements.btnAnalyzeFilter.addEventListener('click', async () => {
            if (!checkSettings()) return;
            setLoading(elements.btnAnalyzeFilter, true);
            // Don't clear bq_gov_results — only clear the DataTable.
            clearModuleCache([], ['#filter-results-table']);
            const params = {
                org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
                region: state.region,
                focus_projects: state.focusProjects,
                audit_type: 'filter'
            };
            try {
                const response = await fetch('/api/governance/analyze', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(buildPayload('/api/governance/analyze', params))
                });
                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(detailToMessage(err.detail, 'Failed to scan governance'));
                }
                const govData = await response.json();
                renderFilterResults(govData.filter_issues || []);

                let cachedGov = {};
                try {
                    cachedGov = JSON.parse(localStorage.getItem('bq_gov_results')) || {};
                } catch(e) {}
                cachedGov.filter_issues = govData.filter_issues || [];
                safeSetLocalStorage('bq_gov_results', JSON.stringify(cachedGov));

                showNotification('Partitioned tables filter scan completed.', 'success');
            } catch (error) {
                console.error("Gov Filter Error:", error);
                showNotification(error.message, 'error');
            } finally {
                setLoading(elements.btnAnalyzeFilter, false);
            }
        });
    }

    if (elements.btnAnalyzeMvRejections) {
        elements.btnAnalyzeMvRejections.addEventListener('click', async () => {
            if (!checkSettings()) return;
            setLoading(elements.btnAnalyzeMvRejections, true);
            clearModuleCache(['bq_mv_rejection_results'], ['#mv-rejections-table']);
            const params = {
                org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
                region: state.region,
                focus_projects: state.focusProjects,
                lookback_days: 30
            };
            try {
                const response = await fetch('/api/mv/analyze', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(buildPayload('/api/mv/analyze', params))
                });
                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(detailToMessage(err.detail, 'Failed to scan MV rejections'));
                }
                const data = await response.json();
                renderMvRejectionResults(data);
                safeSetLocalStorage('bq_mv_rejection_results', JSON.stringify(data));
                showNotification('Materialized View rejections scan completed.', 'success');
            } catch (error) {
                console.error("MV Rejections Error:", error);
                showNotification(error.message, 'error');
            } finally {
                setLoading(elements.btnAnalyzeMvRejections, false);
            }
        });
    }

    if (elements.btnAnalyzeWarnings) {
        elements.btnAnalyzeWarnings.addEventListener('click', async () => {
            if (!checkSettings()) return;
            setLoading(elements.btnAnalyzeWarnings, true);
            clearModuleCache(['bq_resource_warning_results'], ['#resource-warnings-table']);
            const params = {
                org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
                region: state.region,
                focus_projects: state.focusProjects,
                lookback_days: 30
            };
            try {
                const response = await fetch('/api/resource_warnings/analyze', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(buildPayload('/api/resource_warnings/analyze', params))
                });
                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(detailToMessage(err.detail, 'Failed to scan resource warnings'));
                }
                const data = await response.json();
                renderWarningResults(data);
                safeSetLocalStorage('bq_resource_warning_results', JSON.stringify(data));
                showNotification('Proactive resource warnings scan completed.', 'success');
            } catch (error) {
                console.error("Resource Warnings Error:", error);
                showNotification(error.message, 'error');
            } finally {
                setLoading(elements.btnAnalyzeWarnings, false);
            }
        });
    }

    const cachedMvResults = localStorage.getItem('bq_mv_results');
    if (cachedMvResults) {
        try {
            renderMvResults(JSON.parse(cachedMvResults));
        } catch (e) { console.warn("Failed to parse cached MV results", e); }
    }

    const cachedAntiPatternsResults = localStorage.getItem('bq_antipatterns_results');
    if (cachedAntiPatternsResults) {
        try {
            renderAntiPatternsResults(JSON.parse(cachedAntiPatternsResults));
        } catch (e) { console.warn("Failed to parse cached anti-patterns results", e); }
    }

    const cachedSkewResults = localStorage.getItem('bq_skew_results');
    if (cachedSkewResults) {
        try {
            renderSkewResults(JSON.parse(cachedSkewResults));
        } catch (e) { console.warn("Failed to parse cached skew results", e); }
    }

    const cachedBatchResults = localStorage.getItem('bq_batch_results');
    if (cachedBatchResults) {
        try {
            renderBatchCandidatesResults(JSON.parse(cachedBatchResults));
        } catch (e) { console.warn("Failed to parse cached batch results", e); }
    }

    const cachedMvRejectionResults = localStorage.getItem('bq_mv_rejection_results');
    if (cachedMvRejectionResults) {
        try {
            renderMvRejectionResults(JSON.parse(cachedMvRejectionResults));
        } catch (e) { console.warn("Failed to parse cached MV rejection results", e); }
    }

    const cachedCostAttributionResults = localStorage.getItem('bq_cost_attribution_results');
    if (cachedCostAttributionResults) {
        try {
            const parsedData = JSON.parse(cachedCostAttributionResults);
            const attributions = parsedData.attributions || parsedData;
            renderCostAttributionResults(attributions);
        } catch (e) { console.warn("Failed to parse cached cost attribution results", e); }
    }

    const cachedWarningResults = localStorage.getItem('bq_resource_warning_results');
    if (cachedWarningResults) {
        try {
            renderWarningResults(JSON.parse(cachedWarningResults));
        } catch (e) { console.warn("Failed to parse cached warning results", e); }
    }

    const cachedLinterResults = localStorage.getItem('bq_linter_results');
    if (cachedLinterResults) {
        try {
            renderLinterResults(JSON.parse(cachedLinterResults));
        } catch (e) { console.warn("Failed to parse cached linter results", e); }
    }

    const cachedGovResults = localStorage.getItem('bq_gov_results');
    if (cachedGovResults) {
        try {
            const govData = JSON.parse(cachedGovResults);
            renderExpirationResults(govData.expiration_issues || []);
            renderFilterResults(govData.filter_issues || []);
        } catch (e) { console.warn("Failed to parse cached governance results", e); }
    }

    const cachedPerformanceResults = localStorage.getItem('bq_performance_results');
    if (cachedPerformanceResults) {
        try {
            renderPerformanceResults(JSON.parse(cachedPerformanceResults));
        } catch (e) { console.warn("Failed to parse cached performance results", e); }
    }

    const renderBiResults = (data) => {
        const tbody = document.querySelector('#bi-results-table tbody');
        if (!tbody) return;
        tbody.innerHTML = '';

        let totalSaved = 0;
        let fullAccelerated = 0;

        data.forEach(row => {
            totalSaved += row.estimated_dollars_saved;
            if (row.bi_engine_mode === 'FULL') fullAccelerated++;

            const tr = document.createElement('tr');
            const modeClass = row.bi_engine_mode === 'FULL' ? 'physical' : (row.bi_engine_mode === 'PARTIAL' ? 'logical' : 'error');
            tr.innerHTML = `
                <td>${row.user_email}</td>
                <td style="font-family: monospace; font-size: 0.85rem;">${row.job_id.substring(0, 12)}...</td>
                <td>${row.processed_gb.toFixed(2)}</td>
                <td>${row.billed_gb.toFixed(2)}</td>
                <td><span class="badge ${modeClass}">${row.bi_engine_mode}</span></td>
                <td style="font-size: 0.85rem; color: var(--text-secondary);">${row.failure_reasons}</td>
            `;
            tbody.appendChild(tr);
        });

        document.getElementById('bi-total-saved').innerText = `$${totalSaved.toFixed(2)}`;
        const rate = data.length > 0 ? (fullAccelerated / data.length * 100).toFixed(2) : 0;
        document.getElementById('bi-full-rate').innerText = `${rate}%`;

        if ($.fn.DataTable.isDataTable('#bi-results-table')) {
            $('#bi-results-table').DataTable().destroy();
        }
        $('#bi-results-table').DataTable({ pageLength: 10, order: [[2, 'desc']], responsive: true });
    };

    if (elements.btnAnalyzeBi) {
        elements.btnAnalyzeBi.addEventListener('click', async () => {
            if (!state.orgProject) {
                showNotification('Please configure settings first.', 'error');
                Router.navigate('settings');
                return;
            }

            setLoading(elements.btnAnalyzeBi, true);
            clearModuleCache(['bq_bi_results'], ['#bi-results-table']);

            const params = {
                org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
                region: state.region,
                focus_projects: state.focusProjects,
                lookback_days: 7,
                limit: 50
            };

            try {
                const response = await fetch('/api/bi/analyze', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(buildPayload('/api/bi/analyze', params))
                });

                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(errorData.detail || 'Failed to analyze BI Engine');
                }

                const data = await response.json();
                renderBiResults(data);
                safeSetLocalStorage('bq_bi_results', JSON.stringify(data));
                showNotification('BI Engine analysis completed.', 'success');
            } catch (error) {
                console.error("BI Error:", error);
                showNotification(error.message, 'error');
            } finally {
                setLoading(elements.btnAnalyzeBi, false);
            }
        });
    }

    const cachedBiResults = localStorage.getItem('bq_bi_results');
    if (cachedBiResults) {
        try {
            renderBiResults(JSON.parse(cachedBiResults));
        } catch (e) { console.warn("Failed to parse cached BI results", e); }
    }

    // XSS-safe escape for raw (un-proxy-escaped) fields like optimized_query,
    // query, and migration_applied_yaml, which are now exempted from the
    // global sanitizeData proxy so they remain usable for clipboard / POST.
    // Apply this helper when interpolating them into innerHTML.  [R3]
    const escHtml = s => String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');

    // Module-scoped variable to hold current results for Copy SQL reference
    let currentAiResults = [];

    const renderAiResults = (data) => {
        const panel = document.getElementById('ai-results-panel');
        if (panel) panel.style.display = 'block';
        const oldTbody = document.querySelector('#ai-results-table tbody');
        if (!oldTbody) return;
        const tbody = oldTbody.cloneNode(false);
        oldTbody.parentNode.replaceChild(tbody, oldTbody);
        currentAiResults = data;

        // --- KPI Summary Strip ---
        const kpiStrip = document.getElementById('aidoc-kpis');
        const filtersBar = document.getElementById('aidoc-filters');
        if (kpiStrip && data.length > 0) {
            let totalCostUsd = 0, totalBytes = 0;
            let nHigh = 0, nMed = 0, nLow = 0;
            let totalReferenced = 0, totalFound = 0;
            let nMigration = 0, nSchemaGap = 0, nRepeat = 0;

            data.forEach(r => {
                const rate = r.on_demand_rate_usd_per_tb || 6.25;
                const bytes = r.bytes_billed_original || r.bytes_scanned_original || 0;
                totalBytes += bytes;
                totalCostUsd += (bytes / (1024**4)) * rate;
                if (r.severity === 'HIGH') nHigh++;
                else if (r.severity === 'MEDIUM') nMed++;
                else if (r.severity === 'LOW') nLow++;
                totalReferenced += (r.tables_referenced_count || 0);
                totalFound += (r.tables_found_count || 0);
                if (r.migration_applied_yaml) nMigration++;
                if ((r.tables_referenced_count || 0) > (r.tables_found_count || 0)) nSchemaGap++;
                if (r.execution_count && r.execution_count > 1) nRepeat++;
            });

            // Populate KPI values
            const spendEl = document.getElementById('kpi-spend');
            const bytesEl = document.getElementById('kpi-bytes');
            if (spendEl) spendEl.textContent = `$${Math.round(totalCostUsd).toLocaleString()}`;
            const totalTib = totalBytes / (1024**4);
            if (bytesEl) bytesEl.textContent = totalTib >= 1 ? `${totalTib.toFixed(1)} TiB scanned` : `${Math.round(totalBytes / (1024**3))} GiB scanned`;

            const nHighEl = document.getElementById('kpi-n-high');
            const nMedEl = document.getElementById('kpi-n-med');
            const nLowEl = document.getElementById('kpi-n-low');
            if (nHighEl) nHighEl.textContent = nHigh;
            if (nMedEl) nMedEl.textContent = nMed;
            if (nLowEl) nLowEl.textContent = nLow;

            const covEl = document.getElementById('kpi-coverage');
            const covMeta = document.getElementById('kpi-coverage-meta');
            if (covEl) covEl.textContent = totalReferenced > 0 ? `${Math.round((totalFound / totalReferenced) * 100)}%` : 'N/A';
            if (covMeta) covMeta.textContent = `${totalFound}/${totalReferenced} DDLs supplied to model`;

            kpiStrip.style.display = 'grid';

            // Populate filter pill counts
            const setCount = (id, n) => { const el = document.getElementById(id); if (el) el.textContent = n; };
            setCount('pill-all', data.length);
            setCount('pill-high', nHigh);
            setCount('pill-med', nMed);
            setCount('pill-migration', nMigration);
            setCount('pill-schemagap', nSchemaGap);
            setCount('pill-repeat', nRepeat);
            if (filtersBar) filtersBar.style.display = 'flex';
        }

        // Severity badge config with numeric rank for DataTable sorting [R2]
        const severityRank = { HIGH: 0, MEDIUM: 1, LOW: 2 };
        const severityColors = {
            HIGH:   { bg: 'rgba(239, 68, 68, 0.15)',  border: 'rgba(239, 68, 68, 0.4)',  text: '#ef4444', icon: 'fa-circle-exclamation' },
            MEDIUM: { bg: 'rgba(245, 158, 11, 0.15)', border: 'rgba(245, 158, 11, 0.4)', text: '#f59e0b', icon: 'fa-triangle-exclamation' },
            LOW:    { bg: 'rgba(34, 197, 94, 0.15)',   border: 'rgba(34, 197, 94, 0.4)',  text: '#22c55e', icon: 'fa-circle-check' }
        };

        data.forEach(row => {
            const tr = document.createElement('tr');
            // Severity-based row stripe + filter data attributes
            if (row.severity) {
                tr.className = `severity-${row.severity.toLowerCase()}`;
            }
            tr.dataset.severity = (row.severity || '').toUpperCase();
            tr.dataset.migration = row.migration_applied_yaml ? '1' : '0';
            tr.dataset.schemagap = (row.tables_referenced_count || 0) > (row.tables_found_count || 0) ? '1' : '0';
            tr.dataset.repeat = (row.execution_count && row.execution_count > 1) ? '1' : '0';
            
            // --- Severity Badge [R2] ---
            let severityBadge = '<span style="color: var(--text-secondary);">—</span>';
            let severityOrder = 3;
            if (row.severity) {
                severityOrder = severityRank[row.severity] ?? 3;
                const s = severityColors[row.severity] || severityColors.LOW;
                severityBadge = `
                    <span style="background: ${s.bg}; border: 1px solid ${s.border}; color: ${s.text};
                        padding: 0.25rem 0.6rem; border-radius: 6px; font-size: 0.8rem; font-weight: 600;
                        display: inline-flex; align-items: center; gap: 4px; white-space: nowrap;">
                        <i class="fa-solid ${s.icon}" style="font-size: 0.85rem;"></i> ${row.severity}
                    </span>`;
            }

            // --- Zero-Click Original Cost [R7] ---
            const rate = row.on_demand_rate_usd_per_tb || 6.25;
            const billedBytes = row.bytes_billed_original || 0;
            const scannedBytes = row.bytes_scanned_original || 0;
            const displayBytes = billedBytes > 0 ? billedBytes : scannedBytes;
            const costLabel = billedBytes > 0 ? 'Billed' : 'Scanned';
            let originalCost = '<span style="color: var(--text-secondary);">—</span>';
            if (displayBytes > 0) {
                const gib = displayBytes / (1024**3);
                const sizeLabel = gib >= 1024
                    ? `${Math.round(gib / 1024)} TiB`
                    : `${Math.round(gib)} GiB`;
                const usd = Math.round((displayBytes / (1024**4)) * rate);
                const execBadge = row.execution_count && row.execution_count > 1 
                    ? `<div style="color: #38bdf8; font-size: 0.75rem; margin-top: 2px;"><i class="fa-solid fa-repeat"></i> ${row.execution_count.toLocaleString()} runs</div>`
                    : '';
                originalCost = `
                    <div style="font-size: 0.85rem;">
                        <div style="color: #e2e8f0; font-weight: 600;">${sizeLabel}</div>
                        <div style="color: ${billedBytes > 0 ? '#f59e0b' : '#94a3b8'}; font-size: 0.8rem;">
                            ~$${usd} <span style="font-size: 0.7rem;">(${costLabel})</span>
                        </div>
                        ${execBadge}
                    </div>`;
            }

            // --- Schema Coverage Badge ---
            let coverageBadge = '<span style="color: var(--text-secondary); font-size: 0.85rem;">N/A</span>';
            const referenced = row.tables_referenced_count || 0;
            const found = row.tables_found_count || 0;
            
            if (referenced > 0) {
                const missing = referenced - found;
                const schemaNote = `This recommendation used schema context. ${found} table DDL(s) were sent to Vertex AI.${missing > 0 ? ` (${missing} referenced table(s) could not be retrieved — see cross-project/permission notes.)` : ''}`;
                if (found === referenced) {
                    coverageBadge = `
                        <span style="background: rgba(56, 189, 248, 0.12); border: 1px solid rgba(56, 189, 248, 0.3); color: #38bdf8; padding: 0.25rem 0.6rem; border-radius: 6px; font-size: 0.8rem; font-weight: 600; display: inline-flex; align-items: center; gap: 4px; white-space: nowrap; cursor: help;" title="${schemaNote}">
                            <i class="fa-solid fa-circle-check" style="font-size: 0.85rem;"></i> ${found}/${referenced} DDLs
                        </span>`;
                } else {
                    const isInfoSchema = row.query && row.query.toUpperCase().includes('INFORMATION_SCHEMA');
                    const badgeTitle = isInfoSchema
                        ? `System views (INFORMATION_SCHEMA) do not have DDL schemas. The AI is auditing this query using standard optimization patterns.`
                        : schemaNote;
                    const badgeBg = isInfoSchema ? "rgba(148, 163, 184, 0.12)" : "rgba(245, 158, 11, 0.12)";
                    const badgeBorder = isInfoSchema ? "1px solid rgba(148, 163, 184, 0.3)" : "1px solid rgba(245, 158, 11, 0.3)";
                    const badgeColor = isInfoSchema ? "#94a3b8" : "#f59e0b";
                    const badgeIcon = isInfoSchema ? "fa-solid fa-circle-info" : "fa-solid fa-triangle-exclamation";
                    
                    coverageBadge = `
                        <span style="background: ${badgeBg}; border: ${badgeBorder}; color: ${badgeColor}; padding: 0.25rem 0.6rem; border-radius: 6px; font-size: 0.8rem; font-weight: 600; display: inline-flex; align-items: center; gap: 4px; white-space: nowrap; cursor: help;" title="${badgeTitle}">
                            <i class="${badgeIcon}" style="font-size: 0.85rem;"></i> ${found}/${referenced} DDLs
                        </span>`;
                }
            }


            const renderMarkdown = (text) => {
                if (!text) return '';
                let html = text;
                const codeBlocks = [];
                html = html.replace(/```(?:[a-zA-Z0-9\-]+)?\n([\s\S]*?)\n```/g, (match, code) => {
                    const placeholder = `__CODE_BLOCK_PLACEHOLDER_${codeBlocks.length}__`;
                    codeBlocks.push(`<pre style="background: rgba(15, 23, 42, 0.65); border: 1px solid rgba(255,255,255,0.08); padding: 1rem; border-radius: 0.5rem; font-family: monospace; font-size: 0.85rem; color: #e2e8f0; overflow-x: auto; margin: 0.75rem 0; line-height: 1.5; white-space: pre;"><code style="color: #38bdf8;">${code}</code></pre>`);
                    return placeholder;
                });
                html = html.replace(/^### (.*?)$/gm, '<h3 style="margin: 1rem 0 0.5rem 0; color: white; font-size: 1.05rem; font-weight: 600;">$1</h3>');
                html = html.replace(/^#### (.*?)$/gm, '<h4 style="margin: 0.75rem 0 0.25rem 0; color: #94a3b8; font-size: 0.95rem; font-weight: 600;">$1</h4>');
                html = html.replace(/`(.*?)`/g, '<code style="background: rgba(255,255,255,0.08); padding: 0.15rem 0.35rem; border-radius: 4px; font-family: monospace; font-size: 0.85rem; color: #38bdf8;">$1</code>');
                html = html.replace(/\*\*(.*?)\*\*/g, '<strong style="color: white; font-weight: 600;">$1</strong>');
                html = html.replace(/^\s*[\*\-]\s+(.*?)$/gm, '<li style="margin-left: 1rem; list-style-type: disc; margin-bottom: 0.35rem; color: #cbd5e1;">$1</li>');
                html = html.replace(/\n\n/g, '<div style="margin-bottom: 0.75rem;"></div>');
                html = html.replace(/\n/g, '<br>');
                // Auto-linkify fully qualified BigQuery table references (project.dataset.table)
                html = html.replace(/\b([a-z][a-z0-9\-]{5,29})\.([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\b/g, (match, proj, ds, tbl) => {
                    const url = `https://console.cloud.google.com/bigquery?project=${encodeURIComponent(proj)}&ws=!1m5!1m4!4m3!1s${encodeURIComponent(proj)}!2s${encodeURIComponent(ds)}!3s${encodeURIComponent(tbl)}`;
                    return `<a href="${url}" target="_blank" rel="noopener" class="console-link" title="Open ${tbl} in Console">${match}</a>`;
                });

                codeBlocks.forEach((blockHtml, index) => {
                    html = html.replace(`__CODE_BLOCK_PLACEHOLDER_${index}__`, blockHtml);
                });
                return html;
            };

            // --- Query SQL Preview ---
            let originalQueryCell = '<span style="color: var(--text-secondary); font-size: 0.85rem;">—</span>';
            if (row.query) {
                const escapedOrigSqlPreview = escHtml(
                    row.query.length > 200
                        ? row.query.substring(0, 200) + '...'
                        : row.query
                );
                originalQueryCell = `
                    <div style="position: relative; min-width: 220px;">
                        <pre style="background: rgba(15, 23, 42, 0.65); border: 1px solid rgba(251, 113, 133, 0.2);
                            padding: 0.75rem; border-radius: 0.5rem; font-family: monospace; font-size: 0.78rem;
                            color: #fb7185; overflow-x: auto; max-height: 120px; overflow-y: auto; white-space: pre-wrap;
                            word-break: break-all; margin: 0;">${escapedOrigSqlPreview}</pre>
                        <div style="display: flex; gap: 0.5rem; margin-top: 0.5rem;">
                            <button class="copy-orig-sql-btn" style="background: rgba(251,113,133,0.15); border: 1px solid rgba(251,113,133,0.3);
                                color: #fb7185; padding: 0.3rem 0.6rem; border-radius: 6px; font-size: 0.75rem;
                                cursor: pointer; display: inline-flex; align-items: center; gap: 4px;">
                                <i class="fa-solid fa-copy"></i> Copy SQL
                            </button>
                        </div>
                    </div>`;
            }

            // --- Optimized Query Cell [R3] ---
            let optimizedCell = '<span style="color: var(--text-secondary); font-size: 0.85rem;">—</span>';
            if (row.optimized_query) {
                const escapedSqlPreview = escHtml(
                    row.optimized_query.length > 200
                        ? row.optimized_query.substring(0, 200) + '...'
                        : row.optimized_query
                );
                let approxBadge = '';
                if (row.approx_warning_flag) {
                    approxBadge = `<div style="margin-top: 0.4rem; font-size: 0.7rem; color: #f59e0b;">
                        <i class="fa-solid fa-triangle-exclamation"></i> Uses APPROX_COUNT_DISTINCT
                    </div>`;
                }
                optimizedCell = `
                    <div style="position: relative; min-width: 250px;">
                        <pre style="background: rgba(15, 23, 42, 0.65); border: 1px solid rgba(56, 189, 248, 0.2);
                            padding: 0.75rem; border-radius: 0.5rem; font-family: monospace; font-size: 0.78rem;
                            color: #38bdf8; overflow-x: auto; max-height: 120px; overflow-y: auto; white-space: pre-wrap;
                            word-break: break-all; margin: 0;">${escapedSqlPreview}</pre>
                        ${approxBadge}
                        <div style="display: flex; gap: 0.5rem; margin-top: 0.5rem;">
                            <button class="copy-sql-btn" style="background: rgba(56,189,248,0.15); border: 1px solid rgba(56,189,248,0.3);
                                color: #38bdf8; padding: 0.3rem 0.6rem; border-radius: 6px; font-size: 0.75rem;
                                cursor: pointer; display: inline-flex; align-items: center; gap: 4px;">
                                <i class="fa-solid fa-copy"></i> Copy SQL
                            </button>
                        </div>
                        __YAML_BADGE_PLACEHOLDER__
                    </div>`;
            }

            let yamlBadge = '';
            if (row.migration_applied_yaml) {
                const escapedYaml = escHtml(row.migration_applied_yaml.trim());
                yamlBadge = `
                    <div style="margin-top: 0.5rem; border: 1px solid rgba(56,189,248,0.25); border-radius: 6px; font-size: 0.75rem; color: #38bdf8; overflow: hidden;">
                        <div class="yaml-toggle-btn" style="padding: 0.4rem 0.6rem; background: rgba(56,189,248,0.08); cursor: pointer; display: flex; align-items: center; gap: 4px; user-select: none;">
                            <i class="fa-solid fa-chevron-right yaml-chevron" style="font-size: 0.6rem; transition: transform 0.2s;"></i>
                            <i class="fa-solid fa-wand-magic-sparkles"></i> Migration API Config Applied
                        </div>
                        <div class="yaml-content" style="display: none; padding: 0.4rem 0.6rem; background: rgba(15,23,42,0.4);">
                            <pre style="margin: 0; font-family: monospace; font-size: 0.7rem; color: #94a3b8; white-space: pre-wrap; word-break: break-all;">${escapedYaml}</pre>
                        </div>
                    </div>`;
            }
            optimizedCell = optimizedCell.replace('__YAML_BADGE_PLACEHOLDER__', yamlBadge);

            const slotHours = (row.total_slot_ms || 0) / 3600000;
            const formattedSlotHours = slotHours >= 1000
                ? `${Math.round(slotHours).toLocaleString()} hrs`
                : (slotHours >= 1 ? `${slotHours.toFixed(1)} hrs` : `${slotHours.toFixed(2)} hrs`);

            tr.innerHTML = `
                <td style="font-family: monospace; font-size: 0.85rem;" title="${row.job_id}">${row.job_id.substring(0, 12)}...</td>
                <td>${row.user_email}</td>
                <td data-order="${slotHours}" title="${(row.total_slot_ms || 0).toLocaleString()} slot-ms (${slotHours.toFixed(2)} slot-hours)">${formattedSlotHours}</td>
                <td data-order="${severityOrder}">${severityBadge}</td>
                <td data-order="${displayBytes > 0 ? Math.round((displayBytes / (1024**4)) * rate) : 0}">${originalCost}</td>
                <td>${originalQueryCell}</td>
                <td>${coverageBadge}</td>
                <td style="font-size: 0.85rem; color: var(--text-secondary); line-height: 1.5;">
                    <div class="advice-wrapper" style="position: relative;">
                        <div class="advice-content" style="max-height: 150px; overflow: hidden; transition: max-height 0.3s ease;">
                            ${renderMarkdown(row.gemini_optimization_advice)}
                        </div>
                        ${(row.gemini_optimization_advice || '').length > 300 ? `
                        <div class="advice-toggle"
                            style="text-align: center; padding: 0.3rem; font-size: 0.72rem; color: #38bdf8; cursor: pointer;
                            background: linear-gradient(to bottom, transparent, rgba(15,23,42,0.95) 40%); margin-top: -1.5rem; position: relative; z-index: 1;">
                            ▼ Show more
                        </div>` : ''}
                    </div>
                </td>
                <td>${optimizedCell}</td>
            `;
            tbody.appendChild(tr);
        });

        if ($.fn.DataTable.isDataTable('#ai-results-table')) {
            $('#ai-results-table').DataTable().destroy();
        }
        $('#ai-results-table').DataTable({ pageLength: 10, order: [[4, 'desc']], responsive: true });

        // --- Filter Pills (DataTables custom search) ---
        let activeFilter = 'all';
        // Clear any previously-registered AI Doctor filter functions
        $.fn.dataTable.ext.search = $.fn.dataTable.ext.search.filter(fn => !fn._aidocFilter);
        const filterFn = (settings, searchData, dataIndex, rowData, counter) => {
            if (settings.nTable.id !== 'ai-results-table') return true;
            if (activeFilter === 'all') return true;
            const tr = settings.aoData[dataIndex].nTr;
            if (!tr) return true;
            if (activeFilter === 'high') return tr.dataset.severity === 'HIGH';
            if (activeFilter === 'medium') return tr.dataset.severity === 'MEDIUM';
            if (activeFilter === 'migration') return tr.dataset.migration === '1';
            if (activeFilter === 'schemagap') return tr.dataset.schemagap === '1';
            if (activeFilter === 'repeat') return tr.dataset.repeat === '1';
            return true;
        };
        filterFn._aidocFilter = true;
        $.fn.dataTable.ext.search.push(filterFn);

        if (filtersBar) {
            // Clone to strip stacked event listeners from previous renders
            const freshBar = filtersBar.cloneNode(true);
            filtersBar.parentNode.replaceChild(freshBar, filtersBar);
            freshBar.querySelectorAll('.aidoc-pill').forEach(pill => {
                pill.addEventListener('click', () => {
                    freshBar.querySelectorAll('.aidoc-pill').forEach(p => p.classList.remove('is-active'));
                    pill.classList.add('is-active');
                    activeFilter = pill.dataset.filter;
                    $('#ai-results-table').DataTable().draw();
                });
            });
        }

        // --- Event Delegation for interactive buttons ---
        tbody.addEventListener('click', async (e) => {
            // Copy Optimized SQL button [R-clipboard]
            const copyBtn = e.target.closest('.copy-sql-btn');
            if (copyBtn) {
                const tr = copyBtn.closest('tr');
                const rowIdx = $('#ai-results-table').DataTable().row(tr).index();
                const rowData = currentAiResults[rowIdx];
                const sql = rowData?.optimized_query || '';
                
                try {
                    if (navigator.clipboard && window.isSecureContext) {
                        await navigator.clipboard.writeText(sql);
                    } else {
                        const ta = document.createElement('textarea');
                        ta.value = sql;
                        ta.style.cssText = 'position:fixed;left:-9999px';
                        document.body.appendChild(ta);
                        ta.select();
                        document.execCommand('copy');
                        document.body.removeChild(ta);
                    }
                    showNotification('Optimized SQL copied to clipboard.', 'success');
                } catch (err) {
                    showNotification('Failed to copy — please select and copy manually.', 'error');
                }
                return;
            }

            // Copy Original SQL button
            const copyOrigBtn = e.target.closest('.copy-orig-sql-btn');
            if (copyOrigBtn) {
                const tr = copyOrigBtn.closest('tr');
                const rowIdx = $('#ai-results-table').DataTable().row(tr).index();
                const rowData = currentAiResults[rowIdx];
                const sql = rowData?.query || '';
                
                try {
                    if (navigator.clipboard && window.isSecureContext) {
                        await navigator.clipboard.writeText(sql);
                    } else {
                        const ta = document.createElement('textarea');
                        ta.value = sql;
                        ta.style.cssText = 'position:fixed;left:-9999px';
                        document.body.appendChild(ta);
                        ta.select();
                        document.execCommand('copy');
                        document.body.removeChild(ta);
                    }
                    showNotification('Original SQL copied to clipboard.', 'success');
                } catch (err) {
                    showNotification('Failed to copy — please select and copy manually.', 'error');
                }
                return;
            }

            // YAML accordion toggle
            const yamlBtn = e.target.closest('.yaml-toggle-btn');
            if (yamlBtn) {
                const content = yamlBtn.nextElementSibling;
                const chevron = yamlBtn.querySelector('.yaml-chevron');
                if (content.style.display === 'none') {
                    content.style.display = 'block';
                    if (chevron) chevron.style.transform = 'rotate(90deg)';
                } else {
                    content.style.display = 'none';
                    if (chevron) chevron.style.transform = 'rotate(0deg)';
                }
                return;
            }

            // Advice Show more/less toggle
            const adviceBtn = e.target.closest('.advice-toggle');
            if (adviceBtn) {
                const wrapper = adviceBtn.parentElement;
                const content = wrapper.querySelector('.advice-content');
                if (content.style.maxHeight === '150px') {
                    content.style.maxHeight = 'none';
                    content.style.overflow = 'visible';
                    adviceBtn.textContent = '▲ Show less';
                    adviceBtn.style.marginTop = '0';
                    adviceBtn.style.background = 'none';
                } else {
                    content.style.maxHeight = '150px';
                    content.style.overflow = 'hidden';
                    adviceBtn.textContent = '▼ Show more';
                    adviceBtn.style.marginTop = '-1.5rem';
                    adviceBtn.style.background = 'linear-gradient(to bottom, transparent, rgba(15,23,42,0.95) 40%)';
                }
                return;
            }
        });
    };

    // DDL Learn More Drawer Toggle
    const learnMoreToggle = document.getElementById('ddl-learn-more-toggle');
    const learnMoreDrawer = document.getElementById('ddl-learn-more-drawer');
    const learnMoreClose = document.getElementById('ddl-learn-more-close');

    if (learnMoreToggle && learnMoreDrawer) {
        learnMoreToggle.addEventListener('click', () => {
            const isHidden = learnMoreDrawer.style.display === 'none';
            learnMoreDrawer.style.display = isHidden ? 'block' : 'none';
            learnMoreToggle.textContent = isHidden ? 'Hide details ↑' : 'Learn more →';
        });
    }
    if (learnMoreClose && learnMoreDrawer) {
        learnMoreClose.addEventListener('click', () => {
            learnMoreDrawer.style.display = 'none';
            if (learnMoreToggle) learnMoreToggle.textContent = 'Learn more →';
        });
    }

    // AI Scope (What it checks / Out of scope) Toggle
    const aiScopeToggle = document.getElementById('ai-scope-toggle');
    const aiScopeContent = document.getElementById('ai-scope-content');
    const aiScopeChevron = document.getElementById('ai-scope-chevron');
    if (aiScopeToggle && aiScopeContent) {
        aiScopeToggle.addEventListener('click', () => {
            const isHidden = aiScopeContent.style.display === 'none';
            aiScopeContent.style.display = isHidden ? 'block' : 'none';
            if (aiScopeChevron) aiScopeChevron.style.transform = isHidden ? 'rotate(90deg)' : '';
        });
    }

    // Consent Modal Logic
    const consentModal = document.getElementById('ddl-consent-modal');
    const consentCheckbox = document.getElementById('ddl-consent-checkbox');
    const consentProceedBtn = document.getElementById('ddl-consent-proceed');
    const consentCancelBtn = document.getElementById('ddl-consent-cancel');

    if (consentCheckbox && consentProceedBtn) {
        consentCheckbox.addEventListener('change', () => {
            consentProceedBtn.disabled = !consentCheckbox.checked;
            consentProceedBtn.style.opacity = consentCheckbox.checked ? '1' : '0.5';
            consentProceedBtn.style.cursor = consentCheckbox.checked ? 'pointer' : 'not-allowed';
        });
    }

    if (elements.btnRunAiAnalysis) {
        // Refactored helper function for actual AI execution
        const runActualAiAnalysis = async () => {
            const tableEl = document.getElementById('ai-results-table');
            const container = tableEl ? tableEl.closest('.results-panel') : null;
            if (container) container.style.display = 'block';

            if (tableEl && $.fn.DataTable.isDataTable('#ai-results-table')) {
                $('#ai-results-table').DataTable().destroy();
            }
            
            if (tableEl) {
                UIState.renderTableSkeleton(tableEl, 5);
            }

            const abortController = new AbortController();
            let progress = null;
            
            if (container) {
                progress = UIState.startQueryProgress(container, {
                    message: 'Running LLM-powered semantic query analysis...',
                    onCancel: () => abortController.abort()
                });
            }

            setLoading(elements.btnRunAiAnalysis, true);
            clearModuleCache(['bq_ai_results'], ['#ai-results-table']);

            const params = {
                org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
                region: state.region,
                focus_projects: state.focusProjects,
                limit: parseInt(elements.aiLimit.value),
                discovery_strategy: elements.aiDiscoveryStrategy ? elements.aiDiscoveryStrategy.value : 'composite',
                lookback_days: parseInt(elements.aiLookback ? elements.aiLookback.value : '7'),
                model: elements.aiModel ? elements.aiModel.value : 'gemini-3.6-flash'
            };

            try {
                const response = await fetch('/api/ai/analyze', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(buildPayload('/api/ai/analyze', params)),
                    signal: abortController.signal
                });

                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(errorData.detail || 'Failed to run AI analysis');
                }

                const data = await response.json();
                
                if (progress) progress.stop();

                renderAiResults(data);
                safeSetLocalStorage('bq_ai_results', JSON.stringify(data));

                if (data.length === 0) {
                    const strategy = params.discovery_strategy;
                    const emptyMessages = {
                        execution_frequency: 'All analyzed high-frequency queries passed clean with no anti-patterns found! Try increasing the lookback window or using a different strategy (e.g. Cumulative Cost or Composite ROI).',
                        memory_spill: 'No queries with RAM spill anti-patterns detected in this lookback window. Your workloads are not spilling shuffle data to disk.',
                        composite: 'All analyzed queries in this lookback window passed clean with no anti-patterns detected!',
                        cumulative_cost: 'All analyzed queries in this lookback window passed clean with no anti-patterns detected!',
                        slot_ms: 'All analyzed queries in this lookback window passed clean with no anti-patterns detected!'
                    };
                    showNotification(emptyMessages[strategy] || 'No query anti-patterns found to audit.', 'info');
                } else {
                    showNotification('AI analysis completed.', 'success');
                }
            } catch (error) {
                if (progress && progress.stop) progress.stop();
                console.error("AI Error:", error);
                showNotification(error.message, 'error');
            } finally {
                setLoading(elements.btnRunAiAnalysis, false);
            }
        };

        elements.btnRunAiAnalysis.addEventListener('click', async () => {
            if (!state.orgProject) {
                showNotification('Please configure the GCP Project in Global Settings.', 'error');
                Router.navigate('settings');
                return;
            }

            // Blocking security check for DDL schema egress consent
            const hasConsent = localStorage.getItem('ddl_consent_accepted') === 'true';
            if (!hasConsent) {
                if (consentModal) {
                    consentModal.style.display = 'flex';
                    
                    const handleProceed = () => {
                        localStorage.setItem('ddl_consent_accepted', 'true');
                        consentModal.style.display = 'none';
                        runActualAiAnalysis();
                        cleanup();
                    };
                    
                    const handleCancel = () => {
                        consentModal.style.display = 'none';
                        showNotification('Analysis cancelled. Schema consent is required to run the Doctor.', 'warning');
                        cleanup();
                    };
                    
                    const cleanup = () => {
                        consentProceedBtn.removeEventListener('click', handleProceed);
                        consentCancelBtn.removeEventListener('click', handleCancel);
                    };
                    
                    consentProceedBtn.addEventListener('click', handleProceed);
                    consentCancelBtn.addEventListener('click', handleCancel);
                }
                return;
            }

            // Execute directly if consent has been accepted previously
            runActualAiAnalysis();
        });
    }

    if (elements.aiDiscoveryStrategy) {
        const strategyDescs = {
            composite: '⚖️ Ranks candidates by Cost + Run Frequency + Slot Time + RAM Spill.',
            cumulative_cost: '💰 Ranks workloads by On-Demand cost or On-Demand Equivalent cost for BigQuery Editions (using bytes scanned when bytes billed is 0).',
            execution_frequency: '🔄 Targets dashboard micro-offenders executing repeatedly (COUNT(*) > 1).',
            memory_spill: '💾 Filters for queries spilling intermediate shuffle data from RAM to disk.',
            slot_ms: '⏱️ Focuses on heavy compute queries consuming the highest aggregate CPU slot milliseconds.'
        };
        elements.aiDiscoveryStrategy.addEventListener('change', (e) => {
            const descEl = document.getElementById('ai-strategy-desc');
            if (descEl && strategyDescs[e.target.value]) {
                descEl.textContent = strategyDescs[e.target.value];
            }
        });
    }

    const cachedAiResults = localStorage.getItem('bq_ai_results');
    if (cachedAiResults) {
        try {
            renderAiResults(JSON.parse(cachedAiResults));
        } catch (e) { console.warn("Failed to parse cached AI results", e); }
    }

    // Load cached top spenders data
    const cachedSpenders = localStorage.getItem('bq_top_spenders');
    if (cachedSpenders) {
        try {
            renderTopSpenders(JSON.parse(cachedSpenders));
        } catch (e) { console.warn("Failed to parse cached top spenders", e); }
    }

    // Load cached storage data
    const cachedStorage = localStorage.getItem('bq_storage_results');
    if (cachedStorage) {
        try {
            const storageData = JSON.parse(cachedStorage);
            state.storageData = storageData.datasets;
            renderStorageResults(storageData);
            renderOrgStatus(storageData.org_status);
        } catch (e) { console.warn("Failed to parse cached storage results", e); }
    }

    // Load cached Active Assist recommendations
    const cachedActiveAssist = localStorage.getItem('bq_active_assist_results');
    if (cachedActiveAssist) {
        try {
            const activeAssistData = JSON.parse(cachedActiveAssist);
            state.activeAssistData = activeAssistData;
            renderActiveAssistResults(activeAssistData);
        } catch (e) { console.warn("Failed to parse cached Active Assist results", e); }
    }

    // Load cached Static Schema Audit results
    const cachedStaticAudit = localStorage.getItem('bq_static_audit_results');
    if (cachedStaticAudit) {
        try {
            const staticAuditData = JSON.parse(cachedStaticAudit);
            state.staticAuditData = staticAuditData;
            renderStaticAuditResults(staticAuditData);
        } catch (e) { console.warn("Failed to parse cached Static Schema Audit results", e); }
    }

    // Load cached job data
    const cachedJob = localStorage.getItem('bq_job_results');
    if (cachedJob) {
        try {
            renderJobResults(JSON.parse(cachedJob));
        } catch (e) { console.warn("Failed to parse cached job results", e); }
    }

    // Load cached slots data (recommendation + current reservations tables)
    const cachedSlots = localStorage.getItem('bq_slots_results');
    if (cachedSlots) {
        try {
            renderSlotsResults(JSON.parse(cachedSlots), parseInt(elements.slPercentile?.value || '90') || 90);
        } catch (e) { console.warn("Failed to parse cached slots results", e); }
    }
    
    const cachedTiered = localStorage.getItem('bq_slots_tiered');
    if (cachedTiered) {
        try { renderTieredRecommendations(JSON.parse(cachedTiered)); } catch (e) { console.warn("Failed to parse cached tiered", e); }
    }

    // Load cached simulation results (Edition Matrix Simulation)
    const cachedSimulation = localStorage.getItem('bq_slots_simulation_results');
    if (cachedSimulation) {
        try {
            const data = JSON.parse(cachedSimulation);
            renderSimulationResults(data);
            const panel = document.getElementById('simulation-results-panel');
            if (panel) panel.style.display = 'block';
        } catch (e) {
            console.warn("Failed to parse cached simulation results", e);
        }
    }

    // Load cached "Actual Provisioning" & "Slot usage by capacity" (utilization + provisioning timeline)
    const cachedUtil = localStorage.getItem('bq_slots_utilization');
    const cachedActualProv = localStorage.getItem('bq_slots_actual_provisioning');

    let utilData = null;
    let actualData = null;

    if (cachedUtil) {
        try {
            utilData = JSON.parse(cachedUtil);
        } catch (e) {
            console.warn("Failed to parse cached slots utilization chart", e);
        }
    }

    if (cachedActualProv) {
        try {
            actualData = JSON.parse(cachedActualProv);
            // Standalone timeline fallback if not embedded or to support legacy cache keys
            if (actualData && !actualData.timeline) {
                const cachedProv = localStorage.getItem('bq_slots_provisioning_timeline');
                if (cachedProv) {
                    try {
                        actualData.timeline = JSON.parse(cachedProv);
                    } catch (_) {}
                }
            }
        } catch (e) {
            console.warn("Failed to parse cached actual provisioning", e);
        }
    }

    if (utilData || actualData) {
        try {
            renderSlotsUtilizationAndProvisioning(utilData, actualData);
        } catch (e) {
            console.error("Failed to render cached slots timeline / provisioning data", e);
        }
    }

    // Load cached profiler data
    const cachedSummary = localStorage.getItem('bq_profiler_summary');
    const cachedTimeline = localStorage.getItem('bq_profiler_timeline');
    const cachedQueries = localStorage.getItem('bq_profiler_queries');

    if (cachedSummary) {
        try {
            renderProfilerResults(JSON.parse(cachedSummary));
        } catch (e) { console.warn("Failed to parse cached profiler summary", e); }
    }
    if (cachedTimeline) {
        try {
            renderHeatmap(JSON.parse(cachedTimeline));
        } catch (e) { console.warn("Failed to parse cached profiler timeline", e); }
    }
    if (cachedQueries) {
        try {
            renderProfilerQueries(JSON.parse(cachedQueries));
        } catch (e) { console.warn("Failed to parse cached profiler queries", e); }
    }
    // Snapshot export/import wiring
    const exportBtn = document.getElementById('btn-export-snapshot');
    if (exportBtn) {
        exportBtn.addEventListener('click', () => Snapshot.exportSnapshot());
    }
    const importBtn = document.getElementById('btn-import-snapshot');
    const importInput = document.getElementById('import-snapshot-input');
    if (importBtn && importInput) {
        importBtn.addEventListener('click', () => importInput.click());
        importInput.addEventListener('change', (e) => {
            const file = e.target.files?.[0];
            if (file) Snapshot.importSnapshot(file);
            e.target.value = ''; // reset so re-selecting the same file fires change
        });
    }

    // App Start
    initUI();
});

/**
 * Initialize a DataTable only after verifying the table's DOM is internally
 * consistent. Prevents the `RangeError: Maximum call stack size exceeded`
 * recursion by failing loudly on a thead/tbody column mismatch.
 *
 * @param {string} selector  jQuery selector, e.g. '#fluid-estimate-table'
 * @param {object} options   DataTables options
 * @returns {DataTable|null} the DataTable instance, or null if skipped
 */
function safeInitDataTable(selector, options) {
  const $table = $(selector);
  if ($table.length === 0) {
    console.warn(`[safeInitDataTable] ${selector} not found in DOM; skipping init.`);
    return null;
  }
  const tableEl = $table[0];

  // 1) Always tear down a prior instance cleanly (removes scrollX header clones).
  if ($.fn.DataTable.isDataTable(selector)) {
    $table.DataTable().clear().destroy();
  }

  // 2) Determine the authoritative column count from the LAST header row
  //    (handles multi-row / grouped headers correctly).
  const headerRows = tableEl.tHead ? tableEl.tHead.rows : [];
  if (headerRows.length === 0) {
    throw new Error(`[safeInitDataTable] ${selector} has no <thead> rows.`);
  }
  const lastHeaderRow = headerRows[headerRows.length - 1];
  const expectedCols = Array.from(lastHeaderRow.cells)
    .reduce((sum, th) => sum + (th.colSpan || 1), 0);

  // 3) Validate every body row. Skip "message" rows that use a single
  //    full-width colspan cell (the canonical empty/skeleton pattern).
  const body = tableEl.tBodies[0];
  if (body) {
    Array.from(body.rows).forEach((row, idx) => {
      const cells = Array.from(row.cells);

      // Canonical message row: exactly one cell spanning all columns. Allowed.
      const isMessageRow =
        cells.length === 1 && (cells[0].colSpan || 1) === expectedCols;
      if (isMessageRow) return;

      const actualCols = cells.reduce((sum, td) => sum + (td.colSpan || 1), 0);
      if (actualCols !== expectedCols) {
        throw new Error(
          `[safeInitDataTable] ${selector} column mismatch at body row ${idx}: ` +
          `header expects ${expectedCols}, row has ${actualCols}. ` +
          `Fix the row template or use a single colspan="${expectedCols}" cell ` +
          `for empty/skeleton rows. (Aborting before DataTables recursion.)`
        );
      }
    });
  }

  // 4) Safe to initialize. Force autoWidth:false unless explicitly overridden.
  return $table.DataTable(Object.assign({ autoWidth: false }, options));
}

/* ============================================================
   UI STATE HELPERS
   Single module that handles: skeleton render, long-query progress,
   empty/success/error rendering. All your fetch calls flow through this.
   ============================================================ */

const UIState = (() => {

  // -- Skeleton renderers ---------------------------------------------------

  /** Render N skeleton rows into a tbody, matching the column count. */
  function renderTableSkeleton(tableEl, rowCount = 6) {
    const headerRow = tableEl.tHead ? tableEl.tHead.rows[tableEl.tHead.rows.length - 1] : null;
    const colCount = headerRow ? Array.from(headerRow.cells).reduce((sum, th) => sum + (th.colSpan || 1), 0) : 5;
    const tbody = tableEl.querySelector('tbody');
    if (!tbody) return;

    const rows = Array.from({ length: rowCount }, () => `
      <tr class="skeleton-table-row" aria-hidden="true">
        <td colspan="${colCount}">
          <span class="skeleton skeleton--text" style="width: 100%;"></span>
        </td>
      </tr>
    `).join('');

    tbody.innerHTML = rows;
  }

  /** Render skeleton KPI cards into a container. */
  function renderKpiSkeleton(containerEl, count = 4) {
    const card = `
      <div class="skeleton-kpi" aria-hidden="true">
        <span class="skeleton skeleton--text-sm"></span>
        <span class="skeleton skeleton--number"></span>
      </div>`;
    containerEl.innerHTML = Array(count).fill(card).join('');
  }

  /** Render skeleton tier cards (3 cards matching your tier-card layout). */
  function renderTierCardsSkeleton(containerEl) {
    const card = `
      <div class="skeleton-tier-card" aria-hidden="true">
        <span class="skeleton skeleton--badge"></span>
        <span class="skeleton skeleton--heading" style="margin-top: 0.75rem;"></span>
        <span class="skeleton skeleton--text-sm"></span>
        <span class="skeleton skeleton--number" style="margin: 1rem 0;"></span>
        <span class="skeleton skeleton--button"></span>
      </div>`;
    containerEl.innerHTML = `
      <div class="tier-cards-container">
        ${card}${card}${card}
      </div>`;
  }

  /** Render a chart-shaped skeleton block. */
  function renderChartSkeleton(containerEl) {
    containerEl.innerHTML =
      '<span class="skeleton skeleton--chart" aria-hidden="true"></span>';
  }

  // -- Long-running query progress ------------------------------------------

  /**
   * Show a progress banner above a container with elapsed time.
   * Returns { stop, abort } — call stop() on success, abort() to cancel.
   *
   * Usage:
   *   const progress = UIState.startQueryProgress(container, {
   *     message: 'Scanning slot usage across organization...',
   *     onCancel: () => abortController.abort()
   *   });
   *   ...
   *   progress.stop();
   */
  function startQueryProgress(containerEl, { message, onCancel } = {}) {
    const banner = document.createElement('div');
    banner.className = 'query-progress';
    banner.setAttribute('role', 'status');
    banner.setAttribute('aria-live', 'polite');
    banner.innerHTML = `
      <span class="query-progress__spinner" aria-hidden="true"></span>
      <span class="query-progress__message">
        ${escapeHtml(message || 'Running query...')}
        <span class="query-progress__elapsed">0s</span>
      </span>
      ${onCancel ? '<button type="button" class="query-progress__cancel">Cancel</button>' : ''}
    `;
    containerEl.prepend(banner);

    const elapsedEl = banner.querySelector('.query-progress__elapsed');
    const cancelBtn = banner.querySelector('.query-progress__cancel');
    const startedAt = Date.now();

    const tick = setInterval(() => {
      const seconds = Math.floor((Date.now() - startedAt) / 1000);
      elapsedEl.textContent = `${seconds}s`;

      // Escalate styling at 20s to signal "this is unusually long"
      if (seconds >= 20) banner.classList.add('query-progress--slow');

      // Update message at thresholds so the user knows we're still alive
      if (seconds >= 30 && !banner.dataset.slowMsgShown) {
        banner.dataset.slowMsgShown = '1';
        banner.querySelector('.query-progress__message').firstChild.textContent =
          'Still running — large org scans can take up to a minute. ';
      }
    }, 1000);

    if (cancelBtn && onCancel) {
      cancelBtn.addEventListener('click', () => {
        onCancel();
        stop();
      });
    }

    function stop() {
      clearInterval(tick);
      banner.remove();
    }

    return { stop };
  }

  // -- Empty / success / error states ---------------------------------------

  /**
   * Render an empty state into a container.
   * variant: 'neutral' | 'success' | 'error'
   */
  function renderEmpty(containerEl, {
    variant = 'neutral',
    icon,
    title,
    message,
    actions = []  // [{ label, onClick, primary }]
  }) {
    const defaultIcons = {
      neutral: 'fa-folder-open',
      success: 'fa-circle-check',
      error:   'fa-triangle-exclamation'
    };
    const iconClass = icon || defaultIcons[variant];

    const actionsHtml = actions.length ? `
      <div class="empty-state__actions">
        ${actions.map((a, i) => `
          <button type="button"
                  class="empty-state__action empty-state__action--${a.primary ? 'primary' : 'secondary'}"
                  data-action-index="${i}">
            ${escapeHtml(a.label)}
          </button>
        `).join('')}
      </div>
    ` : '';

    containerEl.innerHTML = `
      <div class="empty-state empty-state--${variant}" role="status">
        <div class="empty-state__icon" aria-hidden="true">
          <i class="fa-solid ${iconClass}"></i>
        </div>
        <h3 class="empty-state__title">${escapeHtml(title)}</h3>
        <p class="empty-state__message">${escapeHtml(message)}</p>
        ${actionsHtml}
      </div>
    `;

    // Wire up action buttons
    containerEl.querySelectorAll('[data-action-index]').forEach(btn => {
      const idx = parseInt(btn.dataset.actionIndex, 10);
      btn.addEventListener('click', actions[idx].onClick);
    });
  }

  /** Convenience: error state with technical details disclosure. */
  function renderError(containerEl, { title, message, error, onRetry }) {
    const detailsHtml = error ? `
      <details class="empty-state__details">
        <summary>Technical details</summary>
        <pre>${escapeHtml(typeof error === 'string' ? error : JSON.stringify(error, null, 2))}</pre>
      </details>
    ` : '';

    renderEmpty(containerEl, {
      variant: 'error',
      title: title || 'Something went wrong',
      message: message || 'The query failed to complete. Check your permissions and try again.',
      actions: onRetry ? [{ label: 'Retry', primary: true, onClick: onRetry }] : []
    });

    // Append details after renderEmpty wrote the DOM
    if (detailsHtml) {
      containerEl.querySelector('.empty-state').insertAdjacentHTML('beforeend', detailsHtml);
    }
  }

  // -- Utilities ------------------------------------------------------------

  function escapeHtml(str) {
    if (str == null) return '';
    return String(str)
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#39;');
  }

  return {
    renderTableSkeleton,
    renderKpiSkeleton,
    renderTierCardsSkeleton,
    renderChartSkeleton,
    startQueryProgress,
    renderEmpty,
    renderError,
    escapeHtml
  };
})();

/* ============================================================
   DASHBOARD CONTROLLER
   ============================================================ */

const Dashboard = (() => {

  const CACHE_KEY = 'dashboard:cache';
  const CACHE_TTL_MS = 60 * 60 * 1000; // 1 hour

  // -- Public API ----------------------------------------------------------

  function init() {
    const refreshBtn = document.getElementById('btn-refresh-dashboard');
    if (!refreshBtn) return;
    if (refreshBtn.dataset.bound === '1') {
      load();
      return;
    }
    refreshBtn.dataset.bound = '1';
    refreshBtn.addEventListener('click', () => load({ force: true }));
    load();
  }

  async function load({ force = false } = {}) {
    const cached = !force ? readCache() : null;

    if (cached) {
      try {
        render(cached.data);
        updateFreshness(cached.fetchedAt);
        return;
      } catch (cacheErr) {
        console.warn('Cached dashboard data failed to render — refetching', cacheErr);
        // Fall through to fresh fetch
      }
    }

    renderSkeletons();
    setRefreshSpinning(true);

    try {
      // Parallel fetch — each widget fails independently
      const [kpis, opportunities, projects, anomalies] = await Promise.allSettled([
        fetchKpis(),
        fetchOpportunities(),
        fetchTopProjects(),
        fetchAnomalies()
      ]);

      const data = {
        kpis: settled(kpis),
        opportunities: settled(opportunities),
        projects: settled(projects),
        anomalies: settled(anomalies)
      };

      render(data);
      writeCache(data);  // only cache after successful render
      updateFreshness(Date.now());
    } catch (err) {
      console.error('Dashboard load failed', err);
    } finally {
      setRefreshSpinning(false);
    }
  }

  // -- Skeleton rendering --------------------------------------------------

  function renderSkeletons() {
    const kpiContainer = document.getElementById('dashboard-kpis');
    kpiContainer.innerHTML = Array(4).fill(`
      <div class="kpi-card" aria-hidden="true">
        <span class="skeleton skeleton--text-sm"></span>
        <span class="skeleton skeleton--number"></span>
        <span class="skeleton skeleton--text-sm" style="width:40%;"></span>
      </div>
    `).join('');

    document.getElementById('dashboard-opportunities').innerHTML =
      Array(5).fill(`
        <div class="opportunity-row" aria-hidden="true">
          <span class="skeleton skeleton--text-sm"></span>
          <span class="skeleton skeleton--text"></span>
          <span class="skeleton skeleton--badge"></span>
          <span class="skeleton skeleton--text-sm" style="width:60px;"></span>
        </div>
      `).join('');

    document.getElementById('dashboard-top-projects').innerHTML = `
      <div class="bar-list">
        ${Array(5).fill(`
          <div>
            <div style="display:flex; justify-content:space-between; margin-bottom:6px;">
              <span class="skeleton skeleton--text-sm"></span>
              <span class="skeleton skeleton--text-sm" style="width:60px;"></span>
            </div>
            <div class="bar-row__bar"><div class="bar-row__fill" style="width:0;"></div></div>
          </div>
        `).join('')}
      </div>`;

    document.getElementById('dashboard-anomalies').innerHTML =
      Array(3).fill(`
        <div class="anomaly-row" aria-hidden="true">
          <span class="skeleton skeleton--avatar" style="width:16px;height:16px;"></span>
          <span class="skeleton skeleton--text"></span>
          <span class="skeleton skeleton--text-sm" style="width:80px;"></span>
        </div>
      `).join('');
  }

  // -- Real rendering ------------------------------------------------------

  function render(data) {
    renderKpis(data.kpis);
    renderOpportunities(data.opportunities);
    renderTopProjects(data.projects);
    renderAnomalies(data.anomalies);
  }

  function renderKpis(kpis) {
    const container = document.getElementById('dashboard-kpis');
    if (!kpis || kpis.stub === true) {
      UIState.renderError(container, {
        title: 'KPIs unavailable',
        message: 'Could not load summary metrics.',
        onRetry: () => load({ force: true })
      });
      return;
    }

    container.innerHTML = `
      ${kpiCard({
        label: 'Month-to-Date Spend',
        value: formatCurrency(kpis.mtdSpend ?? 0),
        delta: kpis.mtdSpendDelta,
        deltaLabel: 'vs last month',
        deltaDirection: (kpis.mtdSpendDelta ?? 0) > 0 ? 'up' : 'down'
      })}
      ${kpiCard({
        label: 'Forecast (EOM)',
        value: formatCurrency(kpis.forecastSpend ?? 0),
        delta: null,
        deltaLabel: `vs ${formatCurrency(kpis.lastMonthSpend ?? 0)} last month`
      })}
      ${kpiCard({
        label: 'Potential Savings',
        value: formatCurrency(kpis.potentialSavings ?? 0),
        delta: null,
        deltaLabel: `${kpis.opportunityCount ?? 0} opportunities`,
        savings: true
      })}
      ${kpiCard({
        label: 'Anomalies Detected',
        value: (kpis.anomalyCount ?? 0).toString(),
        delta: null,
        deltaLabel: 'last 7 days'
      })}
    `;
  }

  function kpiCard({ label, value, delta, deltaLabel, deltaDirection, savings }) {
    const deltaHtml = delta != null
      ? `<span class="kpi-card__delta kpi-card__delta--${deltaDirection}">
           <i class="fa-solid fa-arrow-${deltaDirection}"></i>
           ${Math.abs(delta)}% ${deltaLabel}
         </span>`
      : `<span class="kpi-card__delta">${deltaLabel}</span>`;

    return `
      <div class="kpi-card ${savings ? 'kpi-card--savings' : ''}">
        <span class="kpi-card__label">${label}</span>
        <span class="kpi-card__value">${value}</span>
        ${deltaHtml}
      </div>`;
  }

  function renderOpportunities(items) {
    const container = document.getElementById('dashboard-opportunities');

    if (!items) {
      UIState.renderError(container, {
        title: 'Could not load opportunities',
        onRetry: () => load({ force: true })
      });
      return;
    }
    if (items.length === 0) {
      UIState.renderEmpty(container, {
        variant: 'success',
        title: 'No optimization opportunities',
        message: 'Your environment looks well-optimized. Check back after the next billing cycle.'
      });
      return;
    }

    container.innerHTML = items.slice(0, 5).map((item, i) => `
      <a class="opportunity-row" href="${safeDeepLinkHref(item.deepLink)}">
        <span class="opportunity-row__rank">${i + 1}</span>
        <span class="opportunity-row__label">${escapeHtml(item.label)}</span>
        <span class="opportunity-row__module">${escapeHtml(item.module)}</span>
        <span class="opportunity-row__savings">${formatCurrency(item.monthlySavings)}/mo</span>
      </a>
    `).join('');
  }

  function renderTopProjects(projects) {
    const container = document.getElementById('dashboard-top-projects');

    if (!projects) {
      UIState.renderError(container, {
        title: 'Could not load project costs',
        onRetry: () => load({ force: true })
      });
      return;
    }
    if (projects.length === 0) {
      UIState.renderEmpty(container, {
        variant: 'neutral',
        title: 'No project data yet',
        message: 'Configure cost attribution settings to begin tracking project-level costs.',
        actions: [{ label: 'Open Cost Attribution', primary: true,
                    onClick: () => location.hash = '#cost-attribution' }]
      });
      return;
    }

    const max = Math.max(...projects.map(p => p.cost));
    container.innerHTML = `
      <div class="bar-list">
        ${projects.slice(0, 5).map(p => `
          <div>
            <div class="bar-row">
              <span class="bar-row__label">${escapeHtml(p.projectId)}</span>
              <span class="bar-row__value">${formatCurrency(p.cost)}</span>
            </div>
            <div class="bar-row__bar">
              <div class="bar-row__fill" style="width: ${(p.cost / max * 100).toFixed(1)}%;"></div>
            </div>
          </div>
        `).join('')}
      </div>`;
  }

  function renderAnomalies(anomalies) {
    const container = document.getElementById('dashboard-anomalies');

    if (!anomalies) {
      UIState.renderError(container, {
        title: 'Could not load anomalies',
        onRetry: () => load({ force: true })
      });
      return;
    }
    if (anomalies.length === 0) {
      UIState.renderEmpty(container, {
        variant: 'success',
        title: 'No anomalies detected',
        message: 'Spend patterns over the last 7 days look normal.'
      });
      return;
    }

    container.innerHTML = anomalies.map(a => `
      <div class="anomaly-row">
        <i class="fa-solid fa-triangle-exclamation anomaly-row__icon
           ${a.severity === 'critical' ? 'anomaly-row__icon--critical' : ''}"></i>
        <span class="anomaly-row__text">${escapeHtml(a.message)}</span>
        <a class="anomaly-row__action" href="${safeDeepLinkHref(a.deepLink)}">
          Investigate <i class="fa-solid fa-arrow-right" style="font-size:0.6rem;"></i>
        </a>
      </div>
    `).join('');
  }

  // -- Freshness pill ------------------------------------------------------

  function updateFreshness(timestamp) {
    const text = document.querySelector('#dashboard-freshness .freshness-pill__text');
    const update = () => { text.textContent = `Updated ${timeAgo(timestamp)}`; };
    update();
    // Re-render every minute so "4m ago" stays accurate
    if (Dashboard._freshTimer) clearInterval(Dashboard._freshTimer);
    Dashboard._freshTimer = setInterval(update, 60 * 1000);
  }

  function setRefreshSpinning(on) {
    document.querySelector('#btn-refresh-dashboard')
      .classList.toggle('is-spinning', on);
  }

  // -- Cache ---------------------------------------------------------------

  function readCache() {
    try {
      const raw = sessionStorage.getItem(CACHE_KEY);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      if (Date.now() - parsed.fetchedAt > CACHE_TTL_MS) return null;
      return parsed;
    } catch { return null; }
  }

  function writeCache(data) {
    try {
      sessionStorage.setItem(CACHE_KEY, JSON.stringify({
        fetchedAt: Date.now(),
        data
      }));
    } catch { /* quota exceeded — silent fail */ }
  }

  // -- Fetchers ------------------------------------------------------------
  // TODO: wire these to your real backend endpoints.
  // Each must resolve to the shape commented below, OR throw on failure.

  async function fetchKpis() {
    // Expected: { mtdSpend, mtdSpendDelta, forecastSpend, lastMonthSpend,
    //             potentialSavings, opportunityCount, anomalyCount }
    const r = await fetch('/api/dashboard/kpis');
    if (!r.ok) throw new Error('kpis');
    return r.json();
  }

  async function fetchOpportunities() {
    // Expected: [{ label, module, monthlySavings, deepLink }, ...]
    const r = await fetch('/api/dashboard/opportunities?limit=5');
    if (!r.ok) throw new Error('opportunities');
    return r.json();
  }

  async function fetchTopProjects() {
    // Expected: [{ projectId, cost }, ...] — max 5
    const r = await fetch('/api/dashboard/top-projects?limit=5');
    if (!r.ok) throw new Error('projects');
    return r.json();
  }

  async function fetchAnomalies() {
    // Expected: [{ severity: 'warning'|'critical', html, deepLink }, ...]
    const r = await fetch('/api/dashboard/anomalies');
    if (!r.ok) throw new Error('anomalies');
    return r.json();
  }

  // -- Utilities -----------------------------------------------------------

  function settled(result) {
    return result.status === 'fulfilled' ? result.value : null;
  }

  function formatCurrency(n) {
    if (n == null) return '—';
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      maximumFractionDigits: n >= 1000 ? 0 : 2
    }).format(n);
  }

  function timeAgo(timestamp) {
    const seconds = Math.floor((Date.now() - timestamp) / 1000);
    if (seconds < 60) return 'just now';
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes}m ago`;
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return `${hours}h ago`;
    return `${Math.floor(hours / 24)}d ago`;
  }

  function escapeHtml(s) {
    if (s == null) return '';
    return String(s)
      .replaceAll('&', '&amp;').replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;').replaceAll('"', '&quot;')
      .replaceAll("'", '&#39;');
  }

  function safeDeepLinkHref(link) {
    // deepLink values are always in-page hash-fragment navigation targets
    // (e.g. "#capacity?reservation=..."). Reject anything else outright —
    // an absolute/protocol-relative URL (including a javascript: URI) would
    // execute if a compromised/malicious backend response ever supplied
    // one, since HTML-escaping an href attribute does not neutralize a
    // javascript: scheme.
    return (typeof link === 'string' && link.startsWith('#')) ? link : '#';
  }

  return { init, load };
})();

const FluidScaling = (() => {
  const SKELETON_ROWS = 8;       // number of skeleton rows to render during load

  const fmtUsd = (n) => {
    if (n == null || isNaN(n)) return '—';
    if (n === 0) return '$0';
    return '$' + Math.round(n).toLocaleString('en-US');
  };

  const fmtNumber = (n, decimals = 0) => {
    if (n == null || isNaN(n)) return '—';
    return Number(n).toLocaleString('en-US', {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    });
  };

  const fmtPct = (n) => {
    if (n == null || isNaN(n)) return '—';
    return Number(n).toFixed(1) + '%';
  };

  function init() {
    const btn = document.getElementById('analyze-fluid-btn');
    if (!btn || btn.dataset.bound === '1') return;

    // Load saved data from Local Storage
    const savedEstimate = localStorage.getItem('bq_fluid_estimate_data');
    if (savedEstimate) {
        try {
            const parsed = JSON.parse(savedEstimate);
            if (parsed && Array.isArray(parsed.reservations)) {
                renderResults(parsed.reservations);
                renderConfigStatus(parsed.config_status);
            } else if (Array.isArray(parsed)) {
                renderResults(parsed);
            } else {
                localStorage.removeItem('bq_fluid_estimate_data');
            }
        } catch (e) {
            console.error('Failed to parse saved fluid estimate data', e);
            localStorage.removeItem('bq_fluid_estimate_data');
        }
    }
    
    const savedSim = localStorage.getItem('bq_fluid_simulation_data');
    if (savedSim) {
        try {
            const parsed = JSON.parse(savedSim);
            if (parsed && (Array.isArray(parsed) || Array.isArray(parsed.patterns))) {
                renderFluidSimResults(parsed);
            } else {
                localStorage.removeItem('bq_fluid_simulation_data');
            }
        } catch (e) {
            console.error('Failed to parse saved fluid simulation data', e);
            localStorage.removeItem('bq_fluid_simulation_data');
        }
    }

    const copyBtn = document.getElementById('copy-fs-ddl-btn');
    if (copyBtn) {
        copyBtn.addEventListener('click', () => {
            const output = document.getElementById('fs-ddl-output');
            if (output && output.value) {
                copyToClipboard(output.value).then(() => {
                    showNotification('DDL copied to clipboard!', 'success');
                }).catch(err => {
                    console.error('Failed to copy DDL', err);
                    showNotification('Failed to copy DDL.', 'error');
                });
            }
        });
    }

    btn.dataset.bound = '1';
    btn.addEventListener('click', load);
  }

  // --- Button loading helper (self-contained, no dependency on global setLoading) ---
  function setBtnLoading(btn, isLoading) {
    if (!btn) return;
    const label   = btn.querySelector('.btn-label');
    const spinner = btn.querySelector('.btn-spinner');
    btn.disabled  = isLoading;
    if (label)   label.textContent = isLoading ? 'Running…' : 'Run Estimation';
    if (spinner) spinner.hidden    = !isLoading;
  }

  // --- Status banner helper for the jobs panel ---
  function setJobsStatus(state, message = '') {
    const el = document.getElementById('fluid-jobs-status');
    if (!el) return;
    el.className = 'panel-status' + (state ? ' ' + state : '');
    el.textContent = message;
  }

  async function load() {
    const container = document.getElementById('view-fluid-scaling');
    const btn       = document.getElementById('analyze-fluid-btn');
    const tableEl   = document.getElementById('fluid-estimate-table');

    if (!tableEl) return;

    const orgProject = localStorage.getItem('bq_org_project') || '';
    if (!orgProject) {
      UIState.renderError(container, {
        title: 'Missing Project',
        message: 'Execution Project ID must be set in Settings before running an estimate.',
      });
      return;
    }

    const lookback = parseInt(document.getElementById('fs-lookback').value, 10) || 7;
    const price    = parseFloat(document.getElementById('fs-price').value)     || 0.06;
    const region   = localStorage.getItem('bq_region') || 'region-us';
    const adminProject = localStorage.getItem('bq_admin_project') || '';

    // Reset both tables
    if ($.fn.DataTable.isDataTable('#fluid-estimate-table')) {
      $('#fluid-estimate-table').DataTable().destroy();
    }
    if ($.fn.DataTable.isDataTable('#fluid-simulation-table')) {
      $('#fluid-simulation-table').DataTable().destroy();
    }

    // Reset KPI cards to prevent showing old cached estimates during load
    document.getElementById('fs-total-saved-hours').textContent = '—';
    document.getElementById('fs-total-saved-usd').textContent   = '—';
    document.getElementById('fs-total-monthly').textContent     = '—';
    document.getElementById('fs-total-annual').textContent      = '—';
    const statusPanel = document.getElementById('fs-org-rec-panel');
    if (statusPanel) statusPanel.style.display = 'none';
    const banner = document.getElementById('fluid-disclaimer-banner');
    if (banner) banner.style.display = 'none';

    UIState.renderTableSkeleton(tableEl, SKELETON_ROWS);
    const simTableEl = document.getElementById('fluid-simulation-table');
    if (simTableEl) {
        UIState.renderTableSkeleton(simTableEl, SKELETON_ROWS);
    }
    setJobsStatus('loading', 'Loading job-level simulation…');
    container.querySelectorAll('.query-progress').forEach(b => b.remove());

    setBtnLoading(btn, true);
    const progress = UIState.startQueryProgress(document.getElementById('analyze-fluid-btn').parentElement, {
      message: 'Estimating Fluid Scaling savings. This may take a minute as we process timeline data...',
    });

    // Run BOTH fetches in parallel and tolerate partial failure
    const [estimateResult, jobsResult] = await Promise.allSettled([
      fetchEstimate({ orgProject, adminProject, region, lookback, price, maxBytesBilledGb: state.maxBytesBilledGb }),
      fetchJobSimulation({ orgProject, region, lookback, price, maxBytesBilledGb: state.maxBytesBilledGb }),
    ]);

    if (estimateResult.status === 'fulfilled') {
        renderResults(estimateResult.value.reservations);
        renderConfigStatus(estimateResult.value.config_status);
        safeSetLocalStorage('bq_fluid_estimate_data', JSON.stringify(estimateResult.value));
    } else {
        console.error('Estimate fetch failed:', estimateResult.reason);
        showNotification('Estimate fetch failed: ' + (estimateResult.reason?.message || estimateResult.reason), 'error');
        renderResults([]);
    }

    if (jobsResult.status === 'fulfilled') {
        renderFluidSimResults(jobsResult.value);
        safeSetLocalStorage('bq_fluid_simulation_data', JSON.stringify(jobsResult.value));
    } else {
        console.error('Jobs fetch failed:', jobsResult.reason);
        showNotification('Jobs fetch failed: ' + (jobsResult.reason?.message || jobsResult.reason), 'error');
        setJobsStatus('error', 'Job-level simulation failed.');
    }

    setBtnLoading(btn, false);
    if (progress) progress.stop();
  }

  // ---------------------------------------------------------------
  // Fetch helpers — both throw on non-OK so Promise.allSettled catches
  // ---------------------------------------------------------------
  async function fetchEstimate({ orgProject, adminProject, region, lookback, price, maxBytesBilledGb }) {
    const res = await fetch('/api/fluid-scaling/estimate', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        org_project_id:    orgProject,
        admin_project_id:  adminProject,
        region,
        lookback_days:     lookback,
        price_per_slot_hr: price,
        max_bytes_billed_gb: maxBytesBilledGb,
      }),
    });
    if (!res.ok) {
      const detail = await safeReadDetail(res);
      throw new Error(detail || `HTTP ${res.status}`);
    }
    return res.json();
  }

  async function fetchJobSimulation({ orgProject, region, lookback, price, maxBytesBilledGb }) {
    const res = await fetch('/api/slots/fluid_simulation', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        org_project_id:       orgProject,
        region,
        lookback_days:        lookback,
        edition_slot_hr_rate: price,
        max_bytes_billed_gb:  maxBytesBilledGb,
      }),
    });
    if (!res.ok) {
      const detail = await safeReadDetail(res);
      throw new Error(detail || `HTTP ${res.status}`);
    }
    
    // Guard: refuse to parse absurd payloads (prevents tab freeze / OOM)
    const len = Number(res.headers.get('content-length') || 0);
    const MAX_BYTES = 2 * 1024 * 1024; // 2 MB ceiling (GZipped)
    if (len > MAX_BYTES) {
      throw new Error(
        `Simulation response too large (${(len / 1024 / 1024).toFixed(0)} MB). ` +
        `The backend likely failed to collapse query patterns. Aborting render.`
      );
    }
    return res.json();
  }

  async function safeReadDetail(res) {
    try {
      const j = await res.json();
      return j?.detail || null;
    } catch { return null; }
  }

  // ---------------------------------------------------------------
  // Renderers
  // ---------------------------------------------------------------
  function renderResults(data) {
    const tableBody = document.querySelector('#fluid-estimate-table tbody');
    if (!tableBody) return;

    // Normalize data if it is the response wrapper object instead of the array
    if (data && !Array.isArray(data) && Array.isArray(data.reservations)) {
        data = data.reservations;
    }

    // Fallback to empty array if data is still not an array
    if (!Array.isArray(data)) {
        data = [];
    }

    // Destroy DataTable first if it already exists to prevent column count mismatch warnings on DOM mutation
    if ($.fn.DataTable.isDataTable('#fluid-estimate-table')) {
        $('#fluid-estimate-table').DataTable().destroy();
    }

    // Calculate Totals
    let totalSavedHours = 0;
    let totalSavedUsd = 0;
    let totalMonthly = 0;
    let totalAnnual = 0;

    if (data && data.length > 0) {
        data.forEach(row => {
            const savedHrs = Number(row.slot_hours_saved) || 0;
            const savedUsd = Number(row.estimated_usd_saved_window) || 0;
            const monthly = Number(row.extrapolated_monthly_usd) || 0;
            const annual = Number(row.extrapolated_annual_usd) || 0;

            totalSavedHours += savedHrs;
            totalSavedUsd += savedUsd;
            totalMonthly += monthly;
            totalAnnual += annual;
        });
    }

    // Inject Totals into cards
    document.getElementById('fs-total-saved-hours').textContent = Math.round(totalSavedHours).toLocaleString();
    document.getElementById('fs-total-saved-usd').textContent = `$${Math.round(totalSavedUsd).toLocaleString()}`;
    document.getElementById('fs-total-monthly').textContent = `$${Math.round(totalMonthly).toLocaleString()}`;
    document.getElementById('fs-total-annual').textContent = `$${Math.round(totalAnnual).toLocaleString()}`;

    // If empty response
    if (!data || data.length === 0) {
      tableBody.innerHTML = `
        <tr>
          <td colspan="10" style="text-align:center; padding:1.5rem; color:#94a3b8;">
            No active reservations or fluid scaling candidates found.
          </td>
        </tr>`;
      return;
    }

    // Render Rows
    tableBody.innerHTML = data.map(row => {
      return `
        <tr>
          <td title="${UIState.escapeHtml(row.reservation_id)}">${UIState.escapeHtml(row.reservation_short_name)}</td>
          <td><span class="badge badge-info">${UIState.escapeHtml(row.status)}</span></td>
          <td>${fmtNumber(row.legacy_autoscaler_slot_hours, 1)}</td>
          <td>${fmtNumber(row.fluid_autoscaler_slot_hours, 1)}</td>
          <td title="Total used slot-hours (baseline-inclusive), matches doc's total_pure_used_slots / 3600">
            ${fmtNumber(row.total_pure_used_slot_hours, 1)}
          </td>
          <td style="color: #4ade80;" title="Recoverable cooldown slot-hours (clamped model)">
            ${fmtNumber(row.slot_hours_saved, 1)}
          </td>
          <td style="color: #4ade80; font-weight: bold;" title="Primary savings (clamped cooldown-waste model)">
            ${fmtPct(row.clamped_pct_savings)}
          </td>
          <td style="color: #4ade80;">${fmtUsd(row.estimated_usd_saved_window)}</td>
          <td style="color: #4ade80;">${fmtUsd(row.extrapolated_monthly_usd)}</td>
          <td style="font-weight: bold; color: #4ade80;">${fmtUsd(row.extrapolated_annual_usd)}</td>
        </tr>`;
    }).join('');
    
    safeInitDataTable('#fluid-estimate-table', {
        scrollX: true,
        order: [[9, 'desc']] // Extrapolated annual savings (shifted to index 9)
    });
  }

  function renderConfigStatus(configStatus) {
    const panel  = document.getElementById('fs-org-rec-panel');
    const text   = document.getElementById('fs-org-rec-text');
    const output = document.getElementById('fs-ddl-output');
    const container = document.getElementById('fs-ddl-container');
    const builder = document.getElementById('fs-config-builder');

    if (!panel || !configStatus) return;

    panel.style.display = 'block';

    if (configStatus.enabled) {
      panel.style.borderColor = 'rgba(34, 197, 94, 0.5)'; // Green
      text.innerHTML = `<i class="fa-solid fa-circle-check" style="color: #4ade80;"></i> Fluid Scaling is already enabled for all active reservations in this region. No action needed.`;
      if (container) container.style.display = 'none';
      if (output) output.value = '';
      if (builder) builder.innerHTML = '';
      const copyBtn = document.getElementById('copy-fs-ddl-btn');
      if (copyBtn) copyBtn.style.display = 'none';
    } else {
      panel.style.borderColor = 'rgba(234, 179, 8, 0.5)'; // Yellow
      const missingList = configStatus.missing_reservations.join(', ');
      text.innerHTML = `<i class="fa-solid fa-circle-exclamation" style="color: #facc15;"></i> Fluid Scaling is NOT enabled for the following active reservations: <strong>${UIState.escapeHtml(missingList)}</strong>. Select which reservations to include and copy the generated DDL.`;
      const copyBtn = document.getElementById('copy-fs-ddl-btn');
      if (copyBtn) copyBtn.style.display = '';

      if (container && output && builder) {
        container.style.display = 'block';

        // Read admin project / region from localStorage (same source as checkStatus)
        const adminProject = localStorage.getItem('bq_admin_project') || localStorage.getItem('bq_org_project') || '';
        const region = localStorage.getItem('bq_region') || 'region-us';
        const regionNorm = region.startsWith('region-') ? region : 'region-' + region;

        // Build reservation list: configured (pre-checked) + missing (unchecked)
        const allReservations = [];
        (configStatus.configured_reservations || []).forEach(r => allReservations.push({ name: r, enabled: true }));
        (configStatus.missing_reservations || []).forEach(r => allReservations.push({ name: r, enabled: false }));
        allReservations.sort((a, b) => a.name.localeCompare(b.name));

        // Regenerate DDL based on checked reservations
        function regenerateDDL() {
          const checked = [];
          builder.querySelectorAll('input[type="checkbox"][data-res-name]').forEach(cb => {
            if (cb.checked) checked.push(cb.dataset.resName);
          });
          checked.sort();
          if (checked.length === 0) {
            output.value = '-- No reservations selected';
          } else {
            const listStr = checked.map(r => `'${r}'`).join(', ');
            output.value = `ALTER PROJECT \`${adminProject}\`\nSET OPTIONS (\n  \`${regionNorm}.preflight_fluid_autoscaling_reservations\` = [${listStr}]\n);`;
          }
        }

        // Render checkbox table
        let html = `<table style="width: 100%; border-collapse: collapse; font-size: 0.9rem;">
          <thead>
            <tr style="border-bottom: 1px solid rgba(255,255,255,0.15);">
              <th style="padding: 0.5rem; text-align: left; width: 40px;">
                <input type="checkbox" id="fs-select-all" title="Select / Deselect All"
                  style="accent-color: #38bdf8; cursor: pointer; width: 16px; height: 16px;">
              </th>
              <th style="padding: 0.5rem; text-align: left; color: #94a3b8;">Reservation</th>
              <th style="padding: 0.5rem; text-align: left; color: #94a3b8;">Current Status</th>
            </tr>
          </thead>
          <tbody>`;

        allReservations.forEach(r => {
          const statusColor = r.enabled ? '#4ade80' : '#64748b';
          const statusIcon  = r.enabled ? 'fa-circle-check' : 'fa-circle-minus';
          const statusText  = r.enabled ? 'Enabled' : 'Not Enabled';
          html += `
            <tr style="border-bottom: 1px solid rgba(255,255,255,0.05); transition: background 0.15s;"
                onmouseenter="this.style.background='rgba(255,255,255,0.04)'"
                onmouseleave="this.style.background='transparent'">
              <td style="padding: 0.5rem;">
                <input type="checkbox" data-res-name="${UIState.escapeHtml(r.name)}" ${r.enabled ? 'checked' : ''}
                  style="accent-color: #38bdf8; cursor: pointer; width: 16px; height: 16px;">
              </td>
              <td style="padding: 0.5rem; color: #e2e8f0; font-family: monospace;">${UIState.escapeHtml(r.name)}</td>
              <td style="padding: 0.5rem;">
                <i class="fa-solid ${statusIcon}" style="color: ${statusColor}; margin-right: 0.3rem;"></i>
                <span style="color: ${statusColor};">${statusText}</span>
              </td>
            </tr>`;
        });

        html += `</tbody></table>`;
        builder.innerHTML = html;

        // Wire up Select All and individual checkboxes
        const selectAll = builder.querySelector('#fs-select-all');
        const allBoxes = builder.querySelectorAll('input[data-res-name]');

        // Pre-check all by default (user came here to enable missing ones)
        allBoxes.forEach(cb => { cb.checked = true; });
        if (selectAll) selectAll.checked = true;

        function updateSelectAll() {
          if (selectAll) selectAll.checked = Array.from(allBoxes).every(c => c.checked);
        }

        if (selectAll) {
          selectAll.addEventListener('change', () => {
            allBoxes.forEach(cb => { cb.checked = selectAll.checked; });
            regenerateDDL();
          });
        }

        allBoxes.forEach(cb => {
          cb.addEventListener('change', () => {
            updateSelectAll();
            regenerateDDL();
          });
        });

        // Generate initial DDL (all checked by default)
        if (selectAll) selectAll.checked = true;
        regenerateDDL();
      }
    }
  }

  function renderFluidSimResults(payload) {
    const tbody = document.querySelector('#fluid-simulation-table tbody');
    if (!tbody) return;

    // Destroy DataTable first if it already exists to prevent column count mismatch warnings on DOM mutation
    if ($.fn.DataTable.isDataTable('#fluid-simulation-table')) {
        $('#fluid-simulation-table').DataTable().destroy();
    }

    // Extract rows based on new response model or fallback to payload if it's already an array
    let rows = [];
    if (payload) {
        if (Array.isArray(payload)) {
            rows = payload;
        } else if (Array.isArray(payload.patterns)) {
            rows = payload.patterns;
        }
    }

    setJobsStatus('', '');  // clear status when we have data

    // Tell the user this is a top-N view ranked by impact, not the full list.
    const totalFound = payload?.total_patterns_found ?? rows.length;
    const subtitle = document.getElementById('fluid-simulation-subtitle');
    if (totalFound > rows.length) {
        setJobsStatus('info', `Showing top ${rows.length} of ${totalFound.toLocaleString()} patterns by savings impact.`);
        if (subtitle) {
            subtitle.textContent = `Top ${rows.length} (query pattern × reservation) combinations ranked by estimated savings impact. The same pattern may appear once per reservation it runs on.`;
        }
    } else {
        if (subtitle) {
            subtitle.textContent = `Query pattern × reservation combinations ranked by estimated savings impact. The same pattern may appear once per reservation it runs on.`;
        }
    }

    const MAX_RENDER_ROWS = 500;
    if (rows.length > MAX_RENDER_ROWS) {
        console.warn(`[FluidScaling] truncating ${rows.length} → ${MAX_RENDER_ROWS} rows`);
        setJobsStatus('warning', `Showing top ${MAX_RENDER_ROWS} of ${rows.length} patterns.`);
        rows = rows.slice(0, MAX_RENDER_ROWS);
    }

    // Render disclaimer if present
    const banner = document.getElementById('fluid-disclaimer-banner');
    if (banner) {
        if (payload?.disclaimer && rows.length > 0) {
            banner.textContent = payload.disclaimer;
            banner.style.display = 'block';
        } else {
            banner.style.display = 'none';
        }
    }


    if (rows.length === 0) {
      tbody.innerHTML = `
        <tr>
          <td colspan="9" style="text-align:center; padding:1.5rem; color:#94a3b8;">
            No job-level savings data returned for this window.
          </td>
        </tr>`;
      return;
    }

    tbody.innerHTML = rows.map(row => {
      const patternLabel = row.pattern_label || row.pattern_id || '—';
      const reasons = row.exposure_reasons ? row.exposure_reasons.join('\n') : '';
      const sampleJobId = row.sample_job_id || '';
      const shortJobId = sampleJobId ? (sampleJobId.length > 16 ? sampleJobId.substring(0, 8) + '...' + sampleJobId.slice(-8) : sampleJobId) : '—';
      
      return `
        <tr>
          <td title="${UIState.escapeHtml(row.pattern_id)}">${UIState.escapeHtml(patternLabel)}</td>
          <td class="font-mono" style="font-size: 0.75rem;" title="${UIState.escapeHtml(sampleJobId)}">${UIState.escapeHtml(shortJobId)}</td>
          <td>${UIState.escapeHtml(row.workload_type)}</td>
          <td title="${UIState.escapeHtml(row.reservation_id || '')}">${UIState.escapeHtml(row.reservation_short_name || '—')}</td>
          <td>${UIState.escapeHtml(String(row.job_count))}</td>
          <td>${UIState.escapeHtml(String(row.avg_duration_seconds))}</td>
          <td>${UIState.escapeHtml(String(row.avg_peak_slots))}</td>
          <td title="${UIState.escapeHtml(reasons)}">${UIState.escapeHtml(String(row.cooldown_exposure_score))}</td>
          <td><strong>${fmtUsd(row.indicative_savings_usd)}</strong></td>
        </tr>`;
    }).join('');

    safeInitDataTable('#fluid-simulation-table', {
      pageLength: 10,
      order:      [[8, 'desc']], // Sort by Indicative Savings (Window) descending
      scrollX:    true,
      autoWidth:  false,
      columnDefs: [
        {
          targets: 8,                         // Indicative Savings (Window)
          type: 'num',
          render: function (data, type) {
            // For sorting/filtering, strip $ and commas → real number.
            if (type === 'sort' || type === 'type') {
              const n = parseFloat(String(data).replace(/[~$,]/g, ''));
              return isNaN(n) ? 0 : n;
            }
            return data;  // display: keep the formatted value
          }
        }
      ]
    });
  }

  async function checkStatus() {
    const orgProject = localStorage.getItem('bq_org_project') || '';
    const adminProject = localStorage.getItem('bq_admin_project') || orgProject;
    const region = localStorage.getItem('bq_region') || 'region-us';
    if (!orgProject) return;

    try {
      const res = await fetch('/api/fluid-scaling/status', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          org_project_id: orgProject,
          admin_project_id: adminProject,
          region: region
        })
      });
      if (res.ok) {
        const statuses = await res.json();
        const configured = [];
        const missing = [];
        let ddl = null;
        
        statuses.forEach(s => {
          if (s.enabled) {
            configured.push(s.reservation_id);
          } else {
            missing.push(s.reservation_id);
          }
        });
        
        if (missing.length > 0) {
          const all_list = statuses.map(s => {
            const parts = s.reservation_id.split(/[.:]/);
            const shortName = parts[parts.length - 1];
            return `'${shortName}'`;
          }).join(', ');
          ddl = `ALTER PROJECT \`${adminProject}\`\nSET OPTIONS (\n  \`${region.startsWith('region-') ? region : 'region-' + region}.preflight_fluid_autoscaling_reservations\` = [${all_list}]\n);`;
        }
        
        renderConfigStatus({
          enabled: missing.length === 0,
          configured_reservations: configured,
          missing_reservations: missing,
          ddl: ddl
        });
      }
    } catch (e) {
      console.error('Failed to check fluid scaling status', e);
    }
  }

  function formatCurrency(n) {
    if (!isFinite(n)) return '$0';
    return `$${Math.round(n).toLocaleString()}`;
  }

  return { init, load, checkStatus };
})();

/* ============================================================
   ROUTER
   Single source of truth: URL hash determines current view.
   Event delegation on the nav means new links auto-work.
   ============================================================ */

const Router = (() => {
  const DEFAULT_VIEW = 'storage';

  const onShow = {};   // viewId -> function
  const onHide = {};   // viewId -> function

  function register(viewId, { show, hide } = {}) {
    if (show) onShow[viewId] = show;
    if (hide) onHide[viewId] = hide;
  }

  function getCurrentViewId() {
    return (location.hash || `#${DEFAULT_VIEW}`).replace(/^#/, '').split('?')[0];
  }

  function getQueryParams() {
    const hash = location.hash || '';
    const queryStart = hash.indexOf('?');
    if (queryStart === -1) return {};
    return Object.fromEntries(new URLSearchParams(hash.slice(queryStart + 1)));
  }

  function navigate(viewId, params = {}) {
    const query = new URLSearchParams(params).toString();
    const newHash = `#${viewId}${query ? '?' + query : ''}`;
    if (location.hash === newHash) {
      render();
    } else {
      location.hash = newHash;
    }
  }

  function render() {
    const targetView = getCurrentViewId();
    
    // Global project check: redirect to settings if no project is set (ignore for dashboard/settings)
    if (targetView !== 'settings' && targetView !== 'dashboard' && targetView !== 'about' && !state.orgProject) {
        showNotification('Execution Project ID must be set in Settings before proceeding.', 'warning');
        location.hash = '#settings';
        return;
    }
    
    const params = getQueryParams();

    const allViews = document.querySelectorAll('.view');
    const allNavLinks = document.querySelectorAll('[data-view]');

    let foundView = null;
    let previousView = null;

    allViews.forEach(view => {
      const wasActive = view.classList.contains('is-active');
      const shouldBeActive = view.dataset.view === targetView;

      if (wasActive && !shouldBeActive) {
        previousView = view.dataset.view;
      }
      if (shouldBeActive) {
        foundView = view;
      }

      view.classList.toggle('is-active', shouldBeActive);
    });

    if (!foundView) {
      console.warn(`Router: no view found for "${targetView}", falling back to "${DEFAULT_VIEW}"`);
      const fallback = document.querySelector(`.view[data-view="${DEFAULT_VIEW}"]`);
      if (fallback) {
        fallback.classList.add('is-active');
        foundView = fallback;
      }
    }

    allNavLinks.forEach(link => {
      link.classList.toggle('is-active', link.dataset.view === targetView);
    });

    if (previousView && onHide[previousView]) {
      try { onHide[previousView](); }
      catch (e) { console.error(`onHide ${previousView}:`, e); }
    }
    if (foundView && onShow[foundView.dataset.view]) {
      try { onShow[foundView.dataset.view](params); }
      catch (e) { console.error(`onShow ${foundView.dataset.view}:`, e); }
    }

    document.title = `${capitalize(targetView)} · FinOps Optimizer`;
    updateScopeBadge(targetView);

    const viewport = document.querySelector('.dashboard-viewport');
    if (viewport) viewport.scrollTop = 0;
  }

  function capitalize(s) {
    return s.charAt(0).toUpperCase() + s.slice(1).replace(/-/g, ' ');
  }

  function init() {
    document.addEventListener('click', (e) => {
      const link = e.target.closest('a[data-view]');
      if (!link) return;

      const href = link.getAttribute('href') || '';
      if (!href.startsWith('#')) return;

      if (e.metaKey || e.ctrlKey || e.shiftKey || e.button !== 0) return;

      e.preventDefault();
      navigate(link.dataset.view);
    });

    window.addEventListener('hashchange', render);
    render();
  }

  return { init, register, navigate, getCurrentViewId, getQueryParams };
})();

// Boot
document.addEventListener('DOMContentLoaded', async () => {
  await loadScopeMap();
  Router.init();
});

// Register Dashboard
Router.register('dashboard', {
  show: () => Dashboard.init()
});

// Register Fluid Scaling
Router.register('fluid-scaling', {
  show: () => {
    FluidScaling.init();
  }
});

// ---------------------------------------------------------------------------
// About Panel — fetches /api/about and populates the sidebar badge + view
// ---------------------------------------------------------------------------
(function initAboutPanel() {
  let _aboutData = null;

  async function fetchAbout() {
    if (_aboutData) return _aboutData;
    try {
      const res = await fetch('/api/about');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      _aboutData = await res.json();
    } catch (err) {
      console.warn('About: failed to fetch /api/about:', err);
      _aboutData = {
        name: 'BigQuery FinOps Optimizer',
        version: '?.?.?',
        release_date: '—',
        releases: [],
        repo_url: '#',
        changelog_url: '#',
        demo_url: '#',
      };
    }
    return _aboutData;
  }

  // Populate sidebar badge on page load
  document.addEventListener('DOMContentLoaded', async () => {
    const data = await fetchAbout();
    const badge = document.getElementById('sidebar-version-badge');
    if (badge) badge.textContent = `v${data.version}`;
  });

  // Populate About view when navigated to
  Router.register('about', {
    show: async () => {
      const data = await fetchAbout();

      // Header
      const nameEl = document.getElementById('about-app-name');
      if (nameEl) nameEl.textContent = data.name;

      const versionEl = document.getElementById('about-version');
      if (versionEl) versionEl.textContent = `v${data.version}`;

      const dateEl = document.getElementById('about-release-date');
      if (dateEl) dateEl.textContent = data.release_date;

      // Releases
      const releasesContainer = document.getElementById('about-releases-container');
      if (releasesContainer) {
        releasesContainer.innerHTML = (data.releases || []).map((release, index) => {
          const isLatest = index === 0;
          const showDate = release.version !== release.release_date;
          const highlights = release.highlights || [];
          const MAX_VISIBLE = 5;
          const hasOverflow = highlights.length > MAX_VISIBLE;
          const cardId = `release-card-${index}`;

          const renderItem = (h) => {
              let formatted = h.replace(/^\[(\w+)\]\s*/, (_, tag) => {
                  const colors = { Feature: '#38bdf8', Fixed: '#34d399', Security: '#fbbf24', Change: '#94a3b8', Issue: '#f87171', Breaking: '#f87171', Announcement: '#c084fc' };
                  const c = colors[tag] || '#94a3b8';
                  return `<span style="font-weight: 600; color: ${c};">[${tag}]</span> `;
              });
              formatted = formatted.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
              formatted = formatted.replace(/`([^`]+)`/g, '<code style="background: rgba(255,255,255,0.08); padding: 0.1em 0.35em; border-radius: 3px; font-size: 0.88em;">$1</code>');
              return `<li style="padding: 0.3rem 0; color: var(--text-secondary); font-size: 0.95rem;">
                  <i class="fa-solid fa-check" style="color: #34d399; margin-right: 0.5rem; font-size: 0.75rem; ${isLatest ? '' : 'opacity: 0.6;'}"></i>${formatted}
              </li>`;
          };

          const visibleItems = highlights.slice(0, MAX_VISIBLE).map(renderItem).join('');
          const hiddenItems = hasOverflow ? highlights.slice(MAX_VISIBLE).map(renderItem).join('') : '';
          const toggleBtn = hasOverflow ? `
              <button class="release-expand-btn" data-card="${cardId}"
                  style="background: none; border: none; color: #38bdf8; cursor: pointer; font-size: 0.8rem; font-weight: 600; padding: 0.4rem 0 0 0; display: flex; align-items: center; gap: 4px;">
                  <i class="fa-solid fa-chevron-down" style="font-size: 0.55rem; transition: transform 0.2s;"></i>
                  Show all ${highlights.length} items
              </button>` : '';

          return `
            <div style="background: rgba(255, 255, 255, 0.02); border: 1px solid rgba(255, 255, 255, 0.05); border-radius: 8px; padding: 1rem;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem; padding-bottom: 0.5rem; border-bottom: 1px solid rgba(255, 255, 255, 0.05);">
                    <h3 style="margin: 0; font-size: 1.1rem; color: #38bdf8; ${isLatest ? '' : 'opacity: 0.8;'}">
                        ${isLatest ? '<i class="fa-solid fa-sparkles" style="margin-right: 0.5rem;"></i>' : ''}${release.version}
                    </h3>
                    ${showDate ? `<span style="font-size: 0.85rem; color: var(--text-secondary);">${release.release_date}</span>` : ''}
                </div>
                <ul style="list-style: none; padding: 0; margin: 0;">
                    ${visibleItems}
                </ul>
                ${hasOverflow ? `<ul id="${cardId}-hidden" style="list-style: none; padding: 0; margin: 0; display: none;">${hiddenItems}</ul>` : ''}
                ${toggleBtn}
            </div>
          `;
        }).join('');

        // Wire up expand/collapse buttons
        releasesContainer.querySelectorAll('.release-expand-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                const cardId = btn.dataset.card;
                const hidden = document.getElementById(`${cardId}-hidden`);
                if (!hidden) return;
                const isHidden = hidden.style.display === 'none';
                hidden.style.display = isHidden ? 'block' : 'none';
                const chevron = btn.querySelector('i');
                if (chevron) chevron.style.transform = isHidden ? 'rotate(180deg)' : '';
                const total = btn.textContent.match(/\d+/);
                btn.innerHTML = isHidden
                    ? `<i class="fa-solid fa-chevron-up" style="font-size: 0.55rem; transition: transform 0.2s;"></i> Show less`
                    : `<i class="fa-solid fa-chevron-down" style="font-size: 0.55rem; transition: transform 0.2s;"></i> Show all ${total ? total[0] : ''} items`;
            });
        });
      }

      // Links
      const changelogLink = document.getElementById('about-changelog-link');
      if (changelogLink) changelogLink.href = data.changelog_url || '#';

      const demoLink = document.getElementById('about-demo-link');
      if (demoLink) demoLink.href = data.demo_url || '#';

      const repoLink = document.getElementById('about-repo-link');
      if (repoLink) repoLink.href = data.repo_url || '#';
    }
  });
})();
