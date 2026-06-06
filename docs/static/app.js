// BigQuery FinOps Optimizer - Frontend Logic

// State
const state = {
    orgProject: localStorage.getItem('bq_org_project') || '',
    adminProject: localStorage.getItem('bq_admin_project') || '',
    region: localStorage.getItem('bq_region') || 'region-us',
    remoteModel: localStorage.getItem('bq_remote_model') || '',
    storageData: [],
    slotsData: [],
    slotsChart: null,
    actualProvisioningChart: null,
    jobsScatterChart: null,
    debugMode: true
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

  function redactValue(raw) {
    try {
      const obj = JSON.parse(raw);
      const scrub = (o) => {
        if (!o) return;
        if (Array.isArray(o)) {
          o.forEach(scrub);
        } else if (typeof o === 'object') {
          for (const k of Object.keys(o)) {
            const val = o[k];
            if (val === null || val === undefined) continue;
            if (/email/i.test(k) && typeof val === 'string') {
              o[k] = 'redacted@example.com';
            } else if (/^query$|^query_text$|^query_snippet$/i.test(k) && typeof val === 'string') {
              o[k] = '-- [redacted query]';
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

      let written = 0;
      keys.forEach(k => {
        if (k.startsWith(KEY_PREFIX)) {
          const ok = safeSetLocalStorage(k, parsed.data[k]);
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
        cfgRemoteModel: document.getElementById('cfg-remote-model'),
        saveSettingsBtn: document.getElementById('save-settings-btn'),
        
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
        hboStatusList: document.getElementById('hbo-status-list'),
        
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
        
        // AI Reviewer

        btnRunAiAnalysis: document.getElementById('run-ai-analysis-btn'),
        aiLimit: document.getElementById('ai-limit')
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
        if (elements.cfgRemoteModel) elements.cfgRemoteModel.value = state.remoteModel;
        
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
        state.orgProject = elements.cfgOrgProject.value.trim();
        elements.cfgOrgProject.value = state.orgProject;
        if (elements.cfgAdminProject) {
            state.adminProject = elements.cfgAdminProject.value.trim();
            elements.cfgAdminProject.value = state.adminProject;
            localStorage.setItem('bq_admin_project', state.adminProject);
        }
        state.region = elements.cfgRegion.value;
        
        if (elements.cfgRemoteModel) {
            state.remoteModel = elements.cfgRemoteModel.value.trim();
            elements.cfgRemoteModel.value = state.remoteModel;
            localStorage.setItem('bq_remote_model', state.remoteModel);
        }

        localStorage.setItem('bq_org_project', state.orgProject);
        localStorage.setItem('bq_region', state.region);

        elements.currentProject.textContent = state.orgProject || 'Not Set';
        if (elements.currentAdminProject) elements.currentAdminProject.textContent = state.adminProject || 'Not Set';
        elements.currentRegion.textContent = state.region;

        showNotification('Settings saved successfully.', 'success');
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
                navigator.clipboard.writeText(elements.edDdlOutput.value).then(() => {
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
                navigator.clipboard.writeText(output.value).then(() => {
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
            org_project_id: state.orgProject
        };

        try {
            debug_log("Fetching storage analysis with params:", params);
            const response = await fetch('/api/storage/analyze', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(params)
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
            showNotification('Storage analysis completed.', 'success');
        } catch (error) {
            logger_error(error);
            showNotification(error.message, 'error');
        } finally {
            setLoading(elements.btnAnalyzeStorage, false);
        }
    });

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

            const params = {
                on_demand_rate_per_tb: parseFloat(document.getElementById('jb-od-rate').value),
                edition_slot_hr_rate: parseFloat(document.getElementById('jb-ed-rate').value),
                slot_step_size: parseInt(document.getElementById('jb-slot-step').value),
                lookback_days: parseInt(document.getElementById('jb-lookback').value),
                region: state.region,
                org_project_id: state.orgProject,
                min_bytes_billed: parseInt(document.getElementById('jb-min-size').value) * 1024 * 1024,
                limit_jobs: parseInt(document.getElementById('jb-limit').value),
                fluid_scaling: document.getElementById('jb-fluid-scaling').checked
            };

            try {
                debug_log("Fetching job analysis with params:", params);
                const response = await fetch('/api/jobs/analyze', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(params)
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
                    <td>${row.project_id}</td>
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
            data.top_jobs.forEach(row => {
                const tr = document.createElement('tr');
                const betterOn = row.on_demand_cost <= row.editions_cost ? 'On-Demand' : 'Editions';
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
                
                tr.innerHTML = `
                    <td>${row.project_id}</td>
                    <td style="font-family: monospace; font-size: 0.85rem;">${row.job_id.substring(0, 12)}...</td>
                    <td><span class="badge" style="background: rgba(56, 189, 248, 0.15); color: #38bdf8;">On-Demand</span></td>
                    <td><span class="badge" style="background: ${betterOn === 'On-Demand' ? 'rgba(56, 189, 248, 0.15)' : 'rgba(168, 85, 247, 0.15)'}; color: ${betterColor}; font-weight: 600;">${betterOn}</span></td>
                    <td><span class="badge" style="background: ${categoryBg}; color: ${categoryColor};">${row.category}</span></td>
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
                    navigator.clipboard.writeText(jobId).then(() => {
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

        datasets.forEach((row, index) => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${row.project_name}</td>
                <td>${row.dataset_name}</td>
                <td><span class="badge ${row.currently_on}">${row.currently_on}</span></td>
                <td><span class="badge ${row.better_on}">${row.better_on}</span></td>
                <td>${formatNumber(row.monthly_savings)}</td>
                <td>${(row.monthly_savings_pct * 100).toFixed(2)}%</td>
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
                    navigator.clipboard.writeText(rowData.ddl).then(() => {
                        showNotification('DDL copied to clipboard!', 'success');
                    }).catch(err => {
                        logger_error(err);
                        showNotification('Failed to copy DDL.', 'error');
                    });
                }
            });
        });
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
                    throw new Error(err.detail || 'Simulation failed');
                }

                const data = await response.json();
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
                    const ddl = `ALTER PROJECT \`${adminProj}\` SET OPTIONS (\`${region}.preflight_fluid_autoscaling_reservations\` = ['${resId}']);`;
                    
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
                    navigator.clipboard.writeText(ddl).then(() => {
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

      console.log('Rendering tier cards for reservation:', mainRes.reservation_id, mainRes);

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

        navigator.clipboard.writeText(ddl)
          .then(() => {
            const original = fresh.textContent;
            fresh.textContent = '✓ COPIED';
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

        data.forEach(row => {
            console.log("[BIGQUERY-OPTIMIZER] Row data:", row);
            
            const avgBytes = row.avg_bytes_processed || 0;
            const recommendation = row.recommendation || 'N/A';
            const isCandidate = recommendation !== 'N/A';
            const badgeBg = isCandidate ? 'rgba(34, 197, 94, 0.15)' : 'rgba(148, 163, 184, 0.15)';
            const badgeColor = isCandidate ? '#22c55e' : '#94a3b8';
            const badgeText = isCandidate ? 'Candidate' : 'N/A';
            
            table.row.add([
                `<div style="font-family: monospace; font-size: 0.8rem; max-width: 400px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${row.query}">${row.query}</div>`,
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

        // Add event listeners for copy buttons
        document.querySelectorAll('.copy-job-id-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const jobId = e.target.closest('button').getAttribute('data-job-id');
                if (jobId) {
                    navigator.clipboard.writeText(jobId).then(() => {
                        showNotification('Job ID copied to clipboard', 'success');
                    }).catch(err => {
                        console.error('Failed to copy Job ID', err);
                        showNotification('Failed to copy Job ID', 'error');
                    });
                }
            });
        });
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
        const notification = document.createElement('div');
        notification.className = `notification ${type}`;
        
        let icon = 'fa-circle-info';
        if (type === 'success') icon = 'fa-circle-check';
        if (type === 'error') icon = 'fa-circle-exclamation';
        if (type === 'warning') icon = 'fa-triangle-exclamation';

        notification.innerHTML = `
            <i class="fa-solid ${icon}"></i>
            <div class="notif-content">${message}</div>
        `;

        elements.notificationContainer.appendChild(notification);

        setTimeout(() => {
            notification.style.animation = 'fadeOut 0.3s ease-out forwards';
            setTimeout(() => notification.remove(), 300);
        }, 4000);
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

                await fetch('/api/cost-attribution/config', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(config)
                });

                // Then calculate
                const params = {
                    billing_month_start: monthStart,
                    billing_month_end: monthEnd,
                    org_project_id: state.orgProject,
                    region: state.region,
                    admin_project_id: state.adminProject
                };

                const response = await fetch('/api/cost-attribution/calculate', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(params)
                });

                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(err.detail || 'Calculation failed');
                }

                const data = await response.json();
                renderCostAttributionResults(data);
                showNotification('Cost attribution calculated successfully.', 'success');
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
        console.log("Top 5 Spender Projects identified for HBO:", state.top5Projects);
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

            const params = {
                org_project_id: state.orgProject,
                region: state.region,
                lookback_days: parseInt(elements.slLookback.value) || 7,
                admin_project_id: state.adminProject
            };

            try {
                const response = await fetch('/api/slots/profiler', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(params)
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
                    body: JSON.stringify(params)
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

            const params = {
                org_project_id: state.orgProject,
                region: state.region,
                lookback_days: parseInt(elements.slLookback.value) || 7,
                admin_project_id: state.adminProject,
                od_price: parseFloat(document.getElementById('jb-od-rate').value) || 6.25,
                ed_price: parseFloat(document.getElementById('jb-ed-rate').value) || 0.06
            };

            try {
                const response = await fetch('/api/users/top_spenders', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(params)
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
                <td>${formatNumber(row.total_bytes_billed / (1024**4))} TB</td>
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

    const renderHboResults = (data) => {
        let table;
        if ($.fn.DataTable.isDataTable('#hbo-results-table')) {
            table = $('#hbo-results-table').DataTable();
        } else {
            table = $('#hbo-results-table').DataTable({
                pageLength: 10,
                order: [[1, 'desc']],
                responsive: true
            });
        }
        
        table.clear();
        
        let totalSlotsSaved = 0;
        let totalDollarsSaved = 0;

        data.forEach(row => {
            totalSlotsSaved += row.saved_slot_hours || 0;
            totalDollarsSaved += row.estimated_savings_usd || 0;

            table.row.add([
                row.job_id,
                `${row.percent_execution_time_saved.toFixed(2)}%`,
                row.new_elapsed_ms.toLocaleString(),
                row.original_elapsed_ms.toLocaleString()
            ]);
        });

        table.draw();

        const slotsEl = document.getElementById('hbo-total-slots');
        const dollarsEl = document.getElementById('hbo-total-dollars');

        if (slotsEl) slotsEl.textContent = totalSlotsSaved.toFixed(2);
        if (dollarsEl) dollarsEl.textContent = formatCurrency(totalDollarsSaved);
    };

    const renderHboStatus = (data) => {
        const panel = elements.hboStatusPanel;
        const list = elements.hboStatusList;
        if (!panel || !list) return;

        panel.style.display = 'block';
        list.innerHTML = '';

        data.forEach(item => {
            const div = document.createElement('div');
            div.style.marginBottom = '0.5rem';
            
            if (item.enabled) {
                div.innerHTML = `<i class="fa-solid fa-circle-check" style="color: #4ade80; margin-right: 5px;"></i> Project <strong>${item.project_id}</strong>: HBO is Enabled.`;
            } else {
                div.innerHTML = `<i class="fa-solid fa-circle-exclamation" style="color: #facc15; margin-right: 5px;"></i> Project <strong>${item.project_id}</strong>: HBO is Disabled.`;
                if (item.ddl) {
                    const ddlDiv = document.createElement('div');
                    ddlDiv.style.marginTop = '0.25rem';
                    ddlDiv.style.padding = '0.5rem';
                    ddlDiv.style.background = 'rgba(15, 23, 42, 0.5)';
                    ddlDiv.style.border = '1px solid rgba(255, 255, 255, 0.1)';
                    ddlDiv.style.borderRadius = '0.25rem';
                    ddlDiv.style.fontFamily = 'monospace';
                    ddlDiv.style.fontSize = '0.8rem';
                    ddlDiv.textContent = item.ddl;
                    div.appendChild(ddlDiv);
                }
            }
            list.appendChild(div);
        });
    };

    if (elements.btnAnalyzeHbo) {
        elements.btnAnalyzeHbo.addEventListener('click', async () => {
            if (!state.orgProject) {
                showNotification('Please configure settings first.', 'error');
                Router.navigate('settings');
                return;
            }

            setLoading(elements.btnAnalyzeHbo, true);

            const projectOverride = document.getElementById('hbo-project-override')?.value;
            const lookbackOverride = document.getElementById('hbo-lookback-override')?.value;

            const targetProject = projectOverride || state.orgProject;

            const params = {
                org_project_id: targetProject,
                region: state.region,
                lookback_days: lookbackOverride ? parseInt(lookbackOverride) : (parseInt(elements.slLookback.value) || 30),
                limit: 10
            };

            debug_log("Fetching HBO analysis with params:", params);

            try {
                const [analyzeRes, statusRes, summaryRes] = await Promise.all([
                    fetch('/api/hbo/analyze', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(params)
                    }),
                    fetch('/api/hbo/status', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(params)
                    }),
                    fetch('/api/hbo/summary', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(params)
                    })
                ]);

                if (!analyzeRes.ok || !statusRes.ok || !summaryRes.ok) {
                    throw new Error('One or more API calls failed');
                }

                const analyzeData = await analyzeRes.json();
                const statusData = await statusRes.json();
                const summaryData = await summaryRes.json();

                renderHboResults(analyzeData.slice(0, 10));
                renderHboStatus(statusData);

                // Update tiles
                const slotsEl = document.getElementById('hbo-total-slots');
                const dollarsEl = document.getElementById('hbo-total-dollars');
                if (slotsEl) slotsEl.textContent = formatNumber(summaryData.total_saved_slot_hours || 0);
                if (dollarsEl) dollarsEl.textContent = formatCurrency(summaryData.total_estimated_savings_usd || 0);

                safeSetLocalStorage('bq_hbo_results', JSON.stringify(analyzeData.slice(0, 10)));
                safeSetLocalStorage('bq_hbo_status', JSON.stringify(statusData));

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

            const projectOverride = document.getElementById('perf-project-override')?.value;
            const lookbackOverride = document.getElementById('perf-lookback-override')?.value;

            const params = {
                org_project_id: projectOverride || state.orgProject,
                region: state.region,
                lookback_days: lookbackOverride ? parseInt(lookbackOverride) : 7
            };

            try {
                const response = await fetch('/api/hbo/performance_insights', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(params)
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
            renderHboResults(JSON.parse(cachedHboResults));
        } catch (e) { console.warn("Failed to parse cached HBO results", e); }
    }
    const cachedHboStatus = localStorage.getItem('bq_hbo_status');
    if (cachedHboStatus) {
        try {
            renderHboStatus(JSON.parse(cachedHboStatus));
        } catch (e) { console.warn("Failed to parse cached HBO status", e); }
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
            totalSize += row.live_table_gb || 0;
            totalTtSize += row.time_travel_gb || 0;
            if (row.health_status && row.health_status.toUpperCase() === 'HIGH CHURN/RECREATE DETECTED') {
                highChurnCount++;
                const wastedSize = (5 / 7) * (row.time_travel_gb || 0);
                potentialSavings += wastedSize * actPhyRate;
            }

            const tr = document.createElement('tr');
            const badgeBg = row.health_status === 'Healthy' ? 'rgba(34, 197, 94, 0.15)' : 'rgba(239, 68, 68, 0.15)';
            const badgeColor = row.health_status === 'Healthy' ? '#22c55e' : '#ef4444';

            tr.innerHTML = `
                <td>${row.dataset}</td>
                <td>${row.table_name}</td>
                <td>${row.live_table_gb.toFixed(2)}</td>
                <td>${row.time_travel_gb.toFixed(2)}</td>
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
        $('#hygiene-results-table').DataTable({ pageLength: 10, order: [[4, 'desc']], responsive: true });
    };

    if (elements.btnAnalyzeHygiene) {
        elements.btnAnalyzeHygiene.addEventListener('click', async () => {
            if (!state.orgProject) {
                showNotification('Please configure settings first.', 'error');
                Router.navigate('settings');
                return;
            }

            setLoading(elements.btnAnalyzeHygiene, true);

            const params = {
                org_project_id: state.orgProject,
                region: state.region,
                limit: 20
            };

            try {
                const response = await fetch('/api/storage/hygiene', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(params)
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

    const renderBatchCandidatesResults = (data) => {
        const tbody = document.querySelector('#batch-candidates-results-table tbody');
        if (!tbody) return;
        tbody.innerHTML = '';

        data.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${row.project_id}</td>
                <td style="font-family: monospace; font-size: 0.85rem;">${row.job_id.substring(0, 12)}...</td>
                <td>${row.user_email}</td>
                <td><span class="badge logical">${row.batch_candidate_reason}</span></td>
                <td>${row.duration_minutes.toFixed(1)}</td>
                <td>${row.total_slot_ms.toLocaleString()}</td>
            `;
            tbody.appendChild(tr);
        });

        if ($.fn.DataTable.isDataTable('#batch-candidates-results-table')) {
            $('#batch-candidates-results-table').DataTable().destroy();
        }
        $('#batch-candidates-results-table').DataTable({ pageLength: 10, order: [[5, 'desc']], responsive: true });
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

        data.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${row.user_email}</td>
                <td>${row.project_id}</td>
                <td>${row.job_id}</td>
                <td>${row.billed_gb.toFixed(2)}</td>
                <td><span class="badge logical">${row.abuse_type}</span></td>
                <td>${formatCurrency(row.estimated_waste_usd || 0)}</td>
                <td style="font-family: monospace; font-size: 0.85rem;">${row.query_snippet}</td>
                <td>${row.suggested_fix || 'N/A'}</td>
            `;
            tbody.appendChild(tr);
        });

        if ($.fn.DataTable.isDataTable('#linter-results-table')) {
            $('#linter-results-table').DataTable().destroy();
        }
        $('#linter-results-table').DataTable({ pageLength: 10, order: [[3, 'desc']], responsive: true });
    };

    const renderAntiPatternsResults = (data) => {
        const tbody = document.querySelector('#antipatterns-results-table tbody');
        if (!tbody) return;
        tbody.innerHTML = '';

        data.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${row.user_email}</td>
                <td>${row.project_id}</td>
                <td>${row.insert_job_count.toLocaleString()}</td>
                <td>${row.wasted_slot_hours.toFixed(2)}</td>
                <td><span class="badge" style="background: rgba(239, 68, 68, 0.15); color: #ef4444;">Migrate to Storage Write API</span></td>
            `;
            tbody.appendChild(tr);
        });

        if ($.fn.DataTable.isDataTable('#antipatterns-results-table')) {
            $('#antipatterns-results-table').DataTable().destroy();
        }
        $('#antipatterns-results-table').DataTable({ pageLength: 10, order: [[2, 'desc']], responsive: true });
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
                <td style="font-family: monospace; font-size: 0.85rem;">${row.job_id.substring(0, 12)}...</td>
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
            const params = {
                org_project_id: state.orgProject,
                region: state.region,
                lookback_days: 7,
                limit_per_project: 100
            };
            try {
                const response = await fetch('/api/antipatterns/linter', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(params)
                });
                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(err.detail || 'Failed to scan query linter');
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
            const params = {
                org_project_id: state.orgProject,
                region: state.region,
                lookback_days: 1,
                threshold: 1000
            };
            try {
                const response = await fetch('/api/antipatterns/dml', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(params)
                });
                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(err.detail || 'Failed to scan DML abuse');
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
            const params = {
                org_project_id: state.orgProject,
                region: state.region,
                lookback_days: 7
            };
            try {
                const response = await fetch('/api/antipatterns/mv', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(params)
                });
                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(err.detail || 'Failed to scan MV costs');
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
            const params = {
                org_project_id: state.orgProject,
                region: state.region,
                lookback_days: 7,
                limit_per_project: 50
            };
            try {
                const response = await fetch('/api/antipatterns/skew', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(params)
                });
                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(err.detail || 'Failed to scan data skew');
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
            const params = {
                org_project_id: state.orgProject,
                region: state.region,
                lookback_days: 7,
                limit_per_project: 50
            };
            try {
                const response = await fetch('/api/antipatterns/batch_candidates', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(params)
                });
                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(err.detail || 'Failed to scan batch candidates');
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
            const params = {
                org_project_id: state.orgProject,
                region: state.region
            };
            try {
                const response = await fetch('/api/governance/analyze', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(params)
                });
                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(err.detail || 'Failed to scan governance');
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
            const params = {
                org_project_id: state.orgProject,
                region: state.region
            };
            try {
                const response = await fetch('/api/governance/analyze', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(params)
                });
                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(err.detail || 'Failed to scan governance');
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
            const params = {
                org_project_id: state.orgProject,
                region: state.region
            };
            try {
                const response = await fetch('/api/mv/analyze', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(params)
                });
                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(err.detail || 'Failed to scan MV rejections');
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
            const params = {
                org_project_id: state.orgProject,
                region: state.region
            };
            try {
                const response = await fetch('/api/resource_warnings/analyze', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(params)
                });
                if (!response.ok) {
                    const err = await response.json();
                    throw new Error(err.detail || 'Failed to scan resource warnings');
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

            const params = {
                org_project_id: state.orgProject,
                region: state.region,
                lookback_days: 7,
                limit: 50
            };

            try {
                const response = await fetch('/api/bi/analyze', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(params)
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

    const renderAiResults = (data) => {
        const tbody = document.querySelector('#ai-results-table tbody');
        if (!tbody) return;
        tbody.innerHTML = '';

        data.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td style="font-family: monospace; font-size: 0.85rem;">${row.job_id.substring(0, 12)}...</td>
                <td>${row.user_email}</td>
                <td>${row.total_slot_ms.toLocaleString()}</td>
                <td style="font-size: 0.85rem; color: var(--text-secondary); white-space: pre-wrap;">${row.gemini_optimization_advice}</td>
            `;
            tbody.appendChild(tr);
        });

        if ($.fn.DataTable.isDataTable('#ai-results-table')) {
            $('#ai-results-table').DataTable().destroy();
        }
        $('#ai-results-table').DataTable({ pageLength: 10, order: [[2, 'desc']], responsive: true });
    };

    if (elements.btnRunAiAnalysis) {
        elements.btnRunAiAnalysis.addEventListener('click', async () => {
            const modelName = state.remoteModel;
            if (!modelName) {
                showNotification('Please configure the Remote Model Name in Global Settings.', 'error');
                Router.navigate('settings');
                return;
            }

            const tableEl = document.getElementById('ai-results-table');
            const container = tableEl ? tableEl.closest('.results-panel') : null;

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

            const params = {
                org_project_id: state.orgProject,
                region: state.region,
                model_name: modelName,
                limit: parseInt(elements.aiLimit.value)
            };

            try {
                const response = await fetch('/api/ai/analyze', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(params),
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
                showNotification('AI analysis completed.', 'success');
            } catch (error) {
                if (typeof progress !== 'undefined' && progress && progress.stop) {
                    progress.stop();
                }
                console.error("AI Error:", error);
                showNotification(error.message, 'error');
            } finally {
                setLoading(elements.btnRunAiAnalysis, false);
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
      render(cached.data);
      updateFreshness(cached.fetchedAt);
      return;
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

      writeCache(data);
      render(data);
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
    if (!kpis) {
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
        value: formatCurrency(kpis.mtdSpend),
        delta: kpis.mtdSpendDelta,
        deltaLabel: 'vs last month',
        deltaDirection: kpis.mtdSpendDelta > 0 ? 'up' : 'down'
      })}
      ${kpiCard({
        label: 'Forecast (EOM)',
        value: formatCurrency(kpis.forecastSpend),
        delta: null,
        deltaLabel: `vs ${formatCurrency(kpis.lastMonthSpend)} last month`
      })}
      ${kpiCard({
        label: 'Potential Savings',
        value: formatCurrency(kpis.potentialSavings),
        delta: null,
        deltaLabel: `${kpis.opportunityCount} opportunities`,
        savings: true
      })}
      ${kpiCard({
        label: 'Anomalies Detected',
        value: kpis.anomalyCount.toString(),
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
      <a class="opportunity-row" href="${item.deepLink}">
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
        <span class="anomaly-row__text">${a.html /* pre-sanitized server-side */}</span>
        <a class="anomaly-row__action" href="${a.deepLink}">
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
                navigator.clipboard.writeText(output.value).then(() => {
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
      fetchEstimate({ orgProject, adminProject, region, lookback, price }),
      fetchJobSimulation({ orgProject, region, lookback, price }),
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
  async function fetchEstimate({ orgProject, adminProject, region, lookback, price }) {
    const res = await fetch('/api/fluid-scaling/estimate', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        org_project_id:    orgProject,
        admin_project_id:  adminProject,
        region,
        lookback_days:     lookback,
        price_per_slot_hr: price,
      }),
    });
    if (!res.ok) {
      const detail = await safeReadDetail(res);
      throw new Error(detail || `HTTP ${res.status}`);
    }
    return res.json();
  }

  async function fetchJobSimulation({ orgProject, region, lookback, price }) {
    const res = await fetch('/api/slots/fluid_simulation', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        org_project_id:       orgProject,
        region,
        lookback_days:        lookback,
        edition_slot_hr_rate: price,
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

    if (!panel || !configStatus) return;

    panel.style.display = 'block';

    if (configStatus.enabled) {
      panel.style.borderColor = 'rgba(34, 197, 94, 0.5)'; // Green
      text.innerHTML = `<i class="fa-solid fa-circle-check" style="color: #4ade80;"></i> Fluid Scaling is already enabled for all active reservations in this region. No action needed.`;
      if (container) container.style.display = 'none';
      if (output) output.value = '';
    } else {
      panel.style.borderColor = 'rgba(234, 179, 8, 0.5)'; // Yellow
      const missingList = configStatus.missing_reservations.join(', ');
      text.innerHTML = `<i class="fa-solid fa-circle-exclamation" style="color: #facc15;"></i> Fluid Scaling is NOT enabled for the following active reservations: <strong>${UIState.escapeHtml(missingList)}</strong>. We recommend enabling it to get per-second billing and avoid the 60-second cooldown window.`;
      if (container && output) {
        container.style.display = 'block';
        output.value = configStatus.ddl || '';
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

    console.debug('[FluidScaling] job simulation patterns:', rows.length);

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
    if (targetView !== 'settings' && targetView !== 'dashboard' && !state.orgProject) {
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
document.addEventListener('DOMContentLoaded', () => {
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
