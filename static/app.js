// BigQuery FinOps Optimizer - Frontend Logic

document.addEventListener('DOMContentLoaded', () => {
    // State
    const state = {
        orgProject: localStorage.getItem('bq_org_project') || '',
        adminProject: localStorage.getItem('bq_admin_project') || '',
        region: localStorage.getItem('bq_region') || 'region-us',
        storageData: [],
        slotsData: [],
        slotsChart: null,
        actualProvisioningChart: null
    };

    // DOM Elements
    const elements = {
        // Nav
        navStorage: document.getElementById('nav-storage'),
        navJobs: document.getElementById('nav-jobs'),
        navSlots: document.getElementById('nav-slots'),
        navSlotsSimulator: document.getElementById('nav-slots-simulator'),
        navSettings: document.getElementById('nav-settings'),
        
        // Views
        viewStorage: document.getElementById('view-storage'),
        viewJobs: document.getElementById('view-jobs'),
        viewSlots: document.getElementById('view-slots'),
        viewSlotsSimulator: document.getElementById('view-slots-simulator'),
        viewSettings: document.getElementById('view-settings'),
        
        // Top Bar
        currentProject: document.getElementById('current-project'),
        currentAdminProject: document.getElementById('current-admin-project'),
        currentRegion: document.getElementById('current-region'),
        
        // Settings Form
        cfgOrgProject: document.getElementById('cfg-org-project'),
        cfgAdminProject: document.getElementById('cfg-admin-project'),
        cfgRegion: document.getElementById('cfg-region'),
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
        navCostAttribution: document.getElementById('nav-cost-attribution'),
        viewCostAttribution: document.getElementById('view-cost-attribution'),
        
        // Workload Profiler
        navProfiler: document.getElementById('nav-profiler'),
        viewProfiler: document.getElementById('view-profiler'),
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
        navUsers: document.getElementById('nav-users'),
        viewUsers: document.getElementById('view-users'),
        btnAnalyzeUsers: document.getElementById('analyze-users-btn')
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
            switchView('settings');
        }
    };

    // View Switching
    const switchView = (viewId) => {
        elements.viewStorage.style.display = 'none';
        if (elements.viewJobs) elements.viewJobs.style.display = 'none';
        if (elements.viewSlots) elements.viewSlots.style.display = 'none';
        if (elements.viewSlotsSimulator) elements.viewSlotsSimulator.style.display = 'none';
        if (elements.viewCostAttribution) elements.viewCostAttribution.style.display = 'none';
        if (elements.viewProfiler) elements.viewProfiler.style.display = 'none';
        if (elements.viewUsers) elements.viewUsers.style.display = 'none';
        elements.viewSettings.style.display = 'none';
        
        elements.navStorage.classList.remove('active');
        if (elements.navJobs) elements.navJobs.classList.remove('active');
        if (elements.navSlots) elements.navSlots.classList.remove('active');
        if (elements.navSlotsSimulator) elements.navSlotsSimulator.classList.remove('active');
        if (elements.navCostAttribution) elements.navCostAttribution.classList.remove('active');
        if (elements.navProfiler) elements.navProfiler.classList.remove('active');
        if (elements.navUsers) elements.navUsers.classList.remove('active');
        elements.navSettings.classList.remove('active');

        if (viewId === 'storage') {
            elements.viewStorage.style.display = 'block';
            elements.navStorage.classList.add('active');
        } else if (viewId === 'jobs') {
            if (elements.viewJobs) elements.viewJobs.style.display = 'block';
            if (elements.navJobs) elements.navJobs.classList.add('active');
        } else if (viewId === 'slots') {
            if (elements.viewSlots) elements.viewSlots.style.display = 'block';
            if (elements.navSlots) elements.navSlots.classList.add('active');
        } else if (viewId === 'slots-simulator') {
            if (elements.viewSlotsSimulator) elements.viewSlotsSimulator.style.display = 'block';
            if (elements.navSlotsSimulator) elements.navSlotsSimulator.classList.add('active');
        } else if (viewId === 'cost-attribution') {
            if (elements.viewCostAttribution) elements.viewCostAttribution.style.display = 'block';
            if (elements.navCostAttribution) elements.navCostAttribution.classList.add('active');
        } else if (viewId === 'profiler') {
            if (elements.viewProfiler) elements.viewProfiler.style.display = 'block';
            if (elements.navProfiler) elements.navProfiler.classList.add('active');
        } else if (viewId === 'users') {
            if (elements.viewUsers) elements.viewUsers.style.display = 'block';
            if (elements.navUsers) elements.navUsers.classList.add('active');
        } else if (viewId === 'settings') {
            elements.viewSettings.style.display = 'block';
            elements.navSettings.classList.add('active');
        }
    };

    // Event Listeners for Nav
    elements.navStorage.addEventListener('click', (e) => { e.preventDefault(); switchView('storage'); });
    if (elements.navJobs) elements.navJobs.addEventListener('click', (e) => { e.preventDefault(); switchView('jobs'); });
    if (elements.navSlots) elements.navSlots.addEventListener('click', (e) => { e.preventDefault(); switchView('slots'); });
    if (elements.navProfiler) elements.navProfiler.addEventListener('click', (e) => { e.preventDefault(); switchView('profiler'); });
    if (elements.navUsers) elements.navUsers.addEventListener('click', (e) => { e.preventDefault(); switchView('users'); });
    if (elements.navSlotsSimulator) elements.navSlotsSimulator.addEventListener('click', async (e) => { 
        e.preventDefault(); 
        switchView('slots-simulator'); 
        
        // Fetch peak slots to auto-populate Max Baseline
        if (state.orgProject) {
            try {
                const response = await fetch('/api/slots/peak', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        org_project_id: state.orgProject,
                        region: state.region,
                        lookback_days: 30
                    })
                });
                if (response.ok) {
                    const data = await response.json();
                    const peakSlots = data.peak_slots;
                    const simMaxBaselineInput = document.getElementById('sim-max-baseline');
                    if (simMaxBaselineInput) {
                        // Round up to the next 500 for a clean limit, default to 1000 if 0
                        const recommendedMax = Math.ceil(peakSlots / 500) * 500 || 1000;
                        simMaxBaselineInput.value = recommendedMax;
                        console.log(`Auto-populated max baseline to ${recommendedMax} based on peak slots ${peakSlots}`);
                    }
                }
            } catch (error) {
                console.warn("Failed to fetch peak slots for auto-population:", error);
            }
        }
    });
    elements.navSettings.addEventListener('click', (e) => { e.preventDefault(); switchView('settings'); });

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

        localStorage.setItem('bq_org_project', state.orgProject);
        localStorage.setItem('bq_region', state.region);

        elements.currentProject.textContent = state.orgProject || 'Not Set';
        if (elements.currentAdminProject) elements.currentAdminProject.textContent = state.adminProject || 'Not Set';
        elements.currentRegion.textContent = state.region;

        showNotification('Settings saved successfully.', 'success');
        switchView('storage');
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
            switchView('settings');
            return;
        }

        setLoading(elements.btnAnalyzeStorage, true);

        const params = {
            active_logical_price: parseFloat(elements.stActLog.value),
            long_term_logical_price: parseFloat(elements.stLtLog.value),
            active_physical_price: parseFloat(elements.stActPhy.value),
            long_term_physical_price: parseFloat(elements.stLtPhy.value),
            time_travel_rescale: parseFloat(elements.stTtRescale.value),
            time_travel_hours: elements.stTtHours.value ? parseFloat(elements.stTtHours.value) : null,
            min_monthly_saving: parseFloat(elements.stMinSave.value),
            min_monthly_saving_pct: parseFloat(elements.stMinSavePct.value),
            region: state.region,
            org_project_id: state.orgProject
        };

        try {
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
            renderStorageResults(responseData.datasets);
            renderOrgStatus(responseData.org_status);
            localStorage.setItem('bq_storage_results', JSON.stringify(responseData));
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
                switchView('settings');
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
                limit_jobs: parseInt(document.getElementById('jb-limit').value)
            };

            try {
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
                localStorage.setItem('bq_job_results', JSON.stringify(responseData));
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

        // Render Project Summaries
        if (data.project_summaries) {
            data.project_summaries.forEach(row => {
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
        // Calculate KPIs
        const totalSavings = data.reduce((sum, row) => sum + (row.monthly_savings || 0), 0);
        const datasetCount = new Set(data.map(row => `${row.project_name}.${row.dataset_name}`)).size;
        const oppCount = data.length;

        elements.stTotalSavings.textContent = formatCurrency(totalSavings);
        elements.stDatasetCount.textContent = datasetCount;
        elements.stOppCount.textContent = oppCount;

        // Populate Table
        const tbody = document.querySelector('#storage-results-table tbody');
        tbody.innerHTML = '';

        data.forEach((row, index) => {
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
            text.innerHTML = `<i class="fa-solid fa-circle-check" style="color: #4ade80;"></i> Your organization's default storage billing model for this region is already <strong>${orgStatus.current_model}</strong>! No action needed.`;
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
    if (elements.btnAnalyzeSlots) {
        elements.btnAnalyzeSlots.addEventListener('click', async () => {
            if (!state.orgProject) {
                showNotification('Please configure settings first.', 'error');
                switchView('settings');
                return;
            }

            setLoading(elements.btnAnalyzeSlots, true);

            const params = {
                org_project_id: state.orgProject,
                region: state.region,
                lookback_days: parseInt(elements.slLookback.value),
                window_minutes: parseInt(elements.slWindow.value),
                percentile: parseInt(elements.slPercentile.value)
            };

            try {
                const response = await fetch('/api/slots/analyze', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(params)
                });

                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(errorData.detail || 'Failed to analyze slots');
                }

                const responseData = await response.json();
                renderSlotsResults(responseData, params.percentile);

                // Fetch utilization timeline
                const utilParams = {
                    org_project_id: state.orgProject,
                    region: state.region,
                    lookback_days: params.lookback_days,
                    timezone: 'America/New_York',
                    resolution: elements.slResolution.value
                };

                const utilResponse = await fetch('/api/slots/utilization', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(utilParams)
                });

                if (utilResponse.ok) {
                    const utilData = await utilResponse.json();
                    
                    // Fetch actual provisioning
                    const actualParams = {
                        org_project_id: state.orgProject,
                        region: state.region,
                        lookback_days: params.lookback_days,
                        timezone: 'America/New_York',
                        edition: 'ENTERPRISE',
                        admin_project_id: state.adminProject
                    };

                    let provisioningTimeline = null;
                    try {
                        const actualResponse = await fetch('/api/slots/actual_provisioning', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify(actualParams)
                        });

                        if (actualResponse.ok) {
                            const actualData = await actualResponse.json();
                            document.getElementById('act-autoscaled-hours').textContent = formatNumber(Math.round(actualData.autoscaled_slot_hours));
                            document.getElementById('act-baseline-hours').textContent = formatNumber(Math.round(actualData.baseline_slot_hours));
                            document.getElementById('act-total-hours').textContent = formatNumber(Math.round(actualData.total_slot_hours));
                            provisioningTimeline = actualData.timeline;
                            
                            renderActualProvisioningDonut(actualData.autoscaled_slot_hours, actualData.baseline_slot_hours);
                        }
                    } catch (error) {
                        console.warn("Failed to fetch actual provisioning:", error);
                    }

                    renderSlotsChart(utilData, provisioningTimeline);
                    


                    // Automatically set Max Baseline Slots for the simulator based on peak usage
                    const simMaxBaselineInput = document.getElementById('sim-max-baseline');
                    if (simMaxBaselineInput && utilData.length > 0) {
                        const peakSlots = Math.max(...utilData.map(d => d.max_slots));
                        // Round up to the next 500 for a clean limit, default to 1000 if 0
                        const recommendedMax = Math.ceil(peakSlots / 500) * 500 || 1000;
                        simMaxBaselineInput.value = recommendedMax;
                        console.log(`Auto-set simulator max baseline to ${recommendedMax} based on peak usage of ${peakSlots}`);
                    }
                } else {
                    console.warn("Failed to fetch slot utilization timeline");
                }

                showNotification('Slots analysis completed.', 'success');
            } catch (error) {
                console.error("Slots Analysis Error:", error);
                showNotification(error.message, 'error');
            } finally {
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
                switchView('settings');
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
        const reservationsTbody = document.querySelector('#current-reservations-table tbody');
        const recommendationsTbody = document.querySelector('#slots-recommendations-table tbody');
        
        if (reservationsTbody) reservationsTbody.innerHTML = '';
        if (recommendationsTbody) recommendationsTbody.innerHTML = '';

        // Update label
        const lblPercentile = document.getElementById('lbl-percentile');
        if (lblPercentile) lblPercentile.textContent = targetPercentile;

        // Render Current Reservations
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
                `;
                reservationsTbody.appendChild(tr);
            });
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
                recommendationsTbody.appendChild(tr);
            });
        }

        // Initialize DataTables
        if ($.fn.DataTable.isDataTable('#current-reservations-table')) {
            $('#current-reservations-table').DataTable().destroy();
        }
        $('#current-reservations-table').DataTable({ pageLength: 5, responsive: true });

        if ($.fn.DataTable.isDataTable('#slots-recommendations-table')) {
            $('#slots-recommendations-table').DataTable().destroy();
        }
        $('#slots-recommendations-table').DataTable({ pageLength: 5, order: [[1, 'desc']], responsive: true });
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
        const tbody = document.querySelector('#profiler-queries-table tbody');
        if (!tbody) return;
        tbody.innerHTML = '';

        const formatSlotHours = (num) => {
            if (num > 0 && num < 0.01) {
                return new Intl.NumberFormat('en-US', { maximumFractionDigits: 6 }).format(num);
            }
            return new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 }).format(num);
        };

        data.forEach(row => {
            console.log("[BIGQUERY-OPTIMIZER] Row data:", row);
            const tr = document.createElement('tr');
            
            const avgBytes = row.avg_bytes_processed || 0;
            const recommendation = row.recommendation || 'N/A';
            const isCandidate = recommendation !== 'N/A';
            const badgeBg = isCandidate ? 'rgba(34, 197, 94, 0.15)' : 'rgba(148, 163, 184, 0.15)';
            const badgeColor = isCandidate ? '#22c55e' : '#94a3b8';
            const badgeText = isCandidate ? 'Candidate' : 'N/A';
            
            tr.innerHTML = `
                <td style="font-family: monospace; font-size: 0.8rem; max-width: 400px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${row.query}">${row.query}</td>
                <td>
                    <span style="font-family: monospace; font-size: 0.8rem; max-width: 150px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${row.project_id || ''}">${row.project_id || 'N/A'}</span>
                </td>
                <td>
                    <div style="display: flex; align-items: center; gap: 0.5rem;">
                        <span style="font-family: monospace; font-size: 0.8rem; max-width: 150px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${row.example_job_id || ''}">${row.example_job_id || 'N/A'}</span>
                        ${row.example_job_id ? `<button class="btn-action copy-job-id-btn" data-job-id="${row.example_job_id}" title="Copy Job ID" style="padding: 2px 5px; font-size: 0.75rem;"><i class="fa-solid fa-copy"></i></button>` : ''}
                    </div>
                </td>
                <td>${formatNumber(row.frequency)}</td>
                <td>${formatSlotHours(row.avg_slot_hours)}</td>
                <td>${formatNumber(row.avg_duration_seconds)}</td>
                <td>${formatNumber(avgBytes / (1024 * 1024))} MB</td>
                <td><span class="badge" style="background: ${badgeBg}; color: ${badgeColor};" title="${recommendation}">${badgeText}</span></td>
            `;
            tbody.appendChild(tr);

            const copyBtn = tr.querySelector('.copy-job-id-btn');
            if (copyBtn) {
                copyBtn.addEventListener('click', () => {
                    navigator.clipboard.writeText(row.example_job_id).then(() => {
                        showNotification('Job ID copied to clipboard', 'success');
                        console.log(`[BIGQUERY-OPTIMIZER] Copied Job ID: ${row.example_job_id}`);
                    }).catch(err => {
                        console.error('[BIGQUERY-OPTIMIZER] Failed to copy: ', err);
                        showNotification('Failed to copy Job ID', 'error');
                    });
                });
            }
        });

        // Initialize DataTable
        if ($.fn.DataTable.isDataTable('#profiler-queries-table')) {
            $('#profiler-queries-table').DataTable().destroy();
        }
        $('#profiler-queries-table').DataTable({ pageLength: 10, order: [[3, 'desc']], responsive: true });
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
            switchView('cost-attribution');
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
                switchView('settings');
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
        });
        
        table.draw();
    };

    if (elements.btnAnalyzeProfiler) {
        elements.btnAnalyzeProfiler.addEventListener('click', async () => {
            if (!state.orgProject) {
                showNotification('Please configure settings first.', 'error');
                switchView('settings');
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
                localStorage.setItem('bq_profiler_summary', JSON.stringify(data.summary));
                localStorage.setItem('bq_profiler_timeline', JSON.stringify(data.timeline));
                
                // Fetch top queries
                const queriesResponse = await fetch('/api/slots/profiler/queries', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(params)
                });
                
                if (queriesResponse.ok) {
                    const queriesData = await queriesResponse.json();
                    renderProfilerQueries(queriesData);
                    localStorage.setItem('bq_profiler_queries', JSON.stringify(queriesData));
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
                switchView('settings');
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
                localStorage.setItem('bq_top_spenders', JSON.stringify(data));
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
            renderStorageResults(storageData.datasets);
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

    // App Start
    initUI();
});
