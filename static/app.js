// BigQuery FinOps Optimizer - Frontend Logic

document.addEventListener('DOMContentLoaded', () => {
    // State
    const state = {
        orgProject: localStorage.getItem('bq_org_project') || '',
        region: localStorage.getItem('bq_region') || 'region-us',
        storageData: [],
        slotsData: [],
        slotsChart: null,

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
        currentRegion: document.getElementById('current-region'),
        
        // Settings Form
        cfgOrgProject: document.getElementById('cfg-org-project'),
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
        slPercentile: document.getElementById('sl-percentile'),
        
        notificationContainer: document.getElementById('notification-container')
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
        elements.cfgRegion.value = state.region;
        
        elements.currentProject.textContent = state.orgProject || 'Not Set';
        elements.currentRegion.textContent = state.region;

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
        elements.viewSettings.style.display = 'none';
        
        elements.navStorage.classList.remove('active');
        if (elements.navJobs) elements.navJobs.classList.remove('active');
        if (elements.navSlots) elements.navSlots.classList.remove('active');
        if (elements.navSlotsSimulator) elements.navSlotsSimulator.classList.remove('active');
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
        } else if (viewId === 'settings') {
            elements.viewSettings.style.display = 'block';
            elements.navSettings.classList.add('active');
        }
    };

    // Event Listeners for Nav
    elements.navStorage.addEventListener('click', (e) => { e.preventDefault(); switchView('storage'); });
    if (elements.navJobs) elements.navJobs.addEventListener('click', (e) => { e.preventDefault(); switchView('jobs'); });
    if (elements.navSlots) elements.navSlots.addEventListener('click', (e) => { e.preventDefault(); switchView('slots'); });
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
        elements.cfgOrgProject.value = state.orgProject; // Update input field with trimmed value
        state.region = elements.cfgRegion.value;

        localStorage.setItem('bq_org_project', state.orgProject);
        localStorage.setItem('bq_region', state.region);

        elements.currentProject.textContent = state.orgProject || 'Not Set';
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
                    timezone: 'America/New_York'
                };

                const utilResponse = await fetch('/api/slots/utilization', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(utilParams)
                });

                if (utilResponse.ok) {
                    const utilData = await utilResponse.json();
                    renderSlotsChart(utilData);
                    
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

    const renderSlotsChart = (data) => {
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
        
        state.slotsChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [
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
                ]
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

    // App Start
    initUI();
});
