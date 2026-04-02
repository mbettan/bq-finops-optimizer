// BigQuery FinOps Optimizer - Frontend Logic

document.addEventListener('DOMContentLoaded', () => {
    // State
    const state = {
        orgProject: localStorage.getItem('bq_org_project') || '',
        region: localStorage.getItem('bq_region') || 'region-us',
        storageData: [],

    };

    // DOM Elements
    const elements = {
        // Nav
        navStorage: document.getElementById('nav-storage'),

        navSettings: document.getElementById('nav-settings'),
        
        // Views
        viewStorage: document.getElementById('view-storage'),

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
        

        
        notificationContainer: document.getElementById('notification-container')
    };

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
        elements.viewSettings.style.display = 'none';
        
        elements.navStorage.classList.remove('active');
        elements.navSettings.classList.remove('active');

        if (viewId === 'storage') {
            elements.viewStorage.style.display = 'block';
            elements.navStorage.classList.add('active');
        } else if (viewId === 'settings') {
            elements.viewSettings.style.display = 'block';
            elements.navSettings.classList.add('active');
        }
    };



    // Event Listeners for Nav
    elements.navStorage.addEventListener('click', (e) => { e.preventDefault(); switchView('storage'); });

    elements.navSettings.addEventListener('click', (e) => { e.preventDefault(); switchView('settings'); });

    // Save Settings
    elements.saveSettingsBtn.addEventListener('click', () => {
        state.orgProject = elements.cfgOrgProject.value.trim();
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

    


    // Helpers
    const formatCurrency = (amount) => {
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
