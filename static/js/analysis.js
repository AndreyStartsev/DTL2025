// Analysis Dashboard Rendering (Performance-focused, MV hidden)

let analysisCharts = [];

function destroyAnalysisCharts() {
    analysisCharts.forEach(c => {
        try {
            c.destroy();
        } catch (e) { /* ignore */ }
    });
    analysisCharts = [];
}

function renderAnalysis(data) {
    const root = document.getElementById('analysis-root');
    destroyAnalysisCharts();
    root.innerHTML = '';

    if (!data || typeof data !== 'object') {
        root.innerHTML = `<div class="alert alert-warning">No analysis data available.</div>`;
        return;
    }

    const viz = data.visualizations || {};
    const raw = data.raw_report || {};
    const exec = viz.executive_summary || {};
    const metrics = Array.isArray(exec.metrics) ? exec.metrics : [];
    const optPotential = exec.optimization_potential || (raw.executive_summary ? raw.executive_summary.optimization_potential : '—');

    const totalRowsNumeric = raw?.database_profile?.total_rows_numeric || 0;

    const qp = viz.query_performance || {};
    const topQueries = Array.isArray(qp.top_queries) ? qp.top_queries : [];
    const totalExecutions = qp.total_executions || 0;

    const agg = viz.aggregation_usage || {};
    const aggLabels = agg.labels || [];
    const aggValues = agg.data || [];

    const join = viz.join_patterns || {};
    const joinLabels = join.labels || [];
    const joinValues = join.data || [];

    const recs = (viz.recommendations && Array.isArray(viz.recommendations.priority_matrix)) ? viz.recommendations.priority_matrix : [];
    const implOrder = (viz.recommendations && Array.isArray(viz.recommendations.implementation_order)) ? viz.recommendations.implementation_order : [];

    const ctePct = raw?.query_patterns?.cte_usage_percent ?? null;

    root.innerHTML = `
        <div class="row g-3">
            <!-- KPI Row -->
            <div class="col-12">
                <div class="row g-3">
                    ${metrics.map(m => {
                        const isDbSize = String(m.label || '').toLowerCase().includes('db size');
                        let displayValue = m.value ?? '—';

                        if (isDbSize) {
                            // --- START: ENHANCED DB Size Logic & Logging ---
                            const valueIsExactString = (typeof m.value === 'string') && m.value.trim() === '0.0 GB';
                            const valueIsZeroFloat = parseFloat(m.value) === 0;

                            // Detailed logging as requested
                            console.group("--- Debugging DB Size Metric ---");
                            console.log("Raw metric object:", m);
                            console.log(`Raw value: "${m.value}" (type: ${typeof m.value})`);
                            console.log(`totalRowsNumeric: ${totalRowsNumeric}`);
                            console.log(`Hook check: Does value trim to "0.0 GB"? -> ${valueIsExactString}`);
                            console.log(`Fallback check: Does parseFloat(value) === 0? -> ${valueIsZeroFloat}`);

                            if (valueIsExactString || valueIsZeroFloat) {
                                console.log("✅ Condition MET. Checking row count for fallback.");
                                if (totalRowsNumeric > 0) {
                                    const estimatedBytes = totalRowsNumeric * 1024;
                                    displayValue = `<span title="Estimated based on ${nf(totalRowsNumeric)} rows">~ ${formatBytes(estimatedBytes)} (Est.)</span>`;
                                    console.log(`-> Rows > 0. Displaying estimated size: ${displayValue}`);
                                } else {
                                    displayValue = 'N/A';
                                    console.log(`-> Rows are 0. Displaying "N/A".`);
                                }
                            } else {
                                console.log("❌ Condition FAILED. Using original value from backend.");
                            }
                            console.groupEnd();
                            // --- END: ENHANCED DB Size Logic & Logging ---
                        }

                        const alert = m.alert ? 'alert border border-danger-subtle' : '';
                        const icon = iconClass(m.icon);
                        return `
                        <div class="col-6 col-md-4 col-xl-2">
                            <div class="card h-100 kpi-card ${alert}">
                                <div class="card-body">
                                    <div class="d-flex align-items-center gap-3">
                                        <div class="icon-wrap"><i class="bi ${icon}"></i></div>
                                        <div>
                                            <div class="text-muted small">${m.label || ''}</div>
                                            <div class="fs-5">${displayValue}</div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>`;
                    }).join('')}
                    <div class="col-12 col-md-8 col-xl-4">
                        <div class="card h-100">
                            <div class="card-body d-flex align-items-center justify-content-between">
                                <div>
                                    <div class="text-muted small">Optimization Potential</div>
                                    <div class="mt-2">
                                        <span class="badge ${String(optPotential).toLowerCase().includes('high') ? 'bg-success' : (String(optPotential).toLowerCase().includes('medium') ? 'bg-warning text-dark' : 'bg-secondary')} fs-6">
                                            ${optPotential}
                                        </span>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Query Performance & Join Patterns -->
            <div class="col-12 col-lg-8">
                <div class="card h-100">
                    <div class="card-header">
                        <i class="bi bi-graph-up"></i> High-Volume Query Performance
                        <small class="text-muted">(Total: ${nf(totalExecutions)} executions)</small>
                    </div>
                    <div class="card-body">
                        <div class="chart-container" style="height: 320px;">
                            <canvas id="hvqChart"></canvas>
                        </div>
                    </div>
                </div>
            </div>

            <div class="col-12 col-lg-4">
                <div class="card h-100">
                    <div class="card-header"><i class="bi bi-diagram-2"></i> Join Patterns</div>
                    <div class="card-body">
                        <div class="chart-container" style="height: 320px;">
                            <canvas id="joinChart"></canvas>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Aggregation & CTE Usage -->
            <div class="col-12 col-lg-6">
                <div class="card h-100">
                    <div class="card-header"><i class="bi bi-calculator"></i> Aggregation Functions</div>
                    <div class="card-body">
                        <div class="chart-container">
                            <canvas id="aggChart"></canvas>
                        </div>
                    </div>
                </div>
            </div>

            <div class="col-12 col-lg-6">
                <div class="card h-100">
                    <div class="card-header"><i class="bi bi-layers"></i> CTE Usage</div>
                    <div class="card-body">
                        <div class="chart-container">
                            <canvas id="cteChart"></canvas>
                        </div>
                        ${ctePct === null ? '<div class="text-muted small mt-2 text-center">No CTE data available</div>' : ''}
                    </div>
                </div>
            </div>

            <!-- Recommendations & Priority -->
            <div class="col-12 col-lg-6">
                <div class="card h-100">
                    <div class="card-header"><i class="bi bi-lightbulb"></i> Optimization Recommendations</div>
                    <div class="card-body" id="recsList"></div>
                </div>
            </div>

            <div class="col-12 col-lg-6">
                <div class="card h-100">
                    <div class="card-header"><i class="bi bi-list-ol"></i> Implementation Priority</div>
                    <div class="card-body">
                        <ol id="priorityList" class="mb-0"></ol>
                    </div>
                </div>
            </div>
        </div>
    `;

    // Render charts & lists
    renderQueryPerformanceChart(topQueries);
    renderJoinPatternsChart(joinLabels, joinValues);
    renderAggregationChart(aggLabels, aggValues);
    renderCTEChart(ctePct);
    renderRecommendations(recs);
    renderImplementationOrder(implOrder);

    // Raw JSON viewer
    const rawEl = document.getElementById('analysis-raw-json');
    if (rawEl) {
        try {
            rawEl.textContent = JSON.stringify(data, null, 2);
        } catch {
            rawEl.textContent = 'Raw JSON unavailable.';
        }
    }
}

function renderQueryPerformanceChart(topQueries) {
    const labels = topQueries.map(d => d.id || '—');
    const values = topQueries.map(d => toNumber(d.executions));
    const ctx = document.getElementById('hvqChart');
    if (!ctx) return;

    const chart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'Executions',
                data: values,
                backgroundColor: coloredBars(chartColors.primary, values.length)
            }]
        },
        options: { ...baseChartOptions(), indexAxis: 'y' }
    });
    analysisCharts.push(chart);
}

function renderJoinPatternsChart(labels, values) {
    const ctx = document.getElementById('joinChart');
    if (!ctx) return;

    const chart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'Count',
                data: values.map(toNumber),
                backgroundColor: coloredBars(chartColors.purple, labels.length)
            }]
        },
        options: baseChartOptions()
    });
    analysisCharts.push(chart);
}

function renderAggregationChart(labels, values) {
    const ctx = document.getElementById('aggChart');
    if (!ctx) return;

    const chart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'Usage',
                data: values.map(toNumber),
                backgroundColor: coloredBars(chartColors.warning, labels.length)
            }]
        },
        options: baseChartOptions()
    });
    analysisCharts.push(chart);
}

function renderCTEChart(ctePct) {
    const ctx = document.getElementById('cteChart');
    if (!ctx) return;

    if (ctePct === null || ctePct === undefined || isNaN(Number(ctePct))) {
        ctx.parentElement.innerHTML = '<div class="text-muted text-center p-5">No CTE data available</div>';
        return;
    }

    const chart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['CTE Used', 'No CTE'],
            datasets: [{
                data: [Number(ctePct), Math.max(0, 100 - Number(ctePct))],
                backgroundColor: [chartColors.success, chartColors.slate],
                borderColor: '#00000000'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: { color: chartColors.text }
                },
                tooltip: {
                    callbacks: {
                        label: (c) => `${c.label}: ${c.raw.toFixed(1)}%`
                    }
                }
            }
        }
    });
    analysisCharts.push(chart);
}

function renderRecommendations(recs) {
    const recsDiv = document.getElementById('recsList');
    if (!recsDiv) return;

    const filteredRecs = recs.filter(r =>
        !String(r.name).toLowerCase().includes('materialized view') &&
        !String(r.description).toLowerCase().includes('materialized view')
    );

    if (!filteredRecs.length) {
        recsDiv.innerHTML = `<div class="text-muted">No recommendations available.</div>`;
        return;
    }

    const prBadge = (p) => {
        const v = String(p || '').toLowerCase();
        if (v === 'high') return 'bg-success';
        if (v === 'medium') return 'bg-warning text-dark';
        return 'bg-secondary';
    };

    const effortBadge = (e) => {
        const v = String(e || '').toLowerCase();
        if (v === 'low') return 'bg-success';
        if (v === 'medium') return 'bg-warning text-dark';
        if (v === 'high') return 'bg-danger';
        return 'bg-secondary';
    };

    recsDiv.innerHTML = filteredRecs.map(r => `
        <div class="border rounded p-3 mb-2">
            <div class="d-flex justify-content-between align-items-center mb-1">
                <div class="fw-semibold">${escapeHtml(r.name || 'Recommendation')}</div>
                <div class="d-flex gap-2">
                    <span class="badge ${prBadge(r.priority)}">${escapeHtml(r.priority || '')}</span>
                    ${r.effort ? `<span class="badge ${effortBadge(r.effort)}">Effort: ${escapeHtml(r.effort)}</span>` : ''}
                </div>
            </div>
            ${r.description ? `<div class="text-muted small mb-2">${escapeHtml(r.description)}</div>` : ''}
            ${r.improvement ? `<div class="small text-success"><i class="bi bi-graph-up-arrow"></i> ${escapeHtml(r.improvement)}</div>` : ''}
        </div>
    `).join('');
}

function renderImplementationOrder(implOrder) {
    const list = document.getElementById('priorityList');
    if (!list) return;

    const filteredOrder = implOrder.filter(item => !String(item).toLowerCase().includes('materialized view'));

    if (filteredOrder.length === 0) {
        list.innerHTML = '<li class="text-muted">No implementation order available.</li>';
        return;
    }

    const cleanItems = filteredOrder.map(item => String(item).replace(/^\d+[\.\)]\s*/, ''));
    list.innerHTML = cleanItems.map(item => `<li class="mb-2">${escapeHtml(item)}</li>`).join('');
}