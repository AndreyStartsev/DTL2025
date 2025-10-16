// Schema Overview Rendering (Structure-focused, with new visualizations)

function renderSchemaOverview(report) {
    const root = document.getElementById('schema-root');
    if (!root) return;

    if (!report || typeof report !== 'object') {
        root.innerHTML = '<div class="alert alert-warning">No schema data available</div>';
        return;
    }

    const tables = report.tables || [];
    const indexCoverage = report.index_coverage || {};
    const colTypeDist = report.column_types_distribution || {};
    const dataQuality = report.data_quality || {};
    const queryCoverage = report.query_coverage || {};
    const partitioningCandidates = report.partitioning_candidates || [];
    const denormOpportunities = report.denormalization_opportunities || {};
    const totalColumns = report.total_columns || 0;

    // Build layout with new and updated cards
    root.innerHTML = `
        <div class="row g-3">
            <!-- Schema Metrics Overview -->
            <div class="col-12">
                <div class="row g-3">
                    <div class="col-6 col-md-3">
                        <div class="schema-metric">
                            <div class="schema-metric-value">${tables.length}</div>
                            <div class="schema-metric-label">Total Tables</div>
                        </div>
                    </div>
                    <div class="col-6 col-md-3">
                        <div class="schema-metric">
                            <div class="schema-metric-value">${totalColumns}</div>
                            <div class="schema-metric-label">Total Columns</div>
                        </div>
                    </div>
                    <div class="col-6 col-md-3">
                        <div class="schema-metric">
                            <div class="schema-metric-value">${indexCoverage.total_indexes || 0}</div>
                            <div class="schema-metric-label">Total Indexes</div>
                        </div>
                    </div>
                    <div class="col-6 col-md-3">
                        <div class="schema-metric">
                            <div class="schema-metric-value">${Object.keys(colTypeDist).length}</div>
                            <div class="schema-metric-label">Column Types</div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Tables List -->
            <div class="col-12 col-lg-7">
                <div class="card h-100">
                    <div class="card-header d-flex justify-content-between align-items-center">
                        <span><i class="bi bi-table"></i> Tables Overview</span>
                        <span class="badge bg-primary">${tables.length}</span>
                    </div>
                    <div class="card-body">
                        <div class="table-responsive" style="max-height: 500px; overflow-y: auto;">
                            <table class="table table-sm table-hover mb-0">
                                <thead class="sticky-top bg-dark">
                                    <tr>
                                        <th>Table Name</th>
                                        <th class="text-end">Columns</th>
                                        <th class="text-end">Est. Rows</th>
                                        <th class="text-center">Primary Key</th>
                                    </tr>
                                </thead>
                                <tbody id="schema-tables-list"></tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Data Types Distribution -->
            <div class="col-12 col-lg-5">
                <div class="card h-100">
                    <div class="card-header"><i class="bi bi-pie-chart"></i> Column Type Distribution</div>
                    <div class="card-body d-flex align-items-center justify-content-center">
                        <div class="chart-container" style="height: 300px; width: 100%;">
                            <canvas id="dataTypesChart"></canvas>
                        </div>
                    </div>
                </div>
            </div>

            <!-- NEW: Table Usage in Queries -->
            <div class="col-12">
                <div class="card">
                    <div class="card-header"><i class="bi bi-bullseye"></i> Table Usage in Queries</div>
                    <div class="card-body" id="table-usage-info"></div>
                </div>
            </div>

            <!-- ENHANCED: Data Quality -->
            <div class="col-12 col-lg-6">
                <div class="card h-100">
                    <div class="card-header"><i class="bi bi-shield-check"></i> Data Quality Insights</div>
                    <div class="card-body" id="data-quality-info"></div>
                </div>
            </div>

            <!-- Index Coverage (already good) -->
            <div class="col-12 col-lg-6">
                <div class="card h-100">
                    <div class="card-header"><i class="bi bi-lightning-charge"></i> Index Coverage</div>
                    <div class="card-body" id="index-coverage-info"></div>
                </div>
            </div>

            <!-- NEW: Denormalization Opportunities -->
            <div class="col-12 col-lg-6">
                 <div class="card h-100">
                    <div class="card-header"><i class="bi bi-link-45deg"></i> Denormalization Opportunities</div>
                    <div class="card-body" id="denorm-opportunities-info"></div>
                </div>
            </div>

            <!-- ENHANCED: Partitioning -->
            <div class="col-12 col-lg-6">
                <div class="card h-100">
                    <div class="card-header"><i class="bi bi-scissors"></i> Partitioning Recommendations</div>
                    <div class="card-body" id="partitioning-candidates" style="max-height: 400px; overflow-y: auto;"></div>
                </div>
            </div>
        </div>
    `;

    // Render tables list
    const tablesList = document.getElementById('schema-tables-list');
    if (tablesList) {
        if (tables.length > 0) {
            tablesList.innerHTML = tables.sort((a,b) => b.column_count - a.column_count).map(t => `
                <tr>
                    <td><code class="inline">${escapeHtml(t.name || 'Unknown')}</code></td>
                    <td class="text-end">${nf(t.column_count || 0)}</td>
                    <td class="text-end">${nf(t.estimated_rows || 0)}</td>
                    <td class="text-center">
                        ${t.has_primary_key
                            ? '<i class="bi bi-check-circle-fill text-success" title="Has Primary Key"></i>'
                            : '<i class="bi bi-x-circle text-danger" title="No Primary Key"></i>'}
                    </td>
                </tr>
            `).join('');
        } else {
            tablesList.innerHTML = '<tr><td colspan="4" class="text-center text-muted">No tables found</td></tr>';
        }
    }

    // Call render functions for all cards
    renderDataTypesChart(colTypeDist);
    renderTableUsage(queryCoverage);
    renderDataQuality(dataQuality);
    renderIndexCoverage(indexCoverage, tables);
    renderDenormalizationOpportunities(denormOpportunities);
    renderPartitioningCandidates(partitioningCandidates);
}

function renderDataTypesChart(colTypeDist) {
    const dtCtx = document.getElementById('dataTypesChart');
    if (!dtCtx) return;

    const existingChart = Chart.getChart(dtCtx);
    if (existingChart) existingChart.destroy();

    const dtLabels = Object.keys(colTypeDist);
    const dtValues = Object.values(colTypeDist).map(toNumber);

    if (dtLabels.length > 0) {
        new Chart(dtCtx, {
            type: 'doughnut',
            data: { labels: dtLabels, datasets: [{ data: dtValues, backgroundColor: coloredBars(chartColors.info, dtLabels.length), borderColor: '#00000000' }] },
            options: {
                responsive: true, maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'right', labels: { color: chartColors.text, font: { size: 11 } } },
                    tooltip: {
                        callbacks: {
                            label: (i) => {
                                const total = dtValues.reduce((a, b) => a + b, 0);
                                const pct = total > 0 ? ((i.raw / total) * 100).toFixed(1) : 0;
                                return `${i.label}: ${nf(i.raw)} (${pct}%)`;
                            }
                        }
                    }
                }
            }
        });
    } else {
        dtCtx.parentElement.innerHTML = '<div class="text-muted text-center p-3">No data type information</div>';
    }
}

function renderTableUsage(queryCoverage) {
    const container = document.getElementById('table-usage-info');
    if (!container) return;

    const usage = queryCoverage.table_usage || {};
    const unused = queryCoverage.unused_tables || [];
    const usedEntries = Object.entries(usage).filter(([name, count]) => count > 0).sort((a,b) => b[1] - a[1]);
    const maxCount = usedEntries.length > 0 ? usedEntries[0][1] : 0;

    let html = '';
    if (usedEntries.length > 0) {
        html += `<h6>Used Tables (${usedEntries.length})</h6>
                 <div class="table-responsive" style="max-height: 250px; overflow-y: auto;">
                    <table class="table table-sm table-borderless"><tbody>`;
        html += usedEntries.map(([name, count]) => {
            const percentage = maxCount > 0 ? ((count / maxCount) * 100) : 0;
            return `<tr>
                <td style="width: 40%;"><code class="inline">${escapeHtml(name)}</code></td>
                <td style="width: 15%;" class="text-end"><span class="badge bg-primary">${nf(count)}</span></td>
                <td><div class="progress" style="height: 1rem;"><div class="progress-bar" role="progressbar" style="width: ${percentage}%"></div></div></td>
            </tr>`;
        }).join('');
        html += '</tbody></table></div>';
    } else {
        html += '<h6>Used Tables</h6><div class="alert alert-secondary">No table usage data found in queries.</div>';
    }

    if (unused.length > 0) {
        html += `<h6 class="mt-4">Unused Tables (${unused.length})</h6>
                 <div class="alert alert-warning d-flex flex-wrap gap-2">`
        html += unused.map(name => `<div><code class="inline-badge">${escapeHtml(name)}</code></div>`).join('');
        html += `</div>
                 <small class="text-muted">Consider archiving or removing these tables if they are no longer needed.</small>`;
    }
    container.innerHTML = html;
}

function renderDataQuality(dataQuality) {
    const container = document.getElementById('data-quality-info');
    if (!container) return;

    if (!dataQuality || !dataQuality.recommendations) {
        container.innerHTML = '<div class="text-muted">No data quality information available.</div>';
        return;
    }

    const { nullable_columns_percent, tables_without_pk, orphaned_tables, recommendations } = dataQuality;

    container.innerHTML = `
        <div class="row g-3 mb-3">
            <div class="col-4 text-center">
                <div class="text-muted small">Tables w/o PK</div>
                <div class="fs-4 ${tables_without_pk > 0 ? 'text-warning' : 'text-success'}">${nf(tables_without_pk)}</div>
            </div>
             <div class="col-4 text-center">
                <div class="text-muted small">Orphaned Tables</div>
                <div class="fs-4 ${orphaned_tables > 0 ? 'text-warning' : 'text-success'}">${nf(orphaned_tables)}</div>
            </div>
            <div class="col-4 text-center">
                <div class="text-muted small">Nullable Cols</div>
                <div class="fs-4">${(nullable_columns_percent || 0).toFixed(1)}%</div>
            </div>
        </div>
        <div class="alert alert-info">
             <strong><i class="bi bi-lightbulb"></i> Recommendations:</strong>
             <ul class="mb-0 mt-2 small">
                ${recommendations.map(rec => `<li>${escapeHtml(rec)}</li>`).join('')}
             </ul>
        </div>
    `;
}

function renderIndexCoverage(indexCoverage, tables) {
    const container = document.getElementById('index-coverage-info');
    if (!container) return;

    const coveragePct = toNumber(indexCoverage.coverage_percent || 0);
    const coverageClass = coveragePct >= 70 ? 'success' : (coveragePct >= 30 ? 'warning' : 'danger');

    container.innerHTML = `
        <div class="row g-3 mb-3">
            <div class="col-4 text-center">
                <div class="text-muted small">Indexed Tables</div>
                <div class="fs-4">${indexCoverage.indexed_tables || 0} / ${tables.length}</div>
            </div>
            <div class="col-4 text-center">
                <div class="text-muted small">Total Indexes</div>
                <div class="fs-4">${indexCoverage.total_indexes || 0}</div>
            </div>
            <div class="col-4 text-center">
                <div class="text-muted small">Coverage</div>
                <div class="fs-4">
                    <span class="badge bg-${coverageClass}">${coveragePct.toFixed(1)}%</span>
                </div>
            </div>
        </div>
        ${indexCoverage.recommendations ? `
            <div class="alert alert-${coverageClass === 'success' ? 'info' : 'warning'} mb-0">
                <i class="bi bi-lightbulb"></i> ${escapeHtml(indexCoverage.recommendations)}
            </div>
        ` : ''}
    `;
}

function renderDenormalizationOpportunities(denormData) {
    const container = document.getElementById('denorm-opportunities-info');
    if (!container) return;

    const level = denormData.opportunity_level || 'N/A';
     const levelClass = level === 'high' ? 'danger' : level === 'medium' ? 'warning' : 'secondary';

    container.innerHTML = `
        <div class="mb-3">
            <div class="text-muted small">Opportunity Level</div>
            <span class="badge fs-6 bg-${levelClass}">${escapeHtml(level.charAt(0).toUpperCase() + level.slice(1))}</span>
        </div>
        <ul class="list-group list-group-flush">
            <li class="list-group-item d-flex justify-content-between align-items-center">
                Total Join Operations
                <span class="badge bg-primary rounded-pill">${nf(denormData.total_join_operations || 0)}</span>
            </li>
            <li class="list-group-item d-flex justify-content-between align-items-center">
                Complex Join Queries
                <span class="badge bg-primary rounded-pill">${nf(denormData.complex_join_queries || 0)}</span>
            </li>
        </ul>
        ${denormData.recommendations ? `
            <div class="alert alert-info mt-3 mb-0 small">
                <i class="bi bi-lightbulb"></i> ${denormData.recommendations.join('<br>')}
            </div>
        ` : ''}
    `;
}

function renderPartitioningCandidates(candidates) {
    const container = document.getElementById('partitioning-candidates');
    if (!container) return;

    if (!candidates || candidates.length === 0) {
        container.innerHTML = `<div class="text-muted">No specific partitioning candidates identified.</div>`;
        return;
    }

    container.innerHTML = candidates.map(tableGroup => `
        <div class="mb-3">
            <h6 class="border-bottom pb-2 mb-2">Table: <code>${escapeHtml(tableGroup.table)}</code></h6>
            <ul class="list-unstyled">
            ${tableGroup.candidates.map(col => `
                <li class="mb-2">
                    <div class="d-flex justify-content-between">
                        <span><i class="bi bi-bar-chart-steps"></i> <code>${escapeHtml(col.column)}</code></span>
                        <span class="badge bg-info">${escapeHtml(col.strategy || 'Partition')}</span>
                    </div>
                    <small class="text-muted ps-3">${escapeHtml(col.reason)}</small>
                </li>
            `).join('')}
            </ul>
        </div>
    `).join('');
}