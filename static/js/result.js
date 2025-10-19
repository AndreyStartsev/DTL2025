/**
 * /static/js/result.js
 * Handles rendering of task results (DDL, migrations, queries)
 */

async function renderResult(taskId, status) {
    const container = document.getElementById('result-content');
    if (!container) return;

    const spinnerHtml = '<div class="text-center p-5"><div class="spinner-border" role="status"></div></div>';
    container.innerHTML = spinnerHtml;

    if (status !== 'DONE') {
        container.innerHTML = `<div class="alert alert-info">Task is not yet DONE.</div>`;
        return;
    }

    try {
        const res = await fetch(`/getresult?task_id=${taskId}`);
        if (!res.ok) {
            const errorData = await res.json();
            throw new Error(errorData.detail || 'Failed to fetch result');
        }
        const data = await res.json();

        const ddlHtml = data.ddl && data.ddl.length > 0
            ? `<pre><code>${escapeHtml(data.ddl.map(d => d.statement + ';').join('\n'))}</code></pre>`
            : '<div class="alert alert-warning">No DDL statements found.</div>';

        const migrationsHtml = data.migrations && data.migrations.length > 0
            ? `<pre><code>${escapeHtml(data.migrations.map(m => m.statement + ';').join('\n'))}</code></pre>`
            : '<div class="alert alert-warning">No migration scripts found.</div>';

        const queriesHtml = data.queries
            ? `<pre><code>${escapeHtml(JSON.stringify(data.queries, null, 2))}</code></pre>`
            : '<div class="alert alert-warning">No rewritten queries found.</div>';

        container.innerHTML = `
            <div class="result-section mb-4">
                <h6 class="result-section-title">
                    <i class="bi bi-database"></i> Optimized DDL
                </h6>
                ${ddlHtml}
            </div>

            <div class="result-section mb-4">
                <h6 class="result-section-title">
                    <i class="bi bi-arrow-left-right"></i> Migration Scripts
                </h6>
                ${migrationsHtml}
            </div>

            <div class="result-section mb-4">
                <h6 class="result-section-title">
                    <i class="bi bi-file-code"></i> Rewritten Queries
                </h6>
                ${queriesHtml}
            </div>
        `;
    } catch (e) {
        console.error('Error rendering result:', e);
        container.innerHTML = `<div class="alert alert-danger">Error loading result: ${e.message}</div>`;
    }
}