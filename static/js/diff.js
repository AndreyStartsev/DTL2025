/**
 * /static/js/diff.js
 * Handles query diff rendering and comparison
 */

async function renderColoredDiff(taskId, status) {
    const diffContainer = document.getElementById('diff-content');
    if (!diffContainer) return;

    if (status !== 'DONE') {
        diffContainer.innerHTML = '<div class="alert alert-info">Task is not yet complete.</div>';
        return;
    }

    diffContainer.innerHTML = '<div class="text-center p-3"><div class="spinner-border spinner-border-sm"></div> Loading diff...</div>';

    try {
        const res = await fetch(`/task/${taskId}/diff`);
        if (!res.ok) throw new Error('Failed to fetch diff data');
        const diffData = await res.json();

        if (!diffData.diffs || diffData.diffs.length === 0) {
            diffContainer.innerHTML = '<div class="alert alert-warning">No queries to compare.</div>';
            return;
        }

        const html = diffData.diffs.map((d, index) => {
            const queryId = d.queryid;
            const original = d.original || '';
            const optimized = d.optimized || '';

            const hasChanges = original !== optimized;
            const statusBadge = !optimized
                ? '<span class="badge bg-danger">Missing</span>'
                : (hasChanges
                    ? '<span class="badge bg-success">Optimized</span>'
                    : '<span class="badge bg-secondary">Unchanged</span>');

            return `
                <div class="query-comparison mb-4">
                    <div class="query-comparison-header">
                        <div class="d-flex justify-content-between align-items-center">
                            <div>
                                <i class="bi bi-database"></i>
                                <strong>Query ${index + 1}</strong>
                                <code class="ms-2 query-id">${escapeHtml(queryId.substring(0, 13))}...</code>
                                ${statusBadge}
                            </div>
                            <div class="text-muted small">
                                ${d.original_length} → ${d.optimized_length} chars
                            </div>
                        </div>
                    </div>

                    <div class="row g-0">
                        <div class="col-md-6">
                            <div class="query-panel query-panel-original">
                                <div class="query-panel-header"><i class="bi bi-file-earmark-code"></i> Original Query</div>
                                <div class="query-panel-body"><pre class="sql-code"><code>${escapeHtml(original)}</code></pre></div>
                            </div>
                        </div>
                        <div class="col-md-6">
                            <div class="query-panel query-panel-optimized">
                                <div class="query-panel-header"><i class="bi bi-rocket"></i> Optimized Query</div>
                                <div class="query-panel-body"><pre class="sql-code"><code>${optimized ? escapeHtml(optimized) : '<span class="text-muted">No optimized version</span>'}</code></pre></div>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        }).join('');

        diffContainer.innerHTML = html;
    } catch (e) {
        console.error('Error rendering diff:', e);
        diffContainer.innerHTML = `<div class="alert alert-danger">Error loading queries: ${e.message}</div>`;
    }
}