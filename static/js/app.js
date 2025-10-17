// Main Application Logic

mermaid.initialize({
    startOnLoad: false,
    theme: 'dark',
    themeVariables: { darkMode: true }
});

let mermaidRendered = false;
let currentFilter = '';
let currentTaskId = null; // Stays for non-modal context if any, but modal is primary

// (updateTaskTimingCell, getStatusBadge, fetchTasks, deleteTask, renderColoredDiff functions are unchanged)
// ... paste unchanged functions here ...
async function updateTaskTimingCell(task) {
    const cell = document.querySelector(`.duration-cell[data-task-id="${task.taskid}"]`);
    if (!cell) return;

    const submitted = parseDateSafe(task.submitted_at);
    const completed = parseDateSafe(task.completed_at);

    let processingMs = null, queueMs = null, totalMs = (submitted && completed) ? (completed - submitted) : null;
    let first = null, last = null;

    try {
        const res = await fetch(`/task/${task.taskid}/log`);
        if (!res.ok) throw new Error('log fetch failed');
        const logs = await res.json();

        const times = Array.isArray(logs)
            ? logs.map(l => parseDateSafe(l.timestamp)).filter(Boolean).sort((a, b) => a - b)
            : [];
        first = times[0] || null;
        last = times[times.length - 1] || (completed || null);

        if (first && last) processingMs = Math.max(0, last - first);
        if (submitted && first) queueMs = Math.max(0, first - submitted);
    } catch (e) {
        // Fallback to total if log fetch fails
    }

    const mainMs = (processingMs != null) ? processingMs : (totalMs != null ? totalMs : null);
    const mainText = mainMs != null ? formatMs(mainMs) : '—';

    const status = String(task.status || '').toUpperCase();
    const colorClass =
        status === 'DONE' ? 'text-success' :
            status === 'FAILED' ? 'text-danger' :
                'text-info';

    const tooltipHtml = `
        <div><strong>Processing</strong>: ${processingMs != null ? formatMs(processingMs) : '—'}</div>
        ${queueMs != null && queueMs > 0 ? `<div><strong>Queued</strong>: ${formatMs(queueMs)}</div>` : ''}
        <div><strong>Total</strong>: ${totalMs != null ? formatMs(totalMs) : '—'}</div>
        <hr class="my-1"/>
        <div><strong>Submitted</strong>: ${submitted ? submitted.toLocaleString() : '—'}</div>
        <div><strong>First activity</strong>: ${first ? first.toLocaleString() : '—'}</div>
        <div><strong>Completed</strong>: ${completed ? completed.toLocaleString() : '—'}</div>
    `.replace(/\n\s+/g, '');

    const titleAttr = tooltipHtml.replace(/"/g, '&quot;');
    cell.innerHTML = `
        <i class="bi bi-clock-history me-1 text-secondary"></i>
        <span class="duration-text ${colorClass}"
              data-bs-toggle="tooltip"
              data-bs-placement="top"
              data-bs-html="true"
              title="${titleAttr}">
          ${mainText}
        </span>
    `;

    const el = cell.querySelector('.duration-text');
    if (el) new bootstrap.Tooltip(el);
}

function getStatusBadge(status) {
    const badges = {
        'RUNNING': 'bg-info text-dark',
        'DONE': 'bg-success',
        'FAILED': 'bg-danger'
    };
    return `<span class="badge ${badges[status] || 'bg-secondary'} status-badge">${status}</span>`;
}

async function fetchTasks(status = '') {
    const taskTableBody = document.getElementById('task-table-body');
    const loadingSpinner = document.getElementById('loadingSpinner');

    loadingSpinner.classList.remove('d-none');
    taskTableBody.innerHTML = '';

    try {
        const response = await fetch(`/tasks?status=${status}`);
        const tasks = await response.json();

        if (tasks.length === 0) {
            taskTableBody.innerHTML = `<tr><td colspan="6" class="text-center text-muted">No tasks found.</td></tr>`;
        } else {
            tasks.forEach(task => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td><small>${task.taskid}</small></td>
                    <td>${getStatusBadge(task.status)}</td>
                    <td>${formatDateTime(task.submitted_at)}</td>
                    <td>${formatDateTime(task.completed_at)}</td>
                    <td class="duration-cell" data-task-id="${task.taskid}">
                        <span class="duration-placeholder">
                            <span class="spinner-border spinner-border-sm me-1" role="status"></span>
                            Calculating…
                        </span>
                    </td>
                    <td class="text-end">
                        <button class="btn btn-sm btn-outline-light view-details"
                                data-task-id="${task.taskid}" data-status="${task.status}"
                                data-bs-toggle="modal" data-bs-target="#detailModal">
                            <i class="bi bi-eye"></i> View
                        </button>
                        <button class="btn btn-sm btn-outline-danger delete-task" data-task-id="${task.taskid}">
                            <i class="bi bi-trash"></i>
                        </button>
                    </td>
                `;
                taskTableBody.appendChild(row);
                updateTaskTimingCell(task);
            });
        }
    } catch (error) {
        taskTableBody.innerHTML = `<tr><td colspan="6" class="text-center text-danger">Error fetching tasks.</td></tr>`;
        console.error('Error fetching tasks:', error);
    } finally {
        loadingSpinner.classList.add('d-none');
    }
}

async function deleteTask(taskId) {
    if (!confirm(`Are you sure you want to delete task ${taskId}?`)) return;

    try {
        const response = await fetch(`/task/${taskId}`, { method: 'DELETE' });
        if (response.ok) {
            fetchTasks(currentFilter);
        } else {
            alert('Failed to delete task.');
        }
    } catch (error) {
        alert('Error deleting task.');
        console.error('Error deleting task:', error);
    }
}

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


// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', () => {
    // Initialize all Bootstrap tooltips on the page
    const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
    tooltipTriggerList.forEach(tooltipTriggerEl => new bootstrap.Tooltip(tooltipTriggerEl));

    // (Get element references are unchanged)
    const refreshButton = document.getElementById('refreshButton');
    const filterButtons = document.querySelectorAll('#statusFilters button');
    const createTaskForm = document.getElementById('createTaskForm');
    const createSubmitBtn = document.getElementById('createSubmitBtn');
    const createSpinner = document.getElementById('createSpinner');
    const createError = document.getElementById('createError');
    const taskTableBody = document.getElementById('task-table-body');
    const detailModal = document.getElementById('detailModal');
    const statsModal = document.getElementById('statsModal');
    let loadedTabs = new Set();
    let currentModalTaskId = null;
    let analysisReportCache = null;

    // (Event listeners for refresh, filters, delete are unchanged)
    refreshButton.addEventListener('click', () => fetchTasks(currentFilter));
    filterButtons.forEach(button => {
        button.addEventListener('click', () => {
            filterButtons.forEach(btn => btn.classList.remove('active'));
            button.classList.add('active');
            currentFilter = button.dataset.status;
            fetchTasks(currentFilter);
        });
    });
    taskTableBody.addEventListener('click', (e) => {
        const deleteBtn = e.target.closest('.delete-task');
        if (deleteBtn) {
            deleteTask(deleteBtn.dataset.taskId);
        }
    });

    // (Create task form listener is unchanged)
    createTaskForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        createSubmitBtn.disabled = true;
        createSpinner.classList.remove('d-none');
        createError.classList.add('d-none');

        let data;
        try {
            data = JSON.parse(document.getElementById('taskDataJson').value);
            if (!data.url || !data.ddl || !data.queries) {
                throw new Error('The JSON data must include "url", "ddl", and "queries" keys.');
            }

            const payload = {
                ...data, // spread the url, ddl, queries
                config: {
                    strategy: document.getElementById('strategySelect').value,
                    model_id: document.getElementById('modelSelect').value,
                    context_length: 10000,
                    batch_size: 15
                }
            };

            const response = await fetch('/new', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            if (response.ok) {
                bootstrap.Modal.getInstance(document.getElementById('createTaskModal')).hide();
                createTaskForm.reset();
                fetchTasks(currentFilter);
            } else {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Failed to create task.');
            }
        } catch (error) {
            createError.textContent = `Error: ${error.message}`;
            createError.classList.remove('d-none');
        } finally {
            createSubmitBtn.disabled = false;
            createSpinner.classList.add('d-none');
        }
    });

    // --- Modal Logic ---
    detailModal.addEventListener('show.bs.modal', async (event) => {
        // (This initial part is mostly unchanged)
        const button = event.relatedTarget;
        currentModalTaskId = button.dataset.taskId;

        // Reset state
        loadedTabs.clear();
        analysisReportCache = null;
        document.getElementById('detailModalTitle').textContent = `Task Details: ${currentModalTaskId.substring(0, 8)}...`;
        document.getElementById('detailSpinner').classList.remove('d-none');
        document.getElementById('detailContent').classList.add('d-none');
        document.getElementById('detailError').classList.add('d-none');

        ['schema-root', 'analysis-root', 'result-content', 'diff-content', 'log-content', 'original-schema-viz', 'optimized-schema-viz'].forEach(id => {
           const el = document.getElementById(id);
           if (el) el.innerHTML = '';
        });

        try {
            const analysisRes = await fetch(`/task/${currentModalTaskId}/analysis`);
            if (!analysisRes.ok) throw new Error('Failed to load analysis report');
            analysisReportCache = await analysisRes.json();

            console.log("Full analysis report cached:", analysisReportCache);

        } catch (e) {
            console.error('Fatal: could not load analysis report', e);
            document.getElementById('detailError').textContent = `Could not load the main analysis report: ${e.message}`;
            document.getElementById('detailError').classList.remove('d-none');
            document.getElementById('detailSpinner').classList.add('d-none');
            return;
        }

        document.getElementById('detailSpinner').classList.add('d-none');
        document.getElementById('detailContent').classList.remove('d-none');

        const defaultTab = document.querySelector('#detailTab .nav-link.active');
        await handleTabContent(defaultTab);
    });

    document.querySelectorAll('#detailTab .nav-link').forEach(tab => {
        tab.addEventListener('shown.bs.tab', async event => handleTabContent(event.target));
    });

    // UPDATED handleTabContent function
    async function handleTabContent(tabElement) {
        if (!tabElement) return;
        const tabId = tabElement.getAttribute('data-bs-target');
        if (!currentModalTaskId || loadedTabs.has(tabId)) return;

        const status = document.querySelector(`.view-details[data-task-id="${currentModalTaskId}"]`)?.dataset.status;
        const spinnerHtml = '<div class="text-center p-5"><div class="spinner-border" role="status"></div></div>';

        loadedTabs.add(tabId);

        try {
            if (tabId === '#visualizer') {
                if (status === 'DONE') {
                    // Get original schema from the already-fetched analysis report
                    const originalSchemaData = analysisReportCache?.raw_report?.schema_overview;
                    const originalMermaidCode = generateMermaidFromSchemaObject(originalSchemaData);
                    renderMermaidDiagram('original-schema-viz', originalMermaidCode);

                    // Fetch optimized DDL from the result endpoint
                    const res = await fetch(`/getresult?task_id=${currentModalTaskId}`);
                    if (!res.ok) throw new Error(`Failed to fetch optimized DDL: ${(await res.json()).detail}`);
                    const resultData = await res.json();
                    const optimizedDdlString = resultData.ddl.map(d => d.statement).join(';\n');
                    const optimizedMermaidCode = generateMermaidFromDDL(optimizedDdlString);
                    renderMermaidDiagram('optimized-schema-viz', optimizedMermaidCode);
                } else {
                    document.getElementById('original-schema-viz').innerHTML = `<div class="alert alert-info m-2">Task is not yet complete. Diagrams will be generated once the task is DONE.</div>`;
                    document.getElementById('optimized-schema-viz').innerHTML = ``;
                }
            } else if (tabId === '#schema') {
                const schemaData = analysisReportCache?.raw_report?.schema_overview;
                if (schemaData) {
                    renderSchemaOverview(schemaData);
                } else {
                    document.getElementById('schema-root').innerHTML = '<div class="alert alert-warning">Schema data (`raw_report.schema_overview`) was not found.</div>';
                }
            } else if (tabId === '#analysis') {
                renderAnalysis(analysisReportCache);
            } else if (tabId === '#result') {
                const container = document.getElementById('result-content');
                container.innerHTML = spinnerHtml;
                if (status === 'DONE') {
                    const res = await fetch(`/getresult?task_id=${currentModalTaskId}`);
                    if (!res.ok) throw new Error((await res.json()).detail);
                    const data = await res.json();
                    container.innerHTML = `
                        <h6>Optimized DDL</h6><pre><code>${escapeHtml(data.ddl.map(d => d.statement + ';').join('\n'))}</code></pre>
                        <h6>Migration Scripts</h6><pre><code>${escapeHtml(data.migrations.map(m => m.statement + ';').join('\n'))}</code></pre>
                        <h6>Rewritten Queries</h6><pre><code>${escapeHtml(JSON.stringify(data.queries, null, 2))}</code></pre>
                    `;
                } else {
                    container.innerHTML = `<div class="alert alert-info">Task is not yet DONE.</div>`;
                }
            } else if (tabId === '#log') {
                const container = document.getElementById('log-content');
                container.innerHTML = spinnerHtml;
                const res = await fetch(`/task/${currentModalTaskId}/log`);
                const logs = await res.json();
                renderLogs(logs);
            } else if (tabId === '#diff') {
                await renderColoredDiff(currentModalTaskId, status);
            }
        } catch (e) {
            const errorContainer = document.querySelector(tabId);
            if(errorContainer) {
                 errorContainer.innerHTML = `<div class="alert alert-danger">Error loading content for this tab: ${e.message}</div>`;
            }
            console.error(`Error loading tab ${tabId}:`, e);
        }
    }

    // (Stats modal listeners are unchanged)
    // ... paste unchanged stats modal listeners here ...
    statsModal.addEventListener('show.bs.modal', async () => {
        try {
            const response = await fetch('/tasks');
            const tasks = await response.json();
            document.getElementById('totalTasks').textContent = tasks.length;
            document.getElementById('runningTasks').textContent = tasks.filter(t => t.status === 'RUNNING').length;
            document.getElementById('doneTasks').textContent = tasks.filter(t => t.status === 'DONE').length;
            document.getElementById('failedTasks').textContent = tasks.filter(t => t.status === 'FAILED').length;
        } catch (e) {
            console.error('Could not fetch stats', e);
        }
    });

    statsModal.addEventListener('shown.bs.modal', async () => {
        if (!mermaidRendered) {
            const element = document.getElementById('mermaid-diagram');
            const mermaidCode = `
stateDiagram-v2
    direction LR
    [*] --> RUNNING: POST /new
    RUNNING --> DONE: Success
    RUNNING --> FAILED: Error/Timeout
    DONE --> [*]
    FAILED --> [*]
            `;
            try {
                element.innerHTML = '';
                const { svg } = await mermaid.render('mermaid-' + Date.now(), mermaidCode);
                element.innerHTML = svg;
                mermaidRendered = true;
            } catch (error) {
                console.error('Mermaid rendering error:', error);
                element.innerHTML = '<p class="text-muted text-center">Diagram could not be rendered</p>';
            }
        }
    });

    statsModal.addEventListener('hidden.bs.modal', () => {
        mermaidRendered = false;
        document.getElementById('mermaid-diagram').innerHTML = '';
    });


    // Initial load
    fetchTasks();
});