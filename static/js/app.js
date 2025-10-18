/**
 * /static/js/app.js
 * Main Application Logic - Task Management and Modal Handling
 */

mermaid.initialize({
    startOnLoad: false,
    theme: 'dark',
    themeVariables: { darkMode: true }
});

let mermaidRendered = false;
let currentFilter = '';
let currentModalTaskId = null;
let loadedTabs = new Set();
let analysisReportCache = null;

// ============================================================================
// Task Timing and Status
// ============================================================================

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

// ============================================================================
// Task List Management
// ============================================================================

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

// ============================================================================
// Modal Tab Content Handler
// ============================================================================

async function handleTabContent(tabElement) {
    if (!tabElement) return;
    const tabId = tabElement.getAttribute('data-bs-target');
    if (!currentModalTaskId || loadedTabs.has(tabId)) return;

    const status = document.querySelector(`.view-details[data-task-id="${currentModalTaskId}"]`)?.dataset.status;

    loadedTabs.add(tabId);

    try {
        if (tabId === '#visualizer') {
            if (status === 'DONE') {
                Promise.all([
                    fetch(`/task_info/${currentModalTaskId}`).then(async res => {
                        if (!res.ok) throw new Error(`Original DDL fetch failed: ${res.statusText}`);
                        return res.json();
                    }),
                    fetch(`/getresult?task_id=${currentModalTaskId}`).then(async res => {
                        if (!res.ok) throw new Error(`Optimized DDL fetch failed: ${res.statusText}`);
                        return res.json();
                    })
                ]).then(([taskInfoData, resultData]) => {
                    // Process Original Schema
                    if (taskInfoData && taskInfoData.original_input && Array.isArray(taskInfoData.original_input.ddl)) {
                        const originalDdlString = taskInfoData.original_input.ddl.map(d => d.statement).join(';\n');
                        const originalMermaidCode = generateMermaidFromDDL(originalDdlString);
                        const originalStats = calculateStatsFromMermaid(originalMermaidCode);
                        renderMermaidDiagram('original-schema-viz', originalMermaidCode, originalStats);
                    } else {
                        throw new Error("Original DDL not found in /task_info response.");
                    }

                    // Process Optimized Schema
                    const optimizedDdlString = resultData.ddl.map(d => d.statement).join(';\n');
                    const optimizedMermaidCode = generateMermaidFromDDL(optimizedDdlString);
                    const optimizedStats = calculateStatsFromMermaid(optimizedMermaidCode);
                    renderMermaidDiagram('optimized-schema-viz', optimizedMermaidCode, optimizedStats);

                }).catch(e => {
                    console.error("Error loading schema visualizer data:", e);
                    document.getElementById('original-schema-viz').innerHTML = `<div class="alert alert-danger m-2">Error loading diagrams: ${e.message}</div>`;
                    document.getElementById('optimized-schema-viz').innerHTML = '';
                });
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
            await renderResult(currentModalTaskId, status);
        } else if (tabId === '#log') {
            await renderLogs(currentModalTaskId);
        } else if (tabId === '#diff') {
            await renderColoredDiff(currentModalTaskId, status);
        }
    } catch (e) {
        const errorContainer = document.querySelector(tabId);
        if (errorContainer) {
            errorContainer.innerHTML = `<div class="alert alert-danger">Error loading content for this tab: ${e.message}</div>`;
        }
        console.error(`Error loading tab ${tabId}:`, e);
    }
}

// ============================================================================
// DOM Ready - Event Listeners
// ============================================================================

document.addEventListener('DOMContentLoaded', () => {
    // Initialize Bootstrap tooltips
    const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
    tooltipTriggerList.forEach(tooltipTriggerEl => new bootstrap.Tooltip(tooltipTriggerEl));

    // Get DOM elements
    const refreshButton = document.getElementById('refreshButton');
    const filterButtons = document.querySelectorAll('#statusFilters button');
    const createTaskForm = document.getElementById('createTaskForm');
    const createSubmitBtn = document.getElementById('createSubmitBtn');
    const createSpinner = document.getElementById('createSpinner');
    const createError = document.getElementById('createError');
    const taskTableBody = document.getElementById('task-table-body');
    const detailModal = document.getElementById('detailModal');
    const statsModal = document.getElementById('statsModal');

    // ============================================================================
    // Event Listeners - Task List
    // ============================================================================

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

    // ============================================================================
    // Event Listeners - Create Task Form
    // ============================================================================

    createTaskForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        createSubmitBtn.disabled = true;
        createSpinner.classList.remove('d-none');
        createError.classList.add('d-none');

        try {
            const data = JSON.parse(document.getElementById('taskDataJson').value);
            if (!data.url || !data.ddl || !data.queries) {
                throw new Error('The JSON data must include "url", "ddl", and "queries" keys.');
            }

            const payload = {
                ...data,
                config: {
                    strategy: document.getElementById('strategySelect').value,
                    model_id: document.getElementById('modelSelect').value,
                    context_length: 16000,
                    batch_size: 5
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

    // ============================================================================
    // Event Listeners - Detail Modal
    // ============================================================================

    detailModal.addEventListener('show.bs.modal', async (event) => {
        const button = event.relatedTarget;
        currentModalTaskId = button.dataset.taskId;

        // Reset state
        loadedTabs.clear();
        analysisReportCache = null;
        document.getElementById('detailModalTitle').textContent = `Task Details: ${currentModalTaskId.substring(0, 8)}...`;
        document.getElementById('detailSpinner').classList.remove('d-none');
        document.getElementById('detailContent').classList.add('d-none');
        document.getElementById('detailError').classList.add('d-none');

        // Clear all tab contents
        ['schema-root', 'analysis-root', 'result-content', 'diff-content', 'log-content', 'original-schema-viz', 'optimized-schema-viz'].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.innerHTML = '';
        });

        try {
            const analysisRes = await fetch(`/task/${currentModalTaskId}/analysis`);
            if (!analysisRes.ok) throw new Error('Failed to load analysis report');
            analysisReportCache = await analysisRes.json();

            console.log("Analysis report cached:", analysisReportCache);

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

    // ============================================================================
    // Event Listeners - Stats Modal
    // ============================================================================

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