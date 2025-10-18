/**
 * /static/js/logs.js
 * Handles rendering of task execution logs
 */

async function renderLogs(taskId) {
    const container = document.getElementById('log-content');
    if (!container) return;

    const spinnerHtml = '<div class="text-center p-5"><div class="spinner-border" role="status"></div></div>';
    container.innerHTML = spinnerHtml;

    try {
        const res = await fetch(`/task/${taskId}/log`);
        if (!res.ok) throw new Error('Failed to fetch logs');
        const logs = await res.json();

        if (!Array.isArray(logs) || logs.length === 0) {
            container.innerHTML = '<div class="alert alert-info">No logs available for this task.</div>';
            return;
        }

        const logHtml = logs.map(log => {
            const timestamp = log.timestamp ? formatDateTime(log.timestamp) : 'N/A';
            const level = (log.level || 'INFO').toUpperCase();
            const message = escapeHtml(log.message || '');

            // Map level to CSS class
            const levelClass = {
                'ERROR': 'error',
                'WARN': 'warning',
                'WARNING': 'warning',
                'INFO': 'info',
                'DEBUG': 'info',
                'SUCCESS': 'success'
            }[level] || 'info';

            return `
                <div class="log-row ${levelClass}">
                    <span class="log-time">${timestamp}</span>
                    <span class="log-level-badge">${level}</span>
                    <span class="log-message">${message}</span>
                </div>
            `;
        }).join('');

        container.innerHTML = `
            <div class="mb-3 d-flex justify-content-between align-items-center">
                <h6 class="mb-0">
                    <i class="bi bi-terminal"></i>
                    Execution Logs
                    <span class="badge bg-secondary">${logs.length} entries</span>
                </h6>
            </div>
            <div class="log-entries-container">
                ${logHtml}
            </div>
        `;
    } catch (e) {
        console.error('Error rendering logs:', e);
        container.innerHTML = `<div class="alert alert-danger">Error loading logs: ${e.message}</div>`;
    }
}