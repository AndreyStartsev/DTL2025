// Utility Functions

// Date parsing and formatting
function parseDateSafe(value) {
    if (!value) return null;
    if (value instanceof Date) return value;
    let s = String(value).trim();
    if (s.indexOf('T') === -1 && s.indexOf(' ') !== -1) s = s.replace(' ', 'T');
    if (!/[zZ]|[+\-]\d{2}:?\d{2}$/.test(s)) s += 'Z';
    const d = new Date(s);
    return isNaN(d) ? null : d;
}

function formatDateTime(value) {
    const d = parseDateSafe(value);
    return d ? d.toLocaleString() : 'N/A';
}

function formatMs(ms) {
    if (ms == null || isNaN(ms) || ms < 0) return 'N/A';
    let seconds = Math.floor(ms / 1000);
    const hours = Math.floor(seconds / 3600);
    seconds %= 3600;
    const minutes = Math.floor(seconds / 60);
    seconds %= 60;
    let out = '';
    if (hours > 0) out += `${hours}h `;
    if (minutes > 0) out += `${minutes}m `;
    out += `${seconds}s`;
    return out.trim();
}

// Byte formatting utility
function formatBytes(bytes, decimals = 2) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}


// HTML escaping
function escapeHtml(s) {
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

// Number formatting
const toNumber = (v) => {
    if (typeof v === 'number') return v;
    if (typeof v === 'string') return Number(v.replace(/,/g, '')) || 0;
    return 0;
};

const nf = (v) => toNumber(v).toLocaleString();

// Chart colors
const chartColors = {
    text: '#adb5bd',
    grid: '#2b3035',
    primary: '#0d6efd',
    info: '#0dcaf0',
    success: '#198754',
    warning: '#ffc107',
    danger: '#dc3545',
    purple: '#6f42c1',
    slate: '#6c757d'
};

// Generate color bars for charts
function coloredBars(baseColor, count) {
    const map = {
        [chartColors.primary]: ['#0d6efd', '#3878ff', '#5c8bff', '#80a0ff', '#a6b7ff'],
        [chartColors.success]: ['#198754', '#1f9961', '#26ab6f', '#2ebd7d', '#38cf8c'],
        [chartColors.info]: ['#0dcaf0', '#29d0f2', '#46d7f4', '#64def6', '#83e5f8'],
        [chartColors.warning]: ['#ffc107', '#ffca2c', '#ffd24d', '#ffdb70', '#ffe493'],
        [chartColors.danger]: ['#dc3545', '#e24a59', '#e9606d', '#ef7782', '#f58f97'],
        [chartColors.purple]: ['#6f42c1', '#7c55c8', '#8969cf', '#977dd7', '#a593de'],
        [chartColors.slate]: ['#6c757d', '#788189', '#848e95', '#919ba1', '#9ea9ad']
    };
    const palette = map[baseColor] || map[chartColors.slate];
    return Array.from({ length: count }, (_, i) => palette[i % palette.length]);
}

// Base chart options
function baseChartOptions() {
    return {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: { labels: { color: chartColors.text } },
            tooltip: { mode: 'index', intersect: false }
        },
        scales: {
            x: { ticks: { color: chartColors.text }, grid: { color: chartColors.grid } },
            y: { ticks: { color: chartColors.text }, grid: { color: chartColors.grid } }
        }
    };
}

// Icon mapping
function iconClass(name) {
    const m = {
        database: 'bi-database',
        table: 'bi-table',
        search: 'bi-search',
        'exclamation-triangle': 'bi-exclamation-triangle'
    };
    return m[name] || 'bi-circle';
}

// Log level utilities
function levelClass(level) {
    const L = String(level || '').toUpperCase();
    if (L === 'WARN' || L === 'WARNING') return 'warning';
    if (['ERROR', 'FAILED', 'FAIL', 'CRITICAL'].includes(L)) return 'error';
    if (L === 'SUCCESS' || L === 'OK') return 'success';
    return 'info';
}

function levelLabel(level) {
    const L = String(level || '').toUpperCase();
    if (L === 'WARN') return 'WARNING';
    if (['ERROR', 'FAILED', 'FAIL', 'CRITICAL', 'SUCCESS', 'OK', 'INFO', 'DEBUG', 'TRACE', 'WARNING'].includes(L)) return L;
    return level || 'LOG';
}

// Render logs
function renderLogs(logs) {
    const container = document.getElementById('log-content');
    if (!container) return;
    if (!Array.isArray(logs) || logs.length === 0) {
        container.innerHTML = '<div class="text-muted">No logs available.</div>';
        return;
    }
    const rows = logs.map(l => {
        const cls = levelClass(l.level);
        const label = levelLabel(l.level);
        const time = escapeHtml(formatDateTime(l.timestamp));
        const msg = escapeHtml(l.message || '');
        return `
            <div class="log-row ${cls}">
                <span class="log-time">${time}</span>
                <span class="log-level-badge ${cls}">${label}</span>
                <span class="log-msg">${msg}</span>
            </div>
        `;
    }).join('');
    container.innerHTML = rows;
}