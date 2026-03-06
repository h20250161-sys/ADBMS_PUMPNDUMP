/* ================================================================
   Market Surveillance Dashboard – Frontend Logic
   ================================================================
   Fetches data from Flask API endpoints and renders Chart.js charts
   with a clean light-themed colour palette.
   ================================================================ */

// ── Colour palette ───────────────────────────────────────────────
const COLORS = {
    red:    '#dc2626',
    blue:   '#2563eb',
    green:  '#16a34a',
    yellow: '#ca8a04',
    cyan:   '#0891b2',
    purple: '#9333ea',
    orange: '#ea580c',
    pink:   '#db2777',
    teal:   '#0d9488',
    indigo: '#4f46e5',
};

const SECTOR_COLORS = {
    'Technology':            COLORS.blue,
    'Consumer Discretionary': COLORS.orange,
    'Financials':            COLORS.green,
    'Healthcare':            COLORS.cyan,
    'Energy':                COLORS.yellow,
};

const CHART_DEFAULTS = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
        legend: {
            labels: { color: '#4b5063', font: { size: 11, family: 'Inter' } }
        },
        tooltip: {
            backgroundColor: '#ffffff',
            titleColor: '#1a1d26',
            bodyColor: '#4b5063',
            borderColor: '#e2e5ec',
            borderWidth: 1,
            cornerRadius: 8,
            padding: 10,
            bodyFont: { family: 'Inter' },
            titleFont: { family: 'Inter', weight: 600 },
        },
    },
    scales: {
        x: {
            ticks: { color: '#6b7189', font: { size: 10, family: 'Inter' } },
            grid:  { color: 'rgba(0, 0, 0, 0.06)' },
        },
        y: {
            ticks: { color: '#6b7189', font: { size: 10, family: 'Inter' } },
            grid:  { color: 'rgba(0, 0, 0, 0.06)' },
        },
    },
};

// ── Chart instances (for destruction on refresh) ─────────────────
let chartInstances = {};

function destroyChart(id) {
    if (chartInstances[id]) {
        chartInstances[id].destroy();
        delete chartInstances[id];
    }
}

// ── Helpers ──────────────────────────────────────────────────────
async function fetchJSON(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`API error: ${res.status}`);
    return res.json();
}

function formatNumber(n) {
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
    if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K';
    return n.toLocaleString();
}

function anomalyBadge(pct) {
    let cls = 'low', label = 'LOW';
    if (pct >= 8)      { cls = 'high';   label = 'HIGH'; }
    else if (pct >= 6) { cls = 'medium'; label = 'MED'; }
    return `<span class="anomaly-badge ${cls}">${pct.toFixed(1)}% ${label}</span>`;
}

function updateTimestamp() {
    const el = document.getElementById('last-updated');
    if (el) el.textContent = 'Updated ' + new Date().toLocaleTimeString();
}

// ================================================================
//  1. OVERVIEW CARDS
// ================================================================
async function loadOverview() {
    try {
        const d = await fetchJSON('/api/overview');
        document.getElementById('stat-total').textContent = formatNumber(d.total_rows);
        document.getElementById('stat-anomalies').textContent = formatNumber(d.anomaly_count);
        document.getElementById('stat-tickers').textContent = d.ticker_count;
        document.getElementById('stat-sectors').textContent = d.sector_count;
        document.getElementById('stat-anomaly-pct').textContent = `${d.anomaly_pct}% of all data points`;

        if (d.min_ts && d.max_ts) {
            const from = new Date(d.min_ts).toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
            const to   = new Date(d.max_ts).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
            document.getElementById('stat-date-range').textContent = `${from} – ${to}`;
        }
    } catch (e) {
        console.error('Overview load error:', e);
    }
}

// ================================================================
//  2. SECTOR RADAR CHART
// ================================================================
async function loadSectorRadar() {
    try {
        const data = await fetchJSON('/api/sector-score');
        const labels = data.map(d => d.sector);
        const scores = data.map(d => d.anomaly_score);
        const colors = labels.map(l => SECTOR_COLORS[l] || COLORS.purple);

        destroyChart('chart-sector-radar');
        const ctx = document.getElementById('chart-sector-radar').getContext('2d');
        chartInstances['chart-sector-radar'] = new Chart(ctx, {
            type: 'radar',
            data: {
                labels,
                datasets: [{
                    label: 'Anomaly Score',
                    data: scores,
                    backgroundColor: 'rgba(220, 38, 38, 0.10)',
                    borderColor: COLORS.red,
                    borderWidth: 2,
                    pointBackgroundColor: colors,
                    pointBorderColor: '#fff',
                    pointRadius: 5,
                    pointHoverRadius: 7,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: { display: false },
                    tooltip: CHART_DEFAULTS.plugins.tooltip,
                },
                scales: {
                    r: {
                        angleLines: { color: 'rgba(0, 0, 0, 0.08)' },
                        grid:       { color: 'rgba(0, 0, 0, 0.08)' },
                        pointLabels: {
                            color: '#4b5063',
                            font: { size: 11, family: 'Inter', weight: 500 },
                        },
                        ticks: {
                            color: '#6b7189',
                            backdropColor: 'transparent',
                            font: { size: 9 },
                        },
                    },
                },
            },
        });
    } catch (e) {
        console.error('Sector radar error:', e);
    }
}

// ================================================================
//  3. SECTOR BAR CHART
// ================================================================
async function loadSectorBar() {
    try {
        const data = await fetchJSON('/api/sector-avg');
        const labels = data.map(d => d.sector);
        const bgColors = labels.map(l => SECTOR_COLORS[l] || COLORS.purple);

        destroyChart('chart-sector-bar');
        const ctx = document.getElementById('chart-sector-bar').getContext('2d');
        chartInstances['chart-sector-bar'] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: [
                    {
                        label: 'Avg Z-Score',
                        data: data.map(d => d.avg_z_score),
                        backgroundColor: bgColors.map(c => c + '99'),
                        borderColor: bgColors,
                        borderWidth: 1,
                        borderRadius: 4,
                    },
                    {
                        label: 'Avg Volume Spike',
                        data: data.map(d => d.avg_volume_spike),
                        backgroundColor: bgColors.map(c => c + '44'),
                        borderColor: bgColors.map(c => c + 'aa'),
                        borderWidth: 1,
                        borderRadius: 4,
                    },
                ],
            },
            options: {
                ...CHART_DEFAULTS,
                plugins: {
                    ...CHART_DEFAULTS.plugins,
                    legend: {
                        position: 'top',
                        labels: { color: '#4b5063', font: { size: 11, family: 'Inter' }, usePointStyle: true, pointStyle: 'circle' },
                    },
                },
                scales: {
                    ...CHART_DEFAULTS.scales,
                    y: { ...CHART_DEFAULTS.scales.y, beginAtZero: true },
                },
            },
        });
    } catch (e) {
        console.error('Sector bar error:', e);
    }
}

// ================================================================
//  4. MONTHLY TREND CHART
// ================================================================
async function loadMonthlyTrend() {
    try {
        const data = await fetchJSON('/api/monthly-trend');
        const labels = data.map(d => d.label);

        destroyChart('chart-monthly-trend');
        const ctx = document.getElementById('chart-monthly-trend').getContext('2d');
        chartInstances['chart-monthly-trend'] = new Chart(ctx, {
            type: 'line',
            data: {
                labels,
                datasets: [
                    {
                        label: 'Anomaly Count',
                        data: data.map(d => d.anomaly_count),
                        borderColor: COLORS.red,
                        backgroundColor: 'rgba(220, 38, 38, 0.08)',
                        fill: true,
                        tension: 0.3,
                        pointRadius: 5,
                        pointHoverRadius: 7,
                        pointBackgroundColor: COLORS.red,
                        yAxisID: 'y',
                    },
                    {
                        label: 'Anomaly %',
                        data: data.map(d => d.anomaly_pct),
                        borderColor: COLORS.cyan,
                        backgroundColor: 'transparent',
                        borderDash: [5, 5],
                        tension: 0.3,
                        pointRadius: 5,
                        pointHoverRadius: 7,
                        pointBackgroundColor: COLORS.cyan,
                        yAxisID: 'y1',
                    },
                ],
            },
            options: {
                ...CHART_DEFAULTS,
                scales: {
                    x: CHART_DEFAULTS.scales.x,
                    y: {
                        ...CHART_DEFAULTS.scales.y,
                        position: 'left',
                        title: { display: true, text: 'Anomaly Count', color: '#6b7189', font: { size: 11 } },
                    },
                    y1: {
                        ...CHART_DEFAULTS.scales.y,
                        position: 'right',
                        title: { display: true, text: 'Anomaly %', color: '#6b7189', font: { size: 11 } },
                        grid: { drawOnChartArea: false },
                    },
                },
                plugins: {
                    ...CHART_DEFAULTS.plugins,
                    legend: {
                        labels: { color: '#4b5063', font: { size: 11, family: 'Inter' }, usePointStyle: true, pointStyle: 'circle' },
                    },
                },
            },
        });
    } catch (e) {
        console.error('Monthly trend error:', e);
    }
}

// ================================================================
//  5. HOURLY HEATMAP (vertical bar)
// ================================================================
async function loadHourlyHeatmap() {
    try {
        const data = await fetchJSON('/api/hourly-heatmap');
        const labels = data.map(d => `${String(d.hour).padStart(2, '0')}:00`);
        const anomalyPcts = data.map(d => d.anomaly_pct);

        // Gradient: green → yellow → red based on value
        const max = Math.max(...anomalyPcts, 1);
        const barColors = anomalyPcts.map(v => {
            const ratio = v / max;
            if (ratio < 0.5) return COLORS.green;
            if (ratio < 0.75) return COLORS.yellow;
            return COLORS.red;
        });

        destroyChart('chart-hourly-heatmap');
        const ctx = document.getElementById('chart-hourly-heatmap').getContext('2d');
        chartInstances['chart-hourly-heatmap'] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: [{
                    label: 'Anomaly %',
                    data: anomalyPcts,
                    backgroundColor: barColors.map(c => c + 'cc'),
                    borderColor: barColors,
                    borderWidth: 1,
                    borderRadius: 3,
                }],
            },
            options: {
                ...CHART_DEFAULTS,
                indexAxis: 'y',
                plugins: {
                    ...CHART_DEFAULTS.plugins,
                    legend: { display: false },
                },
                scales: {
                    x: {
                        ...CHART_DEFAULTS.scales.x,
                        title: { display: true, text: 'Anomaly %', color: '#6b7189', font: { size: 10 } },
                    },
                    y: CHART_DEFAULTS.scales.y,
                },
            },
        });
    } catch (e) {
        console.error('Hourly heatmap error:', e);
    }
}

// ================================================================
//  6. TICKER TABLE
// ================================================================
async function loadTickerTable() {
    try {
        const data = await fetchJSON('/api/ticker-summary');
        const tbody = document.getElementById('ticker-tbody');
        tbody.innerHTML = '';

        data.forEach(row => {
            const sectorColor = SECTOR_COLORS[row.sector] || '#6b7189';
            const tr = document.createElement('tr');
            tr.setAttribute('data-ticker', row.ticker);
            tr.onclick = () => loadStockDrilldown(row.ticker);
            tr.innerHTML = `
                <td><strong style="color:#1a1d26">${row.ticker}</strong></td>
                <td><span style="color:${sectorColor}"><i class="bi bi-circle-fill me-1" style="font-size:0.5rem"></i>${row.sector}</span></td>
                <td>${row.avg_z_score.toFixed(4)}</td>
                <td><span class="text-warning">${row.max_abs_z_score.toFixed(2)}</span></td>
                <td>${row.avg_volume_spike.toFixed(4)}</td>
                <td>${(row.avg_price_change_pct * 100).toFixed(3)}%</td>
                <td>${anomalyBadge(row.anomaly_pct)}</td>
                <td><button class="btn btn-sm btn-outline-info btn-drilldown"><i class="bi bi-zoom-in"></i></button></td>
            `;
            tbody.appendChild(tr);
        });
    } catch (e) {
        console.error('Ticker table error:', e);
    }
}

// ================================================================
//  7. STOCK DRILL-DOWN TIMELINE
// ================================================================
async function loadStockDrilldown(ticker) {
    const section = document.getElementById('drilldown-section');
    section.style.display = 'block';
    document.getElementById('drilldown-title').textContent = `${ticker} – Anomaly Score Timeline`;

    // Highlight active row
    document.querySelectorAll('#ticker-tbody tr').forEach(r => r.classList.remove('active-row'));
    const activeRow = document.querySelector(`#ticker-tbody tr[data-ticker="${ticker}"]`);
    if (activeRow) activeRow.classList.add('active-row');

    // Scroll into view
    section.scrollIntoView({ behavior: 'smooth', block: 'nearest' });

    try {
        const data = await fetchJSON(`/api/stock-timeseries/${ticker}`);
        if (!data.length) return;

        const timestamps = data.map(d => d.ts);
        const scores     = data.map(d => d.anomaly_score);
        const isAnomaly  = data.map(d => d.is_anomaly);

        // Split into normal and anomaly points
        const normalPts  = [];
        const anomalyPts = [];
        data.forEach((d, i) => {
            const point = { x: d.ts, y: d.anomaly_score };
            if (d.is_anomaly) {
                anomalyPts.push(point);
                normalPts.push({ x: d.ts, y: null });
            } else {
                normalPts.push(point);
                anomalyPts.push({ x: d.ts, y: null });
            }
        });

        destroyChart('chart-stock-timeline');
        const ctx = document.getElementById('chart-stock-timeline').getContext('2d');
        chartInstances['chart-stock-timeline'] = new Chart(ctx, {
            type: 'line',
            data: {
                labels: timestamps,
                datasets: [
                    {
                        label: 'Anomaly Score',
                        data: scores,
                        borderColor: COLORS.blue,
                        backgroundColor: 'rgba(37, 99, 235, 0.08)',
                        fill: true,
                        borderWidth: 1.2,
                        pointRadius: 0,
                        tension: 0.1,
                    },
                    {
                        label: `Anomalies (${anomalyPts.filter(p => p.y !== null).length})`,
                        data: anomalyPts.map(p => p.y),
                        borderColor: 'transparent',
                        backgroundColor: COLORS.red,
                        pointRadius: anomalyPts.map(p => p.y !== null ? 4 : 0),
                        pointHoverRadius: 6,
                        pointBorderColor: '#b91c1c',
                        pointBorderWidth: 1,
                        showLine: false,
                        type: 'scatter',
                    },
                ],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    ...CHART_DEFAULTS.plugins,
                    legend: {
                        labels: { color: '#4b5063', font: { size: 11, family: 'Inter' }, usePointStyle: true, pointStyle: 'circle' },
                    },
                },
                scales: {
                    x: {
                        type: 'time',
                        time: { unit: 'hour', displayFormats: { hour: 'MMM dd HH:mm' } },
                        ticks: { color: '#6b7189', font: { size: 10 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 12 },
                        grid: { color: 'rgba(0, 0, 0, 0.06)' },
                    },
                    y: {
                        ...CHART_DEFAULTS.scales.y,
                        title: { display: true, text: 'Composite Score', color: '#6b7189', font: { size: 11 } },
                        beginAtZero: true,
                    },
                },
                interaction: {
                    mode: 'index',
                    intersect: false,
                },
            },
        });
    } catch (e) {
        console.error('Stock drill-down error:', e);
    }
}

function closeDrilldown() {
    document.getElementById('drilldown-section').style.display = 'none';
    document.querySelectorAll('#ticker-tbody tr').forEach(r => r.classList.remove('active-row'));
    destroyChart('chart-stock-timeline');
}

// ================================================================
//  INIT – Load everything
// ================================================================
async function refreshAll() {
    updateTimestamp();
    await Promise.all([
        loadOverview(),
        loadSectorRadar(),
        loadSectorBar(),
        loadMonthlyTrend(),
        loadHourlyHeatmap(),
        loadTickerTable(),
    ]);
}

// Boot
document.addEventListener('DOMContentLoaded', refreshAll);
