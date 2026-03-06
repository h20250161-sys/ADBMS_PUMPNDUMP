/* ================================================================
   Pump & Dump Detection System – Stock Analysis Page Logic
   ================================================================ */

const COLORS = {
    red: '#ef4444', blue: '#3b82f6', green: '#22c55e',
    yellow: '#eab308', cyan: '#06b6d4', purple: '#a855f7',
    orange: '#f97316', pink: '#ec4899',
};

const CHART_DEFAULTS = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
        legend: {
            labels: { color: '#555', font: { size: 11, family: 'Inter' } }
        },
        tooltip: {
            backgroundColor: '#fff',
            titleColor: '#111',
            bodyColor: '#555',
            borderColor: '#e5e7eb',
            borderWidth: 1,
            cornerRadius: 8,
            padding: 10,
        },
    },
    scales: {
        x: {
            ticks: { color: '#888', font: { size: 10, family: 'Inter' } },
            grid:  { color: 'rgba(0, 0, 0, 0.05)' },
        },
        y: {
            ticks: { color: '#888', font: { size: 10, family: 'Inter' } },
            grid:  { color: 'rgba(0, 0, 0, 0.05)' },
        },
    },
};

let chartInstances = {};

function destroyChart(id) {
    if (chartInstances[id]) { chartInstances[id].destroy(); delete chartInstances[id]; }
}

async function fetchJSON(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`API ${res.status}`);
    return res.json();
}

// ================================================================
//  NAV SEARCH (inline)
// ================================================================
(function initNavSearch() {
    const input = document.getElementById('nav-search-input');
    const dropdown = document.getElementById('nav-search-dropdown');
    if (!input || !dropdown) return;

    input.addEventListener('input', () => {
        const q = input.value.trim().toUpperCase();
        if (!q) { dropdown.classList.remove('active'); return; }
        const matches = ALL_TICKERS.filter(t =>
            t.toUpperCase().includes(q) ||
            (SECTOR_MAP[t] || '').toUpperCase().includes(q)
        ).slice(0, 8);

        if (!matches.length) { dropdown.classList.remove('active'); return; }

        dropdown.innerHTML = matches.map(t => `
            <a href="/stock/${encodeURIComponent(t)}" class="search-item">
                <span class="search-item-ticker">${t}</span>
                <span class="search-item-sector">${SECTOR_MAP[t] || ''}</span>
            </a>
        `).join('');
        dropdown.classList.add('active');
    });

    input.addEventListener('keydown', e => {
        if (e.key === 'Enter') {
            const q = input.value.trim().toUpperCase();
            const match = ALL_TICKERS.find(t => t.toUpperCase() === q) ||
                          ALL_TICKERS.find(t => t.toUpperCase().includes(q));
            if (match) window.location.href = `/stock/${encodeURIComponent(match)}`;
        }
        if (e.key === 'Escape') dropdown.classList.remove('active');
    });

    document.addEventListener('click', e => {
        if (!input.contains(e.target) && !dropdown.contains(e.target)) dropdown.classList.remove('active');
    });
})();

// ================================================================
//  MAIN – Load all data for this stock
// ================================================================
let stockData = null;       // PnD ranking row for this ticker
let timeseriesData = null;  // Time-series data
let sectorData = null;      // Sector averages

async function loadAll() {
    try {
        const [ranking, ts, sectors] = await Promise.all([
            fetchJSON('/api/pnd-ranking'),
            fetchJSON(`/api/stock-timeseries/${encodeURIComponent(TICKER)}`),
            fetchJSON('/api/sector-avg'),
        ]);

        stockData = ranking.find(r => r.ticker === TICKER) || null;
        timeseriesData = ts;
        sectorData = sectors;

        renderMetricCards();
        renderVerdict();
        renderAnalysisPanel();
        renderParamTable();
        renderRiskBreakdown();
        renderTimelineChart();
        renderZScoreChart();
        renderVolatilityChart();
        renderReasons();
        renderSectorComparison();
    } catch (e) {
        console.error('Failed to load stock data:', e);
        document.getElementById('analysis-body').innerHTML =
            '<div class="text-center text-danger py-4"><i class="bi bi-exclamation-circle me-2"></i>Failed to load data. Make sure databases are running and the pipeline has been executed.</div>';
    }
}

// ================================================================
//  METRIC CARDS
// ================================================================
function renderMetricCards() {
    if (!stockData) return;

    const d = stockData;
    const scoreEl = document.getElementById('m-pnd-score');
    const barEl   = document.getElementById('m-pnd-bar');
    const levelEl = document.getElementById('m-pnd-level');

    scoreEl.textContent = d.pnd_score.toFixed(2);
    const barPct = Math.min((d.pnd_score / 8) * 100, 100);
    barEl.style.width = barPct + '%';

    let levelText, levelColor;
    if (d.pnd_score >= 5)       { levelText = '🔴 CRITICAL RISK'; levelColor = COLORS.red; }
    else if (d.pnd_score >= 3)  { levelText = '🟠 HIGH RISK';     levelColor = COLORS.orange; }
    else if (d.pnd_score >= 1.5){ levelText = '🟡 MEDIUM RISK';   levelColor = COLORS.yellow; }
    else                        { levelText = '🟢 LOW RISK';      levelColor = COLORS.green; }
    levelEl.innerHTML = `<span style="color:${levelColor};font-weight:700">${levelText}</span>`;

    document.getElementById('m-zscore').textContent = d.avg_abs_z.toFixed(2);
    document.getElementById('m-anomaly-pct').textContent = d.anomaly_pct.toFixed(1) + '%';
    document.getElementById('m-anomaly-count').textContent = `${d.anomaly_count} anomalies out of ${d.total_rows} data points`;
    document.getElementById('m-vol-spike').textContent = d.avg_vol_spike.toFixed(2) + '×';
}

// ================================================================
//  TOP-RIGHT VERDICT CARD
// ================================================================
function renderVerdict() {
    const card = document.getElementById('verdict-card');
    if (!stockData) {
        card.innerHTML = '<div class="verdict-loading"><i class="bi bi-x-circle me-2"></i>No data</div>';
        return;
    }

    const d = stockData;
    let cls, text, sub;
    if (d.pnd_score >= 3 || d.anomaly_pct >= 8) {
        cls = 'danger'; text = 'UNSAFE';
        sub = 'High pump-and-dump risk detected';
    } else if (d.pnd_score >= 1.5 || d.anomaly_pct >= 5) {
        cls = 'risky'; text = 'CAUTION';
        sub = 'Moderate risk — investigate further';
    } else {
        cls = 'safe'; text = 'SAFER';
        sub = 'Low anomaly activity detected';
    }

    card.innerHTML = `
        <div class="verdict-label">INVESTMENT SAFETY</div>
        <div class="verdict-text ${cls}">${text}</div>
        <div class="verdict-sub">${sub}</div>
    `;
}

// ================================================================
//  AI ANALYSIS PANEL
// ================================================================
function renderAnalysisPanel() {
    const body = document.getElementById('analysis-body');
    if (!stockData) {
        body.innerHTML = '<div class="text-center text-muted py-4">No data available for analysis.</div>';
        return;
    }

    const d = stockData;
    let verdictClass, icon, headline, explanation;

    if (d.pnd_score >= 5) {
        verdictClass = 'danger';
        icon = 'bi-x-octagon-fill';
        headline = `🚨 CRITICAL: ${TICKER} shows strong pump-and-dump characteristics`;
        explanation = `This stock has a P&D risk score of <strong>${d.pnd_score.toFixed(2)}</strong>, which is in the <strong>CRITICAL</strong> range. ` +
            `The average volume z-score (${d.avg_abs_z.toFixed(2)}) is well above normal, with extreme spikes up to ${d.max_abs_z.toFixed(2)} standard deviations. ` +
            `Volume activity is ${d.avg_vol_spike.toFixed(1)}× above typical levels on average, and peaks at ${d.max_vol_spike.toFixed(1)}×. ` +
            `A total of <strong>${d.anomaly_pct.toFixed(1)}%</strong> of all data points were flagged as anomalous. ` +
            `These patterns are consistent with coordinated pump-and-dump manipulation — sudden volume surges followed by price instability.`;
    } else if (d.pnd_score >= 3) {
        verdictClass = 'danger';
        icon = 'bi-exclamation-triangle-fill';
        headline = `⚠️ HIGH RISK: ${TICKER} has significant anomalous patterns`;
        explanation = `With a P&D score of <strong>${d.pnd_score.toFixed(2)}</strong>, this stock shows elevated pump-and-dump risk. ` +
            `Volume z-scores averaging ${d.avg_abs_z.toFixed(2)} with peaks at ${d.max_abs_z.toFixed(2)}σ indicate unusual trading surges. ` +
            `The anomaly rate of ${d.anomaly_pct.toFixed(1)}% is above the safe threshold. ` +
            `While not conclusively a P&D scheme, the volume and volatility patterns warrant serious caution.`;
    } else if (d.pnd_score >= 1.5) {
        verdictClass = 'risky';
        icon = 'bi-info-circle-fill';
        headline = `⚡ MODERATE RISK: ${TICKER} shows some irregular patterns`;
        explanation = `The P&D risk score of <strong>${d.pnd_score.toFixed(2)}</strong> places this stock in the MEDIUM risk zone. ` +
            `Volume activity shows some spikes (avg ${d.avg_vol_spike.toFixed(2)}×, max ${d.max_vol_spike.toFixed(1)}×) but they could be driven by legitimate news or earnings events. ` +
            `The ${d.anomaly_pct.toFixed(1)}% anomaly rate is moderate. Exercise normal due diligence before investing.`;
    } else {
        verdictClass = 'safe';
        icon = 'bi-check-circle-fill';
        headline = `✅ LOW RISK: ${TICKER} appears to have normal trading patterns`;
        explanation = `With a P&D score of only <strong>${d.pnd_score.toFixed(2)}</strong>, this stock shows typical market behavior. ` +
            `Volume z-scores (avg ${d.avg_abs_z.toFixed(2)}) and volume spikes (avg ${d.avg_vol_spike.toFixed(2)}×) are within normal ranges. ` +
            `Only ${d.anomaly_pct.toFixed(1)}% of data points were flagged — well within expected bounds for a healthy stock.`;
    }

    // Build factor badges
    const factors = [];
    if (d.avg_abs_z > 2)       factors.push({ text: `Z-Score: ${d.avg_abs_z.toFixed(2)}`, cls: 'factor-bad' });
    else if (d.avg_abs_z > 1)  factors.push({ text: `Z-Score: ${d.avg_abs_z.toFixed(2)}`, cls: 'factor-warn' });
    else                       factors.push({ text: `Z-Score: ${d.avg_abs_z.toFixed(2)}`, cls: 'factor-good' });

    if (d.max_abs_z > 5)       factors.push({ text: `Peak Z: ${d.max_abs_z.toFixed(1)}σ`, cls: 'factor-bad' });
    else if (d.max_abs_z > 3)  factors.push({ text: `Peak Z: ${d.max_abs_z.toFixed(1)}σ`, cls: 'factor-warn' });
    else                       factors.push({ text: `Peak Z: ${d.max_abs_z.toFixed(1)}σ`, cls: 'factor-good' });

    if (d.avg_vol_spike > 2)   factors.push({ text: `Vol Spike: ${d.avg_vol_spike.toFixed(1)}×`, cls: 'factor-bad' });
    else if (d.avg_vol_spike > 1.5) factors.push({ text: `Vol Spike: ${d.avg_vol_spike.toFixed(1)}×`, cls: 'factor-warn' });
    else                       factors.push({ text: `Vol Spike: ${d.avg_vol_spike.toFixed(1)}×`, cls: 'factor-good' });

    if (d.anomaly_pct > 8)     factors.push({ text: `Anomaly: ${d.anomaly_pct.toFixed(1)}%`, cls: 'factor-bad' });
    else if (d.anomaly_pct > 4) factors.push({ text: `Anomaly: ${d.anomaly_pct.toFixed(1)}%`, cls: 'factor-warn' });
    else                       factors.push({ text: `Anomaly: ${d.anomaly_pct.toFixed(1)}%`, cls: 'factor-good' });

    if (d.is_known_suspect)    factors.push({ text: '⚠ Known P&D Suspect', cls: 'factor-bad' });

    const factorHTML = factors.map(f =>
        `<span class="verdict-factor ${f.cls}">${f.text}</span>`
    ).join('');

    body.innerHTML = `
        <div class="verdict-summary">
            <div class="verdict-icon-large ${verdictClass}">
                <i class="bi ${icon}"></i>
            </div>
            <div>
                <div class="verdict-headline">${headline}</div>
                <p class="verdict-explanation">${explanation}</p>
                <div class="verdict-factors">${factorHTML}</div>
            </div>
        </div>
    `;
}

// ================================================================
//  PARAMETER TABLE
// ================================================================
function renderParamTable() {
    const tbody = document.getElementById('param-tbody');
    if (!stockData) return;

    const d = stockData;
    const params = [
        { name: 'Avg |Z-Score|',       value: d.avg_abs_z.toFixed(4),           threshold: '2.0',  exceed: d.avg_abs_z > 2 },
        { name: 'Max |Z-Score|',       value: d.max_abs_z.toFixed(4),           threshold: '5.0',  exceed: d.max_abs_z > 5 },
        { name: 'Avg Volume Spike',    value: d.avg_vol_spike.toFixed(4) + '×', threshold: '2.0×', exceed: d.avg_vol_spike > 2 },
        { name: 'Max Volume Spike',    value: d.max_vol_spike.toFixed(4) + '×', threshold: '3.0×', exceed: d.max_vol_spike > 3 },
        { name: 'Avg |Price Change %|',value: d.avg_abs_price_chg.toFixed(4) + '%', threshold: '2.0%', exceed: d.avg_abs_price_chg > 2 },
        { name: 'Anomaly Rate',        value: d.anomaly_pct.toFixed(2) + '%',   threshold: '5.0%', exceed: d.anomaly_pct > 5 },
        { name: 'P&D Risk Score',      value: d.pnd_score.toFixed(4),           threshold: '3.0',  exceed: d.pnd_score > 3 },
    ];

    tbody.innerHTML = params.map(p => `
        <tr>
            <td style="font-weight:600;color:#111">${p.name}</td>
            <td><code style="color:#0369a1">${p.value}</code></td>
            <td><code style="color:#999">${p.threshold}</code></td>
            <td><span class="param-status ${p.exceed ? 'exceed' : 'normal'}">${p.exceed ? '⚠ EXCEEDS' : '✓ NORMAL'}</span></td>
        </tr>
    `).join('');
}

// ================================================================
//  RISK BREAKDOWN DOUGHNUT
// ================================================================
function renderRiskBreakdown() {
    if (!stockData) return;
    const d = stockData;

    // Replicate the PnD score formula weights
    const components = [
        { label: 'Avg |Z-Score| (30%)',  value: 0.30 * d.avg_abs_z, color: COLORS.red },
        { label: 'Max |Z-Score| (25%)',  value: 0.25 * d.max_abs_z, color: COLORS.orange },
        { label: 'Avg Vol Spike (20%)',  value: 0.20 * d.avg_vol_spike, color: COLORS.yellow },
        { label: 'Max Vol Spike (15%)',  value: 0.15 * d.max_vol_spike, color: COLORS.cyan },
        { label: 'Avg |Price Chg| (10%)',value: 0.10 * d.avg_abs_price_chg, color: COLORS.purple },
    ];

    destroyChart('chart-risk-breakdown');
    const ctx = document.getElementById('chart-risk-breakdown').getContext('2d');
    chartInstances['chart-risk-breakdown'] = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: components.map(c => c.label),
            datasets: [{
                data: components.map(c => Math.max(c.value, 0.01)),
                backgroundColor: components.map(c => c.color + 'cc'),
                borderColor: components.map(c => c.color),
                borderWidth: 2,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            cutout: '55%',
            plugins: {
                legend: {
                    position: 'right',
                    labels: { color: '#555', font: { size: 10, family: 'Inter' }, padding: 12 },
                },
                tooltip: CHART_DEFAULTS.plugins.tooltip,
            },
        },
    });
}

// ================================================================
//  ANOMALY TIMELINE CHART
// ================================================================
function renderTimelineChart() {
    if (!timeseriesData || !timeseriesData.length) return;

    const data = timeseriesData;
    const timestamps = data.map(d => d.ts);
    const scores = data.map(d => d.anomaly_score);
    const anomalyPts = data.map(d => d.is_anomaly ? d.anomaly_score : null);

    destroyChart('chart-anomaly-timeline');
    const ctx = document.getElementById('chart-anomaly-timeline').getContext('2d');
    chartInstances['chart-anomaly-timeline'] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: timestamps,
            datasets: [
                {
                    label: 'Anomaly Score',
                    data: scores,
                    borderColor: COLORS.blue,
                    backgroundColor: 'rgba(59, 130, 246, 0.08)',
                    fill: true,
                    borderWidth: 1.2,
                    pointRadius: 0,
                    tension: 0.1,
                },
                {
                    label: `Anomalies (${data.filter(d => d.is_anomaly).length})`,
                    data: anomalyPts,
                    borderColor: 'transparent',
                    backgroundColor: COLORS.red,
                    pointRadius: anomalyPts.map(v => v !== null ? 4 : 0),
                    pointHoverRadius: 6,
                    showLine: false,
                },
            ],
        },
        options: {
            ...CHART_DEFAULTS,
            scales: {
                x: {
                    type: 'time',
                    time: { unit: 'hour', displayFormats: { hour: 'MMM dd HH:mm' } },
                    ticks: { ...CHART_DEFAULTS.scales.x.ticks, maxRotation: 0, autoSkip: true, maxTicksLimit: 12 },
                    grid: CHART_DEFAULTS.scales.x.grid,
                },
                y: {
                    ...CHART_DEFAULTS.scales.y,
                    title: { display: true, text: 'Composite Score', color: '#888', font: { size: 11 } },
                    beginAtZero: true,
                },
            },
        },
    });
}

// ================================================================
//  Z-SCORE CHART
// ================================================================
function renderZScoreChart() {
    if (!timeseriesData || !timeseriesData.length) return;

    const data = timeseriesData;
    // Downsample if too many points
    const step = Math.max(1, Math.floor(data.length / 500));
    const sampled = data.filter((_, i) => i % step === 0);

    destroyChart('chart-zscore');
    const ctx = document.getElementById('chart-zscore').getContext('2d');
    chartInstances['chart-zscore'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: sampled.map(d => d.ts),
            datasets: [{
                label: 'Z-Score',
                data: sampled.map(d => d.z_score),
                backgroundColor: sampled.map(d =>
                    Math.abs(d.z_score) > 2 ? COLORS.red + 'aa' :
                    Math.abs(d.z_score) > 1 ? COLORS.orange + 'aa' : COLORS.blue + '66'
                ),
                borderWidth: 0,
                borderRadius: 1,
            }],
        },
        options: {
            ...CHART_DEFAULTS,
            plugins: { ...CHART_DEFAULTS.plugins, legend: { display: false } },
            scales: {
                x: {
                    type: 'time',
                    time: { unit: 'day', displayFormats: { day: 'MMM dd' } },
                    ticks: { ...CHART_DEFAULTS.scales.x.ticks, maxTicksLimit: 8, maxRotation: 0 },
                    grid: CHART_DEFAULTS.scales.x.grid,
                },
                y: {
                    ...CHART_DEFAULTS.scales.y,
                    title: { display: true, text: 'Volume Z-Score', color: '#888', font: { size: 10 } },
                },
            },
        },
    });
}

// ================================================================
//  VOLATILITY CHART
// ================================================================
function renderVolatilityChart() {
    if (!timeseriesData || !timeseriesData.length) return;

    const data = timeseriesData;
    const step = Math.max(1, Math.floor(data.length / 500));
    const sampled = data.filter((_, i) => i % step === 0);

    destroyChart('chart-volatility');
    const ctx = document.getElementById('chart-volatility').getContext('2d');
    chartInstances['chart-volatility'] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: sampled.map(d => d.ts),
            datasets: [{
                label: 'Volatility',
                data: sampled.map(d => d.volatility),
                borderColor: COLORS.yellow,
                backgroundColor: 'rgba(234, 179, 8, 0.1)',
                fill: true,
                borderWidth: 1.5,
                pointRadius: 0,
                tension: 0.2,
            }],
        },
        options: {
            ...CHART_DEFAULTS,
            plugins: { ...CHART_DEFAULTS.plugins, legend: { display: false } },
            scales: {
                x: {
                    type: 'time',
                    time: { unit: 'day', displayFormats: { day: 'MMM dd' } },
                    ticks: { ...CHART_DEFAULTS.scales.x.ticks, maxTicksLimit: 8, maxRotation: 0 },
                    grid: CHART_DEFAULTS.scales.x.grid,
                },
                y: {
                    ...CHART_DEFAULTS.scales.y,
                    title: { display: true, text: 'Rolling Volatility', color: '#888', font: { size: 10 } },
                    beginAtZero: true,
                },
            },
        },
    });
}

// ================================================================
//  REASONS TO INVEST / AVOID
// ================================================================
function renderReasons() {
    const investList = document.getElementById('reasons-invest');
    const avoidList  = document.getElementById('reasons-avoid');
    if (!stockData) return;

    const d = stockData;
    const invest = [];
    const avoid  = [];

    // Positive reasons
    if (d.pnd_score < 1.5)
        invest.push('Low P&D risk score — trading patterns appear normal and organic.');
    if (d.anomaly_pct < 3)
        invest.push(`Very low anomaly rate (${d.anomaly_pct.toFixed(1)}%) — consistent with healthy, stable trading.`);
    if (d.avg_abs_z < 1)
        invest.push(`Volume z-score (${d.avg_abs_z.toFixed(2)}) well within normal range — no unusual volume surges.`);
    if (d.avg_vol_spike < 1.5)
        invest.push(`Average volume spike (${d.avg_vol_spike.toFixed(1)}×) is minimal — no signs of coordinated buying.`);
    if (d.avg_abs_price_chg < 1)
        invest.push(`Low average price volatility (${d.avg_abs_price_chg.toFixed(2)}%) suggests price stability.`);
    if (['Technology', 'Financials', 'Healthcare', 'Energy', 'Consumer Discretionary'].includes(d.sector))
        invest.push(`Belongs to established sector (${d.sector}) — typically less prone to manipulation.`);
    if (!d.is_known_suspect)
        invest.push('Not on any known pump-and-dump suspect watchlist.');
    if (d.max_abs_z < 3)
        invest.push(`Peak volume z-score (${d.max_abs_z.toFixed(1)}σ) is moderate — no extreme outlier events.`);

    // Negative reasons
    if (d.pnd_score >= 3)
        avoid.push(`High P&D risk score (${d.pnd_score.toFixed(2)}) — strong statistical signals of manipulation.`);
    if (d.pnd_score >= 1.5 && d.pnd_score < 3)
        avoid.push(`Moderate P&D risk score (${d.pnd_score.toFixed(2)}) — some suspicious patterns detected.`);
    if (d.anomaly_pct >= 5)
        avoid.push(`High anomaly rate (${d.anomaly_pct.toFixed(1)}%) — significantly more anomalies than a healthy stock.`);
    if (d.avg_abs_z >= 2)
        avoid.push(`Elevated volume z-score (${d.avg_abs_z.toFixed(2)}) — unusual volume surges are frequent.`);
    if (d.max_abs_z >= 5)
        avoid.push(`Extreme peak z-score (${d.max_abs_z.toFixed(1)}σ) — at least one massive volume spike detected.`);
    if (d.avg_vol_spike >= 2)
        avoid.push(`Volume regularly spikes ${d.avg_vol_spike.toFixed(1)}× above normal — classic pump signal.`);
    if (d.max_vol_spike >= 5)
        avoid.push(`Volume peaked at ${d.max_vol_spike.toFixed(1)}× normal — extreme pump event detected.`);
    if (d.is_known_suspect)
        avoid.push('⚠ This ticker is on the known pump-and-dump suspect watchlist.');
    if (['Meme Stock', 'Penny Stock', 'Crypto', 'Crypto Mining'].includes(d.sector))
        avoid.push(`Sector (${d.sector}) is historically prone to pump-and-dump manipulation.`);
    if (d.avg_abs_price_chg >= 2)
        avoid.push(`High average price swings (${d.avg_abs_price_chg.toFixed(2)}%) — unstable and unpredictable.`);

    // Ensure at least one item in each
    if (!invest.length) invest.push('No strong positive indicators found based on current data.');
    if (!avoid.length)  avoid.push('No significant red flags detected based on current analysis.');

    investList.innerHTML = invest.map(r =>
        `<li><span class="reason-icon">✅</span>${r}</li>`
    ).join('');

    avoidList.innerHTML = avoid.map(r =>
        `<li><span class="reason-icon">🚩</span>${r}</li>`
    ).join('');
}

// ================================================================
//  SECTOR COMPARISON CHART
// ================================================================
function renderSectorComparison() {
    if (!stockData || !sectorData || !sectorData.length) return;

    const d = stockData;
    const thisSector = sectorData.find(s => s.sector === d.sector);

    const labels = ['Avg Z-Score', 'Avg Volume Spike', 'Avg Price Change %'];
    const stockValues = [d.avg_abs_z, d.avg_vol_spike, Math.abs(d.avg_abs_price_chg)];
    const sectorValues = thisSector
        ? [Math.abs(thisSector.avg_z_score), thisSector.avg_volume_spike, Math.abs(thisSector.avg_price_change_pct)]
        : [0, 0, 0];

    destroyChart('chart-sector-compare');
    const ctx = document.getElementById('chart-sector-compare').getContext('2d');
    chartInstances['chart-sector-compare'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [
                {
                    label: TICKER,
                    data: stockValues,
                    backgroundColor: COLORS.red + 'bb',
                    borderColor: COLORS.red,
                    borderWidth: 1,
                    borderRadius: 6,
                },
                {
                    label: `${d.sector} Average`,
                    data: sectorValues,
                    backgroundColor: COLORS.blue + '88',
                    borderColor: COLORS.blue,
                    borderWidth: 1,
                    borderRadius: 6,
                },
            ],
        },
        options: {
            ...CHART_DEFAULTS,
            plugins: {
                ...CHART_DEFAULTS.plugins,
                legend: {
                    labels: { color: '#555', font: { size: 11, family: 'Inter' }, usePointStyle: true, pointStyle: 'circle' },
                },
            },
            scales: {
                ...CHART_DEFAULTS.scales,
                y: { ...CHART_DEFAULTS.scales.y, beginAtZero: true },
            },
        },
    });
}

// ================================================================
//  INIT
// ================================================================
document.addEventListener('DOMContentLoaded', loadAll);
