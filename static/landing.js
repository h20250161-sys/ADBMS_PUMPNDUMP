/* ================================================================
   PumpGuard – Landing Page Logic
   ================================================================ */

// ── Helpers ──────────────────────────────────────────────────────
async function fetchJSON(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`API error: ${res.status}`);
    return res.json();
}

function formatNumber(n) {
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
    if (n >= 1_000)     return (n / 1_000).toFixed(1) + 'K';
    return String(n);
}

// ================================================================
//  1. SEARCH – Autocomplete
// ================================================================
(function initSearch() {
    const input    = document.getElementById('search-input');
    const dropdown = document.getElementById('search-dropdown');
    if (!input || !dropdown) return;

    input.addEventListener('input', () => {
        const q = input.value.trim().toUpperCase();
        if (!q) { dropdown.classList.remove('active'); return; }

        const matches = ALL_TICKERS.filter(t =>
            t.toUpperCase().includes(q) ||
            (SECTOR_MAP[t] || '').toUpperCase().includes(q)
        ).slice(0, 12);

        if (!matches.length) {
            dropdown.innerHTML = '<div class="search-item" style="justify-content:center;color:var(--text-muted-d)">No results</div>';
            dropdown.classList.add('active');
            return;
        }

        dropdown.innerHTML = matches.map(t => {
            const isSuspect = PND_SUSPECTS.includes(t);
            return `
                <a href="/stock/${encodeURIComponent(t)}" class="search-item">
                    <div>
                        <span class="search-item-ticker">${t}</span>
                        ${isSuspect ? '<span class="search-item-suspect">⚠ P&D SUSPECT</span>' : ''}
                    </div>
                    <span class="search-item-sector">${SECTOR_MAP[t] || 'Unknown'}</span>
                </a>
            `;
        }).join('');
        dropdown.classList.add('active');
    });

    input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            const q = input.value.trim().toUpperCase();
            const match = ALL_TICKERS.find(t => t.toUpperCase() === q);
            if (match) {
                window.location.href = `/stock/${encodeURIComponent(match)}`;
            } else if (ALL_TICKERS.some(t => t.toUpperCase().includes(q))) {
                const first = ALL_TICKERS.find(t => t.toUpperCase().includes(q));
                window.location.href = `/stock/${encodeURIComponent(first)}`;
            }
        }
        if (e.key === 'Escape') {
            dropdown.classList.remove('active');
        }
    });

    document.addEventListener('click', (e) => {
        if (!input.contains(e.target) && !dropdown.contains(e.target)) {
            dropdown.classList.remove('active');
        }
    });
})();

// ================================================================
//  2. QUICK TAGS (trending suspects)
// ================================================================
(function initQuickTags() {
    const container = document.getElementById('quick-tags');
    if (!container) return;

    const trending = ['GME', 'AMC', 'DOGE-USD', 'SHIB-USD', 'BBBY', 'NKLA', 'AAPL', 'BTC-USD'];
    trending.forEach(t => {
        const isSuspect = PND_SUSPECTS.includes(t);
        const a = document.createElement('a');
        a.href = `/stock/${encodeURIComponent(t)}`;
        a.className = `quick-tag${isSuspect ? ' suspect' : ''}`;
        a.textContent = t;
        container.appendChild(a);
    });
})();

// ================================================================
//  3. STATS BAR – Load overview numbers
// ================================================================
async function loadStats() {
    try {
        const d = await fetchJSON('/api/overview');
        document.getElementById('stat-total-rows').textContent = formatNumber(d.total_rows);
        document.getElementById('stat-anomalies').textContent = formatNumber(d.anomaly_count);
        document.getElementById('stat-tickers').textContent = d.ticker_count;
        document.getElementById('stat-sectors').textContent = d.sector_count;

        const heroDP = document.getElementById('hero-data-points');
        if (heroDP) heroDP.textContent = formatNumber(d.total_rows);
        const heroTC = document.getElementById('hero-ticker-count');
        if (heroTC) heroTC.textContent = d.ticker_count;
    } catch (e) {
        console.warn('Stats load error:', e);
    }
}

// ================================================================
//  4. P&D SUSPECT WATCHLIST
// ================================================================
async function loadSuspects() {
    const container = document.getElementById('suspect-cards');
    if (!container) return;

    try {
        const data = await fetchJSON('/api/pnd-ranking');
        // Top 8 suspects
        const top = data.filter(d => d.is_known_suspect).slice(0, 8);
        if (!top.length) {
            container.innerHTML = '<div class="col-12 text-center text-muted py-4">No suspect data available yet. Run the pipeline first.</div>';
            return;
        }

        const maxScore = Math.max(...top.map(d => d.pnd_score), 1);

        container.innerHTML = top.map(d => {
            let riskClass = 'risk-low', riskLabel = 'LOW';
            if (d.pnd_score >= 5)      { riskClass = 'risk-critical'; riskLabel = 'CRITICAL'; }
            else if (d.pnd_score >= 3) { riskClass = 'risk-high';     riskLabel = 'HIGH'; }
            else if (d.pnd_score >= 1.5){ riskClass = 'risk-medium';   riskLabel = 'MEDIUM'; }

            const barPct = Math.min((d.pnd_score / maxScore) * 100, 100);
            const barColor = d.pnd_score >= 5 ? 'var(--accent-red)' :
                             d.pnd_score >= 3 ? 'var(--accent-orange)' :
                             d.pnd_score >= 1.5 ? 'var(--accent-yellow)' : 'var(--accent-green)';

            return `
                <div class="col-lg-3 col-md-4 col-sm-6">
                    <a href="/stock/${encodeURIComponent(d.ticker)}" class="suspect-card">
                        <div class="suspect-card-header">
                            <span class="suspect-ticker">${d.ticker}</span>
                            <span class="suspect-risk ${riskClass}">${riskLabel}</span>
                        </div>
                        <div class="text-muted" style="font-size:0.78rem">${d.sector}</div>
                        <div class="suspect-score-bar">
                            <div class="suspect-score-fill" style="width:${barPct}%;background:${barColor}"></div>
                        </div>
                        <div class="suspect-meta">
                            <span>Score: <strong>${d.pnd_score.toFixed(2)}</strong></span>
                            <span>Anomaly: <strong>${d.anomaly_pct.toFixed(1)}%</strong></span>
                        </div>
                    </a>
                </div>
            `;
        }).join('');
    } catch (e) {
        container.innerHTML = '<div class="col-12 text-center text-muted py-4">Could not load suspect data. Ensure databases are running.</div>';
        console.warn('Suspect load error:', e);
    }
}

// ================================================================
//  5. P&D NEWS / HISTORY
// ================================================================
function loadPnDNews() {
    const grid = document.getElementById('news-grid');
    if (!grid) return;

    const news = [
        {
            year: '2021',
            title: 'GameStop (GME) Short Squeeze',
            desc: 'Reddit\'s WallStreetBets community coordinated a massive short squeeze on GameStop, sending the stock from $17 to $483 in weeks. Hedge funds lost billions as retail investors pumped the stock to astronomical levels.',
            tag: 'Social Media Pump',
        },
        {
            year: '2021',
            title: 'AMC Entertainment Meme Rally',
            desc: 'Following GameStop, AMC became the next meme stock target. The stock surged 3,000% as retail traders organized online to squeeze short sellers, creating extreme volatility.',
            tag: 'Meme Stock',
        },
        {
            year: '2021',
            title: 'Dogecoin Elon Musk Pump',
            desc: 'Elon Musk\'s tweets about Dogecoin repeatedly caused price surges of 20-50% within hours. The "Dogefather" effect became a textbook example of celebrity-driven pump-and-dump patterns.',
            tag: 'Crypto P&D',
        },
        {
            year: '2022',
            title: 'BBBY Bed Bath & Beyond Crash',
            desc: 'Ryan Cohen\'s stake in BBBY caused a massive pump, but his quiet exit triggered a devastating dump. The stock rose 400% then crashed 70% in days, eventually leading to bankruptcy.',
            tag: 'Insider Dump',
        },
        {
            year: '2023',
            title: 'SHIB & PEPE Meme Coin Mania',
            desc: 'Meme coins like SHIB and PEPE experienced classic pump-and-dump cycles — coordinated Telegram/Discord groups pumped prices before early insiders dumped their holdings.',
            tag: 'Crypto P&D',
        },
        {
            year: '2022',
            title: 'Nikola (NKLA) Fraud Exposed',
            desc: 'Nikola\'s founder Trevor Milton was convicted of fraud after faking EV truck demonstrations. The stock, which was heavily pumped by SPAC hype, collapsed over 90% from its peak.',
            tag: 'SPAC Fraud',
        },
    ];

    grid.innerHTML = news.map(n => `
        <div class="col-lg-4 col-md-6">
            <div class="news-card">
                <span class="news-year">${n.year}</span>
                <h6 class="news-title">${n.title}</h6>
                <p class="news-desc">${n.desc}</p>
                <span class="news-tag">${n.tag}</span>
            </div>
        </div>
    `).join('');
}

// ================================================================
//  6. ALL TICKERS GRID
// ================================================================
function loadAllTickers() {
    const grid = document.getElementById('all-tickers-grid');
    if (!grid) return;

    grid.innerHTML = ALL_TICKERS.map(t => {
        const isSuspect = PND_SUSPECTS.includes(t);
        return `
            <a href="/stock/${encodeURIComponent(t)}" class="ticker-chip ${isSuspect ? 'is-suspect' : ''}">
                <span class="ticker-chip-symbol">${t}</span>
                <span class="ticker-chip-sector">${SECTOR_MAP[t] || ''}</span>
            </a>
        `;
    }).join('');
}

// ================================================================
//  INIT
// ================================================================
document.addEventListener('DOMContentLoaded', () => {
    loadStats();
    loadSuspects();
    loadPnDNews();
    loadAllTickers();
});
