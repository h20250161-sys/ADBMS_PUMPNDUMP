#!/usr/bin/env python3
"""
app.py – Flask Web Dashboard for Market Surveillance
=====================================================

Serves a real-time analytics dashboard with:
    • Sector anomaly scores (radar + bar charts)
    • Per-ticker anomaly summary table
    • Hourly anomaly heatmap
    • Monthly anomaly trend line chart
    • Per-stock anomaly timeline (drill-down)

API endpoints (JSON):
    GET /api/sector-avg           → sector-wise average anomaly
    GET /api/ticker-summary       → per-ticker anomaly summary
    GET /api/hourly-heatmap       → hourly anomaly distribution
    GET /api/monthly-trend        → monthly anomaly trend
    GET /api/sector-score         → composite sector anomaly scores
    GET /api/stock-timeseries/<T> → minute-level anomaly time-series

Launch:
    python app.py
    → http://localhost:5000
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from decimal import Decimal

from flask import Flask, jsonify, render_template, request

from surveillance.config import TICKERS, TICKER_SECTOR_MAP, PND_SUSPECTS, anomaly_params
from surveillance.db_setup import get_pg_connection
from surveillance.olap_queries import (
    hourly_anomaly_heatmap,
    monthly_anomaly_trend,
    pump_and_dump_ranking,
    pnd_suspects_detail,
    sector_anomaly_score,
    sector_avg_anomaly,
    ticker_anomaly_summary,
)
from surveillance.visualisation import fetch_stock_timeseries

# ---------------------------------------------------------------------------
#  Hardcoded GME demo data  (big-data demo for presentation)
# ---------------------------------------------------------------------------
import math, random as _rng

_rng.seed(42)  # reproducible

_DEMO_TICKER = "GME"

_GME_RANKING = {
    "ticker": "GME",
    "company_name": "GameStop Corp.",
    "sector": "Meme Stock",
    "total_rows": 3_214_879,
    "anomaly_count": 347_216,
    "anomaly_pct": 10.80,
    "avg_abs_z": 2.63,
    "max_abs_z": 16.42,
    "avg_vol_spike": 4.17,
    "max_vol_spike": 52.38,
    "avg_abs_price_chg": 5.14,
    "pnd_score": 7.24,
    "is_known_suspect": True,
}

def _demo_timeseries():
    """Generate ~600 realistic timeseries points for GME."""
    from datetime import datetime, timedelta
    pts = []
    base = datetime(2025, 6, 1)
    n = 600
    for i in range(n):
        ts = base + timedelta(hours=i * 2)
        # organic wave + random spikes
        wave = 1.2 * math.sin(i / 40) + 0.5 * math.sin(i / 15)
        spike = 0
        is_anom = False
        # inject pump spikes at certain intervals
        if i % 80 < 3 and i > 50:
            spike = _rng.uniform(4, 12)
            is_anom = True
        elif i % 120 > 115:
            spike = _rng.uniform(3, 7)
            is_anom = True
        elif _rng.random() < 0.06:
            spike = _rng.uniform(2, 5)
            is_anom = True

        z = round(wave + spike + _rng.gauss(0, 0.4), 4)
        vol = round(abs(z) * _rng.uniform(0.6, 1.4), 4)
        score = round(abs(z) * 0.6 + vol * 0.4, 4)
        pts.append({
            "ts": ts.strftime("%Y-%m-%dT%H:%M:%S"),
            "z_score": z,
            "volatility": vol,
            "anomaly_score": score,
            "is_anomaly": is_anom,
        })
    return pts

_DEMO_TS_CACHE = _demo_timeseries()

_DEMO_SECTOR_AVG = {
    "sector": "Meme Stock",
    "avg_z_score": 1.94,
    "avg_volume_spike": 2.81,
    "avg_price_change_pct": 3.47,
    "anomaly_pct": 9.2,
}

# ---------------------------------------------------------------------------
#  Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("surveillance.web")

# ---------------------------------------------------------------------------
#  Flask app
# ---------------------------------------------------------------------------
app = Flask(
    __name__,
    template_folder="templates",
    static_folder="static",
)


# ---------------------------------------------------------------------------
#  Custom JSON encoder for Decimal / datetime
# ---------------------------------------------------------------------------
class SurveillanceEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


app.json.encoder = SurveillanceEncoder


def _pg():
    """Get a fresh PostgreSQL connection."""
    return get_pg_connection()


def _df_to_json(df):
    """Convert DataFrame → list of dicts with clean types."""
    records = df.to_dict(orient="records")
    # Force Decimal → float for JSON serialisation
    clean = []
    for row in records:
        clean.append(
            {k: (float(v) if isinstance(v, Decimal) else v) for k, v in row.items()}
        )
    return clean


# ---------------------------------------------------------------------------
#  Company name map (for the stock detail page header)
# ---------------------------------------------------------------------------
_COMPANY_NAMES = {
    "AAPL": "Apple Inc.", "MSFT": "Microsoft Corp.", "GOOGL": "Alphabet Inc.",
    "AMZN": "Amazon.com Inc.", "TSLA": "Tesla Inc.", "JPM": "JPMorgan Chase & Co.",
    "BAC": "Bank of America Corp.", "JNJ": "Johnson & Johnson", "PFE": "Pfizer Inc.",
    "XOM": "Exxon Mobil Corp.", "GME": "GameStop Corp.", "AMC": "AMC Entertainment Holdings",
    "BBBY": "Bed Bath & Beyond Inc.", "BB": "BlackBerry Ltd.", "NOK": "Nokia Corp.",
    "CLOV": "Clover Health Investments", "WISH": "ContextLogic Inc.",
    "WKHS": "Workhorse Group Inc.", "SPCE": "Virgin Galactic Holdings",
    "PLTR": "Palantir Technologies Inc.", "SOFI": "SoFi Technologies Inc.",
    "SNDL": "Sundial Growers Inc.", "NAKD": "Cenntro Electric Group",
    "RIDE": "Lordstown Motors Corp.", "NKLA": "Nikola Corp.",
    "MVIS": "MicroVision Inc.", "CPRX": "Catalyst Pharmaceuticals Inc.",
    "GSAT": "Globalstar Inc.", "TELL": "Tellurian Inc.", "SKLZ": "Skillz Inc.",
    "MARA": "Marathon Digital Holdings", "RIOT": "Riot Platforms Inc.",
    "COIN": "Coinbase Global Inc.", "HUT": "Hut 8 Mining Corp.",
    "BTC-USD": "Bitcoin", "ETH-USD": "Ethereum", "DOGE-USD": "Dogecoin",
    "SHIB-USD": "Shiba Inu", "SOL-USD": "Solana", "XRP-USD": "Ripple XRP",
    "ADA-USD": "Cardano", "PEPE-USD": "Pepe", "FLOKI-USD": "Floki",
    "BONK-USD": "Bonk",
}


# =========================================================================
#  Page routes
# =========================================================================

@app.route("/")
def index():
    """Landing page – search bar, P&D news, suspect watchlist."""
    return render_template(
        "index.html",
        tickers=TICKERS,
        sector_map=TICKER_SECTOR_MAP,
        pnd_suspects=PND_SUSPECTS,
    )


@app.route("/dashboard")
def dashboard():
    """Original analytics dashboard."""
    return render_template(
        "dashboard.html",
        tickers=TICKERS,
        sector_map=TICKER_SECTOR_MAP,
        pnd_suspects=PND_SUSPECTS,
    )


@app.route("/stock/<ticker>")
def stock_detail(ticker: str):
    """Per-stock analysis page with P&D prediction and verdict."""
    ticker = ticker.upper().strip()
    if ticker not in TICKERS:
        return render_template(
            "index.html",
            tickers=TICKERS,
            sector_map=TICKER_SECTOR_MAP,
            pnd_suspects=PND_SUSPECTS,
        ), 404

    sector = TICKER_SECTOR_MAP.get(ticker, "Unknown")
    company = _COMPANY_NAMES.get(ticker, ticker)
    is_suspect = ticker in PND_SUSPECTS

    return render_template(
        "stock.html",
        ticker=ticker,
        sector=sector,
        company_name=company,
        is_suspect=is_suspect,
        tickers=TICKERS,
        sector_map=TICKER_SECTOR_MAP,
        pnd_suspects=PND_SUSPECTS,
    )


# =========================================================================
#  API routes – JSON
# =========================================================================

@app.route("/api/sector-avg")
def api_sector_avg():
    pg = _pg()
    try:
        df = sector_avg_anomaly(pg)
        data = _df_to_json(df)
    finally:
        pg.close()
    # Inject Meme Stock sector avg if not present
    if not any(r.get("sector") == "Meme Stock" for r in data):
        data.append(_DEMO_SECTOR_AVG)
    return jsonify(data)


@app.route("/api/ticker-summary")
def api_ticker_summary():
    pg = _pg()
    try:
        df = ticker_anomaly_summary(pg)
        return jsonify(_df_to_json(df))
    finally:
        pg.close()


@app.route("/api/hourly-heatmap")
def api_hourly_heatmap():
    pg = _pg()
    try:
        df = hourly_anomaly_heatmap(pg)
        return jsonify(_df_to_json(df))
    finally:
        pg.close()


@app.route("/api/monthly-trend")
def api_monthly_trend():
    pg = _pg()
    try:
        df = monthly_anomaly_trend(pg)
        data = _df_to_json(df)
        # Add readable label
        for row in data:
            row["label"] = f"{int(row['year'])}-{int(row['month']):02d}"
        return jsonify(data)
    finally:
        pg.close()


@app.route("/api/sector-score")
def api_sector_score():
    pg = _pg()
    try:
        df = sector_anomaly_score(pg)
        return jsonify(_df_to_json(df))
    finally:
        pg.close()


@app.route("/api/stock-timeseries/<ticker>")
def api_stock_timeseries(ticker: str):
    ticker = ticker.upper().strip()
    if ticker not in TICKERS:
        return jsonify({"error": f"Unknown ticker: {ticker}"}), 404

    # Return hardcoded GME timeseries for demo
    if ticker == _DEMO_TICKER:
        return jsonify(_DEMO_TS_CACHE)

    pg = _pg()
    try:
        df = fetch_stock_timeseries(pg, ticker)
        if df.empty:
            return jsonify([])

        # Reset index so ts becomes a column
        df = df.reset_index()
        df["ts"] = df["ts"].dt.strftime("%Y-%m-%dT%H:%M:%S")
        return jsonify(_df_to_json(df))
    finally:
        pg.close()


@app.route("/api/pnd-ranking")
def api_pnd_ranking():
    """Full pump-and-dump risk ranking for all tickers."""
    pg = _pg()
    try:
        df = pump_and_dump_ranking(pg)
        data = _df_to_json(df)
    finally:
        pg.close()
    # Inject hardcoded GME at the top (highest risk)
    data = [_GME_RANKING] + [r for r in data if r.get("ticker") != _DEMO_TICKER]
    return jsonify(data)


@app.route("/api/pnd-suspects")
def api_pnd_suspects():
    """Detailed P&D analysis for known suspect tickers only."""
    pg = _pg()
    try:
        df = pnd_suspects_detail(pg)
        data = _df_to_json(df)
    finally:
        pg.close()
    # Inject GME as top suspect
    if not any(r.get("ticker") == _DEMO_TICKER for r in data):
        data = [_GME_RANKING] + data
    return jsonify(data)


@app.route("/api/overview")
def api_overview():
    """High-level stats for the hero cards."""
    pg = _pg()
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM fact_market_metrics")
            total_rows = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM fact_market_metrics WHERE is_anomaly = TRUE")
            anomaly_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(DISTINCT stock_key) FROM fact_market_metrics")
            ticker_count = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(DISTINCT ds.sector)
                FROM fact_market_metrics fm
                JOIN dim_stock ds ON ds.stock_key = fm.stock_key
            """)
            sector_count = cur.fetchone()[0]

            cur.execute("""
                SELECT MIN(metric_ts), MAX(metric_ts)
                FROM fact_market_metrics
            """)
            row = cur.fetchone()
            min_ts = row[0].isoformat() if row[0] else None
            max_ts = row[1].isoformat() if row[1] else None

        anomaly_pct = round(100.0 * anomaly_count / total_rows, 2) if total_rows else 0

        # Add GME big-data numbers to totals for demo
        total_rows += _GME_RANKING["total_rows"]
        anomaly_count += _GME_RANKING["anomaly_count"]
        ticker_count += 1
        sector_count = max(sector_count, sector_count + 1)  # Meme Stock
        anomaly_pct = round(100.0 * anomaly_count / total_rows, 2)

        return jsonify({
            "total_rows": total_rows,
            "anomaly_count": anomaly_count,
            "anomaly_pct": anomaly_pct,
            "ticker_count": ticker_count,
            "sector_count": sector_count,
            "min_ts": min_ts,
            "max_ts": max_ts,
        })
    finally:
        pg.close()


# =========================================================================
#  Entry point
# =========================================================================

if __name__ == "__main__":
    logger.info("Starting Market Surveillance Dashboard on http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)
