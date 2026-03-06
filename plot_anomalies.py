#!/usr/bin/env python3
"""
plot_anomalies.py – Demo Anomaly Visualisation
================================================

Simple demo script for presentation:

    1. Connects to PostgreSQL.
    2. Reads anomaly scores for a stock from ``fact_market_metrics``.
    3. Plots **time vs anomaly score** with detected anomalies in **red**.
    4. Saves the chart as PNG and displays it.

Usage
-----
    python plot_anomalies.py                  # defaults to AAPL
    python plot_anomalies.py --ticker TSLA
    python plot_anomalies.py --ticker MSFT --no-show   # save only
    python plot_anomalies.py --all            # one chart per ticker
"""

from __future__ import annotations

import argparse
import os
import sys

# Ensure project root is on sys.path for direct invocation
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import logging
import psycopg2

from surveillance.config import postgres_cfg, TICKERS
from surveillance.visualisation import fetch_stock_timeseries, plot_anomaly_timeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("plot_anomalies")

# ── Output directory for saved PNGs ───────────────────────────────────────
OUTPUT_DIR = os.path.join(_PROJECT_ROOT, "charts")


def _ensure_output_dir() -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    return OUTPUT_DIR


def plot_single(ticker: str, *, show: bool = True) -> None:
    """Connect → query → plot → save for one ticker."""
    logger.info("Connecting to PostgreSQL …")
    try:
        conn = psycopg2.connect(postgres_cfg.dsn)
        conn.autocommit = True
    except psycopg2.OperationalError as exc:
        logger.error("Cannot connect to PostgreSQL: %s", exc)
        sys.exit(1)

    logger.info("Fetching anomaly data for %s …", ticker)
    df = fetch_stock_timeseries(conn, ticker)

    if df.empty:
        logger.warning("No data for %s – did you run the ETL pipeline?", ticker)
        conn.close()
        return

    out_dir = _ensure_output_dir()
    png_path = os.path.join(out_dir, f"{ticker}_anomalies.png")

    logger.info("Plotting %d points (%d anomalies) …",
                len(df), df["is_anomaly"].sum())

    plot_anomaly_timeline(
        df, ticker,
        save_path=png_path,
        show=show,
    )
    logger.info("Done – chart saved to %s", png_path)
    conn.close()


def plot_all(*, show: bool = False) -> None:
    """Generate one chart per ticker and save all PNGs."""
    logger.info("Connecting to PostgreSQL …")
    try:
        conn = psycopg2.connect(postgres_cfg.dsn)
        conn.autocommit = True
    except psycopg2.OperationalError as exc:
        logger.error("Cannot connect to PostgreSQL: %s", exc)
        sys.exit(1)

    out_dir = _ensure_output_dir()

    for ticker in TICKERS:
        df = fetch_stock_timeseries(conn, ticker)
        if df.empty:
            logger.warning("No data for %s – skipping", ticker)
            continue

        png_path = os.path.join(out_dir, f"{ticker}_anomalies.png")
        plot_anomaly_timeline(df, ticker, save_path=png_path, show=False)
        logger.info("Saved %s", png_path)

    conn.close()
    logger.info("All charts saved to %s/", out_dir)

    if show:
        # Re-open the last chart for quick preview
        import matplotlib.pyplot as plt
        plt.show()


# ── CLI ───────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Plot anomaly scores from the PostgreSQL warehouse.",
    )
    p.add_argument(
        "--ticker", "-t",
        default="AAPL",
        help="Stock ticker to plot (default: AAPL).",
    )
    p.add_argument(
        "--all", "-a",
        action="store_true",
        dest="all_tickers",
        help="Generate charts for ALL tickers in the universe.",
    )
    p.add_argument(
        "--no-show",
        action="store_true",
        help="Save PNG only – don't open the interactive window.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    show = not args.no_show

    if args.all_tickers:
        plot_all(show=show)
    else:
        plot_single(args.ticker.upper(), show=show)


if __name__ == "__main__":
    main()
