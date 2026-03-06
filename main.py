#!/usr/bin/env python3
"""
main.py – Market Surveillance Pipeline Orchestrator
====================================================

Runs the full pipeline end-to-end:

    1. Bootstrap databases  (Cassandra keyspace/tables, PG star-schema)
    2. Ingest minute-level OHLC data  (yfinance → Cassandra)
    3. Detect anomalies               (Cassandra → Cassandra)
    4. ETL to warehouse               (Cassandra → PostgreSQL)
    5. Run OLAP queries               (PostgreSQL)
    6. Plot anomaly timelines          (PostgreSQL → PNG charts)

Usage
-----
    python main.py                 # full pipeline
    python main.py --step ingest   # single step
    python main.py --step anomaly
    python main.py --step etl
    python main.py --step olap
    python main.py --step plot
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional

from surveillance.db_setup import (
    bootstrap_all,
    CassandraSessionManager,
    cassandra_session,
    get_cassandra_session,
    get_pg_connection,
    create_cassandra_tables,
    create_pg_star_schema,
    ensure_monthly_partition,
)
from surveillance.ingestion import ingest_all
from surveillance.anomaly import detect_all
from surveillance.warehouse_etl import etl_all
from surveillance.olap_queries import (
    sector_avg_anomaly,
    ticker_anomaly_summary,
    monthly_anomaly_trend,
    sector_anomaly_score,
)
from surveillance.visualisation import fetch_stock_timeseries, plot_anomaly_timeline

# ---------------------------------------------------------------------------
#  Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("surveillance.main")


# ---------------------------------------------------------------------------
#  Pipeline steps
# ---------------------------------------------------------------------------

def step_bootstrap():
    """Create keyspaces, tables, and star-schema."""
    logger.info("=" * 60)
    logger.info("STEP 0 – Bootstrapping databases")
    logger.info("=" * 60)
    cass, pg, cass_mgr = bootstrap_all()
    return cass, pg, cass_mgr


def step_ingest(cass):
    """Download OHLC data from Yahoo Finance → Cassandra."""
    logger.info("=" * 60)
    logger.info("STEP 1 – Ingesting OHLC data")
    logger.info("=" * 60)
    total = ingest_all(cass)
    logger.info("Ingestion finished: %d rows written", total)
    return total


def step_anomaly(cass):
    """Compute rolling volatility + z-score anomaly → Cassandra."""
    logger.info("=" * 60)
    logger.info("STEP 2 – Anomaly detection")
    logger.info("=" * 60)
    total = detect_all(cass)
    logger.info("Anomaly detection finished: %d metric rows", total)
    return total


def step_etl(cass, pg):
    """Transfer anomaly metrics from Cassandra → PostgreSQL star schema."""
    logger.info("=" * 60)
    logger.info("STEP 3 – ETL to PostgreSQL warehouse")
    logger.info("=" * 60)
    total = etl_all(cass, pg)
    logger.info("ETL finished: %d fact rows upserted", total)
    return total


def step_olap(pg):
    """Run OLAP queries and print results."""
    logger.info("=" * 60)
    logger.info("STEP 4 – OLAP analytics")
    logger.info("=" * 60)

    print("\n" + "─" * 60)
    print("  SECTOR-WISE AVERAGE ANOMALY")
    print("─" * 60)
    sector_df = sector_avg_anomaly(pg)
    if sector_df.empty:
        print("  (no data)")
    else:
        print(sector_df.to_string(index=False))

    print("\n" + "─" * 60)
    print("  PER-TICKER ANOMALY SUMMARY")
    print("─" * 60)
    ticker_df = ticker_anomaly_summary(pg)
    if ticker_df.empty:
        print("  (no data)")
    else:
        print(ticker_df.to_string(index=False))

    print("\n" + "─" * 60)
    print("  MONTHLY ANOMALY TREND")
    print("─" * 60)
    monthly_df = monthly_anomaly_trend(pg)
    if monthly_df.empty:
        print("  (no data)")
    else:
        print(monthly_df.to_string(index=False))

    print("\n" + "─" * 60)
    print("  SECTOR ANOMALY SCORE  (composite)")
    print("─" * 60)
    score_df = sector_anomaly_score(pg)
    if score_df.empty:
        print("  (no data)")
    else:
        print(score_df.to_string(index=False))

    print()
    return sector_df, ticker_df


def step_plot(pg):
    """Generate anomaly timeline charts for all tickers → charts/ dir."""
    import os
    from surveillance.config import TICKERS

    logger.info("=" * 60)
    logger.info("STEP 5 – Generating anomaly plots")
    logger.info("=" * 60)

    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "charts")
    os.makedirs(out_dir, exist_ok=True)

    for ticker in TICKERS:
        df = fetch_stock_timeseries(pg, ticker)
        if df.empty:
            logger.warning("No data for %s – skipping plot", ticker)
            continue
        png = os.path.join(out_dir, f"{ticker}_anomalies.png")
        plot_anomaly_timeline(df, ticker, save_path=png, show=False)
        logger.info("Saved %s", png)

    logger.info("All charts saved to %s/", out_dir)


# ---------------------------------------------------------------------------
#  CLI entry-point
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Distributed Market Surveillance Pipeline",
    )
    parser.add_argument(
        "--step",
        choices=["ingest", "anomaly", "etl", "olap", "plot", "all"],
        default="all",
        help="Run a single pipeline step (default: all)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)

    # Always bootstrap – it's idempotent
    cass, pg, cass_mgr = step_bootstrap()

    try:
        steps = {
            "ingest":  lambda: step_ingest(cass),
            "anomaly": lambda: step_anomaly(cass),
            "etl":     lambda: step_etl(cass, pg),
            "olap":    lambda: step_olap(pg),
            "plot":    lambda: step_plot(pg),
        }

        if args.step == "all":
            for name, fn in steps.items():
                fn()
        else:
            steps[args.step]()

        logger.info("Pipeline complete ✓")
    finally:
        # Graceful shutdown – always release database connections
        cass_mgr.shutdown()
        pg.close()
        logger.info("Database connections closed")


if __name__ == "__main__":
    main()
