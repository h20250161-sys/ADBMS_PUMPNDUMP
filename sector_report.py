#!/usr/bin/env python3
"""
sector_report.py – Standalone OLAP report
==========================================

Connects to PostgreSQL, computes a composite **anomaly score** per sector
from ``fact_market_metrics JOIN dim_stock``, and prints a nicely formatted
table to stdout.

Usage
-----
    python sector_report.py

The script reuses the project's centralised config (env-var overrides
supported) and the ``sector_anomaly_score`` query from the OLAP module.
It can also be run completely independently — a raw ``psycopg2`` fallback
is included in case the surveillance package is not on ``sys.path``.
"""

from __future__ import annotations

import sys
import os

# ---------------------------------------------------------------------------
#  Ensure the project root is on sys.path so `surveillance.*` imports work
#  when the script is invoked directly (python sector_report.py).
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import logging
import psycopg2
import pandas as pd

from surveillance.config import postgres_cfg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("sector_report")

# ╔═════════════════════════════════════════════════════════════════════════╗
# ║  SQL – Composite anomaly score per sector                             ║
# ╚═════════════════════════════════════════════════════════════════════════╝

SECTOR_ANOMALY_SQL = """
    SELECT
        ds.sector,
        ROUND(AVG(ABS(fm.z_score))::NUMERIC, 4)            AS avg_abs_z_score,
        ROUND(AVG(fm.volatility)::NUMERIC, 6)               AS avg_volatility,
        ROUND(AVG(fm.volume_spike)::NUMERIC, 4)             AS avg_volume_spike,
        ROUND(AVG(ABS(fm.price_change_pct))::NUMERIC, 4)   AS avg_abs_price_change_pct,
        ROUND((
            0.4 * AVG(ABS(fm.z_score))
          + 0.3 * AVG(fm.volatility) * 100
          + 0.2 * AVG(fm.volume_spike)
          + 0.1 * AVG(ABS(fm.price_change_pct))
        )::NUMERIC, 4)                                      AS anomaly_score,
        SUM(CASE WHEN fm.is_anomaly THEN 1 ELSE 0 END)     AS anomaly_count,
        COUNT(*)                                            AS total_rows
    FROM fact_market_metrics fm
    JOIN dim_stock ds ON ds.stock_key = fm.stock_key
    GROUP BY ds.sector
    ORDER BY anomaly_score DESC;
"""


# ╔═════════════════════════════════════════════════════════════════════════╗
# ║  Pretty-printer                                                       ║
# ╚═════════════════════════════════════════════════════════════════════════╝

def _fmt_table(df: pd.DataFrame) -> str:
    """
    Return a nicely formatted ASCII table string from a DataFrame.

    Uses dynamic column widths and right-aligns numeric columns.
    """
    if df.empty:
        return "  (no data – run the ETL pipeline first)\n"

    # Determine column widths
    headers = list(df.columns)
    col_widths: list[int] = []
    for col in headers:
        max_data = df[col].astype(str).str.len().max()
        col_widths.append(max(len(col), max_data) + 2)

    # Header
    sep = "+" + "+".join("-" * w for w in col_widths) + "+"
    hdr = "|" + "|".join(h.center(w) for h, w in zip(headers, col_widths)) + "|"

    lines = [sep, hdr, sep]

    # Rows
    for _, row in df.iterrows():
        cells: list[str] = []
        for col, w in zip(headers, col_widths):
            val = row[col]
            text = f"{val}" if val is not None else ""
            # Right-align numbers, left-align text
            if isinstance(val, (int, float)):
                cells.append(text.rjust(w))
            else:
                cells.append(text.ljust(w))
        lines.append("|" + "|".join(cells) + "|")

    lines.append(sep)
    return "\n".join(lines)


# ╔═════════════════════════════════════════════════════════════════════════╗
# ║  Main                                                                 ║
# ╚═════════════════════════════════════════════════════════════════════════╝

def main() -> None:
    # ── 1. Connect to PostgreSQL ──────────────────────────────────────
    logger.info("Connecting to PostgreSQL (%s @ %s:%s) …",
                postgres_cfg.dbname, postgres_cfg.host, postgres_cfg.port)

    try:
        conn = psycopg2.connect(postgres_cfg.dsn)
        conn.autocommit = True
    except psycopg2.OperationalError as exc:
        logger.error("Cannot connect to PostgreSQL: %s", exc)
        sys.exit(1)

    logger.info("Connected ✓")

    # ── 2. Execute the OLAP query ─────────────────────────────────────
    try:
        df = pd.read_sql_query(SECTOR_ANOMALY_SQL, conn)
    except Exception as exc:
        logger.error("Query failed: %s", exc)
        conn.close()
        sys.exit(1)

    logger.info("Query returned %d sector rows", len(df))

    # ── 3. Print nicely formatted result ──────────────────────────────
    print()
    print("=" * 72)
    print("   SECTOR ANOMALY SCORE REPORT")
    print("   fact_market_metrics  JOIN  dim_stock  GROUP BY sector")
    print("=" * 72)
    print()
    print(_fmt_table(df))
    print()

    # Also show which sector is most anomalous
    if not df.empty:
        top = df.iloc[0]
        print(f"  ⚠  Highest anomaly score: {top['sector']}  "
              f"(score = {top['anomaly_score']})")
        print(f"     {top['anomaly_count']} anomalies out of "
              f"{top['total_rows']} total observations")
        print()

    conn.close()
    logger.info("Connection closed – done")


if __name__ == "__main__":
    main()
