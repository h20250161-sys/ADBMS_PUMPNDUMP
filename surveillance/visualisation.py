"""
Visualisation helpers for the Market Surveillance pipeline.

Provides reusable functions to:
    1. **Query** per-stock anomaly time-series from PostgreSQL.
    2. **Plot** time vs composite anomaly score with detected anomalies
       highlighted in red – ready for a demo presentation.

The composite anomaly score is computed in SQL so the DataFrame is
self-contained::

    anomaly_score = 0.4 × |z_score|
                  + 0.3 × volatility × 100
                  + 0.2 × volume_spike
                  + 0.1 × |price_change_pct|

Usage (standalone)::

    from surveillance.db_setup import get_pg_connection
    from surveillance.visualisation import fetch_stock_timeseries, plot_anomaly_timeline

    pg = get_pg_connection()
    df = fetch_stock_timeseries(pg, "AAPL")
    plot_anomaly_timeline(df, "AAPL", save_path="aapl_anomalies.png")
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from psycopg2.extensions import connection as PgConnection

logger = logging.getLogger(__name__)

# ── Presentation defaults ─────────────────────────────────────────────────
plt.rcParams.update({
    "figure.facecolor": "#f8f9fa",
    "axes.facecolor":   "#ffffff",
    "axes.grid":        True,
    "grid.alpha":       0.3,
    "font.size":        11,
})


# =========================================================================
#  1. QUERY – fetch time-series from PostgreSQL
# =========================================================================

STOCK_TIMESERIES_SQL = """
    SELECT
        dt.full_ts                                          AS ts,
        fm.z_score,
        fm.volatility,
        fm.volume_spike,
        fm.price_change_pct,
        ROUND((
            0.4 * ABS(fm.z_score)
          + 0.3 * fm.volatility * 100
          + 0.2 * fm.volume_spike
          + 0.1 * ABS(fm.price_change_pct)
        )::NUMERIC, 4)                                      AS anomaly_score,
        fm.is_anomaly
    FROM fact_market_metrics fm
    JOIN dim_stock ds ON ds.stock_key = fm.stock_key
    JOIN dim_time  dt ON dt.time_key  = fm.time_key
    WHERE ds.ticker = %(ticker)s
    ORDER BY dt.full_ts;
"""


def fetch_stock_timeseries(
    pg: PgConnection,
    ticker: str,
) -> pd.DataFrame:
    """
    Read the anomaly time-series for *ticker* from the warehouse.

    Returns a DataFrame indexed by ``ts`` (UTC) with columns:
        z_score, volatility, volume_spike, price_change_pct,
        anomaly_score, is_anomaly
    """
    df = pd.read_sql_query(
        STOCK_TIMESERIES_SQL,
        pg,
        params={"ticker": ticker},
        parse_dates=["ts"],
    )
    if not df.empty:
        df.set_index("ts", inplace=True)
        df.sort_index(inplace=True)
    logger.info("Fetched %d rows for %s from PostgreSQL", len(df), ticker)
    return df


# =========================================================================
#  2. PLOT – time vs anomaly score with red anomaly highlights
# =========================================================================

def plot_anomaly_timeline(
    df: pd.DataFrame,
    ticker: str,
    *,
    save_path: Optional[str | Path] = None,
    show: bool = True,
    figsize: tuple[float, float] = (14, 5),
) -> plt.Figure:
    """
    Plot anomaly score over time for a single stock.

    * Blue line  – anomaly score at every minute bar.
    * Red dots   – bars where ``is_anomaly == True``.
    * Grey band  – z-score threshold (visual reference).

    Parameters
    ----------
    df : DataFrame
        Output of :func:`fetch_stock_timeseries`.
    ticker : str
        Used in the chart title.
    save_path : str or Path, optional
        If given, save the figure to this file (PNG, PDF, SVG …).
    show : bool
        Whether to call ``plt.show()``.  Set to ``False`` when
        generating many plots in a loop.
    figsize : tuple
        Figure size in inches.

    Returns
    -------
    matplotlib.figure.Figure
    """
    fig, ax = plt.subplots(figsize=figsize)

    if df.empty:
        ax.text(
            0.5, 0.5,
            f"No data for {ticker}",
            transform=ax.transAxes, ha="center", va="center", fontsize=16,
        )
        ax.set_title(f"{ticker} – Anomaly Score Timeline")
        if save_path:
            fig.savefig(str(save_path), dpi=150, bbox_inches="tight")
        if show:
            plt.show()
        return fig

    score = df["anomaly_score"].astype(float)
    anomalies = df[df["is_anomaly"] == True]                     # noqa: E712
    normal    = df[df["is_anomaly"] == False]                    # noqa: E712

    # ── main line ─────────────────────────────────────────────────────
    ax.plot(
        score.index, score.values,
        color="#4a90d9", linewidth=0.9, alpha=0.85,
        label="Anomaly score",
    )

    # ── normal points (small, translucent) ────────────────────────────
    if not normal.empty:
        ax.scatter(
            normal.index,
            normal["anomaly_score"].astype(float),
            color="#4a90d9", s=6, alpha=0.3, zorder=3,
        )

    # ── anomaly points (large, RED) ───────────────────────────────────
    if not anomalies.empty:
        ax.scatter(
            anomalies.index,
            anomalies["anomaly_score"].astype(float),
            color="#e74c3c", s=40, edgecolors="darkred", linewidths=0.6,
            zorder=5, label=f"Anomaly ({len(anomalies)})",
        )

    # ── threshold reference line ──────────────────────────────────────
    from surveillance.config import anomaly_params
    threshold_score = 0.4 * anomaly_params.zscore_threshold
    ax.axhline(
        threshold_score, color="orange", linestyle="--",
        linewidth=1, alpha=0.7,
        label=f"z-score threshold component ({threshold_score:.2f})",
    )

    # ── cosmetics ─────────────────────────────────────────────────────
    ax.set_title(
        f"{ticker} – Anomaly Score over Time",
        fontsize=14, fontweight="bold", pad=12,
    )
    ax.set_xlabel("Time")
    ax.set_ylabel("Composite Anomaly Score")
    ax.legend(loc="upper left", fontsize=9, framealpha=0.9)

    # Readable date labels
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d\n%H:%M"))
    fig.autofmt_xdate(rotation=0, ha="center")

    fig.tight_layout()

    if save_path:
        fig.savefig(str(save_path), dpi=150, bbox_inches="tight")
        logger.info("Saved plot → %s", save_path)

    if show:
        plt.show()

    return fig
