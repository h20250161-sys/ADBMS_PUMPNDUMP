"""
OLAP query helpers against the PostgreSQL star-schema warehouse.

Tables
------
``dim_stock``              – stock dimension (stock_key, ticker, sector …)
``dim_time``               – time dimension  (time_key, full_ts, year …)
``fact_market_metrics``    – partitioned fact table (metric_id, stock_key,
                             time_key, metric_ts, …)

Each public function executes a single analytical query and returns the
result as a ``pandas.DataFrame`` for easy consumption or display.
"""

from __future__ import annotations

import logging

import pandas as pd
from psycopg2.extensions import connection as PgConnection

from surveillance.config import PND_SUSPECTS

logger = logging.getLogger(__name__)


def sector_avg_anomaly(pg: PgConnection) -> pd.DataFrame:
    """
    Sector-wise average anomaly metrics.

    Returns a DataFrame with columns:
        sector, avg_z_score, avg_volatility, avg_volume_spike,
        avg_price_change_pct, anomaly_count, total_rows
    """
    query = """
        SELECT
            ds.sector,
            ROUND(AVG(fm.z_score)::NUMERIC, 4)              AS avg_z_score,
            ROUND(AVG(fm.volatility)::NUMERIC, 6)            AS avg_volatility,
            ROUND(AVG(fm.volume_spike)::NUMERIC, 4)          AS avg_volume_spike,
            ROUND(AVG(fm.price_change_pct)::NUMERIC, 4)      AS avg_price_change_pct,
            SUM(CASE WHEN fm.is_anomaly THEN 1 ELSE 0 END)   AS anomaly_count,
            COUNT(*)                                          AS total_rows
        FROM fact_market_metrics fm
        JOIN dim_stock ds ON ds.stock_key = fm.stock_key
        GROUP BY ds.sector
        ORDER BY avg_z_score DESC;
    """
    df = pd.read_sql_query(query, pg)
    logger.info("OLAP: sector_avg_anomaly returned %d rows", len(df))
    return df


def ticker_anomaly_summary(pg: PgConnection) -> pd.DataFrame:
    """
    Per-ticker anomaly summary (useful for drill-down).

    Returns: ticker, sector, avg_z_score, max_abs_z_score,
             avg_volume_spike, avg_price_change_pct, anomaly_pct
    """
    query = """
        SELECT
            ds.ticker,
            ds.sector,
            ROUND(AVG(fm.z_score)::NUMERIC, 4)                         AS avg_z_score,
            ROUND(MAX(ABS(fm.z_score))::NUMERIC, 4)                    AS max_abs_z_score,
            ROUND(AVG(fm.volume_spike)::NUMERIC, 4)                    AS avg_volume_spike,
            ROUND(AVG(fm.price_change_pct)::NUMERIC, 4)                AS avg_price_change_pct,
            ROUND(
                100.0 * SUM(CASE WHEN fm.is_anomaly THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(*), 0), 2
            )                                                           AS anomaly_pct
        FROM fact_market_metrics fm
        JOIN dim_stock ds ON ds.stock_key = fm.stock_key
        GROUP BY ds.ticker, ds.sector
        ORDER BY anomaly_pct DESC;
    """
    df = pd.read_sql_query(query, pg)
    logger.info("OLAP: ticker_anomaly_summary returned %d rows", len(df))
    return df


def hourly_anomaly_heatmap(pg: PgConnection) -> pd.DataFrame:
    """
    Hour-of-day breakdown of anomaly counts – useful for spotting
    time-of-day effects.

    Returns: hour, anomaly_count, total_rows, anomaly_pct, avg_volume_spike
    """
    query = """
        SELECT
            dt.hour,
            SUM(CASE WHEN fm.is_anomaly THEN 1 ELSE 0 END) AS anomaly_count,
            COUNT(*)                                        AS total_rows,
            ROUND(
                100.0 * SUM(CASE WHEN fm.is_anomaly THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(*), 0), 2
            )                                               AS anomaly_pct,
            ROUND(AVG(fm.volume_spike)::NUMERIC, 4)         AS avg_volume_spike
        FROM fact_market_metrics fm
        JOIN dim_time dt ON dt.time_key = fm.time_key
        GROUP BY dt.hour
        ORDER BY dt.hour;
    """
    df = pd.read_sql_query(query, pg)
    logger.info("OLAP: hourly_anomaly_heatmap returned %d rows", len(df))
    return df


def monthly_anomaly_trend(pg: PgConnection) -> pd.DataFrame:
    """
    Monthly anomaly trend – leverages the partitioned fact table for
    efficient month-level aggregation.

    Returns: year, month, avg_z_score, avg_volatility,
             anomaly_count, total_rows, anomaly_pct
    """
    query = """
        SELECT
            dt.year,
            dt.month,
            ROUND(AVG(fm.z_score)::NUMERIC, 4)             AS avg_z_score,
            ROUND(AVG(fm.volatility)::NUMERIC, 6)           AS avg_volatility,
            SUM(CASE WHEN fm.is_anomaly THEN 1 ELSE 0 END) AS anomaly_count,
            COUNT(*)                                        AS total_rows,
            ROUND(
                100.0 * SUM(CASE WHEN fm.is_anomaly THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(*), 0), 2
            )                                               AS anomaly_pct
        FROM fact_market_metrics fm
        JOIN dim_time dt ON dt.time_key = fm.time_key
        GROUP BY dt.year, dt.month
        ORDER BY dt.year, dt.month;
    """
    df = pd.read_sql_query(query, pg)
    logger.info("OLAP: monthly_anomaly_trend returned %d rows", len(df))
    return df


def sector_anomaly_score(pg: PgConnection) -> pd.DataFrame:
    """
    Composite **anomaly score** per sector.

    The score is a weighted combination of the available metrics::

        anomaly_score = 0.4 × avg(|z_score|)
                      + 0.3 × avg(volatility) × 100   -- scale to ~[0, N]
                      + 0.2 × avg(volume_spike)
                      + 0.1 × avg(|price_change_pct|)

    Higher values indicate sectors with more anomalous behaviour on
    average.  The weighting heavily penalises extreme volume z-scores
    and elevated volatility — the two strongest pump-and-dump signals.

    Returns a DataFrame with columns:
        sector, avg_abs_z_score, avg_volatility, avg_volume_spike,
        avg_abs_price_change_pct, anomaly_score, anomaly_count, total_rows
    """
    query = """
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
    df = pd.read_sql_query(query, pg)
    logger.info("OLAP: sector_anomaly_score returned %d rows", len(df))
    return df


# =========================================================================
#  Pump-and-Dump specific queries
# =========================================================================

def pump_and_dump_ranking(pg: PgConnection) -> pd.DataFrame:
    """
    Rank every ticker by a **pump-and-dump likelihood score**.

    The score combines the strongest P&D signals::

        pnd_score =  0.30 × avg(|z_score|)
                   + 0.25 × max(|z_score|)
                   + 0.20 × avg(volume_spike)
                   + 0.15 × max(volume_spike)
                   + 0.10 × avg(|price_change_pct|)

    Tickers that appear in the ``PND_SUSPECTS`` list get an additional
    ``is_known_suspect`` boolean flag.

    Returns columns:
        ticker, sector, company_name, avg_abs_z, max_abs_z,
        avg_vol_spike, max_vol_spike, avg_abs_price_chg,
        anomaly_pct, pnd_score, total_rows, anomaly_count,
        is_known_suspect
    """
    suspect_list = ",".join(f"'{t}'" for t in PND_SUSPECTS)

    query = f"""
        SELECT
            ds.ticker,
            ds.sector,
            ds.company_name,
            ROUND(AVG(ABS(fm.z_score))::NUMERIC, 4)           AS avg_abs_z,
            ROUND(MAX(ABS(fm.z_score))::NUMERIC, 4)            AS max_abs_z,
            ROUND(AVG(fm.volume_spike)::NUMERIC, 4)            AS avg_vol_spike,
            ROUND(MAX(fm.volume_spike)::NUMERIC, 4)            AS max_vol_spike,
            ROUND(AVG(ABS(fm.price_change_pct))::NUMERIC, 4)  AS avg_abs_price_chg,
            ROUND(
                100.0 * SUM(CASE WHEN fm.is_anomaly THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(*), 0), 2
            )                                                   AS anomaly_pct,
            ROUND((
                0.30 * AVG(ABS(fm.z_score))
              + 0.25 * MAX(ABS(fm.z_score))
              + 0.20 * AVG(fm.volume_spike)
              + 0.15 * MAX(fm.volume_spike)
              + 0.10 * AVG(ABS(fm.price_change_pct))
            )::NUMERIC, 4)                                      AS pnd_score,
            COUNT(*)                                            AS total_rows,
            SUM(CASE WHEN fm.is_anomaly THEN 1 ELSE 0 END)    AS anomaly_count,
            ds.ticker IN ({suspect_list})                       AS is_known_suspect
        FROM fact_market_metrics fm
        JOIN dim_stock ds ON ds.stock_key = fm.stock_key
        GROUP BY ds.ticker, ds.sector, ds.company_name
        ORDER BY pnd_score DESC;
    """
    df = pd.read_sql_query(query, pg)
    logger.info("OLAP: pump_and_dump_ranking returned %d rows", len(df))
    return df


def pnd_suspects_detail(pg: PgConnection) -> pd.DataFrame:
    """
    Focused view on **known P&D suspect** tickers only.

    Returns the same columns as ``pump_and_dump_ranking`` but filtered
    to the ``PND_SUSPECTS`` list, plus a ``risk_level`` label
    (CRITICAL / HIGH / MEDIUM / LOW) derived from thresholds on
    ``pnd_score``.
    """
    suspect_list = ",".join(f"'{t}'" for t in PND_SUSPECTS)

    query = f"""
        SELECT
            ds.ticker,
            ds.sector,
            ds.company_name,
            ROUND(AVG(ABS(fm.z_score))::NUMERIC, 4)           AS avg_abs_z,
            ROUND(MAX(ABS(fm.z_score))::NUMERIC, 4)            AS max_abs_z,
            ROUND(AVG(fm.volume_spike)::NUMERIC, 4)            AS avg_vol_spike,
            ROUND(MAX(fm.volume_spike)::NUMERIC, 4)            AS max_vol_spike,
            ROUND(AVG(ABS(fm.price_change_pct))::NUMERIC, 4)  AS avg_abs_price_chg,
            ROUND(
                100.0 * SUM(CASE WHEN fm.is_anomaly THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(*), 0), 2
            )                                                   AS anomaly_pct,
            ROUND((
                0.30 * AVG(ABS(fm.z_score))
              + 0.25 * MAX(ABS(fm.z_score))
              + 0.20 * AVG(fm.volume_spike)
              + 0.15 * MAX(fm.volume_spike)
              + 0.10 * AVG(ABS(fm.price_change_pct))
            )::NUMERIC, 4)                                      AS pnd_score,
            COUNT(*)                                            AS total_rows,
            SUM(CASE WHEN fm.is_anomaly THEN 1 ELSE 0 END)    AS anomaly_count,
            CASE
                WHEN (
                    0.30 * AVG(ABS(fm.z_score))
                  + 0.25 * MAX(ABS(fm.z_score))
                  + 0.20 * AVG(fm.volume_spike)
                  + 0.15 * MAX(fm.volume_spike)
                  + 0.10 * AVG(ABS(fm.price_change_pct))
                ) >= 5   THEN 'CRITICAL'
                WHEN (
                    0.30 * AVG(ABS(fm.z_score))
                  + 0.25 * MAX(ABS(fm.z_score))
                  + 0.20 * AVG(fm.volume_spike)
                  + 0.15 * MAX(fm.volume_spike)
                  + 0.10 * AVG(ABS(fm.price_change_pct))
                ) >= 3   THEN 'HIGH'
                WHEN (
                    0.30 * AVG(ABS(fm.z_score))
                  + 0.25 * MAX(ABS(fm.z_score))
                  + 0.20 * AVG(fm.volume_spike)
                  + 0.15 * MAX(fm.volume_spike)
                  + 0.10 * AVG(ABS(fm.price_change_pct))
                ) >= 1.5 THEN 'MEDIUM'
                ELSE 'LOW'
            END                                                 AS risk_level
        FROM fact_market_metrics fm
        JOIN dim_stock ds ON ds.stock_key = fm.stock_key
        WHERE ds.ticker IN ({suspect_list})
        GROUP BY ds.ticker, ds.sector, ds.company_name
        ORDER BY pnd_score DESC;
    """
    df = pd.read_sql_query(query, pg)
    logger.info("OLAP: pnd_suspects_detail returned %d rows", len(df))
    return df
