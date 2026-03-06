"""
ETL module: Cassandra ``anomaly_metric`` → PostgreSQL star-schema warehouse.

Pipeline per stock
------------------
1. **Extract**   – read ``anomaly_metric`` rows from Cassandra.
2. **Transform** – derive dimension keys (``DimStock``, ``DimTime``),
   derive ``is_anomaly`` flag, cast types.
3. **Load**      – ensure the target monthly partition exists, then
   batch-upsert into ``fact_market_metrics``.

Monthly partitioning
~~~~~~~~~~~~~~~~~~~~
``fact_market_metrics`` is partitioned by ``RANGE (metric_ts)``.
Before inserting rows for a given month the ETL calls
``ensure_monthly_partition(conn, year, month)`` so that the child
table exists.  A default partition catches any stragglers.

Error handling
~~~~~~~~~~~~~~
Each ticker is processed in its own transaction.  If one fails the
others continue unaffected.  An ``ETLResult`` dataclass captures
per-stock outcomes.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import timezone
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
from cassandra.cluster import Session as CassSession
from psycopg2.extensions import connection as PgConnection

from surveillance.config import TICKERS, TICKER_SECTOR_MAP, anomaly_params
from surveillance.db_setup import ensure_monthly_partition

logger = logging.getLogger(__name__)

_PG_BATCH_SIZE = 200          # rows per executemany chunk
_COMPANY_NAMES: Dict[str, str] = {
    # ── Blue-chip / reference ─────────────────────────────────────────
    "AAPL":    "Apple Inc.",
    "MSFT":    "Microsoft Corp.",
    "GOOGL":   "Alphabet Inc.",
    "AMZN":    "Amazon.com Inc.",
    "TSLA":    "Tesla Inc.",
    "JPM":     "JPMorgan Chase & Co.",
    "BAC":     "Bank of America Corp.",
    "JNJ":     "Johnson & Johnson",
    "PFE":     "Pfizer Inc.",
    "XOM":     "Exxon Mobil Corp.",
    # ── Meme / pump-and-dump stocks ───────────────────────────────────
    "GME":     "GameStop Corp.",
    "AMC":     "AMC Entertainment Holdings",
    "BBBY":    "Bed Bath & Beyond Inc.",
    "BB":      "BlackBerry Ltd.",
    "NOK":     "Nokia Corp.",
    "CLOV":    "Clover Health Investments",
    "WISH":    "ContextLogic Inc.",
    "WKHS":    "Workhorse Group Inc.",
    "SPCE":    "Virgin Galactic Holdings",
    "PLTR":    "Palantir Technologies Inc.",
    "SOFI":    "SoFi Technologies Inc.",
    # ── Penny stocks / micro-caps ─────────────────────────────────────
    "SNDL":    "Sundial Growers Inc.",
    "NAKD":    "Cenntro Electric Group",
    "RIDE":    "Lordstown Motors Corp.",
    "NKLA":    "Nikola Corp.",
    "MVIS":    "MicroVision Inc.",
    "CPRX":    "Catalyst Pharmaceuticals Inc.",
    "GSAT":    "Globalstar Inc.",
    "TELL":    "Tellurian Inc.",
    "SKLZ":    "Skillz Inc.",
    # ── Crypto mining / blockchain ────────────────────────────────────
    "MARA":    "Marathon Digital Holdings",
    "RIOT":    "Riot Platforms Inc.",
    "COIN":    "Coinbase Global Inc.",
    "HUT":     "Hut 8 Mining Corp.",
    # ── Cryptocurrency coins ──────────────────────────────────────────
    "BTC-USD":  "Bitcoin",
    "ETH-USD":  "Ethereum",
    "DOGE-USD": "Dogecoin",
    "SHIB-USD": "Shiba Inu",
    "SOL-USD":  "Solana",
    "XRP-USD":  "Ripple XRP",
    "ADA-USD":  "Cardano",
    "PEPE-USD": "Pepe",
    "FLOKI-USD":"Floki",
    "BONK-USD": "Bonk",
}


# =========================================================================
#  Result container
# =========================================================================

@dataclass
class ETLResult:
    """Outcome of ETL for a single stock."""
    stock_id: str
    rows_extracted: int = 0
    rows_loaded: int = 0
    partitions_created: int = 0
    elapsed_sec: float = 0.0
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None


# =========================================================================
#  1. EXTRACT – Cassandra anomaly_metric reader
# =========================================================================

def extract_anomaly_metrics(
    session: CassSession,
    stock_id: str,
) -> pd.DataFrame:
    """
    Read all ``anomaly_metric`` rows for *stock_id* from Cassandra.

    Returns a DataFrame with columns::

        ts  volatility  z_score  volume_spike  price_change_pct

    sorted by ``ts`` ascending, with UTC-aware timestamps.

    Raises
    ------
    RuntimeError
        If bucket discovery or all reads fail.
    """
    try:
        bucket_rows = session.execute(
            "SELECT DISTINCT stock_id, date_bucket FROM anomaly_metric"
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed to discover anomaly buckets for {stock_id}: {exc}"
        ) from exc

    buckets = [r.date_bucket for r in bucket_rows if r.stock_id == stock_id]
    if not buckets:
        raise RuntimeError(f"No anomaly buckets found for {stock_id}")

    frames: list[pd.DataFrame] = []
    for bucket in sorted(buckets):
        try:
            rows = session.execute(
                "SELECT timestamp, volatility, z_score, volume_spike, "
                "       price_change_pct "
                "FROM anomaly_metric "
                "WHERE stock_id = %s AND date_bucket = %s",
                (stock_id, bucket),
            )
            data = [
                (r.timestamp, r.volatility, r.z_score,
                 r.volume_spike, r.price_change_pct)
                for r in rows
            ]
            if data:
                frames.append(pd.DataFrame(
                    data,
                    columns=["ts", "volatility", "z_score",
                             "volume_spike", "price_change_pct"],
                ))
        except Exception as exc:
            logger.warning(
                "Failed to read anomaly bucket %s/%s – skipping: %s",
                stock_id, bucket, exc,
            )

    if not frames:
        raise RuntimeError(
            f"All anomaly bucket reads failed or returned 0 rows for {stock_id}"
        )

    df = pd.concat(frames, ignore_index=True)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.sort_values("ts", inplace=True)
    logger.info("Extracted %d anomaly rows for %s from Cassandra",
                len(df), stock_id)
    return df


# =========================================================================
#  2. TRANSFORM – derive dimension keys + anomaly flag
# =========================================================================

def _ensure_dim_stock(cur, ticker: str) -> int:
    """Insert-or-fetch the ``dim_stock`` surrogate key."""
    cur.execute(
        "SELECT stock_key FROM dim_stock WHERE ticker = %s", (ticker,)
    )
    row = cur.fetchone()
    if row:
        return row[0]

    sector = TICKER_SECTOR_MAP.get(ticker, "Unknown")
    company = _COMPANY_NAMES.get(ticker, "")
    cur.execute(
        "INSERT INTO dim_stock (ticker, company_name, sector) "
        "VALUES (%s, %s, %s) RETURNING stock_key",
        (ticker, company, sector),
    )
    return cur.fetchone()[0]


def _ensure_dim_time(cur, ts: pd.Timestamp) -> int:
    """Insert-or-fetch the ``dim_time`` surrogate key."""
    py_ts = ts.to_pydatetime().astimezone(timezone.utc).replace(tzinfo=None)

    cur.execute(
        "SELECT time_key FROM dim_time WHERE full_ts = %s", (py_ts,)
    )
    row = cur.fetchone()
    if row:
        return row[0]

    dow = py_ts.weekday()          # 0=Mon … 6=Sun
    is_weekend = dow >= 5
    quarter = (py_ts.month - 1) // 3 + 1

    cur.execute(
        """
        INSERT INTO dim_time
            (full_ts, year, quarter, month, day, hour, minute,
             day_of_week, is_weekend)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING time_key
        """,
        (py_ts, py_ts.year, quarter, py_ts.month, py_ts.day,
         py_ts.hour, py_ts.minute, dow, is_weekend),
    )
    return cur.fetchone()[0]


def transform_rows(
    df: pd.DataFrame,
    cur,
    stock_key: int,
) -> List[Tuple]:
    """
    Convert the extracted DataFrame into a list of tuples ready for
    ``fact_market_metrics`` INSERT.

    Each tuple:
        (stock_key, time_key, metric_ts, price, volume,
         volatility, z_score, volume_spike, price_change_pct, is_anomaly)
    """
    rows: List[Tuple] = []
    for _, r in df.iterrows():
        time_key = _ensure_dim_time(cur, r["ts"])
        metric_ts = r["ts"].to_pydatetime().astimezone(timezone.utc).replace(tzinfo=None)

        is_anomaly = (
            float(r["z_score"]) > anomaly_params.zscore_threshold
            or float(r["volatility"]) > anomaly_params.volatility_threshold
        )

        rows.append((
            stock_key,
            time_key,
            metric_ts,
            None,                                   # price (not in anomaly_metric)
            None,                                   # volume (not in anomaly_metric)
            float(r["volatility"]),
            float(r["z_score"]),
            float(r["volume_spike"]),
            float(r.get("price_change_pct") or 0.0),
            is_anomaly,
        ))
    return rows


# =========================================================================
#  3. LOAD – batch upsert into FactMarketMetrics (with partition routing)
# =========================================================================

def _discover_months(rows: List[Tuple]) -> Set[Tuple[int, int]]:
    """Return the distinct (year, month) pairs present in *rows*."""
    months: Set[Tuple[int, int]] = set()
    for row in rows:
        ts = row[2]  # metric_ts
        months.add((ts.year, ts.month))
    return months


def load_fact_rows(
    conn: PgConnection,
    rows: List[Tuple],
) -> int:
    """
    Batch-upsert *rows* into ``fact_market_metrics``.

    * Ensures all required monthly partitions exist **before** inserting.
    * Uses ``executemany`` in chunks of ``_PG_BATCH_SIZE``.
    * ``ON CONFLICT (stock_key, time_key, metric_ts) DO UPDATE`` so
      re-runs are idempotent.

    Returns the number of rows upserted.
    """
    if not rows:
        return 0

    # Ensure partitions
    months = _discover_months(rows)
    for year, month in sorted(months):
        ensure_monthly_partition(conn, year, month)

    cur = conn.cursor()
    upsert_sql = """
        INSERT INTO fact_market_metrics
            (stock_key, time_key, metric_ts, price, volume,
             volatility, z_score, volume_spike, price_change_pct, is_anomaly)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (stock_key, time_key, metric_ts) DO UPDATE SET
            price            = EXCLUDED.price,
            volume           = EXCLUDED.volume,
            volatility       = EXCLUDED.volatility,
            z_score          = EXCLUDED.z_score,
            volume_spike     = EXCLUDED.volume_spike,
            price_change_pct = EXCLUDED.price_change_pct,
            is_anomaly       = EXCLUDED.is_anomaly
    """

    loaded = 0
    for i in range(0, len(rows), _PG_BATCH_SIZE):
        chunk = rows[i : i + _PG_BATCH_SIZE]
        cur.executemany(upsert_sql, chunk)
        loaded += len(chunk)

    cur.close()
    conn.commit()
    return loaded


# =========================================================================
#  Public API
# =========================================================================

def etl_ticker(
    cass: CassSession,
    pg: PgConnection,
    ticker: str,
) -> ETLResult:
    """
    Full ETL pipeline for **one** stock:  extract → transform → load.

    Never raises – errors are captured in the returned ``ETLResult``.
    """
    t0 = time.monotonic()
    result = ETLResult(stock_id=ticker)

    # ── extract ────────────────────────────────────────────────────────
    try:
        df = extract_anomaly_metrics(cass, ticker)
    except RuntimeError as exc:
        result.error = str(exc)
        result.elapsed_sec = time.monotonic() - t0
        logger.error("ETL extract failed for %s: %s", ticker, exc)
        return result

    result.rows_extracted = len(df)

    # ── transform ──────────────────────────────────────────────────────
    try:
        cur = pg.cursor()
        stock_key = _ensure_dim_stock(cur, ticker)
        cur.close()
        pg.commit()

        cur = pg.cursor()
        rows = transform_rows(df, cur, stock_key)
        cur.close()
        pg.commit()
    except Exception as exc:
        result.error = f"Transform failed: {exc}"
        result.elapsed_sec = time.monotonic() - t0
        logger.error("ETL transform failed for %s: %s", ticker, exc)
        return result

    # ── load ───────────────────────────────────────────────────────────
    try:
        months_before = len(_discover_months(rows))
        loaded = load_fact_rows(pg, rows)
        result.rows_loaded = loaded
        result.partitions_created = months_before   # upper bound
    except Exception as exc:
        result.error = f"Load failed: {exc}"
        result.elapsed_sec = time.monotonic() - t0
        logger.error("ETL load failed for %s: %s", ticker, exc)
        return result

    result.elapsed_sec = time.monotonic() - t0
    logger.info(
        "ETL %s: extracted=%d  loaded=%d  months=%d  (%.1fs)",
        ticker, result.rows_extracted, result.rows_loaded,
        result.partitions_created, result.elapsed_sec,
    )
    return result


def etl_all(
    cass: CassSession,
    pg: PgConnection,
    tickers: Optional[List[str]] = None,
) -> int:
    """
    Run ETL for every ticker in the universe.

    Returns total rows successfully loaded.  Individual ticker failures
    are logged but do **not** abort remaining tickers.
    """
    tickers = tickers or TICKERS
    results: List[ETLResult] = []

    for tkr in tickers:
        results.append(etl_ticker(cass, pg, tkr))

    total_loaded = sum(r.rows_loaded for r in results)
    errors = [r for r in results if not r.ok]

    logger.info(
        "ETL complete – %d rows loaded across %d tickers (%d had errors)",
        total_loaded, len(tickers), len(errors),
    )
    for err in errors:
        logger.warning("  ✗ %s → %s", err.stock_id, err.error)

    return total_loaded
