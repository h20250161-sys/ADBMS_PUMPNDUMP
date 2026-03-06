"""
Anomaly-detection module.

Pipeline (per stock)
--------------------
1. **Read** minute-level tick data from Cassandra ``market_tick``.
2. **Compute** three metrics with *pandas* + *numpy*:
   • Rolling volatility  – std of log-returns over a configurable window.
   • Volume z-score      – (volume − rolling_mean) / rolling_std.
   • Price change %      – percentage change in price vs previous bar.
3. **Flag** anomaly when:
        ``z_score > threshold``  **OR**  ``volatility > volatility_threshold``
4. **Insert** results into Cassandra ``anomaly_metric`` using
   partition-aware batching with retry + exponential back-off.

Error handling
~~~~~~~~~~~~~~
Every I/O boundary (Cassandra read, compute, Cassandra write) is wrapped in
``try / except``.  A ``DetectionResult`` dataclass captures per-stock
outcomes so that one ticker failure never aborts the rest of the universe.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import timezone
from typing import List, Optional

import numpy as np
import pandas as pd
from cassandra.cluster import Session as CassSession
from cassandra.query import BatchStatement, ConsistencyLevel

from surveillance.config import TICKERS, anomaly_params

logger = logging.getLogger(__name__)

_BATCH_SIZE = 50
_BATCH_RETRIES = 3


# =========================================================================
#  Result container
# =========================================================================

@dataclass
class DetectionResult:
    """Outcome of anomaly detection for a single stock."""
    stock_id: str
    rows_read: int = 0
    metrics_computed: int = 0
    rows_written: int = 0
    rows_failed: int = 0
    anomalies_found: int = 0
    elapsed_sec: float = 0.0
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.rows_failed == 0


# =========================================================================
#  Helpers
# =========================================================================

def _date_bucket(ts) -> str:
    """Derive ``YYYY_MM_DD`` bucket from a datetime-like object."""
    return ts.strftime("%Y_%m_%d")


# =========================================================================
#  Step 1 – Read stock minute data from Cassandra
# =========================================================================

def read_ticks_from_cassandra(
    session: CassSession,
    stock_id: str,
) -> pd.DataFrame:
    """
    Fetch all ``market_tick`` rows for *stock_id* across every date bucket.

    Returns a DataFrame with columns ``price, volume`` indexed by a
    UTC-aware ``DatetimeIndex`` named *timestamp*, sorted ascending.

    Raises
    ------
    RuntimeError
        If no data is found or a query fails.
    """
    try:
        bucket_rows = session.execute(
            "SELECT DISTINCT stock_id, date_bucket FROM market_tick"
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed to discover date buckets for {stock_id}: {exc}"
        ) from exc

    buckets = [r.date_bucket for r in bucket_rows if r.stock_id == stock_id]
    if not buckets:
        raise RuntimeError(f"No date buckets found for {stock_id}")

    frames: list[pd.DataFrame] = []
    for bucket in sorted(buckets):
        try:
            rows = session.execute(
                "SELECT timestamp, price, volume "
                "FROM market_tick "
                "WHERE stock_id = %s AND date_bucket = %s",
                (stock_id, bucket),
            )
            data = [(r.timestamp, r.price, r.volume) for r in rows]
            if data:
                frames.append(
                    pd.DataFrame(data, columns=["timestamp", "price", "volume"])
                )
        except Exception as exc:
            logger.warning(
                "Failed to read bucket %s/%s – skipping: %s",
                stock_id, bucket, exc,
            )

    if not frames:
        raise RuntimeError(
            f"All bucket reads failed or returned 0 rows for {stock_id}"
        )

    df = pd.concat(frames, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df.set_index("timestamp", inplace=True)
    df.sort_index(inplace=True)

    logger.info("Read %d tick rows for %s from Cassandra", len(df), stock_id)
    return df


# =========================================================================
#  Step 2 – Compute metrics  (pandas + numpy)
# =========================================================================

def compute_anomaly_metrics(
    df: pd.DataFrame,
    window: int = anomaly_params.rolling_window,
) -> pd.DataFrame:
    """
    Compute anomaly metrics from a price/volume DataFrame.

    Parameters
    ----------
    df : DataFrame
        Must contain ``price`` (float) and ``volume`` (int/float) columns,
        indexed by timestamp.
    window : int
        Rolling window size (default from config).

    Returns
    -------
    DataFrame with columns:
        ``volatility, z_score, volume_spike, price_change_pct, is_anomaly``
    indexed by the same timestamp (rows with insufficient history are
    dropped).

    Formulae
    --------
    * **Rolling volatility** = rolling_std(log_return, window)
    * **Volume z-score**     = (volume − rolling_mean_vol) / rolling_std_vol
    * **Price change %**     = (price / price_prev − 1) × 100
    * **is_anomaly**         = z_score > threshold  OR
                               volatility > volatility_threshold
    """
    min_periods = max(1, window // 2)

    out = df[["price", "volume"]].copy()

    # ── price-based metrics ────────────────────────────────────────────
    # Log returns → rolling volatility
    out["log_return"] = np.log(out["price"] / out["price"].shift(1))

    out["volatility"] = (
        out["log_return"]
        .rolling(window=window, min_periods=min_periods)
        .std()
    )

    # Price change percentage (bar-over-bar)
    out["price_change_pct"] = out["price"].pct_change() * 100.0

    # ── volume z-score ─────────────────────────────────────────────────
    rolling_vol_mean = (
        out["volume"]
        .rolling(window=window, min_periods=min_periods)
        .mean()
    )
    rolling_vol_std = (
        out["volume"]
        .rolling(window=window, min_periods=min_periods)
        .std()
    )
    # Avoid division by zero: where std == 0, z_score = 0
    out["z_score"] = np.where(
        rolling_vol_std == 0,
        0.0,
        (out["volume"] - rolling_vol_mean) / rolling_vol_std,
    )

    # Keep volume_spike for backward compat with warehouse_etl / OLAP
    out["volume_spike"] = np.where(
        rolling_vol_mean == 0,
        0.0,
        out["volume"] / rolling_vol_mean,
    )

    # ── anomaly flag ───────────────────────────────────────────────────
    # Flag if volume z-score exceeds threshold  OR  volatility is extreme
    out["is_anomaly"] = (
        (out["z_score"] > anomaly_params.zscore_threshold) |
        (out["volatility"] > anomaly_params.volatility_threshold)
    )

    # ── clean-up ───────────────────────────────────────────────────────
    out.drop(columns=["log_return", "price", "volume"], inplace=True)
    out.dropna(inplace=True)

    logger.info(
        "Computed metrics: %d rows, %d anomalies (%.1f%%)",
        len(out),
        out["is_anomaly"].sum(),
        100.0 * out["is_anomaly"].mean() if len(out) else 0,
    )
    return out


# =========================================================================
#  Step 3 – Insert results into anomaly_metric  (partition-aware batching)
# =========================================================================

def store_anomaly_cassandra(
    session: CassSession,
    stock_id: str,
    metrics_df: pd.DataFrame,
) -> tuple[int, int]:
    """
    Batch-insert anomaly metrics into ``anomaly_metric``.

    **Batches are grouped by partition key** ``(stock_id, date_bucket)``
    so each CQL BATCH stays within a single Cassandra partition.

    Returns
    -------
    (rows_written, rows_failed)
    """
    if metrics_df.empty:
        return 0, 0

    insert_cql = session.prepare("""
        INSERT INTO anomaly_metric
            (stock_id, date_bucket, timestamp,
             volatility, z_score, volume_spike, price_change_pct)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    # Build a flat frame with stock_id / date_bucket for groupby
    work = metrics_df.copy()
    work["stock_id"] = stock_id
    work["date_bucket"] = [
        _date_bucket(ts.to_pydatetime().astimezone(timezone.utc))
        for ts in work.index
    ]
    work["py_ts"] = [
        ts.to_pydatetime().astimezone(timezone.utc) for ts in work.index
    ]

    total_written = 0
    total_failed = 0

    for (_, bucket), part in work.groupby(
        ["stock_id", "date_bucket"], sort=False,
    ):
        batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
        batch_size = 0

        for _, row in part.iterrows():
            batch.add(insert_cql, (
                stock_id,
                bucket,
                row["py_ts"],
                float(row["volatility"]),
                float(row["z_score"]),
                float(row["volume_spike"]),
                float(row["price_change_pct"]),
            ))
            batch_size += 1

            if batch_size >= _BATCH_SIZE:
                w, f = _execute_batch_with_retry(
                    session, batch, batch_size, stock_id, bucket,
                )
                total_written += w
                total_failed += f
                batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
                batch_size = 0

        if batch_size > 0:
            w, f = _execute_batch_with_retry(
                session, batch, batch_size, stock_id, bucket,
            )
            total_written += w
            total_failed += f

    return total_written, total_failed


def _execute_batch_with_retry(
    session: CassSession,
    batch: BatchStatement,
    size: int,
    stock_id: str,
    bucket: str,
) -> tuple[int, int]:
    """Execute *batch* with exponential back-off.  Returns (written, failed)."""
    for attempt in range(1, _BATCH_RETRIES + 1):
        try:
            session.execute(batch)
            logger.debug(
                "Anomaly batch OK  %s/%s  (%d rows, attempt %d)",
                stock_id, bucket, size, attempt,
            )
            return size, 0
        except Exception as exc:
            wait = 2 ** (attempt - 1)
            logger.warning(
                "Anomaly batch FAIL  %s/%s  (%d rows, attempt %d/%d): %s "
                "— retrying in %ds",
                stock_id, bucket, size, attempt, _BATCH_RETRIES, exc, wait,
            )
            time.sleep(wait)

    logger.error(
        "Anomaly batch ABANDONED  %s/%s  (%d rows) after %d attempts",
        stock_id, bucket, size, _BATCH_RETRIES,
    )
    return 0, size


# =========================================================================
#  Public API
# =========================================================================

def detect_anomalies(
    session: CassSession,
    stock_id: str,
    window: Optional[int] = None,
) -> DetectionResult:
    """
    Full pipeline for **one** stock:  read → compute → store.

    Never raises – all errors are captured in the returned
    ``DetectionResult``.
    """
    t0 = time.monotonic()
    window = window or anomaly_params.rolling_window
    result = DetectionResult(stock_id=stock_id)

    # ── read ───────────────────────────────────────────────────────────
    try:
        ticks = read_ticks_from_cassandra(session, stock_id)
    except RuntimeError as exc:
        result.error = str(exc)
        result.elapsed_sec = time.monotonic() - t0
        logger.error("Read failed for %s: %s", stock_id, exc)
        return result

    result.rows_read = len(ticks)

    # ── compute ────────────────────────────────────────────────────────
    try:
        metrics = compute_anomaly_metrics(ticks, window)
    except Exception as exc:
        result.error = f"Compute failed: {exc}"
        result.elapsed_sec = time.monotonic() - t0
        logger.error("Compute failed for %s: %s", stock_id, exc)
        return result

    result.metrics_computed = len(metrics)
    result.anomalies_found = int(metrics["is_anomaly"].sum())

    # ── store ──────────────────────────────────────────────────────────
    try:
        written, failed = store_anomaly_cassandra(session, stock_id, metrics)
        result.rows_written = written
        result.rows_failed = failed
    except Exception as exc:
        result.error = f"Store failed: {exc}"
        result.rows_failed = result.metrics_computed
        logger.error("Store failed for %s: %s", stock_id, exc)

    result.elapsed_sec = time.monotonic() - t0
    logger.info(
        "Anomaly %s: read=%d  computed=%d  anomalies=%d  "
        "written=%d  failed=%d  (%.1fs)",
        stock_id, result.rows_read, result.metrics_computed,
        result.anomalies_found, result.rows_written,
        result.rows_failed, result.elapsed_sec,
    )
    return result


def detect_all(
    session: CassSession,
    tickers: Optional[List[str]] = None,
    window: Optional[int] = None,
) -> int:
    """
    Run anomaly detection for every ticker in the universe.

    Returns total rows **successfully** written.  Individual ticker
    failures are logged but do **not** abort remaining tickers.
    """
    tickers = tickers or TICKERS
    results: List[DetectionResult] = []

    for tkr in tickers:
        results.append(detect_anomalies(session, tkr, window))

    total_written = sum(r.rows_written for r in results)
    total_failed = sum(r.rows_failed for r in results)
    total_anomalies = sum(r.anomalies_found for r in results)
    errors = [r for r in results if not r.ok]

    logger.info(
        "Anomaly detection complete – %d written, %d failed, "
        "%d anomalies across %d tickers (%d had errors)",
        total_written, total_failed, total_anomalies,
        len(tickers), len(errors),
    )
    for err in errors:
        logger.warning("  ✗ %s → %s", err.stock_id, err.error or "partial failure")

    return total_written
