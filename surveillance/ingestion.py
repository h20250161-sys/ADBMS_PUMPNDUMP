"""
Data ingestion module  — multi-timeframe edition.

Responsibilities
----------------
1. Download OHLC bars from Yahoo Finance via *yfinance* at **multiple
   resolutions** (1-min, 5-min, 1-hour, 1-day) so the total row count
   across all tickers reaches the millions.
2. **Preprocess** each download into a clean ``pandas.DataFrame`` with an
   explicit ``date_bucket`` column (``YYYY_MM_DD``).
3. **Batch-insert** every row into the Cassandra ``market_tick`` table,
   grouping batches by partition key ``(stock_id, date_bucket)`` so that
   each CQL ``BATCH`` stays within a single partition.

yfinance data-availability limits
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    interval  |  max history
    ----------+------------------
    1m        |  7 calendar days
    5m        |  60 calendar days
    15m       |  60 calendar days
    1h        |  730 calendar days
    1d        |  unlimited ("max")

We iterate over ``FETCH_PASSES`` defined in ``config.py`` for every
ticker, accumulating hundreds of thousands of rows per ticker.

Design notes
~~~~~~~~~~~~
* The module is **idempotent** – re-inserting the same
  ``(stock_id, date_bucket, timestamp)`` row simply overwrites with the
  same values (Cassandra upsert semantics).
* Every I/O boundary (yfinance network call, Cassandra batch execute)
  is wrapped in explicit ``try / except`` so that a transient failure for
  one ticker or one batch does **not** kill the rest of the pipeline.
* A per-ticker ``IngestionResult`` dataclass is returned so callers can
  inspect successes and failures programmatically.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import yfinance as yf
from cassandra.cluster import Session as CassSession
from cassandra.query import BatchStatement, ConsistencyLevel

from surveillance.config import TICKERS, FETCH_PASSES, FetchPass, fetch_params

logger = logging.getLogger(__name__)

# Max rows per CQL unlogged batch.  Kept small to stay well under the
# default 5 MB warn threshold and to keep each batch within one partition.
_BATCH_SIZE = 50

# Retry limit for a failed batch before giving up on that chunk.
_BATCH_RETRIES = 3


# =========================================================================
#  Result container
# =========================================================================

@dataclass
class IngestionResult:
    """Outcome of ingesting one ticker."""
    stock_id: str
    rows_fetched: int = 0
    rows_written: int = 0
    rows_failed: int = 0
    elapsed_sec: float = 0.0
    error: Optional[str] = None
    passes_ok: int = 0
    passes_failed: int = 0

    @property
    def ok(self) -> bool:
        return self.error is None and self.rows_failed == 0


# =========================================================================
#  Helpers
# =========================================================================

def _date_bucket(ts: datetime) -> str:
    """Derive a ``YYYY_MM_DD`` date-bucket string from a timestamp."""
    return ts.strftime("%Y_%m_%d")


# =========================================================================
#  Step 1 – Fetch  (one pass)
# =========================================================================

def fetch_ohlc(
    ticker: str,
    period: str = "7d",
    interval: str = "1m",
) -> pd.DataFrame:
    """
    Download OHLC data for *ticker* at the given resolution.

    Returns a tidy DataFrame with columns
    ``open, high, low, close, volume`` and a **UTC-aware DatetimeIndex**.

    Raises
    ------
    RuntimeError
        If yfinance returns an empty frame (market closed, bad ticker …).
    """
    logger.info(
        "Fetching OHLC for %s  (period=%s, interval=%s)",
        ticker, period, interval,
    )

    try:
        df: pd.DataFrame = yf.download(
            tickers=ticker,
            period=period,
            interval=interval,
            progress=False,
            auto_adjust=True,
        )
    except Exception as exc:
        raise RuntimeError(
            f"yfinance download failed for {ticker}: {exc}"
        ) from exc

    if df.empty:
        raise RuntimeError(f"No data returned by yfinance for {ticker} ({period}/{interval})")

    # --- Normalise columns ---------------------------------------------
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)

    df.columns = [c.lower() for c in df.columns]

    # --- Timezone normalisation ----------------------------------------
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    logger.info("Fetched %d bars for %s (%s/%s)", len(df), ticker, period, interval)
    return df


# =========================================================================
#  Step 2 – Preprocess
# =========================================================================

def preprocess_ohlc(ticker: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Turn raw OHLC bars into the shape expected by ``market_tick``.

    Returned columns:
        stock_id, date_bucket, timestamp, price, volume
    """
    out = pd.DataFrame({
        "stock_id":    ticker,
        "date_bucket": df.index.to_series().apply(
            lambda ts: _date_bucket(ts.to_pydatetime())
        ).values,
        "timestamp":   df.index.to_series().apply(
            lambda ts: ts.to_pydatetime().astimezone(timezone.utc)
        ).values,
        "price":       df["close"].astype(float).values,
        "volume":      df["volume"].astype(int).values,
    })

    logger.info(
        "Preprocessed %d rows for %s  (%d unique date buckets)",
        len(out), ticker, out["date_bucket"].nunique(),
    )
    return out


# =========================================================================
#  Step 3 – Cassandra insert (partition-aware batching)
# =========================================================================

def store_ticks_cassandra(
    session: CassSession,
    preprocessed_df: pd.DataFrame,
) -> tuple[int, int]:
    """
    Batch-insert pre-processed rows into Cassandra ``market_tick``.

    **Batches are grouped by partition key** ``(stock_id, date_bucket)``
    so that every ``BATCH`` statement hits exactly one partition.

    Returns
    -------
    (rows_written, rows_failed)
    """
    if preprocessed_df.empty:
        return 0, 0

    insert_cql = session.prepare("""
        INSERT INTO market_tick (stock_id, date_bucket, timestamp, price, volume)
        VALUES (?, ?, ?, ?, ?)
    """)

    total_written = 0
    total_failed = 0

    for (stock_id, bucket), partition_df in preprocessed_df.groupby(
        ["stock_id", "date_bucket"], sort=False,
    ):
        batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
        batch_size = 0

        for _, row in partition_df.iterrows():
            batch.add(insert_cql, (
                str(row["stock_id"]),
                str(row["date_bucket"]),
                row["timestamp"],
                float(row["price"]),
                int(row["volume"]),
            ))
            batch_size += 1

            if batch_size >= _BATCH_SIZE:
                written, failed = _execute_batch_with_retry(
                    session, batch, batch_size, stock_id, bucket,
                )
                total_written += written
                total_failed += failed
                batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
                batch_size = 0

        if batch_size > 0:
            written, failed = _execute_batch_with_retry(
                session, batch, batch_size, stock_id, bucket,
            )
            total_written += written
            total_failed += failed

    return total_written, total_failed


def _execute_batch_with_retry(
    session: CassSession,
    batch: BatchStatement,
    size: int,
    stock_id: str,
    bucket: str,
) -> tuple[int, int]:
    """
    Execute *batch* with exponential back-off retries.
    Returns ``(rows_written, rows_failed)``.
    """
    for attempt in range(1, _BATCH_RETRIES + 1):
        try:
            session.execute(batch)
            logger.debug(
                "Batch OK  %s/%s  (%d rows, attempt %d)",
                stock_id, bucket, size, attempt,
            )
            return size, 0
        except Exception as exc:
            wait = 2 ** (attempt - 1)
            logger.warning(
                "Batch FAIL  %s/%s  (%d rows, attempt %d/%d): %s  "
                "— retrying in %ds",
                stock_id, bucket, size, attempt, _BATCH_RETRIES,
                exc, wait,
            )
            time.sleep(wait)

    logger.error(
        "Batch ABANDONED  %s/%s  (%d rows) after %d attempts",
        stock_id, bucket, size, _BATCH_RETRIES,
    )
    return 0, size


# =========================================================================
#  Public convenience API – single pass
# =========================================================================

def ingest_ticker_pass(
    session: CassSession,
    ticker: str,
    fp: FetchPass,
) -> tuple[int, int, int]:
    """
    Fetch → preprocess → store for one ticker at one resolution.

    Returns (rows_fetched, rows_written, rows_failed).
    """
    try:
        raw_df = fetch_ohlc(ticker, period=fp.period, interval=fp.interval)
    except RuntimeError as exc:
        logger.warning("Fetch %s (%s/%s): %s", ticker, fp.period, fp.interval, exc)
        return 0, 0, 0      # soft skip – the ticker may simply have no data at this resolution

    rows_fetched = len(raw_df)

    try:
        prepped = preprocess_ohlc(ticker, raw_df)
    except Exception as exc:
        logger.error("Preprocess %s (%s/%s): %s", ticker, fp.period, fp.interval, exc)
        return rows_fetched, 0, rows_fetched

    try:
        written, failed = store_ticks_cassandra(session, prepped)
        return rows_fetched, written, failed
    except Exception as exc:
        logger.error("Store %s (%s/%s): %s", ticker, fp.period, fp.interval, exc)
        return rows_fetched, 0, rows_fetched


# =========================================================================
#  Public convenience API – full multi-timeframe ingestion
# =========================================================================

def ingest_ticker(
    session: CassSession,
    ticker: str,
) -> IngestionResult:
    """
    Full **multi-timeframe** fetch → preprocess → store for one ticker.

    Iterates over every ``FetchPass`` in ``FETCH_PASSES``, accumulating
    all data into Cassandra.  Never raises.
    """
    t0 = time.monotonic()
    result = IngestionResult(stock_id=ticker)

    for fp in FETCH_PASSES:
        fetched, written, failed = ingest_ticker_pass(session, ticker, fp)
        result.rows_fetched += fetched
        result.rows_written += written
        result.rows_failed  += failed
        if written > 0:
            result.passes_ok += 1
        elif fetched == 0:
            pass   # no data available at this resolution – not a failure
        else:
            result.passes_failed += 1

    if result.rows_written == 0 and result.rows_fetched == 0:
        result.error = "No data returned across all fetch passes"

    result.elapsed_sec = time.monotonic() - t0
    logger.info(
        "Ingested %s: fetched=%d  written=%d  failed=%d  passes_ok=%d  (%.1fs)",
        ticker, result.rows_fetched, result.rows_written,
        result.rows_failed, result.passes_ok, result.elapsed_sec,
    )
    return result


def ingest_all(
    session: CassSession,
    tickers: Optional[List[str]] = None,
) -> int:
    """
    Download & store ticks for every ticker in the universe across
    all configured time-frames.

    Returns total rows **successfully** written.
    """
    tickers = tickers or TICKERS
    results: List[IngestionResult] = []

    for tkr in tickers:
        results.append(ingest_ticker(session, tkr))

    total_written = sum(r.rows_written for r in results)
    total_failed  = sum(r.rows_failed for r in results)
    errors = [r for r in results if not r.ok]

    logger.info(
        "Ingestion complete – %d written, %d failed across %d tickers "
        "(%d tickers had errors)",
        total_written, total_failed, len(tickers), len(errors),
    )
    for err in errors:
        logger.warning("  ✗ %s → %s", err.stock_id, err.error or "partial failure")

    return total_written
