"""
Centralised configuration for the Market Surveillance pipeline.

All tunables (hosts, credentials, tickers, sectors, hyper-parameters) live
here so that every other module imports a single source of truth.

Ticker universe includes:
  • Blue-chip stocks (reference group)
  • Known / suspected pump-and-dump stocks (meme stocks, SPACs, penny stocks)
  • Cryptocurrency coins (many targeted by P&D groups)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List

# ---------------------------------------------------------------------------
# Cassandra
# ---------------------------------------------------------------------------

@dataclass
class CassandraConfig:
    hosts: List[str] = field(
        default_factory=lambda: os.getenv("CASSANDRA_HOSTS", "127.0.0.1").split(",")
    )
    port: int = int(os.getenv("CASSANDRA_PORT", "9042"))
    keyspace: str = os.getenv("CASSANDRA_KEYSPACE", "market_surveillance")
    replication_factor: int = int(os.getenv("CASSANDRA_RF", "1"))


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------

@dataclass
class PostgresConfig:
    host: str = os.getenv("PG_HOST", "127.0.0.1")
    port: int = int(os.getenv("PG_PORT", "5432"))
    dbname: str = os.getenv("PG_DBNAME", "market_warehouse")
    user: str = os.getenv("PG_USER", "postgres")
    password: str = os.getenv("PG_PASSWORD", "postgres")

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.dbname} "
            f"user={self.user} password={self.password}"
        )


# ---------------------------------------------------------------------------
# Market / ticker universe
# ---------------------------------------------------------------------------

# Map each ticker to a sector – used later in the star-schema dimension.
TICKER_SECTOR_MAP: Dict[str, str] = {
    # ── Blue-chip / reference stocks ──────────────────────────────────
    "AAPL":   "Technology",
    "MSFT":   "Technology",
    "GOOGL":  "Technology",
    "AMZN":   "Consumer Discretionary",
    "TSLA":   "Consumer Discretionary",
    "JPM":    "Financials",
    "BAC":    "Financials",
    "JNJ":    "Healthcare",
    "PFE":    "Healthcare",
    "XOM":    "Energy",

    # ── Meme / pump-and-dump stocks ───────────────────────────────────
    "GME":    "Meme Stock",          # GameStop
    "AMC":    "Meme Stock",          # AMC Entertainment
    "BBBY":   "Meme Stock",          # Bed Bath & Beyond (delisted but history available)
    "BB":     "Meme Stock",          # BlackBerry
    "NOK":    "Meme Stock",          # Nokia
    "CLOV":   "Meme Stock",          # Clover Health
    "WISH":   "Meme Stock",          # ContextLogic
    "WKHS":   "Meme Stock",          # Workhorse Group
    "SPCE":   "Meme Stock",          # Virgin Galactic
    "PLTR":   "Meme Stock",          # Palantir Technologies
    "SOFI":   "Meme Stock",          # SoFi Technologies

    # ── Penny stocks / micro-caps (P&D targets) ──────────────────────
    "SNDL":   "Penny Stock",         # Sundial Growers
    "NAKD":   "Penny Stock",         # Cenntro Electric (ex-Naked Brand)
    "RIDE":   "Penny Stock",         # Lordstown Motors
    "NKLA":   "Penny Stock",         # Nikola Corporation
    "MVIS":   "Penny Stock",         # MicroVision
    "CPRX":   "Penny Stock",         # Catalyst Pharmaceuticals
    "GSAT":   "Penny Stock",         # Globalstar
    "TELL":   "Penny Stock",         # Tellurian
    "SKLZ":   "Penny Stock",         # Skillz

    # ── Crypto mining / blockchain stocks ─────────────────────────────
    "MARA":   "Crypto Mining",       # Marathon Digital
    "RIOT":   "Crypto Mining",       # Riot Platforms
    "COIN":   "Crypto Mining",       # Coinbase
    "HUT":    "Crypto Mining",       # Hut 8 Mining

    # ── Cryptocurrency coins ──────────────────────────────────────────
    "BTC-USD":  "Crypto",            # Bitcoin
    "ETH-USD":  "Crypto",            # Ethereum
    "DOGE-USD": "Crypto",            # Dogecoin (pump-and-dump magnet)
    "SHIB-USD": "Crypto",            # Shiba Inu (pump-and-dump magnet)
    "SOL-USD":  "Crypto",            # Solana
    "XRP-USD":  "Crypto",            # Ripple
    "ADA-USD":  "Crypto",            # Cardano
    "PEPE-USD": "Crypto",            # Pepe (meme coin)
    "FLOKI-USD":"Crypto",            # Floki (meme coin)
    "BONK-USD": "Crypto",            # Bonk (meme coin)
}

TICKERS: List[str] = list(TICKER_SECTOR_MAP.keys())

# Tickers that are *known* or *strongly suspected* pump-and-dump targets.
# The pipeline will label these with a special flag in OLAP queries.
PND_SUSPECTS: List[str] = [
    # Meme stocks driven by coordinated social-media pumps
    "GME", "AMC", "BBBY", "BB", "NOK", "CLOV", "WISH", "WKHS",
    "SPCE", "SOFI",
    # Micro-cap / penny stocks with documented P&D activity
    "SNDL", "NAKD", "RIDE", "NKLA", "MVIS", "SKLZ",
    "GSAT", "TELL",
    # Meme / pump-and-dump crypto coins
    "DOGE-USD", "SHIB-USD", "PEPE-USD", "FLOKI-USD", "BONK-USD",
]


# ---------------------------------------------------------------------------
# Anomaly hyper-parameters
# ---------------------------------------------------------------------------

@dataclass
class AnomalyParams:
    rolling_window: int = int(os.getenv("ROLLING_WINDOW", "20"))
    zscore_threshold: float = float(os.getenv("ZSCORE_THRESHOLD", "2.0"))
    volatility_threshold: float = float(os.getenv("VOLATILITY_THRESHOLD", "0.05"))
    volume_spike_threshold: float = float(os.getenv("VOLUME_SPIKE_THRESHOLD", "2.0"))


# ---------------------------------------------------------------------------
# Multi-timeframe fetch parameters
# ---------------------------------------------------------------------------
# yfinance data-availability constraints:
#   interval  |  max period
#   ----------+------------
#   1m        |  7 days
#   5m        |  60 days
#   15m       |  60 days
#   1h        |  730 days
#   1d        |  unlimited
#
# We fetch multiple time-frames per ticker and merge them so the total
# row-count reaches the millions.

@dataclass
class FetchPass:
    """One yfinance download pass (period + interval)."""
    period: str
    interval: str

FETCH_PASSES: List[FetchPass] = [
    FetchPass(period="7d",   interval="1m"),    # ~390 bars/ticker × N tickers
    FetchPass(period="60d",  interval="5m"),    # ~5 760 bars/ticker
    FetchPass(period="730d", interval="1h"),    # ~4 800 bars/ticker
    FetchPass(period="max",  interval="1d"),    # all-history daily bars
]

@dataclass
class FetchParams:
    """yfinance download parameters (kept for backward-compat)."""
    period: str = os.getenv("FETCH_PERIOD", "7d")
    interval: str = os.getenv("FETCH_INTERVAL", "1m")


# ---------------------------------------------------------------------------
# Convenience singletons
# ---------------------------------------------------------------------------

cassandra_cfg  = CassandraConfig()
postgres_cfg   = PostgresConfig()
anomaly_params = AnomalyParams()
fetch_params   = FetchParams()
