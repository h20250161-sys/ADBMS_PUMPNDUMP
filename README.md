# Distributed Market Surveillance Prototype

A production-ready pipeline that detects anomalous price movements across a
configurable universe of equities, using **Cassandra** for high-write OLTP
storage and **PostgreSQL** for star-schema OLAP analytics.

---

## Architecture

```
yfinance  ──►  Cassandra (ohlc_minutes)
                       │
              anomaly detection
                       │
                       ▼
               Cassandra (anomaly_metrics)
                       │
                    ETL job
                       │
                       ▼
               PostgreSQL (star schema)
                       │
                  OLAP queries
                       │
                       ▼
              sector-wise analytics
```

## Project Structure

```
ADBMS_PUMPNDUMP/
├── main.py                         # CLI entry-point / orchestrator
├── requirements.txt
├── README.md
├── surveillance/
│   ├── __init__.py
│   ├── config.py                   # All settings & hyper-parameters
│   ├── db_setup.py                 # Cassandra + PG schema bootstrap
│   ├── ingestion.py                # yfinance → Cassandra OHLC
│   ├── anomaly.py                  # Rolling vol + z-score → Cassandra
│   ├── warehouse_etl.py            # Cassandra → PG star-schema ETL
│   └── olap_queries.py             # Analytical queries on PG
└── tests/
    └── __init__.py
```

## Prerequisites

| Component   | Required version |
|-------------|-----------------|
| Python      | ≥ 3.10          |
| Cassandra   | ≥ 4.0           |
| PostgreSQL  | ≥ 14            |

Ensure both databases are running and accessible before launching the pipeline.

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure (optional)

All settings have sensible defaults. Override via environment variables:

| Variable            | Default               | Purpose                          |
|---------------------|-----------------------|----------------------------------|
| `CASSANDRA_HOSTS`   | `127.0.0.1`           | Comma-separated Cassandra nodes  |
| `CASSANDRA_PORT`    | `9042`                | Cassandra native transport port  |
| `CASSANDRA_KEYSPACE`| `market_surveillance` | Keyspace name                    |
| `PG_HOST`           | `127.0.0.1`           | PostgreSQL host                  |
| `PG_PORT`           | `5432`                | PostgreSQL port                  |
| `PG_DBNAME`         | `market_warehouse`    | PostgreSQL database              |
| `PG_USER`           | `postgres`            | PostgreSQL user                  |
| `PG_PASSWORD`       | `postgres`            | PostgreSQL password              |
| `ROLLING_WINDOW`    | `20`                  | Rolling window for volatility    |
| `ZSCORE_THRESHOLD`  | `2.0`                 | z-score anomaly threshold        |
| `FETCH_PERIOD`      | `5d`                  | yfinance download period         |
| `FETCH_INTERVAL`    | `1m`                  | yfinance bar interval            |

### 3. Run the full pipeline

```bash
python main.py
```

### 4. Run individual steps

```bash
python main.py --step ingest    # Fetch OHLC → Cassandra
python main.py --step anomaly   # Compute anomalies → Cassandra
python main.py --step etl       # ETL Cassandra → PostgreSQL
python main.py --step olap      # Run OLAP analytics
```

## Database Schemas

### Cassandra – `ohlc_minutes`

| Column | Type      |
|--------|-----------|
| ticker | TEXT (PK) |
| ts     | TIMESTAMP (CK, DESC) |
| open   | DOUBLE    |
| high   | DOUBLE    |
| low    | DOUBLE    |
| close  | DOUBLE    |
| volume | BIGINT    |

### Cassandra – `anomaly_metrics`

| Column              | Type      |
|---------------------|-----------|
| ticker              | TEXT (PK) |
| ts                  | TIMESTAMP (CK, DESC) |
| close               | DOUBLE    |
| rolling_volatility  | DOUBLE    |
| zscore              | DOUBLE    |
| is_anomaly          | BOOLEAN   |

### PostgreSQL – Star Schema (partitioned by month)

```
dim_stock (stock_key PK, ticker, company_name, sector, market, created_at)
dim_time  (time_key PK, full_ts, year, quarter, month, day, hour, minute,
           day_of_week, is_weekend)
fact_market_metrics (metric_id + metric_ts PK, stock_key FK, time_key FK,
                     price, volume, volatility, z_score, volume_spike,
                     price_change_pct, is_anomaly)
    → PARTITION BY RANGE (metric_ts)     -- monthly child tables
    → UNIQUE (stock_key, time_key, metric_ts)
```

## OLAP Queries Included

1. **Sector-wise average anomaly** – average z-score, volatility, price change % grouped by sector.
2. **Per-ticker anomaly summary** – max |z-score|, anomaly percentage, avg price change per ticker.
3. **Hourly anomaly heatmap** – anomaly counts by hour-of-day.
4. **Monthly anomaly trend** – anomaly counts and rates aggregated by calendar month.

## Ticker Universe

| Ticker | Sector                  |
|--------|-------------------------|
| AAPL   | Technology              |
| MSFT   | Technology              |
| GOOGL  | Technology              |
| AMZN   | Consumer Discretionary  |
| TSLA   | Consumer Discretionary  |
| JPM    | Financials              |
| BAC    | Financials              |
| JNJ    | Healthcare              |
| PFE    | Healthcare              |
| XOM    | Energy                  |

Edit `surveillance/config.py` → `TICKER_SECTOR_MAP` to add/remove tickers.
