"""
Distributed Market Surveillance Prototype
==========================================
Modules:
    config          – centralised settings (hosts, ports, tickers, sectors)
    db_setup        – schema bootstrap for Cassandra & PostgreSQL
    ingestion       – fetch minute-level OHLC via yfinance → Cassandra
    anomaly         – rolling volatility + z-score anomaly → Cassandra
    warehouse_etl   – star-schema ETL  Cassandra → PostgreSQL
    olap_queries    – sector-wise OLAP analytics
"""
