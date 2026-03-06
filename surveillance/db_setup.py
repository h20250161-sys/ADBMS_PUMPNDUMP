"""
Database schema bootstrap for Cassandra and PostgreSQL.

* Cassandra  – keyspace + tables ``market_tick`` and ``anomaly_metric``.
* PostgreSQL – star-schema warehouse (dim_stock, dim_time,
  fact_market_metrics partitioned by month).

Session management
------------------
``CassandraSessionManager`` wraps the driver's ``Cluster`` / ``Session``
lifecycle in a **context-manager** so that connections are always shut down
cleanly – even when exceptions occur.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator, Optional

from cassandra.cluster import Cluster, Session as CassSession
from cassandra.policies import RetryPolicy, RoundRobinPolicy
import psycopg2
from psycopg2.extensions import connection as PgConnection

from surveillance.config import cassandra_cfg, postgres_cfg

logger = logging.getLogger(__name__)


# =========================================================================
#  Cassandra – session manager (context-manager pattern)
# =========================================================================

class CassandraSessionManager:
    """
    Best-practice wrapper around ``cassandra.cluster.Cluster``.

    Usage::

        mgr = CassandraSessionManager()
        mgr.connect()           # opens cluster + session
        session = mgr.session   # ready to execute queries
        ...
        mgr.shutdown()          # always shut down cleanly

    Or as a context manager::

        with CassandraSessionManager() as session:
            session.execute(...)
    """

    def __init__(
        self,
        hosts: list[str] | None = None,
        port: int | None = None,
        keyspace: str | None = None,
    ) -> None:
        self._hosts = hosts or cassandra_cfg.hosts
        self._port = port or cassandra_cfg.port
        self._keyspace = keyspace or cassandra_cfg.keyspace
        self._cluster: Cluster | None = None
        self._session: CassSession | None = None

    # --- lifecycle -----------------------------------------------------

    def connect(self) -> CassSession:
        """Open the cluster connection, create the keyspace, and bind."""
        self._cluster = Cluster(
            contact_points=self._hosts,
            port=self._port,
            load_balancing_policy=RoundRobinPolicy(),
            default_retry_policy=RetryPolicy(),
        )
        self._session = self._cluster.connect()

        # Ensure keyspace exists
        self._session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {self._keyspace}
            WITH replication = {{
                'class': 'SimpleStrategy',
                'replication_factor': {cassandra_cfg.replication_factor}
            }}
        """)
        self._session.set_keyspace(self._keyspace)
        logger.info("Connected to Cassandra keyspace '%s'", self._keyspace)
        return self._session

    def shutdown(self) -> None:
        """Gracefully close session and cluster."""
        if self._session:
            self._session.shutdown()
            logger.debug("Cassandra session shut down")
        if self._cluster:
            self._cluster.shutdown()
            logger.debug("Cassandra cluster shut down")
        self._session = None
        self._cluster = None

    @property
    def session(self) -> CassSession:
        if self._session is None:
            raise RuntimeError("Not connected – call .connect() first")
        return self._session

    # --- context-manager protocol --------------------------------------

    def __enter__(self) -> CassSession:
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.shutdown()


@contextmanager
def cassandra_session() -> Generator[CassSession, None, None]:
    """Shorthand context-manager that yields a ready-to-use session."""
    mgr = CassandraSessionManager()
    try:
        yield mgr.connect()
    finally:
        mgr.shutdown()


def get_cassandra_session() -> CassSession:
    """
    Return a *long-lived* Cassandra session (caller must shut down manually).

    Prefer ``cassandra_session()`` context-manager for short-lived work.
    """
    mgr = CassandraSessionManager()
    return mgr.connect()


# =========================================================================
#  Cassandra – table creation
# =========================================================================

def create_cassandra_tables(session: CassSession) -> None:
    """
    Create Cassandra tables for market tick data and anomaly metrics.

    ``market_tick``
        Partition key : (stock_id, date_bucket)
        Clustering    : timestamp  DESC
        Columns       : price, volume

    ``anomaly_metric``
        Partition key : (stock_id, date_bucket)
        Clustering    : timestamp  DESC
        Columns       : volatility, z_score, volume_spike, price_change_pct
    """

    # --- market_tick ---------------------------------------------------
    session.execute("""
        CREATE TABLE IF NOT EXISTS market_tick (
            stock_id        TEXT,
            date_bucket     TEXT,
            timestamp       TIMESTAMP,
            price           DOUBLE,
            volume          BIGINT,
            PRIMARY KEY ((stock_id, date_bucket), timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC)
    """)

    # --- anomaly_metric ------------------------------------------------
    session.execute("""
        CREATE TABLE IF NOT EXISTS anomaly_metric (
            stock_id            TEXT,
            date_bucket         TEXT,
            timestamp           TIMESTAMP,
            volatility          DOUBLE,
            z_score             DOUBLE,
            volume_spike        DOUBLE,
            price_change_pct    DOUBLE,
            PRIMARY KEY ((stock_id, date_bucket), timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC)
    """)

    logger.info("Cassandra tables created (market_tick, anomaly_metric)")


# =========================================================================
#  PostgreSQL
# =========================================================================

def get_pg_connection() -> PgConnection:
    """Return a psycopg2 connection to the PostgreSQL warehouse."""
    conn = psycopg2.connect(postgres_cfg.dsn)
    conn.autocommit = True
    logger.info("Connected to PostgreSQL database '%s'", postgres_cfg.dbname)
    return conn


def create_pg_star_schema(conn: Optional[PgConnection] = None) -> None:
    """
    Create the star schema in PostgreSQL.

    Dimensions
    ----------
    ``dim_stock``  – stock / ticker dimension with surrogate key.
    ``dim_time``   – minute-granularity time dimension with surrogate key.

    Fact
    ----
    ``fact_market_metrics`` – partitioned by month (``RANGE`` on
    ``metric_ts``).  Surrogate ``metric_id`` generated by ``BIGSERIAL``.
    Foreign keys point to ``dim_stock`` and ``dim_time``.

    Partitioning
    ~~~~~~~~~~~~~
    The parent table is created as ``PARTITION BY RANGE (metric_ts)``.
    Individual monthly child tables are created automatically by
    ``ensure_monthly_partition()`` during ETL.

    All statements are idempotent (``IF NOT EXISTS``).
    """
    own_conn = conn is None
    if own_conn:
        conn = get_pg_connection()

    cur = conn.cursor()

    # ── DimStock ──────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_stock (
            stock_key       SERIAL          PRIMARY KEY,
            ticker          VARCHAR(10)     UNIQUE NOT NULL,
            company_name    VARCHAR(120)    NOT NULL DEFAULT '',
            sector          VARCHAR(60)     NOT NULL DEFAULT 'Unknown',
            market          VARCHAR(30)     NOT NULL DEFAULT 'US',
            created_at      TIMESTAMP       NOT NULL DEFAULT NOW()
        );
    """)

    # ── DimTime ───────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_time (
            time_key    SERIAL      PRIMARY KEY,
            full_ts     TIMESTAMP   UNIQUE NOT NULL,
            year        SMALLINT    NOT NULL,
            quarter     SMALLINT    NOT NULL,
            month       SMALLINT    NOT NULL,
            day         SMALLINT    NOT NULL,
            hour        SMALLINT    NOT NULL,
            minute      SMALLINT    NOT NULL,
            day_of_week SMALLINT    NOT NULL,
            is_weekend  BOOLEAN     NOT NULL DEFAULT FALSE
        );
    """)

    # ── FactMarketMetrics (partitioned parent) ────────────────────────
    #    metric_ts is the partition column – must be in the PK for
    #    partitioned tables, so we use (metric_id, metric_ts).
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_market_metrics (
            metric_id           BIGSERIAL,
            stock_key           INT             NOT NULL,
            time_key            INT             NOT NULL,
            metric_ts           TIMESTAMP       NOT NULL,
            price               DOUBLE PRECISION,
            volume              BIGINT,
            volatility          DOUBLE PRECISION,
            z_score             DOUBLE PRECISION,
            volume_spike        DOUBLE PRECISION,
            price_change_pct    DOUBLE PRECISION,
            is_anomaly          BOOLEAN         NOT NULL DEFAULT FALSE,
            PRIMARY KEY (metric_id, metric_ts)
        ) PARTITION BY RANGE (metric_ts);
    """)

    # FK constraints on default partition (partition children inherit them
    # only in PG ≥ 12; we add them on the parent for documentation, and
    # re-add on each child partition in ensure_monthly_partition()).
    # NOTE: FK on partitioned tables requires PG 12+. We wrap in a
    # DO-block so older versions don't abort the migration.
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'fk_fact_stock'
            ) THEN
                ALTER TABLE fact_market_metrics
                    ADD CONSTRAINT fk_fact_stock
                    FOREIGN KEY (stock_key) REFERENCES dim_stock(stock_key);
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'FK fk_fact_stock skipped: %', SQLERRM;
        END $$;
    """)

    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'fk_fact_time'
            ) THEN
                ALTER TABLE fact_market_metrics
                    ADD CONSTRAINT fk_fact_time
                    FOREIGN KEY (time_key) REFERENCES dim_time(time_key);
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'FK fk_fact_time skipped: %', SQLERRM;
        END $$;
    """)

    # Unique constraint for upsert (must include partition key)
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'uq_fact_stock_time'
            ) THEN
                ALTER TABLE fact_market_metrics
                    ADD CONSTRAINT uq_fact_stock_time
                    UNIQUE (stock_key, time_key, metric_ts);
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'UQ uq_fact_stock_time skipped: %', SQLERRM;
        END $$;
    """)

    # Default partition catches anything that doesn't match a monthly child
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_market_metrics_default
            PARTITION OF fact_market_metrics DEFAULT;
    """)

    cur.close()
    logger.info("PostgreSQL star-schema tables created "
                "(dim_stock, dim_time, fact_market_metrics)")

    if own_conn:
        conn.close()


def ensure_monthly_partition(conn: PgConnection, year: int, month: int) -> str:
    """
    Create a monthly child partition of ``fact_market_metrics`` if it does
    not already exist.

    Parameters
    ----------
    conn : PgConnection
    year, month : int
        The calendar month for the partition.

    Returns
    -------
    str
        The partition table name, e.g. ``fact_market_metrics_2026_03``.
    """
    part_name = f"fact_market_metrics_{year:04d}_{month:02d}"
    start = f"{year:04d}-{month:02d}-01"
    if month == 12:
        end = f"{year + 1:04d}-01-01"
    else:
        end = f"{year:04d}-{month + 1:02d}-01"

    cur = conn.cursor()
    cur.execute(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_class WHERE relname = '{part_name}'
            ) THEN
                EXECUTE format(
                    'CREATE TABLE %I PARTITION OF fact_market_metrics '
                    'FOR VALUES FROM (%L) TO (%L)',
                    '{part_name}', '{start}', '{end}'
                );
            END IF;
        END $$;
    """)
    cur.close()
    logger.debug("Ensured partition %s", part_name)
    return part_name


# =========================================================================
#  Convenience: bootstrap everything
# =========================================================================

def bootstrap_all():
    """Idempotent creation of all schemas in both databases."""
    mgr = CassandraSessionManager()
    cass = mgr.connect()
    create_cassandra_tables(cass)

    pg = get_pg_connection()
    create_pg_star_schema(pg)

    # Return the manager alongside the session so callers can shut down later.
    return cass, pg, mgr
