import os
import time
from datetime import datetime, timedelta

import psycopg2
from cassandra.cluster import Cluster
from health_check import wait_for_cassandra, wait_for_postgres

DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]
CASSANDRA_HOSTS = os.environ["CASSANDRA_HOSTS"].split(",")

DB_PORT = os.getenv("DB_PORT", "5432")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "index_optimizer")
CASSANDRA_REPLICATION = int(os.getenv("CASSANDRA_REPLICATION_FACTOR", "2"))
CREATE_THRESHOLD = int(os.getenv("CREATE_THRESHOLD", "50"))
DELETE_THRESHOLD = int(os.getenv("DELETE_THRESHOLD", "5"))
LOOKBACK_CREATE = int(os.getenv("LOOKBACK_CREATE", "30"))
LOOKBACK_DELETE = int(os.getenv("LOOKBACK_DELETE", "120"))
WRITE_RATIO_THRESHOLD = float(os.getenv("WRITE_RATIO_THRESHOLD", "0.2"))

PG_DSN = (
    f"host={DB_HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASS} port={DB_PORT}"
)


def setup_cassandra_schema():
    """Ensures the Cassandra environment is ready for Spark and the Actuator."""
    setup_complete = False
    while not setup_complete:
        try:
            cluster = Cluster(CASSANDRA_HOSTS)
            session = cluster.connect()
            print("Initializing Cassandra Schema...")
            session.execute(
                f"""
                CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE} 
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {CASSANDRA_REPLICATION}}};
                """
            )
            session.execute(f"USE {CASSANDRA_KEYSPACE};")
            session.execute(
                """
                CREATE TABLE IF NOT EXISTS query_stats (
                    table_name text,
                    window_start timestamp,
                    window_end timestamp,
                    column_name text,
                    operator text,
                    query_count int,
                    PRIMARY KEY ((table_name), window_start, window_end, column_name, operator)
                ) WITH default_time_to_live = 86400;
            """
            )
            setup_complete = True
            print("Initialized Cassandra Schema.")
        except Exception as e:
            print(f"Error while connecting to cassandra: {e}")
            time.sleep(5)
    cluster.shutdown()


def get_stats(session, minutes):
    cutoff = datetime.now() - timedelta(minutes=minutes)
    cql = "SELECT table_name, column_name, operator, query_count FROM query_stats WHERE window_start > %s ALLOW FILTERING"
    rows = session.execute(cql, [cutoff])
    stats = {}
    for row in rows:
        key = (row.table_name, row.column_name, row.operator)
        stats[key] = stats.get(key, 0) + row.query_count
    return stats


def is_cardinality_too_low(cur, table, col):
    # Force Postgres to update its stats before we check
    cur.execute(f"ANALYZE {table}")

    cur.execute(
        f"""
        SELECT n_distinct 
        FROM pg_stats 
        WHERE tablename = '{table}' AND attname = '{col}'
    """
    )
    res = cur.fetchone()

    if not res or res[0] == 0:
        return False  # Not enough data yet to decide, assume it's okay

    n_distinct = res[0]

    """
    n_distinct: 
    - If > 0, it's the absolute number of distinct values.
    - If < 0, it's the ratio (e.g., -0.1 is 10%).
    """

    # THRESHOLD LOGIC:
    # If absolute distinct values < 10 (like Gender, StatusCodes)
    # OR if distinct ratio is less than 5% (like Country in a huge table)
    if (n_distinct > 0 and n_distinct < 10) or (
        n_distinct < 0 and abs(n_distinct) < 0.05
    ):
        return True

    return False


def manage_indices():
    cluster = Cluster(CASSANDRA_HOSTS)
    session = cluster.connect("index_optimizer")

    recent_stats = get_stats(session, LOOKBACK_CREATE)
    long_term_stats = get_stats(session, LOOKBACK_DELETE)

    pg_conn = psycopg2.connect(PG_DSN)
    pg_conn.autocommit = True
    cur = pg_conn.cursor()

    processed_cols = set((table, col) for table, col, operator in recent_stats.keys())

    # Create indexes
    for table, col in processed_cols:
        all_ops = {
            o: count
            for (t, c, o), count in recent_stats.items()
            if t == table and c == col
        }
        total_recent = sum(all_ops.values())

        table_writes = long_term_stats.get((table, "__WRITE__", "WRITE"), 0)
        write_ratio = table_writes / total_recent if total_recent > 0 else 0

        # Skip if table is too volatile
        if write_ratio > WRITE_RATIO_THRESHOLD:
            if col != "__WRITE__":
                print(
                    f"HIGH VOLATILITY: Skipping index on {table}.{col} (Write Ratio: {write_ratio:.2%})"
                )

            # Delete existing auto-indexes if volatility is extreme
            if write_ratio > 0.5:  # 50% writes
                pattern = f"auto_idx_%_{table}_{col}"
                cur.execute(
                    """
                    SELECT indexname 
                    FROM pg_indexes 
                    WHERE tablename = %s AND indexname LIKE %s
                """,
                    (table, pattern),
                )

                indexes_to_drop = cur.fetchall()

                for (idx_name,) in indexes_to_drop:
                    print(f"Table {table} is way too write-heavy. Removing {idx_name}")
                    cur.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {idx_name}")
            continue

        if total_recent >= CREATE_THRESHOLD:
            if is_cardinality_too_low(cur, table, col):
                print(f"SKIPPING: {table}.{col} has too low cardinality for an index.")
                continue

            range_ops = [
                ">",
                "<",
                ">=",
                "<=",
                "!=",
                "BETWEEN",
                "LIKE",
                "NOT LIKE",
                "ILIKE",
            ]
            has_range_query = any(op in range_ops for op in all_ops.keys())

            target_type = (
                "btree"
                if (has_range_query or "IN" in all_ops or "NOT IN" in all_ops)
                else "hash"
            )

            # auto_idx_ prefix to differentiate auto generated and user generated indexes
            target_idx = f"auto_idx_{target_type}_{table}_{col}"
            opposite_idx = f"auto_idx_{'hash' if target_type == 'btree' else 'btree'}_{table}_{col}"

            # Promotion: If we need a B-Tree but a Hash exists, upgrade
            cur.execute(
                f"SELECT indexname FROM pg_indexes WHERE indexname = '{opposite_idx}'"
            )
            if cur.fetchone() and target_type == "btree":
                print(
                    f"PROMOTING: {table}.{col} needs range support. Swapping Hash for B-Tree."
                )
                cur.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {opposite_idx}")

            # Create the target index if it doesn't exist
            cur.execute(f"SELECT 1 FROM pg_indexes WHERE indexname = '{target_idx}'")
            if not cur.fetchone():
                print(f"Creating {target_idx} (Recent query count: {total_recent})")
                try:
                    # Note: Hash indexes support CONCURRENTLY in PG 10+
                    cur.execute(
                        f"CREATE INDEX CONCURRENTLY {target_idx} ON {table} USING {target_type} ({col})"
                    )
                except Exception as e:
                    print(f"Creation Error: {e}")

    # Delete stale auto generated indexes
    cur.execute(
        "SELECT indexname, tablename FROM pg_indexes WHERE indexname LIKE 'auto_idx_%'"
    )
    existing_auto_indexes = cur.fetchall()

    for idx_name, table_name in existing_auto_indexes:
        # Index Naming Scheme: (auto_idx_TYPE_TABLE_COL)
        # eg parts looks like: ['auto', 'idx', 'btree', 'users', 'email']
        parts = idx_name.split("_")
        col_name = parts[-1]

        usage = sum(
            count
            for (t, c, o), count in long_term_stats.items()
            if t == table_name and c == col_name
        )

        if usage < DELETE_THRESHOLD:
            print(
                f"Deleting stale index {idx_name} (Only {usage} queries in {LOOKBACK_DELETE}m)"
            )
            cur.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {idx_name}")

    cur.close()
    pg_conn.close()
    cluster.shutdown()


if __name__ == "__main__":
    print(
        f"Actuator active. Creation: {CREATE_THRESHOLD}q/{LOOKBACK_CREATE}m | Cleanup: {DELETE_THRESHOLD}q/{LOOKBACK_DELETE}m"
    )
    # Check service availability before proceeding
    if not wait_for_cassandra(CASSANDRA_HOSTS):
        exit(1)
    if not wait_for_postgres(DB_HOST, DB_NAME, DB_USER, DB_PASS, DB_PORT):
        exit(1)

    setup_cassandra_schema()
    while True:
        try:
            manage_indices()
        except Exception as e:
            print(f"Actuator Error: {e}")
        time.sleep(60)
