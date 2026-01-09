import time
from datetime import datetime, timedelta

import psycopg2
from cassandra.cluster import Cluster

PG_DSN = "host=postgres dbname=testdb user=admin password=password"
C_HOSTS = ["cassandra"]
CREATE_THRESHOLD = 50  # Num queries required in interval to CREATE index
DELETE_THRESHOLD = 5  # Num Queries required in interval to DELETE index
LOOKBACK_CREATE = 30  # Minutes
LOOKBACK_DELETE = 120  # Minutes


def get_stats(session, minutes):
    cutoff = datetime.now() - timedelta(minutes=minutes)
    cql = "SELECT table_name, column_name, operator, query_count FROM query_stats WHERE window_start > %s ALLOW FILTERING"
    rows = session.execute(cql, [cutoff])
    stats = {}
    for row in rows:
        key = (row.table_name, row.column_name, row.operator)
        stats[key] = stats.get(key, 0) + row.query_count
    return stats


def manage_indices():
    cluster = Cluster(C_HOSTS)
    session = cluster.connect("index_optimizer")

    recent_stats = get_stats(session, LOOKBACK_CREATE)
    long_term_stats = get_stats(session, LOOKBACK_DELETE)

    pg_conn = psycopg2.connect(PG_DSN)
    pg_conn.autocommit = True
    cur = pg_conn.cursor()

    processed_cols = set((table, col) for table, col, operator in recent_stats.keys())

    # Create indexes
    for table, col in processed_cols:

        # Compute total count for (table, col) pairs across all operators.
        all_ops = {
            o: count
            for (t, c, o), count in recent_stats.items()
            if t == table and c == col
        }
        total_recent = sum(all_ops.values())

        if total_recent >= CREATE_THRESHOLD:
            has_range_query = any(
                op in [">", "<", ">=", "<=", "!="] for op in all_ops.keys()
            )
            target_type = "btree" if has_range_query else "hash"

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
    while True:
        try:
            manage_indices()
        except Exception as e:
            print(f"Actuator Error: {e}")
        time.sleep(60)
