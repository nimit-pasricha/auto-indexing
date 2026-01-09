import time
import psycopg2
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

PG_DSN = "host=postgres dbname=testdb user=admin password=password"
C_HOSTS = ["cassandra"]
CREATE_THRESHOLD = 50  # Queries in 30 mins to CREATE
DELETE_THRESHOLD = 5  # Queries in 2 hours to DELETE
LOOKBACK_CREATE = 30  # Minutes
LOOKBACK_DELETE = 120  # Minutes (2 hours)


def get_stats(session, minutes):
    cutoff = datetime.now() - timedelta(minutes=minutes)
    cql = "SELECT table_name, column_name, query_count FROM query_stats WHERE window_start > %s ALLOW FILTERING"
    rows = session.execute(cql, [cutoff])
    stats = {}
    for row in rows:
        key = (row.table_name, row.column_name)
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

    # Create indexes
    for (table, col), count in recent_stats.items():
        if count >= CREATE_THRESHOLD:
            # use this 'auto_idx_' prefix to separate auto generated idxs from user generated ones.
            idx_name = f"auto_idx_{table}_{col}"
            # Check if index already exists
            cur.execute(f"SELECT 1 FROM pg_indexes WHERE indexname = '{idx_name}'")
            if not cur.fetchone():
                print(
                    f"Creating index for {table}.{col} ({count} queries in the last {LOOKBACK_CREATE}m)."
                )
                cur.execute(f"CREATE INDEX CONCURRENTLY {idx_name} ON {table} ({col})")

    # Delete stale auto generated indexes
    cur.execute(
        "SELECT indexname, tablename FROM pg_indexes WHERE indexname LIKE 'auto_idx_%'"
    )
    existing_auto_indexes = cur.fetchall()
    for idx_name, table_name in existing_auto_indexes:
        # Extract column name from index name (auto_idx_table_column)
        col_name = idx_name.replace(f"auto_idx_{table_name}_", "")

        current_usage = long_term_stats.get((table_name, col_name), 0)
        if current_usage < DELETE_THRESHOLD:
            print(
                f"Deleting stale index {idx_name} (Only {current_usage} queries in {LOOKBACK_DELETE}m.)"
            )
            cur.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {idx_name}")

    cur.close()
    pg_conn.close()
    cluster.shutdown()


if __name__ == "__main__":
    while True:
        try:
            manage_indices()
        except Exception as e:
            print(f"Actuator Error: {e}")
        time.sleep(60)