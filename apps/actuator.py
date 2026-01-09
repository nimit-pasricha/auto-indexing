import time
import psycopg2
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

PG_DSN = "host=postgres dbname=testdb user=admin password=password"
C_HOSTS = ['cassandra']
THRESHOLD = 50
LOOKBACK_MINUTES = 30

def run_actuator():
    cluster = Cluster(C_HOSTS)
    session = cluster.connect('index_optimizer')

    cutoff_time = datetime.now() - timedelta(minutes=LOOKBACK_MINUTES)
    cql = """
        SELECT table_name, column_name, query_count 
        FROM query_stats 
        WHERE window_start > %s ALLOW FILTERING
    """
    rows = session.execute(cql, [cutoff_time])

    # Aggregate across time periods for each table, column combination.
    stats = {}
    for row in rows:
        key = (row.table_name, row.column_name)
        stats[key] = stats.get(key, 0) + row.query_count


    pg_conn = psycopg2.connect(PG_DSN)
    pg_conn.autocommit = True
    cur = pg_conn.cursor()

    for (table, col), total_count in stats.items():
        if total_count >= THRESHOLD:
            idx_name = f"idx_{table}_{col}"
            
            # Check if index already exists
            cur.execute(f"SELECT 1 FROM pg_indexes WHERE indexname = '{idx_name}'")
            if not cur.fetchone():
                print(f"TREND: {table}.{col} has {total_count} queries in the last {LOOKBACK_MINUTES}m.")
                try:
                    cur.execute(f"CREATE INDEX CONCURRENTLY {idx_name} ON {table} ({col})")
                    print(f"Created index {idx_name}")
                except Exception as e:
                    print(f"Error while creating index: {e}")


    