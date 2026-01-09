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


    