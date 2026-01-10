import time
import psycopg2
from cassandra.cluster import Cluster
from kafka import KafkaAdminClient
from kafka.errors import NoBrokersAvailable


def wait_for_postgres(host, name, user, password, port, max_retries=30, delay=2):
    """Wait for PostgreSQL to be ready."""
    for attempt in range(max_retries):
        try:
            dsn = f"host={host} dbname={name} user={user} password={password} port={port}"
            conn = psycopg2.connect(dsn)
            conn.close()
            print(f"ostgreSQL is ready")
            return True
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"PostgreSQL failed after {max_retries} attempts: {e}")
                return False
            print(f"Waiting for PostgreSQL... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(delay)


def wait_for_cassandra(hosts, max_retries=30, delay=2):
    """Wait for Cassandra to be ready."""
    for attempt in range(max_retries):
        try:
            cluster = Cluster(hosts)
            session = cluster.connect()
            session.close()
            cluster.shutdown()
            print(f"Cassandra is ready")
            return True
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"Cassandra failed after {max_retries} attempts: {e}")
                return False
            print(f"Waiting for Cassandra... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(delay)


def wait_for_kafka(brokers, max_retries=30, delay=2):
    """Wait for Kafka to be ready."""
    for attempt in range(max_retries):
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=brokers)
            admin_client.close()
            print(f"Kafka is ready")
            return True
        except NoBrokersAvailable:
            if attempt == max_retries - 1:
                print(f"Kafka failed after {max_retries} attempts")
                return False
            print(f"Waiting for Kafka... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(delay)
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"Kafka failed after {max_retries} attempts: {e}")
                return False
            time.sleep(delay)