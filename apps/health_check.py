import time


def wait_for_postgres(host, name, user, password, port, max_retries=30, delay=2):
    """Wait for PostgreSQL to be ready."""
    import psycopg2

    for attempt in range(max_retries):
        try:
            dsn = (
                f"host={host} dbname={name} user={user} password={password} port={port}"
            )
            conn = psycopg2.connect(dsn)
            conn.close()
            print(f"PostgreSQL is ready")
            return True
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"PostgreSQL failed after {max_retries} attempts: {e}")
                return False
            print(f"Waiting for PostgreSQL... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(delay)


def wait_for_cassandra(hosts, max_retries=30, delay=5):
    """Wait for Cassandra to be ready."""
    from cassandra.cluster import Cluster

    for attempt in range(max_retries):
        cluster = None
        try:
            cluster = Cluster(hosts)
            session = cluster.connect()
            cluster.shutdown()
            print(f"Cassandra is ready")
            return True
        except Exception as e:
            if cluster:
                cluster.shutdown()
            if attempt == max_retries - 1:
                print(f"Cassandra failed after {max_retries} attempts: {e}")
                return False
            print(f"Waiting for Cassandra... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(delay)


def wait_for_kafka(brokers, max_retries=30, delay=2):
    """Wait for Kafka to be ready."""
    from kafka import KafkaAdminClient
    from kafka.errors import NoBrokersAvailable

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
