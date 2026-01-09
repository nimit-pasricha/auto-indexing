import json
import os
import re
import time

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092").split(",")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "query-logs")
LOG_PATH = os.getenv("LOG_PATH", "/var/log/postgresql/postgresql.log")

# We ignore specific column value to keep data anonymous
LOG_PATTERN = re.compile(
    r'statement: SELECT .* FROM "?([\w\.]+)"? WHERE "?([\w\.]+)"?\s*([<>=!]+)',
    re.IGNORECASE,
)


def ensure_topic_exists():
    """Explicitly creates the Kafka topic if it doesn't exist."""
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS)
    topic = NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{KAFKA_TOPIC}' created.")
    except TopicAlreadyExistsError:
        print(f"Topic '{KAFKA_TOPIC}' already exists.")
    except Exception as e:
        print(f"Error creating topic: {e}")
    finally:
        admin_client.close()


def get_producer(retries=5, delay=5):
    for i in range(retries):
        try:
            print(f"Attempting to connect to Kafka (Attempt {i+1}/{retries})...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # Built-in retries for individual message sends
                retries=5,
                retry_backoff_ms=1000,
            )
            ensure_topic_exists()
            print("Connected to Kafka!")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not ready. Retrying in {delay}s...")
            time.sleep(delay)
    raise Exception("Failed to connect to Kafka after multiple retries.")


def start_watcher():
    print("Watcher active: Monitoring Postgres logs for patterns...")

    producer = get_producer()

    # Wait for log file to exist
    while not os.path.exists(log_path):
        print(f"Waiting for Postgres log file at {LOG_PATH}...")
        time.sleep(2)

    with open(LOG_PATH, "r") as f:
        f.seek(0, 2)  # move to end

        # basically the 'tail -f' functionality
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.1)  # Prevents high CPU usage
                continue

            match = LOG_PATTERN.search(line)
            if match:
                table, col, op = match.groups()

                if (
                    table.lower().startswith("pg_")
                    or table.lower() == "information_schema"
                ):
                    continue

                payload = {
                    "table": table.lower(),
                    "column": col.lower(),
                    "operator": op,
                    "timestamp": time.time(),
                }

                producer.send(KAFKA_TOPIC, payload)
                print(f"Sent: {table}.{col} {op} at {payload['timestamp']}")


if __name__ == "__main__":
    start_watcher()
