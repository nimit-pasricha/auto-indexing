import json
import os
import re
import time

from health_check import wait_for_kafka
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError
from sql_parser import extract_query_details

KAFKA_BROKERS = os.environ["KAFKA_BROKERS"].split(",")
LOG_PATH = os.environ["LOG_PATH"]
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "query-logs")
KAFKA_REPLICATION = int(os.getenv("KAFKA_REPLICATION_FACTOR", "2"))


def ensure_topic_exists():
    """Explicitly creates the Kafka topic if it doesn't exist."""
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS)
    topic = NewTopic(
        name=KAFKA_TOPIC,
        num_partitions=KAFKA_REPLICATION,
        replication_factor=KAFKA_REPLICATION,
    )
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{KAFKA_TOPIC}' created.")
    except TopicAlreadyExistsError:
        print(f"Topic '{KAFKA_TOPIC}' already exists.")
    except Exception as e:
        print(f"Error creating topic: {e}")
    finally:
        admin_client.close()


def get_producer():
    """Create a Kafka producer. Service availability is already checked."""
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


def start_watcher():
    print("Watcher active: Monitoring Postgres logs for patterns...")

    producer = get_producer()

    # Wait for log file to exist
    while not os.path.exists(LOG_PATH):
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

            if "statement: SELECT" in line:
                raw_sql = line.split("statement: ", 1)[1]
                findings = extract_query_details(raw_sql)

                for item in findings:
                    if item["table"].startswith("pg_"):
                        continue

                    item["timestamp"] = time.time()
                    producer.send(KAFKA_TOPIC, item)
                    print(f"Parsed & Sent: {item}")


if __name__ == "__main__":
    # Check service availability before proceeding
    if not wait_for_kafka(KAFKA_BROKERS):
        exit(1)
    start_watcher()
