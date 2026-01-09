import json
import os
import re
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# We ignore specific column value to keep data anonymous
LOG_PATTERN = re.compile(
    r'statement: SELECT .* FROM "?([\w\.]+)"? WHERE "?([\w\.]+)"?\s*([<>=!]+)', re.IGNORECASE
)

def get_producer(retries=5, delay=5):
    for i in range(retries):
        try:
            print(f"Attempting to connect to Kafka (Attempt {i+1}/{retries})...")
            producer = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # Built-in retries for individual message sends
                retries=5,
                retry_backoff_ms=1000
            )
            print("Connected to Kafka!")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not ready. Retrying in {delay}s...")
            time.sleep(delay)
    raise Exception("Failed to connect to Kafka after multiple retries.")


def start_watcher():
    print("Watcher active: Monitoring Postgres logs for patterns...")

    producer = get_producer()

    log_path = "/var/log/postgresql/postgresql.log"

    # Wait for log file to exist
    while not os.path.exists(log_path):
        print("Waiting for Postgres log file...")
        time.sleep(2)

    with open(log_path, "r") as f:
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

                payload = {
                    "table": table.lower(),
                    "column": col.lower(),
                    "operator": op,
                    "timestamp": time.time(),
                }

                producer.send("query-logs", payload)
                print(f"Sent: {table}.{col} {op} at {payload['timestamp']}")


if __name__ == "__main__":
    start_watcher()
