import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def log_query(table, column, op):
    payload = {
        "table": table,
        "column": column,
        "operator": op,
        "timestamp": time.time()
    }
    producer.send('query-logs', payload)
    print(f"Sent to Kafka: {payload}")

log_query("users", "email", "=")
log_query("orders", "created_at", ">")

producer.flush() # Ensure messages are sent before script ends