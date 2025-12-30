import json
from kafka import KafkaConsumer

# 'auto_offset_reset' determines where to start if the script hasn't seen the topic before.
# 'earliest' means it will read all messages currently in the topic from the beginning.
consumer = KafkaConsumer(
    'query-logs',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest', 
    enable_auto_commit=True,
    group_id='analysis-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("--- Listening for Query Logs... (Ctrl+C to stop) ---")

try:
    for message in consumer:
        # message.value is the JSON payload we sent from the producer
        data = message.value
        print(f"Received Log: Table={data['table']} | Col={data['column']} | Op={data['operator']}")
except KeyboardInterrupt:
    print("\nStopping consumer...")