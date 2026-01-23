# Auto Indexing

The goal of this project is to turn a standard PostgreSQL database into a self-driving system that manages its own performance. Instead of a developer manually analyzing logs and creating indexes, this system uses a real-time data pipeline to monitor traffic and automatically apply optimizations where they are needed most.

## How it Works
- Monitor: A watcher tails PostgreSQL logs and parses raw SQL into structured data using pglast.

- Analyze: Query logs are streamed through Kafka to Apache Spark, which identifies frequently queried columns using windowed aggregations.

- Optimize: An autonomous Actuator reads these metrics from Cassandra and automatically creates B-Tree or Hash indexes in PostgreSQL.

- Protect: The system that drops indexes during heavy write bursts to prevent the database from slowing down due to indexing overhead.

## Key Features
- Decreased latency for databases with mixed read/write workloads.

- Fault Tolerant: Uses Spark checkpointing, Kafka offsets, and stores logs in Cassandra to minimize chances of data loss.

- Decoupled Design: The optimization pipeline is independent of the database, ensuring PostgreSQL stays online even if the pipeline fails.
