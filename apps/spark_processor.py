import os

from health_check import wait_for_cassandra, wait_for_kafka
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

KAFKA_BROKERS = os.environ["KAFKA_BROKERS"]
CASSANDRA_HOSTS = os.environ["CASSANDRA_HOSTS"]
SPARK_MASTER_URL = os.environ["SPARK_MASTER_URL"]
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "query-logs")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "index_optimizer")
CASSANDRA_TABLE = os.getenv("CASSANDRA_TABLE", "query_stats")
WINDOW_SIZE = os.getenv("SPARK_WINDOW_SIZE", "10 minutes")
WATERMARK_SIZE = os.getenv("SPARK_WATERMARK_SIZE", "1 hour")

# Check service availability before proceeding
if not wait_for_kafka(KAFKA_BROKERS.split(",")):
    exit(1)
if not wait_for_cassandra(CASSANDRA_HOSTS.split(",")):
    exit(1)

spark = (
    SparkSession.builder.appName("IndexOptimizer")
    .master(SPARK_MASTER_URL)
    .config("spark.cassandra.connection.host", CASSANDRA_HOSTS)
    .config("spark.cassandra.output.consistency.level", "ONE")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType(
    [
        StructField("table", StringType()),
        StructField("column", StringType()),
        StructField("operator", StringType()),
        # not directly casting to timestamp to avoid returning null on failed conversion.
        StructField("timestamp", DoubleType()),
    ]
)


raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)


queries = (
    raw_stream.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("timestamp").cast("timestamp"))
)


windowed_counts = (
    queries.withWatermark("event_time", WATERMARK_SIZE)
    .groupBy(
        window(col("event_time"), WINDOW_SIZE),
        col("table"),
        col("column"),
        col("operator"),
    )
    .count()
)


cassandra_df = windowed_counts.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("table").alias("table_name"),
    col("column").alias("column_name"),
    col("operator"),
    col("count").alias("query_count"),
)


def write_to_cassandra(df, batch_id):
    print(f"BATCH {batch_id}: Writing to Cassandra...")
    # Explicitly using 'append' here. Since keys match, Cassandra will upsert
    df.write.format("org.apache.spark.sql.cassandra").options(
        table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE
    ).mode("append").save()


query = (
    cassandra_df.writeStream.outputMode(
        "update"
    )  # Switched to append for better compatibility with Cassandra sinks
    .foreachBatch(write_to_cassandra)
    .option("checkpointLocation", "/opt/spark-apps/checkpoints/cassandra_sink")
    .start()
)


query.awaitTermination()
