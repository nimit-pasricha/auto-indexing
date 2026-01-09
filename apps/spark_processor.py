from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "query-logs"
CASSANDRA_HOST = "cassandra"
CASSANDRA_KEYSPACE = "index_optimizer"
CASSANDRA_TABLE = "query_stats"

spark = (
    SparkSession.builder.appName("IndexOptimizer")
    .config("spark.cassandra.connection.host", CASSANDRA_HOST)
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
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
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
    queries.withWatermark("event_time", "1 hour")
    .groupBy(
        window(col("event_time"), "5 minutes"),
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
    # We use 'append' here because at the individual batch level,
    # we are just adding new rows/updates to the table.
    # Overall system is still 'update'.
    df.write.format("org.apache.spark.sql.cassandra").options(
        table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE
    ).mode("append").save()


query = (
    cassandra_df.writeStream.outputMode("update")
    .foreachBatch(write_to_cassandra)
    .option("checkpointLocation", "/opt/spark-apps/checkpoints/cassandra_sink")
    .start()
)


query.awaitTermination()
