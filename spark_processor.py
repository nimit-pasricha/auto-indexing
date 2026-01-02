from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = (
    SparkSession.builder.appName("IndexOptimizer")
    .master("spark://localhost:7077")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .get_or_create()
)

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
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "query-logs")
    .load()
)

# 4. Transform: Parse JSON and Handle Time
queries = (
    raw_stream.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("timestamp").cast("timestamp"))
    .withWatermark("event_time", "10 minutes")
)

