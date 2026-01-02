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
        StructField("timestamp", DoubleType()),
    ]
)


df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers")
    .option("subscribe", "query-logs")
    .load()
)


query_data = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)
