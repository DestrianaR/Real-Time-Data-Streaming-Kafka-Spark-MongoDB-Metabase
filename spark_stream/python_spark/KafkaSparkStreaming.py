from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .config("spark.mongodb.output.uri", "mongodb://root:example@mongodb:27017/db.transaction?authSource=db") \
        .getOrCreate()

    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker1:9092,broker2:9093") \
        .option("subscribe", "financialtransactions") \
        .option("startingOffsets", "earliest") \
        .load()

    # Define schema
    schema = StructType([
        StructField("transactionId", StringType(), True),
        StructField("productId", StringType(), True),
        StructField("productName", StringType(), True),
        StructField("productCategory", StringType(), True),
        StructField("productPrice", DoubleType(), True),
        StructField("productQuantity", IntegerType(), True),
        StructField("productBrand", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("customerId", StringType(), True),
        StructField("transactionDate", TimestampType(), True),
        StructField("paymentMethod", StringType(), True)
    ])

    # Parse JSON and select required fields
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("value"))

    message = parsed_df.select(
        col("value.transactionId").alias("transactionId"),
        col("value.productId").alias("productId"),
        col("value.productName").alias("productName"),
        col("value.productCategory").alias("productCategory"),
        col("value.productPrice").alias("productPrice"),
        col("value.productQuantity").alias("productQuantity"),
        col("value.productBrand").alias("productBrand"),
        col("value.currency").alias("currency"),
        col("value.customerId").alias("customerId"),
        col("value.transactionDate").alias("transactionDate"),
        col("value.paymentMethod").alias("paymentMethod")
    ).withColumn("revenue", col("productPrice") * col("productQuantity"))

    # Write stream to MongoDB
    query = message.writeStream \
            .format("mongodb") \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/pyspark-checkpoints") \
            .option("spark.mongodb.connection.uri", "mongodb://root:example@mongodb:27017/") \
            .option("database", "db") \
            .option("collection", "transaction") \
            .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()