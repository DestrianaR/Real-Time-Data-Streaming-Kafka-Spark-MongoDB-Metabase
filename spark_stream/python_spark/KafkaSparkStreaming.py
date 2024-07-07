from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.mongodb.output.uri", "mongodb://root:example@mongodb:27017/db.transaction?authSource=db") \
    .getOrCreate()

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9093") \
    .option("subscribe", "financial_transactions") \
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
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("value")
)

# Add a new column 'revenue' which is productPrice * productQuantity
enriched_df = parsed_df.withColumn("revenue", col("productPrice") * col("productQuantity"))

# Write stream to MongoDB
query = enriched_df.writeStream \
        .format("mongodb") \
        .mode("append") \
        .option("database", "db") \
        .option("collection", "transaction") \
        .start()

query.awaitTermination()