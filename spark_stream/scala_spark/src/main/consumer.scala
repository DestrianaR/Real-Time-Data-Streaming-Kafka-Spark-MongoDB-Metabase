import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object KafkaSparkStreaming {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder
      .appName("KafkaSparkStreaming")
      .config("spark.mongodb.output.uri", "mongodb://root:example@mongodb:27017/db.transaction?authSource=db")
      .getOrCreate()

    // Read from Kafka
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092") // Update with the actual Kafka server addresses
      .option("subscribe", "financial_transactions")
      .option("startingOffsets", "earliest")
      .load()

    // Define schema
    val schema = StructType(Array(
      StructField("transactionId", StringType, true),
      StructField("productId", StringType, true),
      StructField("productName", StringType, true),
      StructField("productCategory", StringType, true),
      StructField("productPrice", DoubleType, true),
      StructField("productQuantity", IntegerType, true),
      StructField("productBrand", StringType, true),
      StructField("currency", StringType, true),
      StructField("customerId", StringType, true),
      StructField("transactionDate", TimestampType, true),
      StructField("paymentMethod", StringType, true)
    ))

    // Parse JSON and select required fields
    val parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("value"))
      .select("value.*")

    // Add a new column 'revenue' which is productPrice * productQuantity
    val enriched_df = parsed_df.withColumn("revenue", col("productPrice") * col("productQuantity"))

    // Write stream to MongoDB
    val query = enriched_df.writeStream
      .format("mongo")
      .outputMode("append")
      .option("database", "db")
      .option("collection", "transaction")
      .start()
      .awaitTermination()
  }
}

