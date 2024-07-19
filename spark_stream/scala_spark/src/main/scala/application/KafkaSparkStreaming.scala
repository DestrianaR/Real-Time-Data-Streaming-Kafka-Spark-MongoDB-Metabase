import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object KafkaSparkStreaming {
  def main(args: Array[String]) = {
    // Initialize Spark session
    val spark = SparkSession.builder
      .appName("KafkaSparkStreaming")
      .master("spark://spark-master:7077")
      .config("spark.mongodb.output.uri", "mongodb://root:example@mongodb:27017/db.transaction?authSource=db")
      .getOrCreate()

    // Read from Kafka
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "broker1:9092,broker2:9093") 
      .option("subscribe", "financialtransactions")
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

    val message = parsed_df.select(
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
      col("value.paymentMethod").alias("paymentMethod"),
    ).withColumn("revenue", col("productPrice") * col("productQuantity"))

    // Write stream to MongoDB
    val query = message.writeStream
      .format("mongodb")
      .outputMode("append")
      .option("checkpointLocation", "/tmp/spark-checkpoints")  
      .option("spark.mongodb.connection.uri", "mongodb://root:example@mongodb:27017/")
      .option("database", "db")
      .option("collection", "transaction")
      .start()
      .awaitTermination()
  }
}
