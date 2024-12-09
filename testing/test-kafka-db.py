from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def create_spark_session():
    """
    Create a Spark session with necessary configurations for Kafka and PostgreSQL
    """
    spark = SparkSession.builder \
        .appName("KafkaPostgresStreaming") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1," + 
                "org.postgresql:postgresql:42.5.1") \
        .getOrCreate()
    return spark

def define_schema():
    """
    Define the schema for incoming Kafka messages
    """
    return StructType([
            StructField("region", StringType(), False),
            StructField("country", StringType(), False),
            StructField("item_type", StringType(), False),
            StructField("sales_channel", StringType(), False),
            StructField("order_priority", StringType(), False),
            StructField("order_date", StringType(), False),
            StructField("order_id", IntegerType(), False),
            StructField("ship_date", StringType(), False),
            StructField("units_sold", IntegerType(), False),
            StructField("unit_price", DoubleType(), False),
            StructField("unit_cost", DoubleType(), False),
            StructField("total_revenue", DoubleType(), False),
            StructField("total_cost", DoubleType(), False),
            StructField("total_profit", DoubleType(), False)
        ])

def kafka_stream_to_postgres(spark):
    """
    Create a streaming DataFrame from Kafka and write to PostgreSQL
    """
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "34.172.245.206:9092") \
        .option("subscribe", "csv-data") \
        .load()

    # Parse the value column 
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), define_schema()).alias("data")
    ).select("data.*") \
     .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
            .withColumn("ship_date", to_date(col("ship_date"), "yyyy-MM-dd"))

    # Define PostgreSQL connection properties
    pg_properties = {
        "url": "jdbc:postgresql://34.46.61.46:5432/mydb",
        "dbtable": "Sales",
        "user": "myuser",
        "password": "mypassword",
        "driver": "org.postgresql.Driver"
    }

    # Write streaming query to PostgreSQL
    query = parsed_df \
        .writeStream \
        .foreachBatch(lambda batch_df, batch_id: 
            batch_df.write \
            .jdbc(
                url=pg_properties["url"],
                table=pg_properties["dbtable"],
                mode="append",
                properties=pg_properties
            )
        ) \
        .start()

    # Await termination
    query.awaitTermination()

def main():
    spark = create_spark_session()
    kafka_stream_to_postgres(spark)

if __name__ == "__main__":
    main()