from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import pyspark.sql.functions as F
import os
from dotenv import load_dotenv

load_dotenv()

from validators import perform_all_validations


class SparkStreamProcessor:
    def __init__(self, host, port):
        
        self.spark = SparkSession.builder \
            .appName("SalesDataStreamProcessor") \
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,' +
                    'org.postgresql:postgresql:42.5.1') \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .getOrCreate()
        
        self.queryManager = self.spark.streams
        
        self.schema = StructType([
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
        
        self.host = host
        self.port = port

    def create_stream(self):
        
        streaming_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", f"{self.host}:{self.port}") \
            .option("subscribe", "csv-data") \
            .load()
        
        streaming_df = streaming_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        
        parsed_df = streaming_df \
            .select(F.from_csv(streaming_df.value, self.schema.simpleString()).alias("data")) \
            .select("data.*") \
            .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
            .withColumn("ship_date", to_date(col("ship_date"), "yyyy-MM-dd"))
        
        return parsed_df
    
    def process_stream(self, streaming_df):

        valid_rows = perform_all_validations(streaming_df)
        return valid_rows

    def start_streaming(self):
        
        try:

            pg_properties = {
                "url": f"jdbc:postgresql://{os.getenv("DB_IP")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}",
                "dbtable": "salesStreamed",
                "user": os.getenv("DB_USER"),
                "password": os.getenv("DB_PASSWORD"),
                "driver": "org.postgresql.Driver"
            }
            
            streaming_df = self.create_stream()
            valid_rows = self.process_stream(streaming_df)

            query = valid_rows \
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


            query.awaitTermination()
            
        except Exception as e:
            print(f"Streaming error: {e}")
        
        finally:
            self.spark.stop()

def main():
    
    stream_processor = SparkStreamProcessor(
        host=os.getenv("KAFKA_IP"),
        port=int(os.getenv("KAFKA_PORT"))
    )
    
    stream_processor.start_streaming()

if __name__ == "__main__":
    main()