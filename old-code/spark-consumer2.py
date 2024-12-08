from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import pyspark.sql.functions as F

from validators2 import perform_all_validations
from utils2 import get_csv_writer_query


class SparkStreamProcessor:
    def __init__(self, host='localhost', port=9092):
        
        self.spark = SparkSession.builder \
            .appName("SalesDataStreamProcessor") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
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
            .option("includeHeaders", "true") \
            .load()
        
        streaming_df = streaming_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "headers")
        
        parsed_df = streaming_df \
            .select(F.from_csv(streaming_df.value, self.schema.simpleString()).alias("data")) \
            .select("data.*") \
            .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
            .withColumn("ship_date", to_date(col("ship_date"), "yyyy-MM-dd"))
        
        return parsed_df

    def process_stream(self, streaming_df):

        queries, writer_queries = perform_all_validations(streaming_df)

        return queries, writer_queries

    def start_streaming(self):
        
        try:
            
            streaming_df = self.create_stream()
            writer = get_csv_writer_query(streaming_df, "./full_data_csv", "./full_data_checkpoint")
            
            queries, writer_queries = self.process_stream(streaming_df)

            for query in queries:
                query.awaitTermination()
                
            for writer_query in writer_queries:
                writer_query.awaitTermination()

            writer.awaitTermination()
        
        except Exception as e:
            print(f"Streaming error: {e}")
        
        finally:
            
            self.spark.stop()

def main():
    
    stream_processor = SparkStreamProcessor(
        host='localhost',
        port=9092
    )
    
    stream_processor.start_streaming()

if __name__ == "__main__":
    main()