from dotenv import load_dotenv
import os


load_dotenv()
STREAM_TYPE = "console"


def write_to_postgres(batch_df, batch_id):

    jdbc_url = f"jdbc:postgresql://{os.getenv("DB_IP")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}"

    connection_properties = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }

    batch_df.write \
        .jdbc(url=jdbc_url, table="Sales", mode="append", properties=connection_properties)

def get_streaming_query(streaming_object, type=STREAM_TYPE):

    if type == "console":

        query = streaming_object \
                .select("*") \
                .writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .start()
        
    elif type == "db":

        query = streaming_object \
                .select("*") \
                .writeStream \
                .foreachBatch(write_to_postgres) \
                .outputMode("append") \
                .start()

    
    return query


def get_csv_writer_query(streaming_object, csv_path, checkpoint_path):

    query = streaming_object \
            .select("*") \
            .writeStream \
            .outputMode("append") \
            .format("csv") \
            .option("path", csv_path) \
            .option("checkpointLocation", checkpoint_path) \
            .start()
    
    return query