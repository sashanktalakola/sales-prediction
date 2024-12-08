from pyspark.sql.functions import col, round
from pyspark.sql import functions as F
from utils2 import get_streaming_query, get_csv_writer_query, STREAM_TYPE


def validate_condition_cost(streaming_df):

    condition_cost = streaming_df.withColumn(
        "is_condition_met",
        (round(col("units_sold") * col("unit_cost"), 2) == col("total_cost"))
    )
    condition_cost_error = condition_cost.filter(col("is_condition_met") == False)
    query = get_streaming_query(condition_cost_error, type=STREAM_TYPE)
    writer_query = get_csv_writer_query(condition_cost_error, csv_path="./write_stream_csv", checkpoint_path="./write_stream_checkpoint")

    return query, writer_query

def validate_condition_price(streaming_df):

    condition_price = streaming_df.withColumn(
        "is_condition_met",
        (round(col("units_sold") * col("unit_price"), 2) == col("total_revenue"))
    )
    condition_price_error = condition_price.filter(col("is_condition_met") == False)
    query = get_streaming_query(condition_price_error, type=STREAM_TYPE)
    writer_query = get_csv_writer_query(condition_price_error, csv_path="./write_stream_csv", checkpoint_path="./write_stream_checkpoint")

    return query, writer_query

def validate_revenue(streaming_df):

    condition_revenue = streaming_df.withColumn(
        "is_condition_met",
        (round(col("total_revenue") - col("total_cost"), 2) == col("total_profit"))
    )
    condition_revenue_error = condition_revenue.filter(col("is_condition_met") == False)
    query = get_streaming_query(condition_revenue_error, type=STREAM_TYPE)
    writer_query = get_csv_writer_query(condition_revenue_error, csv_path="./write_stream_csv", checkpoint_path="./write_stream_checkpoint")

    return query, writer_query

def validate_dates(streaming_df):

    condition_date = streaming_df.withColumn(
        "is_condition_met",
        (col("ship_date") >= col("order_date"))
    )
    condition_date_error = condition_date.filter(col("is_condition_met") == False)
    query = get_streaming_query(condition_date_error, type=STREAM_TYPE)
    writer_query = get_csv_writer_query(condition_date_error, csv_path="./write_stream_csv", checkpoint_path="./write_stream_checkpoint")

    return query, writer_query

# def validate_order_id_date(streaming_df):

#     order_date_count = streaming_df.groupBy("order_id").agg(
#         F.countDistinct("order_date").alias("distinct_order_dates")
#     )
#     invalid_orders = order_date_count.filter(F.col("distinct_order_dates") > 1)

#     query = invalid_orders \
#             .writeStream \
#             .outputMode("append") \
#             .format("console") \
#             .option("truncate", "false") \
#             .start()

#     return query


def validate_region(streaming_df):

    with open("validation-values/region") as f:
        data = f.read()
        valid_regions = data.split(", ")

    streaming_df_with_condition = streaming_df.withColumn(
        "is_condition_met", 
        F.when(F.col("region").isin(valid_regions), True).otherwise(False)
    )

    invalid_region_rows = streaming_df_with_condition.filter(F.col("is_condition_met") == False)
    query = get_streaming_query(invalid_region_rows, type=STREAM_TYPE)
    writer_query = get_csv_writer_query(invalid_region_rows, csv_path="./write_stream_csv", checkpoint_path="./write_stream_checkpoint")

    return query, writer_query


def validate_order_priority(streaming_df):

    with open("validation-values/order_priority") as f:
        data = f.read()
        valid_order_priority = data.split(", ")

    streaming_df_with_condition = streaming_df.withColumn(
        "is_condition_met", 
        F.when(F.col("order_priority").isin(valid_order_priority), True).otherwise(False)
    )

    invalid_order_priority_rows = streaming_df_with_condition.filter(F.col("is_condition_met") == False)
    query = get_streaming_query(invalid_order_priority_rows, type=STREAM_TYPE)
    writer_query = get_csv_writer_query(invalid_order_priority_rows, csv_path="./write_stream_csv", checkpoint_path="./write_stream_checkpoint")

    return query, writer_query


def validate_item_type(streaming_df):

    with open("validation-values/item_type") as f:
        data = f.read()
        valid_item_types = data.split(", ")

    streaming_df_with_condition = streaming_df.withColumn(
        "is_condition_met", 
        F.when(F.col("item_type").isin(valid_item_types), True).otherwise(False)
    )

    invalid_item_type_rows = streaming_df_with_condition.filter(F.col("is_condition_met") == False)
    query = get_streaming_query(invalid_item_type_rows, type=STREAM_TYPE)
    writer_query = get_csv_writer_query(invalid_item_type_rows, csv_path="./write_stream_csv", checkpoint_path="./write_stream_checkpoint")

    return query, writer_query


def validate_sales_channel(streaming_df):

    with open("validation-values/sales_channel") as f:
        data = f.read()
        valid_sales_channels = data.split(", ")

    streaming_df_with_condition = streaming_df.withColumn(
        "is_condition_met", 
        F.when(F.col("sales_channel").isin(valid_sales_channels), True).otherwise(False)
    )

    invalid_sales_channel_rows = streaming_df_with_condition.filter(F.col("is_condition_met") == False)
    query = get_streaming_query(invalid_sales_channel_rows, type=STREAM_TYPE)
    writer_query = get_csv_writer_query(invalid_sales_channel_rows, csv_path="./write_stream_csv", checkpoint_path="./write_stream_checkpoint")

    return query, writer_query


def validate_country(streaming_df):

    with open("validation-values/country") as f:
        data = f.read()
        valid_countries = data.split(", ")

    streaming_df_with_condition = streaming_df.withColumn(
        "is_condition_met", 
        F.when(F.col("country").isin(valid_countries), True).otherwise(False)
    )

    invalid_country_rows = streaming_df_with_condition.filter(F.col("is_condition_met") == False)
    query = get_streaming_query(invalid_country_rows, type=STREAM_TYPE)
    writer_query = get_csv_writer_query(invalid_country_rows, csv_path="./write_stream_csv", checkpoint_path="./write_stream_checkpoint")

    return query, writer_query



def perform_all_validations(streaming_df):

    all_validations = [validate_condition_cost,
                       validate_condition_price,
                       validate_revenue,
                       validate_dates,
                       validate_region,
                       validate_country,
                       validate_item_type,
                       validate_order_priority,
                       validate_sales_channel
                    ]
    
    queries = []
    writer_queries = []

    for validator in all_validations:

        query, writer_query = validator(streaming_df)
        queries.append(query)
        writer_queries.append(writer_query)
    
    return queries, writer_query