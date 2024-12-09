from pyspark.sql.functions import col, round
from pyspark.sql import functions as F


def validate_condition_cost(streaming_df):

    condition_cost = streaming_df.withColumn(
        "is_condition_met",
        (round(col("units_sold") * col("unit_cost"), 2) == col("total_cost"))
    )
    valid_rows = condition_cost.filter(col("is_condition_met") == True)
    valid_rows = valid_rows.drop("is_condition_met")
    return valid_rows

def validate_condition_price(streaming_df):

    condition_price = streaming_df.withColumn(
        "is_condition_met",
        (round(col("units_sold") * col("unit_price"), 2) == col("total_revenue"))
    )
    valid_rows = condition_price.filter(col("is_condition_met") == True)
    valid_rows = valid_rows.drop("is_condition_met")
    return valid_rows

def validate_revenue(streaming_df):

    condition_revenue = streaming_df.withColumn(
        "is_condition_met",
        (round(col("total_revenue") - col("total_cost"), 2) == col("total_profit"))
    )
    valid_rows = condition_revenue.filter(col("is_condition_met") == True)
    valid_rows = valid_rows.drop("is_condition_met")
    return valid_rows

def validate_dates(streaming_df):

    condition_date = streaming_df.withColumn(
        "is_condition_met",
        (col("ship_date") >= col("order_date"))
    )
    valid_rows = condition_date.filter(col("is_condition_met") == True)
    valid_rows = valid_rows.drop("is_condition_met")
    return valid_rows

def validate_region(streaming_df):

    with open("validation-values/region") as f:
        data = f.read()
        valid_regions = data.split(", ")

    streaming_df_with_condition = streaming_df.withColumn(
        "is_condition_met", 
        F.when(F.col("region").isin(valid_regions), True).otherwise(False)
    )

    valid_rows = streaming_df_with_condition.filter(F.col("is_condition_met") == True)
    valid_rows = valid_rows.drop("is_condition_met")
    return valid_rows

def validate_order_priority(streaming_df):

    with open("validation-values/order_priority") as f:
        data = f.read()
        valid_order_priority = data.split(", ")

    streaming_df_with_condition = streaming_df.withColumn(
        "is_condition_met", 
        F.when(F.col("order_priority").isin(valid_order_priority), True).otherwise(False)
    )

    valid_rows = streaming_df_with_condition.filter(F.col("is_condition_met") == True)
    valid_rows = valid_rows.drop("is_condition_met")
    return valid_rows

def validate_item_type(streaming_df):

    with open("validation-values/item_type") as f:
        data = f.read()
        valid_item_types = data.split(", ")

    streaming_df_with_condition = streaming_df.withColumn(
        "is_condition_met", 
        F.when(F.col("item_type").isin(valid_item_types), True).otherwise(False)
    )

    valid_rows = streaming_df_with_condition.filter(F.col("is_condition_met") == True)
    valid_rows = valid_rows.drop("is_condition_met")
    return valid_rows

def validate_sales_channel(streaming_df):

    with open("validation-values/sales_channel") as f:
        data = f.read()
        valid_sales_channels = data.split(", ")

    streaming_df_with_condition = streaming_df.withColumn(
        "is_condition_met", 
        F.when(F.col("sales_channel").isin(valid_sales_channels), True).otherwise(False)
    )

    valid_rows = streaming_df_with_condition.filter(F.col("is_condition_met") == True)
    valid_rows = valid_rows.drop("is_condition_met")
    return valid_rows

def validate_country(streaming_df):

    with open("validation-values/country") as f:
        data = f.read()
        valid_countries = data.split(", ")

    streaming_df_with_condition = streaming_df.withColumn(
        "is_condition_met", 
        F.when(F.col("country").isin(valid_countries), True).otherwise(False)
    )

    valid_rows = streaming_df_with_condition.filter(F.col("is_condition_met") == True)
    valid_rows = valid_rows.drop("is_condition_met")
    return valid_rows

def perform_all_validations(streaming_df):

        all_validations = [
            validate_condition_cost,
            validate_condition_price,
            validate_revenue,
            validate_dates,
            validate_region,
            validate_country,
            validate_item_type,
            validate_order_priority,
            validate_sales_channel
        ]

        valid_rows = streaming_df
        for validator in all_validations:
            valid_rows = validator(valid_rows)

        return valid_rows