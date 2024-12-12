from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import StandardScaler


def main():
    spark = SparkSession.builder \
            .getOrCreate()
    
    df = spark.read.csv("/tmp/sales_data.csv", header=True, inferSchema=True)

    df = df.withColumn("Order_Year", year(col("Order Date"))) \
       .withColumn("Order_Month", month(col("Order Date")))
    
    categorical_columns = ["Region", "Country", "Sales Channel", "Order Priority"]
    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_Index") for col in categorical_columns]
    encoders = [OneHotEncoder(inputCol=f"{col}_Index", outputCol=f"{col}_Vec") for col in categorical_columns]

    feature_columns = ["Order_Year", "Order_Month"] + \
                  [f"{col}_Vec" for col in categorical_columns]
    
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    gbt = GBTRegressor(featuresCol="scaled_features", labelCol="Units Sold", predictionCol="Predicted_Units")

    pipeline = Pipeline(stages=indexers + encoders + [assembler, scaler, gbt])
    model = pipeline.fit(train_data)

    predictions = model.transform(test_data)
    evaluator = RegressionEvaluator(labelCol="Units Sold", predictionCol="Predicted_Units", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print(rmse)

    model.save("/tmp/units_model")

    spark.stop()

if __name__=="__main__":
    main()