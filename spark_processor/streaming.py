import os
from xml.parsers.expat import model
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import joblib

def main():
    broker = os.environ.get("KAFKA_BROKER", "localhost:9092")
    topic_name = "financial_transactions"
    model_path = "/app/model/iforest.joblib"
    
    print("Initializing PySpark Session...")
    spark = SparkSession.builder \
        .appName("FraudDetectionStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Define schema for PaySim JSON
    schema = StructType([
        StructField("step", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("nameOrig", StringType(), True),
        StructField("oldbalanceOrg", DoubleType(), True),
        StructField("newbalanceOrig", DoubleType(), True),
        StructField("nameDest", StringType(), True),
        StructField("oldbalanceDest", DoubleType(), True),
        StructField("newbalanceDest", DoubleType(), True),
        StructField("isFraud", IntegerType(), True),
        StructField("isFlaggedFraud", IntegerType(), True)
    ])

    print("Connecting to Kafka...")
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", broker) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    model = joblib.load(model_path)
    broadcast_model = spark.sparkContext.broadcast(model)

    @pandas_udf(IntegerType())
    def detect_anomaly(
        amount: pd.Series,
        oldbalanceOrg: pd.Series,
        newbalanceOrig: pd.Series,
        oldbalanceDest: pd.Series,
        newbalanceDest: pd.Series,
        type_col: pd.Series
    ) -> pd.Series:
        
        # Load model for this batch. Using joblib inside the UDF.
        model = broadcast_model.value
        
        # Reconstruct DataFrame
        pdf = pd.DataFrame({
            'amount': amount,
            'oldbalanceOrg': oldbalanceOrg,
            'newbalanceOrig': newbalanceOrig,
            'oldbalanceDest': oldbalanceDest,
            'newbalanceDest': newbalanceDest,
            'type': type_col
        })
        
        # Predict (-1 is anomaly, 1 is normal)
        preds = model.predict(pdf)
        return pd.Series([1 if p == -1 else 0 for p in preds])

    # Apply Pandas UDF
    predictions_df = parsed_df.withColumn("prediction", detect_anomaly(
        col("amount"), col("oldbalanceOrg"), col("newbalanceOrig"), 
        col("oldbalanceDest"), col("newbalanceDest"), col("type")
    ))

    # Keep only anomalies
    anomalies_df = predictions_df.filter(col("prediction") == 1)

    # Write anomalies to a single CSV via foreachBatch so Dashboard can read it simply
    def write_to_csv(batch_df, batch_id):
        if not batch_df.isEmpty():
            pdf = batch_df.toPandas()
            csv_file = "/app/data/anomalies.csv"
            write_header = not os.path.exists(csv_file)
            pdf.to_csv(csv_file, mode='a', header=write_header, index=False)
            print(f"[{batch_id}] Wrote {len(pdf)} detected anomalies -> anomalies.csv")

    print("Starting Streaming Query...")
    query = anomalies_df.writeStream \
        .foreachBatch(write_to_csv) \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
