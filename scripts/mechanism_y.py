from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, percent_rank, current_timestamp
from pyspark.sql.window import Window
import boto3, uuid, os
import pandas as pd
from dotenv import load_dotenv

load_dotenv("config/.env")
S3_BUCKET = os.getenv("S3_BUCKET")

spark = SparkSession.builder.appName("MechanismY").getOrCreate()
importance_df = spark.read.option("header", True).csv("data/CustomerImportance.csv")

def detect_patterns():
    s3 = boto3.client('s3')
    objs = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="transactions_chunks/")
    keys = [obj['Key'] for obj in objs.get('Contents', []) if obj['Key'].endswith('.parquet')]

    for key in keys:
        df = spark.read.parquet(f"s3a://{S3_BUCKET}/{key}")
        df = df.join(importance_df, ['CustomerName', 'TransactionType'], 'left')

        results = []

        # Pattern 1
        tx_count = df.groupBy("MerchantId").agg(count("*").alias("tx_count"))
        df_tx = df.join(tx_count, "MerchantId").filter("tx_count > 50000")

        w1 = Window.partitionBy("MerchantId").orderBy(col("weight").asc())
        w2 = Window.partitionBy("MerchantId").orderBy(col("CustomerName"))

        bottom_1 = df_tx.withColumn("rank", percent_rank().over(w1)).filter("rank <= 0.01")
        top_customers = df_tx.groupBy("MerchantId", "CustomerName").count()
        top_1 = top_customers.withColumn("rank", percent_rank().over(w2)).filter("rank >= 0.99")
        pat1 = top_1.join(bottom_1, ["MerchantId", "CustomerName"], "inner").selectExpr(
            "current_timestamp() as detectionTime", "'' as YStartTime", 
            "'PatId1'", "'UPGRADE'", "CustomerName", "MerchantId"
        )

        # Pattern 2
        pat2 = df.groupBy("MerchantId", "CustomerName").agg(
            count("*").alias("tx_count"),
            avg("TransactionValue").alias("avg_val")
        ).filter("tx_count >= 80 AND avg_val < 23").selectExpr(
            "current_timestamp() as detectionTime", "'' as YStartTime", 
            "'PatId2'", "'CHILD'", "CustomerName", "MerchantId"
        )

        # Pattern 3
        gender = df.groupBy("MerchantId", "Gender").count().groupBy("MerchantId").pivot("Gender").sum("count")
        pat3 = gender.filter("Female < Male").selectExpr(
            "current_timestamp() as detectionTime", "'' as YStartTime", 
            "'PatId3'", "'DEI-NEEDED'", "'' as CustomerName", "MerchantId"
        )

        detections = pat1.union(pat2).union(pat3).collect()
        for i in range(0, len(detections), 50):
            subset = detections[i:i+50]
            output_path = f"detections/detection_{uuid.uuid4()}.parquet"
            pd.DataFrame(subset).to_parquet(f"/tmp/{output_path.split('/')[-1]}")
            boto3.client('s3').upload_file(f"/tmp/{output_path.split('/')[-1]}", S3_BUCKET, output_path)


if __name__=="__main__":
    detect_patterns()