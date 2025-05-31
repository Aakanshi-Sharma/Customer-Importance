import pandas as pd
import time, uuid, os
from sqlalchemy import create_engine
import boto3
from dotenv import load_dotenv

load_dotenv("config/.env")

DB_URI = os.getenv("DB_URI")
S3_BUCKET = os.getenv("S3_BUCKET")

def upload_transaction_chunk():
    conn = create_engine(DB_URI)
    offset_df = pd.read_sql("SELECT value FROM offsets WHERE key='transactions_offset'", conn)
    offset = int(offset_df.iloc[0]['value'])

    df = pd.read_csv("data/transactions.csv", skiprows=range(1, offset+1), nrows=10000)
    if df.empty:
        return
    timestamp = int(time.time())
    chunk_file = f"chunk_{timestamp}.parquet"
    tmp_dir = "tmp"
    os.makedirs(tmp_dir, exist_ok=True)
    local_path = os.path.join(tmp_dir, chunk_file)
    s3_path = f"transactions_chunks/{chunk_file}"
    df.to_parquet(local_path)
    boto3.client('s3').upload_file(local_path, S3_BUCKET, s3_path)

    conn.execute("UPDATE offsets SET value = %s WHERE key='transactions_offset'", (offset + len(df),))


if __name__=="__main__":
    upload_transaction_chunk()