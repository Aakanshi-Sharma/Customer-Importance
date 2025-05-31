from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv("config/.env")
engine = create_engine(os.getenv("DB_URI"))

def get_offset():
    return pd.read_sql("SELECT value FROM offsets WHERE key='transactions_offset'", engine).iloc[0]['value']

def update_offset(new_value):
    with engine.begin() as conn:
        conn.execute("UPDATE offsets SET value = %s WHERE key='transactions_offset'", (new_value,))
