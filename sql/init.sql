from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv("config/.env")
engine = create_engine(os.getenv("DB_URI"))

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS offsets (
            key TEXT PRIMARY KEY,
            value INTEGER
        );
    """))
    conn.execute(text("""
        INSERT OR IGNORE INTO offsets (key, value) VALUES ('transactions_offset', 0);
    """))

print("SQLite DB initialized.")
