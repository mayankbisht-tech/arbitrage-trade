import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'arbitrage.db')

def get_conn():
    return sqlite3.connect(DB_PATH)

def create_tables():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS arbitrage_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        symbol TEXT,
        buy_exchange TEXT,
        sell_exchange TEXT,
        buy_price REAL,
        sell_price REAL,
        buy_liquidity REAL,
        sell_liquidity REAL,
        gross_spread REAL,
        net_spread REAL,
        decision TEXT,
        decision_reason TEXT
    )
    """)
    conn.commit()
    conn.close()

def clear_database():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM arbitrage_events")
    conn.commit()
    conn.close()
    print("Database cleared")