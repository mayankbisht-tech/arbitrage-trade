import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_PATH = os.path.join(BASE_DIR, "arbitrage.db")
ISOLATION_DB_PATH = os.path.join(BASE_DIR, "isolation_forest.db")


def get_conn():
    return sqlite3.connect(DB_PATH)


def get_isolation_conn():
    return sqlite3.connect(ISOLATION_DB_PATH)


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

    
    iso_conn = get_isolation_conn()
    iso_cursor = iso_conn.cursor()

    iso_cursor.execute("""
    CREATE TABLE IF NOT EXISTS isolation_features (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        symbol TEXT,
        net_spread_pct REAL,
        gross_spread_pct REAL,
        buy_liquidity REAL,
        sell_liquidity REAL,
        price_gap_absolute REAL,
        spread_velocity REAL,
        rolling_volatility REAL
    )
    """)

    
    iso_cursor.execute("PRAGMA table_info(isolation_features)")
    existing_columns = [col[1] for col in iso_cursor.fetchall()]

    if "anomaly_label" not in existing_columns:
        iso_cursor.execute(
            "ALTER TABLE isolation_features ADD COLUMN anomaly_label INTEGER"
        )
        print(" anomaly_label column added")
    else:
        print(" anomaly_label column already exists")

    iso_conn.commit()
    iso_conn.close()

if __name__ == "__main__":
    create_tables()
