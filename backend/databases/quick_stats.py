import sys
import os

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(ROOT_DIR)

from backend.databases.db import get_conn, get_isolation_conn

def stats():
    conn = get_conn()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM arbitrage_events")
    total = cursor.fetchone()[0]
    print(f"Total: {total}")
    
    if total > 0:
        cursor.execute("SELECT decision, COUNT(*) FROM arbitrage_events GROUP BY decision")
        for decision, count in cursor.fetchall():
            print(f"{decision}: {count}")
        
        cursor.execute("SELECT symbol, COUNT(*) FROM arbitrage_events GROUP BY symbol")
        for symbol, count in cursor.fetchall():
            print(f"{symbol}: {count}")
    
    conn.close()
    
    iso_conn = get_isolation_conn()
    iso_cursor = iso_conn.cursor()
    
    iso_cursor.execute("SELECT COUNT(*) FROM isolation_features")
    iso_total = iso_cursor.fetchone()[0]
    print(f"Total: {iso_total}")
    
    if iso_total > 0:
        iso_cursor.execute("SELECT symbol, COUNT(*) FROM isolation_features GROUP BY symbol")
        for symbol, count in iso_cursor.fetchall():
            print(f"{symbol}: {count}")
    
    iso_cursor.execute("SELECT * FROM isolation_features LIMIT 5")
    rows = cursor.fetchall()

    for row in rows:
        print(row)
    
    
    iso_conn.close()

if __name__ == "__main__":
    stats()