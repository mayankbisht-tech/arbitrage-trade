import sys
import os

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(ROOT_DIR)

from backend.databases.db import get_conn

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
        
        cursor.execute("SELECT timestamp, decision, net_spread FROM arbitrage_events ORDER BY id DESC LIMIT 1")
        latest = cursor.fetchone()
        if latest:
            from datetime import datetime
            dt = datetime.fromtimestamp(latest[0]).strftime('%H:%M:%S')
            print(f"Latest: {dt} | {latest[1]} | {latest[2]}%")
    
    conn.close()

if __name__ == "__main__":
    stats()