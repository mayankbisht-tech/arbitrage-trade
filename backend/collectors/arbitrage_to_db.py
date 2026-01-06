import ccxt
import time
import sys
import os

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(ROOT_DIR)

from backend.databases.db import get_conn

def insertRow(event):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO arbitrage_events (
            timestamp, symbol, buy_exchange, sell_exchange,
            buy_price, sell_price, buy_liquidity, sell_liquidity,
            gross_spread, net_spread, decision, decision_reason
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        event['timestamp'], event['symbol'], event['buy_exchange'], event['sell_exchange'],
        event['buy_price'], event['sell_price'], event['buy_liquidity'], event['sell_liquidity'],
        event['gross_spread'], event['net_spread'], event['decision'], event['decision_reason']
    ))
    conn.commit()
    conn.close()

def fetch_data():
    binance = ccxt.binance()
    kucoin = ccxt.kucoin()
    
    symbols = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'AVAX/USDT', 'LINK/USDT']
    events = []
    
    for symbol in symbols:
        try:
            b_ob = binance.fetch_order_book(symbol)
            k_ob = kucoin.fetch_order_book(symbol)
            
            buy_price = b_ob['asks'][0][0]
            buy_liquidity = b_ob['asks'][0][1]
            sell_price = k_ob['bids'][0][0]
            sell_liquidity = k_ob['bids'][0][1]
            
            gross_spread = (sell_price - buy_price) / buy_price * 100
            net_spread = gross_spread - 0.2
            
            if net_spread > 0:
                decision = "EXECUTE"
                decision_reason = f"Buy at {buy_price}, Sell at {sell_price}, Spread: {net_spread:.2f}%"
            else:
                decision = "IGNORE"
                decision_reason = "No profit"
            
            events.append({
                "timestamp": int(time.time()),
                "symbol": symbol,
                "buy_exchange": "Binance",
                "sell_exchange": "KuCoin",
                "buy_price": buy_price,
                "sell_price": sell_price,
                "buy_liquidity": buy_liquidity,
                "sell_liquidity": sell_liquidity,
                "gross_spread": round(gross_spread, 4),
                "net_spread": round(net_spread, 4),
                "decision": decision,
                "decision_reason": decision_reason
            })
        except:
            pass
    
    return events

def main():
    while True:
        events = fetch_data()
        for event in events:
            insertRow(event)
        time.sleep(2)

if __name__ == "__main__":
    main()