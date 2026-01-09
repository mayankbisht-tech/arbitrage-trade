import ccxt
import time
import sys
import os
import numpy as np

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(ROOT_DIR)

from backend.databases.db import get_conn, get_isolation_conn


class DataCollector:
    def __init__(self):
        self.symbols = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'AVAX/USDT', 'LINK/USDT']


        self.binance = ccxt.binance({'enableRateLimit': True})
        self.kucoin = ccxt.kucoin({'enableRateLimit': True})

        self.previous_data = {}
        self.price_history = {symbol: [] for symbol in self.symbols}

        self.rows_added = 0
        self.print_counter = 0

        self.FEE_PCT = 0.2  

    def fetch_complete_data(self):
        timestamp = int(time.time() * 1000)  
        events = []
        iso_features = []

        for symbol in self.symbols:
            try:
                b_ob = self.binance.fetch_order_book(symbol, limit=1)
                k_ob = self.kucoin.fetch_order_book(symbol, limit=20)

                buy_price, buy_liquidity = b_ob['asks'][0]
                sell_price, sell_liquidity = k_ob['bids'][0]

                gross_spread = (sell_price - buy_price) / buy_price * 100
                net_spread = gross_spread - self.FEE_PCT
                price_gap_absolute = abs(sell_price - buy_price)

                decision = "EXECUTE" if net_spread > 0 else "IGNORE"
                decision_reason = (
                    f"Buy at {buy_price}, Sell at {sell_price}, Spread: {net_spread:.2f}%"
                    if decision == "EXECUTE"
                    else "No profit"
                )

                events.append({
                    "timestamp": timestamp,
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

                spread_velocity = 0
                if symbol in self.previous_data:
                    spread_velocity = net_spread - self.previous_data[symbol]

                self.previous_data[symbol] = net_spread

                self.price_history[symbol].append(buy_price)
                if len(self.price_history[symbol]) > 10:
                    self.price_history[symbol].pop(0)

                rolling_volatility = (
                    np.std(self.price_history[symbol])
                    if len(self.price_history[symbol]) > 1
                    else 0
                )

                iso_features.append({
                    "timestamp": timestamp,
                    "symbol": symbol,
                    "net_spread_pct": round(net_spread, 4),
                    "gross_spread_pct": round(gross_spread, 4),
                    "buy_liquidity": buy_liquidity,
                    "sell_liquidity": sell_liquidity,
                    "price_gap_absolute": price_gap_absolute,
                    "spread_velocity": round(spread_velocity, 4),
                    "rolling_volatility": round(rolling_volatility, 4)
                })

            except Exception as e:
                print(f" {symbol}: {e}")
                time.sleep(1)
                continue

        return events, iso_features

    def insert_arbitrage_data(self, events):
        try:
            conn = get_conn()
            cursor = conn.cursor()

            for event in events:
                cursor.execute("""
                    INSERT INTO arbitrage_events (
                        timestamp, symbol, buy_exchange, sell_exchange,
                        buy_price, sell_price, buy_liquidity, sell_liquidity,
                        gross_spread, net_spread, decision, decision_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    event['timestamp'], event['symbol'],
                    event['buy_exchange'], event['sell_exchange'],
                    event['buy_price'], event['sell_price'],
                    event['buy_liquidity'], event['sell_liquidity'],
                    event['gross_spread'], event['net_spread'],
                    event['decision'], event['decision_reason']
                ))

            conn.commit()
            conn.close()
        except Exception as e:
            print(f"arbitrage_events: {e}")

    def insert_isolation_data(self, features):
        try:
            conn = get_isolation_conn()
            cursor = conn.cursor()

            for feature in features:
                cursor.execute("""
                    INSERT INTO isolation_features (
                        timestamp, symbol, net_spread_pct, gross_spread_pct,
                        buy_liquidity, sell_liquidity, price_gap_absolute,
                        spread_velocity, rolling_volatility
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    feature['timestamp'], feature['symbol'],
                    feature['net_spread_pct'], feature['gross_spread_pct'],
                    feature['buy_liquidity'], feature['sell_liquidity'],
                    feature['price_gap_absolute'],
                    feature['spread_velocity'],
                    feature['rolling_volatility']
                ))

            conn.commit()
            conn.close()
        except Exception as e:
            print(f"isolation_features: {e}")

    def run(self):
        while True:
            events, iso_features = self.fetch_complete_data()

            if events and iso_features:
                self.insert_arbitrage_data(events)
                self.insert_isolation_data(iso_features)

                self.rows_added += len(events)
                self.print_counter += 1

                if self.print_counter % 10 == 0:
                    print(f" Total rows added: {self.rows_added}")

            time.sleep(2)


if __name__ == "__main__":
    collector = DataCollector()
    collector.run()