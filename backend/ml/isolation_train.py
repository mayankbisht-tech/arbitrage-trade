import sqlite3
import pandas as pd
import joblib
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import logging

logging.basicConfig(
    filename='backend/ml/isolation_train.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)
logging.info("Isolation training script started")

DB_PATH = "backend/databases/isolation_forest.db"
MODEL_PATH = "backend/models/iso_model.pkl"
SCALER_PATH = "backend/models/iso_scaler.pkl"

FEATURE_COLS = [
    "net_spread_pct",
    "gross_spread_pct",
    "buy_liquidity",
    "sell_liquidity",
    "price_gap_absolute",
    "spread_velocity",
    "rolling_volatility"
]

def load_data(limit=50000):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(
        f"""
        SELECT {",".join(FEATURE_COLS)}
        FROM isolation_features
        ORDER BY timestamp DESC
        LIMIT {limit}
        """,
        conn
    )
    conn.close()
    return df.dropna()

def train():
    df = load_data()

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df)

    model = IsolationForest(
        n_estimators=200,
        contamination=0.05,
        random_state=42
    )

    model.fit(X_scaled)

    joblib.dump(model, MODEL_PATH)
    joblib.dump(scaler, SCALER_PATH)

    logging.info("Isolation Forest retrained successfully")

if __name__ == "__main__":
    train()