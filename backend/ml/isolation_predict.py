import sqlite3
import time
import pandas as pd
import joblib
import logging

logging.basicConfig(
    filename='backend/ml/isolation_train.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

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

model = joblib.load(MODEL_PATH)
scaler = joblib.load(SCALER_PATH)
last_reload = time.time()

logging.info("Isolation prediction service started")


def fetch_latest_unlabeled(limit=20):
    conn = sqlite3.connect(DB_PATH)

    df = pd.read_sql(
        f"""
        SELECT id, {",".join(FEATURE_COLS)}
        FROM isolation_features
        WHERE anomaly_label IS NULL
        ORDER BY timestamp ASC
        LIMIT {limit}
        """,
        conn
    )

    conn.close()
    return df


def update_predictions(df):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute(
            "UPDATE isolation_features SET anomaly_label=? WHERE id=?",
            (int(row["prediction"]), int(row["id"]))
        )

    conn.commit()
    conn.close()


def reload_model_if_needed():
    global model, scaler, last_reload

    if time.time() - last_reload >= 21600:  
        model = joblib.load(MODEL_PATH)
        scaler = joblib.load(SCALER_PATH)
        last_reload = time.time()
        logging.info("Isolation model reloaded")


def run():
    while True:
        reload_model_if_needed()

        df = fetch_latest_unlabeled()

        if not df.empty:
            X = df[FEATURE_COLS]
            X_scaled = scaler.transform(X)

            preds = model.predict(X_scaled)
            df["prediction"] = preds

            update_predictions(df)
            logging.info(f"Predicted {len(df)} rows")

        time.sleep(5)


if __name__ == "__main__":
    run()
