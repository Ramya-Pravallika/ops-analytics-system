import joblib
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_absolute_error
from airflow.scripts.utils import load_config, get_engine

MODEL_PATH = "/opt/airflow/ml/models/sla_ridge.pkl"

def train():
    cfg = load_config()
    engine = get_engine(cfg)
    df = pd.read_sql("SELECT * FROM ops.features_daily", engine)

    # Encode team
    df = pd.get_dummies(df, columns=["team"], drop_first=True)

    X = df[["total_tickets","sev1_count","avg_utilization","backlog","avg_sla_target","avg_sla_actual"] + [c for c in df.columns if c.startswith("team_")]]
    y = df["sla_breach_rate"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=cfg["ml"]["test_size"], random_state=cfg["ml"]["random_state"])
    model = Ridge(alpha=1.0)
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    print(f"R2: {r2_score(y_test, preds):.3f}, MAE: {mean_absolute_error(y_test, preds):.3f}")

    import os
    os.makedirs("/opt/airflow/ml/models", exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    print("SLA model trained.")
