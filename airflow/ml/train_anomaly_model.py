import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest
from airflow.scripts.utils import load_config, get_engine

MODEL_PATH = "/opt/airflow/ml/models/anomaly_iforest.pkl"

def train():
    cfg = load_config()
    engine = get_engine(cfg)
    df = pd.read_sql("SELECT * FROM ops.features_daily", engine)

    X = df[["total_tickets","sev1_count","avg_utilization","backlog","avg_sla_target","avg_sla_actual","sla_breach_rate"]]
    model = IsolationForest(contamination=cfg["ml"]["isolation_forest"]["contamination"], random_state=cfg["ml"]["random_state"])
    model.fit(X)

    import os
    os.makedirs("/opt/airflow/ml/models", exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    print("Anomaly model trained.")
