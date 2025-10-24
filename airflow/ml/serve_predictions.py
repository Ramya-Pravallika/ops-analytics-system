import joblib
import pandas as pd
from datetime import datetime
from airflow.scripts.utils import load_config, get_engine

SLA_MODEL = "/opt/airflow/ml/models/sla_ridge.pkl"
ANOM_MODEL = "/opt/airflow/ml/models/anomaly_iforest.pkl"

def run():
    cfg = load_config()
    engine = get_engine(cfg)
    df = pd.read_sql("SELECT * FROM ops.features_daily", engine)

    # Prepare features like training
    df_enc = pd.get_dummies(df, columns=["team"], drop_first=True)
    feat_cols = ["total_tickets","sev1_count","avg_utilization","backlog","avg_sla_target","avg_sla_actual"] + [c for c in df_enc.columns if c.startswith("team_")]
    X = df_enc[feat_cols]

    sla_model = joblib.load(SLA_MODEL)
    anom_model = joblib.load(ANOM_MODEL)

    df["predicted_sla_breach_rate"] = sla_model.predict(X)

    # Use decision_function as anomaly score (higher negative -> more anomalous)
    X_anom = df[["total_tickets","sev1_count","avg_utilization","backlog","avg_sla_target","avg_sla_actual","sla_breach_rate"]]
    scores = anom_model.decision_function(X_anom)
    df["anomaly_score"] = scores

    pred = df[["date","team","predicted_sla_breach_rate","anomaly_score"]].copy()
    pred.to_sql("predictions", engine, index=False, schema="ops", if_exists="append")
    print(f"Served {len(pred)} predictions at {datetime.now()}.")
