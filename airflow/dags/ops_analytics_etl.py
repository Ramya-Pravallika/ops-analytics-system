from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pathlib import Path

import sys
sys.path.append("/opt/airflow/scripts")
sys.path.append("/opt/airflow/ml")

from generate_synthetic_data import run as gen_data
from extract import run as extract_run
from transform import run as transform_run
from load import run as load_run
from features import build_daily_features
from train_sla_model import train as train_sla
from train_anomaly_model import train as train_anomaly
from serve_predictions import run as serve_preds

default_args = {"start_date": datetime(2025, 1, 1)}

with DAG("ops_analytics_etl", schedule="@daily", catchup=False, default_args=default_args) as dag:
    generate = PythonOperator(task_id="generate_synthetic_data", python_callable=gen_data)
    extract = PythonOperator(task_id="extract", python_callable=extract_run)
    transform = PythonOperator(task_id="transform", python_callable=transform_run)
    load = PythonOperator(task_id="load", python_callable=load_run)
    features = PythonOperator(task_id="features", python_callable=build_daily_features)
    train_sla_model = PythonOperator(task_id="train_sla_model", python_callable=train_sla)
    train_anomaly = PythonOperator(task_id="train_anomaly_model", python_callable=train_anomaly)
    serve = PythonOperator(task_id="serve_predictions", python_callable=serve_preds)

    generate >> extract >> transform >> load >> features >> [train_sla_model, train_anomaly] >> serve
