import pandas as pd

def run():
    # In real use, call APIs/DBs here. Using synthetic CSVs.
    df_resources = pd.read_csv("/opt/airflow/scripts/data_resources.csv", parse_dates=["ts"])
    df_tickets = pd.read_csv("/opt/airflow/scripts/data_tickets.csv", parse_dates=["created_ts","closed_ts"])
    df_kpi = pd.read_csv("/opt/airflow/scripts/data_kpi.csv", parse_dates=["ts"])

    df_resources.to_parquet("/opt/airflow/scripts/extracted_resources.parquet", index=False)
    df_tickets.to_parquet("/opt/airflow/scripts/extracted_tickets.parquet", index=False)
    df_kpi.to_parquet("/opt/airflow/scripts/extracted_kpi.parquet", index=False)

    print("Extract complete.")
