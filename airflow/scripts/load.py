import pandas as pd
from utils import write_df

def run():
    res = pd.read_parquet("/opt/airflow/scripts/clean_resources.parquet")
    tic = pd.read_parquet("/opt/airflow/scripts/clean_tickets.parquet")
    kpi = pd.read_parquet("/opt/airflow/scripts/clean_kpi.parquet")

    write_df(res, "resources")
    write_df(tic, "sla_metrics")
    write_df(kpi, "kpi_tracking")
    print("Load complete.")
