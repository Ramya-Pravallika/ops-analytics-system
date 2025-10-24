import pandas as pd

def run():
    res = pd.read_parquet("/opt/airflow/scripts/extracted_resources.parquet")
    tic = pd.read_parquet("/opt/airflow/scripts/extracted_tickets.parquet")
    kpi = pd.read_parquet("/opt/airflow/scripts/extracted_kpi.parquet")

    # Normalize resource utilization
    res["utilization"] = res["utilization"].clip(0, 1)

    # Ensure SLA minutes positive
    tic["actual_sla_minutes"] = tic["actual_sla_minutes"].abs().clip(lower=1)

    # KPI clean
    kpi["kpi_value"] = pd.to_numeric(kpi["kpi_value"], errors="coerce")

    res.to_parquet("/opt/airflow/scripts/clean_resources.parquet", index=False)
    tic.to_parquet("/opt/airflow/scripts/clean_tickets.parquet", index=False)
    kpi.to_parquet("/opt/airflow/scripts/clean_kpi.parquet", index=False)
    print("Transform complete.")
