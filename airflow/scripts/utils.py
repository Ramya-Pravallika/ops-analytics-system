import os
import yaml
import pandas as pd
from sqlalchemy import create_engine
import pyodbc

def load_config():
    with open("/opt/airflow/config/config.yaml", "r") as f:
        cfg = yaml.safe_load(f)
    # env interpolation
    conn_str = os.environ.get("AZURE_SQL_CONN_STR")
    if conn_str:
        cfg["storage"]["azure_sql_odbc_conn_str"] = conn_str
    return cfg

def get_engine(cfg):
    target = cfg["storage"]["target"]
    if target == "postgres":
        return create_engine(cfg["storage"]["postgres_uri"])
    elif target == "azure_sql":
        # SQLAlchemy via pyodbc
        odbc = cfg["storage"]["azure_sql_odbc_conn_str"]
        return create_engine(f"mssql+pyodbc:///?odbc_connect={odbc}")
    else:
        raise ValueError("Unsupported storage target")

def write_df(df: pd.DataFrame, table: str, schema: str = "ops", if_exists="append"):
    cfg = load_config()
    engine = get_engine(cfg)
    df.to_sql(table, engine, index=False, schema=schema, if_exists=if_exists)
