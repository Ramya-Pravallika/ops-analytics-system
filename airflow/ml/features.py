import pandas as pd
from sqlalchemy import create_engine
from airflow.scripts.utils import load_config, get_engine

def build_daily_features():
    cfg = load_config()
    engine = get_engine(cfg)

    res = pd.read_sql("SELECT team, ts, utilization FROM ops.resources", engine)
    tic = pd.read_sql("""
      SELECT service_id as team, created_ts::date as date, target_sla_minutes, actual_sla_minutes, met, severity
      FROM ops.sla_metrics
    """, engine)
    kpi = pd.read_sql("""
      SELECT function_id as team, kpi_name, kpi_value, ts::date as date
      FROM ops.kpi_tracking
    """, engine)

    # Resource features
    res["date"] = res["ts"].dt.date
    res_daily = res.groupby(["team","date"]).agg(avg_utilization=("utilization","mean")).reset_index()

    # Ticket features
    tic_daily = tic.groupby(["team","date"]).agg(
        total_tickets=("met","size"),
        sev1_count=("severity", lambda s: (s=="sev1").sum()),
        avg_sla_target=("target_sla_minutes","mean"),
        avg_sla_actual=("actual_sla_minutes","mean"),
        sla_breach_rate=("met", lambda s: 1 - s.mean())
    ).reset_index()

    # KPI features
    backlog = kpi[kpi["kpi_name"]=="backlog"].groupby(["team","date"]).agg(backlog=("kpi_value","mean")).reset_index()

    # Merge
    df = res_daily.merge(tic_daily, on=["team","date"], how="outer").merge(backlog, on=["team","date"], how="left")
    df.fillna({"backlog":0, "avg_utilization":0.0, "total_tickets":0, "sev1_count":0, "avg_sla_target":0, "avg_sla_actual":0, "sla_breach_rate":0}, inplace=True)

    # Write to table
    engine.execute("DELETE FROM ops.features_daily")
    df.to_sql("features_daily", engine, index=False, schema="ops", if_exists="append")
    print("Features built.")
