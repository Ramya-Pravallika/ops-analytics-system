import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def run():
    np.random.seed(42)
    days = 60
    teams = ["Ops-A", "Ops-B", "Ops-C"]
    severities = ["sev1", "sev2", "sev3"]
    start = datetime.now() - timedelta(days=days)

    # Resources
    resources = []
    for d in range(days):
        ts = start + timedelta(days=d)
        for t in teams:
            for i in range(10):
                res_id = f"{t}-R{i}"
                utilization = np.clip(np.random.normal(0.65, 0.15), 0.1, 1.0)
                resources.append([res_id, "agent", t, 1.0, utilization, ts])
    df_resources = pd.DataFrame(resources, columns=["resource_id","resource_type","team","capacity","utilization","ts"])
    df_resources.to_csv("/opt/airflow/scripts/data_resources.csv", index=False)

    # Tickets & SLA
    tickets = []
    for d in range(days):
        day = start + timedelta(days=d)
        for t in teams:
            vol = np.random.poisson(60 if t == "Ops-A" else 45)
            for k in range(vol):
                sev = np.random.choice(severities, p=[0.1, 0.3, 0.6])
                target = 60 if sev=="sev1" else (240 if sev=="sev2" else 720)
                actual = int(np.random.normal(target * np.random.uniform(0.8, 1.3), 30))
                met = actual <= target
                ticket_id = f"{t}-{day.strftime('%Y%m%d')}-{k}"
                created = day + timedelta(minutes=np.random.randint(0, 1440))
                closed = created + timedelta(minutes=max(actual, 5))
                tickets.append([t, ticket_id, target, actual, met, sev, created, closed])
    df_tickets = pd.DataFrame(tickets, columns=["service_id","ticket_id","target_sla_minutes","actual_sla_minutes","met","severity","created_ts","closed_ts"])
    df_tickets.to_csv("/opt/airflow/scripts/data_tickets.csv", index=False)

    # KPI
    kpi = []
    for d in range(days):
        ts = start + timedelta(days=d)
        for t in teams:
            backlog = np.random.poisson(80 if t=="Ops-A" else 50)
            csat = np.clip(np.random.normal(85, 5), 60, 99)
            kpi.append([t, "backlog", backlog, ts])
            kpi.append([t, "csat", csat, ts])
    df_kpi = pd.DataFrame(kpi, columns=["function_id","kpi_name","kpi_value","ts"])
    df_kpi.to_csv("/opt/airflow/scripts/data_kpi.csv", index=False)

    print("Synthetic data generated.")
