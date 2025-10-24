# Intelligent Operations Analytics System

## Quickstart
1. Ensure Docker is running.
2. `docker-compose up -d`
3. Open Airflow at http://localhost:8080 (user/pass: airflow/airflow if prompted).
4. Trigger DAG `ops_analytics_etl` (it will generate data, ETL, train models, and write predictions).

## Switch to Azure SQL
- Set storage.target in config/config.yaml to `azure_sql`.
- Provide AZURE_SQL_CONN_STR in `.env` and ensure ODBC Driver 18 is installed in your environment.
- Re-run the DAG.

## Troubleshooting
- If models not found, ensure train tasks ran before serve_predictions.
- If Power BI cannot connect, confirm Postgres port 5432 is open and tables exist under schema `ops`.

## Extending
- Replace synthetic generators with real extractors (APIs/DB queries).
- Add Airflow alerts via EmailOperator/SlackWebhookOperator for anomalies.
- Schedule retraining weekly and serve daily.
