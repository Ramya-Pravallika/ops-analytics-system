# Intelligent Operations Analytics System
<img width="800" height="500" alt="image" src="https://github.com/user-attachments/assets/0ff9bd93-9acc-4652-9d73-eba9b6937e58" />
<img width="1000" height="600" alt="image" src="https://github.com/user-attachments/assets/f7845846-89f6-45ee-be11-f991c6995b48" />
<img width="1200" height="300" alt="image" src="https://github.com/user-attachments/assets/04b9e1ae-936b-4335-90b7-ec7c98cdc797" />
<img width="600" height="400" alt="image" src="https://github.com/user-attachments/assets/bc525510-82e8-4446-abda-b1a09246fd1f" />
<img width="1000" height="500" alt="image" src="https://github.com/user-attachments/assets/0a1d68d3-e88f-4bdf-aba2-8751eb02567f" />

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
