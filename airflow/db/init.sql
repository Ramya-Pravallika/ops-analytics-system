CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.resources (
  resource_id VARCHAR(64) PRIMARY KEY,
  resource_type VARCHAR(32),
  team VARCHAR(64),
  capacity NUMERIC,
  utilization NUMERIC,
  ts TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS ops.sla_metrics (
  service_id VARCHAR(64),
  ticket_id VARCHAR(64) PRIMARY KEY,
  target_sla_minutes INT,
  actual_sla_minutes INT,
  met BOOLEAN,
  severity VARCHAR(16),
  created_ts TIMESTAMP,
  closed_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ops.kpi_tracking (
  id SERIAL PRIMARY KEY,
  function_id VARCHAR(64),
  kpi_name VARCHAR(64),
  kpi_value NUMERIC,
  ts TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS ops.features_daily (
  id SERIAL PRIMARY KEY,
  date DATE,
  team VARCHAR(64),
  total_tickets INT,
  sev1_count INT,
  avg_utilization NUMERIC,
  backlog INT,
  avg_sla_target NUMERIC,
  avg_sla_actual NUMERIC,
  sla_breach_rate NUMERIC
);

CREATE TABLE IF NOT EXISTS ops.predictions (
  id SERIAL PRIMARY KEY,
  date DATE,
  team VARCHAR(64),
  predicted_sla_breach_rate NUMERIC,
  anomaly_score NUMERIC,
  created_ts TIMESTAMP DEFAULT NOW()
);
