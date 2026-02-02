/*
Project: E-commerce Performance Analytics
Tech Stack: MySQL · Python · Power BI

File: 01-db.sql
Purpose:
- Create the analytics database
- Define core fact and dimension tables
- Establish primary and foreign key relationships
- Lay the foundation for a star schema data model

Notes:
- Tables are designed for analytical workloads
- Business logic and aggregations are handled in downstream views
*/

CREATE DATABASE IF NOT EXISTS ops_portfolio;
USE ops_portfolio;

-- Dimensions
CREATE TABLE dim_date (
  date_key INT PRIMARY KEY,           -- YYYYMMDD
  date_value DATE NOT NULL,
  year INT NOT NULL,
  month INT NOT NULL,
  month_name VARCHAR(15) NOT NULL,
  week_of_year INT NOT NULL,
  day_of_week INT NOT NULL,           -- 1=Mon..7=Sun (we'll fill accordingly)
  day_name VARCHAR(15) NOT NULL
);

CREATE TABLE dim_service (
  service_id INT AUTO_INCREMENT PRIMARY KEY,
  service_name VARCHAR(50) NOT NULL,
  service_category VARCHAR(50) NOT NULL
);

CREATE TABLE dim_team (
  team_id INT AUTO_INCREMENT PRIMARY KEY,
  team_name VARCHAR(50) NOT NULL,
  region VARCHAR(30) NOT NULL
);

CREATE TABLE dim_agent (
  agent_id INT AUTO_INCREMENT PRIMARY KEY,
  agent_name VARCHAR(60) NOT NULL,
  team_id INT NOT NULL,
  hire_date DATE NOT NULL,
  FOREIGN KEY (team_id) REFERENCES dim_team(team_id)
);

-- Facts
CREATE TABLE fact_tickets (
  ticket_id BIGINT PRIMARY KEY,
  date_key INT NOT NULL,
  service_id INT NOT NULL,
  team_id INT NOT NULL,
  agent_id INT NOT NULL,
  priority VARCHAR(10) NOT NULL,       -- Low/Med/High/Critical
  status VARCHAR(15) NOT NULL,         -- Open/Resolved
  created_ts DATETIME NOT NULL,
  resolved_ts DATETIME NULL,
  resolution_minutes INT NULL,
  reopened_flag TINYINT NOT NULL DEFAULT 0,
  csat_score TINYINT NULL,             -- 1..5
  FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
  FOREIGN KEY (service_id) REFERENCES dim_service(service_id),
  FOREIGN KEY (team_id) REFERENCES dim_team(team_id),
  FOREIGN KEY (agent_id) REFERENCES dim_agent(agent_id)
);

CREATE TABLE fact_incidents (
  incident_id BIGINT PRIMARY KEY,
  date_key INT NOT NULL,
  service_id INT NOT NULL,
  team_id INT NOT NULL,
  severity VARCHAR(10) NOT NULL,       -- Sev1..Sev4
  duration_minutes INT NOT NULL,
  customers_affected INT NOT NULL,
  sla_met TINYINT NOT NULL,            -- 0/1
  FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
  FOREIGN KEY (service_id) REFERENCES dim_service(service_id),
  FOREIGN KEY (team_id) REFERENCES dim_team(team_id)
);
