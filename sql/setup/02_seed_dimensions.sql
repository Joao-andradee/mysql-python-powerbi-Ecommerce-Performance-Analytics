/*
Project: E-commerce Performance Analytics
Tech Stack: MySQL · Python · Power BI

File: 02-seed_dimensions.sql
Purpose:
- Populate dimension tables with reference data
- Ensure consistent descriptive attributes for analysis
- Support slicing and filtering in Power BI reports

Notes:
- Dimension data is deterministic and relatively static
- Fact tables are populated separately via ETL scripts

*/

USE ops_portfolio;

INSERT INTO dim_service (service_name, service_category) VALUES
('Payments', 'Core'),
('Login/Auth', 'Core'),
('Mobile App', 'Digital'),
('Website', 'Digital'),
('Reporting', 'Internal'),
('API Gateway', 'Core');

INSERT INTO dim_team (team_name, region) VALUES
('Ops North', 'Canada'),
('Ops East', 'Canada'),
('Ops West', 'Canada'),
('IT Support', 'Canada');

-- Simple agents (you can add more later)
INSERT INTO dim_agent (agent_name, team_id, hire_date) VALUES
('Alex Chen', 1, '2023-03-15'),
('Bruna Silva', 1, '2022-10-10'),
('Diego Santos', 2, '2021-05-03'),
('Emma Brown', 2, '2020-09-21'),
('Fatima Ali', 3, '2024-01-08'),
('Gustavo Lima', 4, '2022-06-30');