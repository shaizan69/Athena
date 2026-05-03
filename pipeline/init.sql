-- Initialise the three medallion-architecture schemas.
-- This script runs automatically when the Postgres container starts for the first time.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
