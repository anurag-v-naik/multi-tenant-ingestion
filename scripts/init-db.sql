-- Database initialization script for PostgreSQL

-- Create database if it doesn't exist (this won't work in init script, but included for reference)
-- CREATE DATABASE multi_tenant_ingestion;

-- Connect to the database
\c multi_tenant_ingestion;

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Create enum types
CREATE TYPE pipeline_status AS ENUM (
    'created', 'active', 'paused', 'running',
    'completed', 'failed', 'deleted'
);

CREATE TYPE check_status AS ENUM (
    'created', 'running', 'completed', 'failed', 'cancelled'
);

CREATE TYPE rule_type AS ENUM (
    'not_null', 'range', 'format', 'uniqueness', 'completeness', 'custom_sql'
);

-- Grant permissions to app user
GRANT ALL PRIVILEGES ON DATABASE multi_tenant_ingestion TO app_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO app_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO app_user;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO app_user;

-- Set default privileges for future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO app_user;
