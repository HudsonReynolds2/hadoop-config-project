-- Runs once on first Postgres startup via docker-entrypoint-initdb.d
-- Creates the user and database Hive's schematool will populate.

CREATE USER hive WITH PASSWORD 'hive';
CREATE DATABASE metastore OWNER hive;
GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;
