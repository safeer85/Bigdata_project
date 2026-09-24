-- Airflow keeps its metadata in a SEPARATE database on the same server (SPEC 8.1).
-- Same server so the laptop runs one Postgres instead of two; separate database so
-- an `airflow db reset` can never touch the fleet tables the report is built from.
CREATE DATABASE airflow OWNER fleet;
