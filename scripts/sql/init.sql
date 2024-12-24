-- init.sql

CREATE DATABASE airflow;

CREATE TABLE IF NOT EXISTS external_links (
    id SERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    crawl_data_version TEXT NOT NULL,
    extracted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
