-- Raw SQL Schema for reference (created automatically by SQLAlchemy)

CREATE TABLE IF NOT EXISTS dim_pages (
    page_id SERIAL PRIMARY KEY,
    page_title VARCHAR NOT NULL UNIQUE,
    category VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_pageviews (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    page_id INTEGER NOT NULL REFERENCES dim_pages(page_id),
    views INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_metrics (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    page_id INTEGER NOT NULL REFERENCES dim_pages(page_id),
    rolling_7d_avg FLOAT,
    rolling_30d_avg FLOAT,
    growth_rate_daily FLOAT,
    growth_rate_weekly FLOAT,
    stl_residual FLOAT,
    anomaly_flag BOOLEAN DEFAULT FALSE,
    anomaly_severity VARCHAR
);

CREATE TABLE IF NOT EXISTS fact_experiments (
    run_id VARCHAR PRIMARY KEY,
    experiment_date DATE DEFAULT CURRENT_DATE,
    metric_name VARCHAR NOT NULL,
    effect_size FLOAT,
    p_value FLOAT,
    confidence_interval_lower FLOAT,
    confidence_interval_upper FLOAT,
    conclusion VARCHAR,
    power_analysis FLOAT
);
