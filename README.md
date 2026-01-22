# Product Growth Analytics + Experimentation Engine

An end-to-end production-ready analytics system that pulls Wikipedia pageviews, detects anomalies, simulates A/B tests, and generates executive reports.

## Features
- **Daily Ingestion**: Fetches pageview data for top tech topics from Wikipedia.
- **Warehouse**: Stores structured data in PostgreSQL.
- **Analytics**: Calculates growth rates, rolling averages, and detects anomalies using STL decomposition.
- **Experimentation**: Simulates A/B testing on growth metrics.
- **Reporting**: Generates datasets for BI tools and automated executive summaries.

## Setup

1.  **Prerequisites**:
    - Python 3.11+
    - Docker & Docker Compose

2.  **Installation**:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Database**:
    ```bash
    docker-compose up -d
    ```

4.  **Run Pipeline**:
    ```bash
    python src/pipelines/daily_etl.py
    ```

## Project Structure
- `src/ingestion`: Data fetching logic.
- `src/warehouse`: Database models and connection.
- `src/pipelines`: Core analytics and ETL logic.
- `notebooks`: EDA and prototyping.
- `reports`: Generated reports.
