# Architecture

## Data Flow

1.  **Ingestion**: `wiki_client.py` fetches daily pageviews from Wikipedia API (VisualEditor/REST).
2.  **Storage**: Raw JSON stored in file system. Parsed records stored in `fact_pageviews` (PostgreSQL).
3.  **Processing**:
    - `feature_engineering.py`: Computes aggregates (7d avg) and growth metrics.
    - `anomaly_detection.py`: Applies Z-score and STL to flag outliers.
    - `experiment_engine.py`: Runs statistical tests on synthetic groups.
4.  **Output**:
    - CSV exports for Tableau/PowerBI.
    - Markdown executive reports.

## Database Schema

- **dim_pages**: Metadata for tracked pages.
- **fact_pageviews**: Daily views per page.
- **fact_metrics**: Derived metrics + anomaly flags.
- **fact_experiments**: Results of simulated A/B tests.
