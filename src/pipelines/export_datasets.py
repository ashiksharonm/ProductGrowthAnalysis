import pandas as pd
from src.warehouse.db import get_db
from src.warehouse.models import Page, PageView, PageMetric

def export_to_csv():
    print("Starting Dataset Export...")
    session = next(get_db())
    try:
        # Export Page Views
        print("Exporting pageviews...")
        query_views = session.query(
            Page.page_title, 
            PageView.date, 
            PageView.views
        ).join(Page, Page.page_id == PageView.page_id)
        
        df_views = pd.read_sql(query_views.statement, session.bind)
        df_views.to_csv('dashboards/tableau_dataset_views.csv', index=False)
        print("  dashboards/tableau_dataset_views.csv created.")
        
        # Export Metrics (Growth + Anomalies)
        print("Exporting metrics...")
        query_metrics = session.query(
            Page.page_title,
            PageMetric.date,
            PageMetric.rolling_7d_avg,
            PageMetric.growth_rate_daily,
            PageMetric.anomaly_flag,
            PageMetric.anomaly_severity
        ).join(Page, Page.page_id == PageMetric.page_id)
        
        df_metrics = pd.read_sql(query_metrics.statement, session.bind)
        df_metrics.to_csv('dashboards/powerbi_dataset_metrics.csv', index=False)
        print("  dashboards/powerbi_dataset_metrics.csv created.")
        
    except Exception as e:
        print(f"Export failed: {e}")
    finally:
        session.close()
    print("Dataset Export Completed.")

if __name__ == "__main__":
    export_to_csv()
