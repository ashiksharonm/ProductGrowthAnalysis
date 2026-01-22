import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from src.warehouse.db import get_db
from src.warehouse.models import Page, PageMetric

def detect_anomalies_for_page(session: Session, page_id: int):
    """
    Apply Z-score and STL residual thresholds to flag anomalies.
    """
    # Load metrics
    query = session.query(PageMetric).filter_by(page_id=page_id).order_by(PageMetric.date)
    df = pd.read_sql(query.statement, session.bind)
    
    if df.empty:
        return
        
    # Criteria 1: Z-score on Daily Growth
    # We calculate z-score on a rolling window (e.g., 30 days) to adapt to trends
    window = 30
    roll_mean = df['growth_rate_daily'].rolling(window=window).mean()
    roll_std = df['growth_rate_daily'].rolling(window=window).std()
    
    df['z_score'] = (df['growth_rate_daily'] - roll_mean) / roll_std
    
    # Criteria 2: STL Residual Threshold
    # We already have stl_residual in DB. Let's get z-score of that too? 
    # Or just absolute threshold. Let's use Z-score of residual.
    resid_mean = df['stl_residual'].rolling(window=window).mean()
    resid_std = df['stl_residual'].rolling(window=window).std()
    df['resid_z_score'] = (df['stl_residual'] - resid_mean) / resid_std
    
    # Thresholds
    Z_THRESHOLD = 3.0
    
    anomalies = []
    
    for idx, row in df.iterrows():
        is_anomaly = False
        severity = None
        
        # Check conditions
        z = abs(row['z_score']) if not pd.isna(row['z_score']) else 0
        resid_z = abs(row['resid_z_score']) if not pd.isna(row['resid_z_score']) else 0
        
        if z > Z_THRESHOLD or resid_z > Z_THRESHOLD:
            is_anomaly = True
            
            # Severity
            max_z = max(z, resid_z)
            if max_z > 5:
                severity = "High"
            elif max_z > 4:
                severity = "Medium"
            else:
                severity = "Low"
                
        # Update DB record if changed
        # Note: In a real batch system, we'd update in bulk. Here we iterate.
        if is_anomaly:
             # We need to fetch the specific object to update it attached to session
             metric = session.query(PageMetric).get(row['id'])
             metric.anomaly_flag = True
             metric.anomaly_severity = severity
             # We rely on commit at the end

    session.commit()

def run_anomaly_detection():
    print("Starting Anomaly Detection...")
    session = next(get_db())
    try:
        pages = session.query(Page).all()
        for page in pages:
            print(f"Detecting anomalies for {page.page_title}...")
            detect_anomalies_for_page(session, page.page_id)
    except Exception as e:
        print(f"Anomaly Detection failed: {e}")
        session.rollback()
    finally:
        session.close()
    print("Anomaly Detection Completed.")

if __name__ == "__main__":
    run_anomaly_detection()
