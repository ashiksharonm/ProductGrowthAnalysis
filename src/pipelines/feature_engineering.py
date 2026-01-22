import pandas as pd
from sqlalchemy.orm import Session
from statsmodels.tsa.seasonal import seasonal_decompose
from src.warehouse.db import get_db
from src.warehouse.models import Page, PageView, PageMetric

def calculate_rolling_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate rolling averages and growth rates.
    Expected DF: index=Date, columns=['views']
    """
    df = df.sort_index()
    
    # Rolling averages
    df['rolling_7d'] = df['views'].rolling(window=7, min_periods=1).mean()
    df['rolling_30d'] = df['views'].rolling(window=30, min_periods=1).mean()
    
    # Growth rates
    df['growth_daily'] = df['views'].pct_change(1)
    df['growth_weekly'] = df['views'].pct_change(7)
    
    df = df.fillna(0)
    return df

def calculate_stl_residual(df: pd.DataFrame) -> pd.Series:
    """
    Perform STL decomposition and return residuals.
    """
    # Need at least 2 cycles (e.g., 14 days for weekly seasonality)
    if len(df) < 14:
        return pd.Series(0, index=df.index)
        
    try:
        # Decompose assuming weekly seasonality (period=7)
        # Using additive model
        result = seasonal_decompose(df['views'], model='additive', period=7)
        return result.resid.fillna(0)
    except Exception as e:
        print(f"STL Decomposition failed: {e}")
        return pd.Series(0, index=df.index)

def process_features_for_page(session: Session, page_id: int):
    """
    Load data, compute features, and save to fact_metrics.
    """
    # Load raw views
    query = session.query(PageView).filter_by(page_id=page_id).order_by(PageView.date)
    df = pd.read_sql(query.statement, session.bind)
    
    if df.empty:
        return

    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    
    # Compute Features
    df_metrics = calculate_rolling_metrics(df)
    residuals = calculate_stl_residual(df)
    df_metrics['stl_residual'] = residuals
    
    # Save to fact_metrics
    # We want to perform upsert. 
    # For simplicity, we can delete existing metrics for this page and re-insert 
    # OR iterate and update. Re-inserting is often cleaner for full-refresh ETLs 
    # but slower for massive data. Given this is small scale, let's just loop-check or bulk insert on new dates.
    # To keep it efficient and robust: let's clear metrics and rewrite (safest for reprocessing) 
    # OR just write new days. 
    # For this task, let's process all and overwrite/update.
    
    # Loading existing metrics to avoid re-writing everything? 
    # Let's just iterate and merge.
    
    for date, row in df_metrics.iterrows():
        date_date = date.date()
        
        # Check if metric exists
        metric = session.query(PageMetric).filter_by(page_id=page_id, date=date_date).first()
        if not metric:
            metric = PageMetric(page_id=page_id, date=date_date)
            
        metric.rolling_7d_avg = float(row['rolling_7d'])
        metric.rolling_30d_avg = float(row['rolling_30d'])
        metric.growth_rate_daily = float(row['growth_daily'])
        metric.growth_rate_weekly = float(row['growth_weekly'])
        metric.stl_residual = float(row['stl_residual']) if not pd.isna(row['stl_residual']) else 0.0
        
        session.add(metric)
    
    session.commit()

def run_feature_engineering():
    print("Starting Feature Engineering...")
    session = next(get_db())
    try:
        pages = session.query(Page).all()
        for page in pages:
            print(f"Processing features for {page.page_title}...")
            process_features_for_page(session, page.page_id)
    except Exception as e:
        print(f"Feature Engineering failed: {e}")
        session.rollback()
    finally:
        session.close()
    print("Feature Engineering Completed.")

if __name__ == "__main__":
    run_feature_engineering()
