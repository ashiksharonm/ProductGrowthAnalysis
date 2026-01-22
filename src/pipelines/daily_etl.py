import sys
import os
import datetime
from sqlalchemy.orm import Session
from src.ingestion.wiki_client import WikiClient
from src.warehouse.db import init_db, get_db
from src.warehouse.models import Page, PageView

# List of topics to track
TOPICS = [
    "Artificial_intelligence",
    "ChatGPT",
    "Python_(programming_language)",
    "Machine_learning",
    "Microsoft_Azure",
    "Data_science",
    "Generative_artificial_intelligence",
    "Large_language_model",
    "Deep_learning",
    "Neural_network"
]

def ingest_data_for_topic(session: Session, topic: str, start_date: str, end_date: str):
    """
    Fetch and store data for a single topic.
    """
    print(f"Fetching data for {topic}...")
    client = WikiClient()
    data = client.fetch_pageviews(topic, start_date, end_date)
    
    if not data or 'items' not in data:
        print(f"No data found for {topic}")
        return

    # Ensure page exists in dimensions table
    page = session.query(Page).filter_by(page_title=topic).first()
    if not page:
        page = Page(page_title=topic, category="Tech")
        session.add(page)
        session.commit()
        session.refresh(page)
    
    # Process items
    new_records = 0
    for item in data['items']:
        date_str = item['timestamp'][:8] # YYYYMMDD00 -> YYYYMMDD
        views = item['views']
        date_obj = datetime.datetime.strptime(date_str, "%Y%m%d").date()
        
        # Check if record exists
        exists = session.query(PageView).filter_by(page_id=page.page_id, date=date_obj).first()
        if not exists:
            record = PageView(date=date_obj, page_id=page.page_id, views=views)
            session.add(record)
            new_records += 1
            
    session.commit()
    print(f"  Saved {new_records} new records for {topic}.")

def run_daily_etl():
    """
    Main entry point for daily ETL.
    """
    print("Starting Daily ETL...")
    
    try:
        init_db()
    except Exception as e:
        print(f"Database initialization failed: {e}")
        sys.exit(1)
        
    session = next(get_db())
    
    # For initial load, we might want 365 days. For daily, maybe ust yesterday?
    # Let's do a logic: If empty, load 365. If not, load last 3 days to catch up data.
    
    # Simple logic for this MVP: Always try to load last 365 days. 
    # Duplicate check handles idempotency.
    
    today = datetime.date.today()
    end_date = today.strftime("%Y%m%d")
    start_date = (today - datetime.timedelta(days=365)).strftime("%Y%m%d")
    
    try:
        for topic in TOPICS:
            ingest_data_for_topic(session, topic, start_date, end_date)
    except Exception as e:
        print(f"ETL Failed: {e}")
        session.rollback()
    finally:
        session.close()
        
    print("Daily ETL Completed.")

if __name__ == "__main__":
    run_daily_etl()
