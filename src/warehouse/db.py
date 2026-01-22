import os
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from src.warehouse.models import Base

# Default to local docker logic if env var not set
# In a real app we'd load this from .env
DB_USER = os.getenv("POSTGRES_USER", "admin")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "growth_analytics")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db(retries=5, delay=2):
    """Initialize database tables, waiting for DB to be ready."""
    for i in range(retries):
        try:
            Base.metadata.create_all(bind=engine)
            print("Database tables created successfully.")
            return
        except OperationalError as e:
            print(f"Database not ready yet, retrying in {delay}s... ({i+1}/{retries})")
            time.sleep(delay)
    raise Exception("Could not connect to database after multiple retries.")

def get_db():
    """Dependency for getting DB session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
