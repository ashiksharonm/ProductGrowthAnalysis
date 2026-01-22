from sqlalchemy import Column, Integer, String, Date, Float, Boolean, ForeignKey, DateTime
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class Page(Base):
    __tablename__ = 'dim_pages'
    
    page_id = Column(Integer, primary_key=True, autoincrement=True)
    page_title = Column(String, unique=True, nullable=False)
    category = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    pageviews = relationship("PageView", back_populates="page")
    metrics = relationship("PageMetric", back_populates="page")

class PageView(Base):
    __tablename__ = 'fact_pageviews'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    page_id = Column(Integer, ForeignKey('dim_pages.page_id'), nullable=False)
    views = Column(Integer, nullable=False)
    
    page = relationship("Page", back_populates="pageviews")

class PageMetric(Base):
    __tablename__ = 'fact_metrics'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    page_id = Column(Integer, ForeignKey('dim_pages.page_id'), nullable=False)
    
    rolling_7d_avg = Column(Float)
    rolling_30d_avg = Column(Float)
    growth_rate_daily = Column(Float) # Day-over-day
    growth_rate_weekly = Column(Float) # Week-over-week
    stl_residual = Column(Float)
    anomaly_flag = Column(Boolean, default=False)
    anomaly_severity = Column(String, nullable=True) # Low, Medium, High
    
    page = relationship("Page", back_populates="metrics")

class ExperimentResult(Base):
    __tablename__ = 'fact_experiments'
    
    run_id = Column(String, primary_key=True) # UUID or generated ID
    experiment_date = Column(Date, server_default=func.current_date())
    metric_name = Column(String, nullable=False)
    effect_size = Column(Float)
    p_value = Column(Float)
    confidence_interval_lower = Column(Float)
    confidence_interval_upper = Column(Float)
    conclusion = Column(String)
    power_analysis = Column(Float, nullable=True)
