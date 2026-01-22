import datetime
from sqlalchemy.orm import Session
from src.warehouse.db import get_db
from src.warehouse.models import Page, PageMetric, ExperimentResult

def generate_markdown_report_content(session: Session) -> str:
    today = datetime.date.today()
    
    # 1. Summary Header
    md = f"# Product Growth Executive Report\n**Date**: {today}\n\n"
    
    # 2. Key Metrics Snapshot (Latest available date)
    md += "## 1. Key Metrics Snapshot\n"
    # Get latest date data
    # (Simplified: assuming all pages have data for same max date)
    md += "| Page | Views (Latest) | 7d Avg | Growth (Daily) | Anomaly? |\n"
    md += "|---|---|---|---|---|\n"
    
    pages = session.query(Page).all()
    for page in pages:
        metric = session.query(PageMetric).filter_by(page_id=page.page_id).order_by(PageMetric.date.desc()).first()
        views = 0
        r7 = 0
        growth = 0
        anomaly = "No"
        
        if metric:
            # We need to join with views to get raw view count if not in metric, but let's just use metric
            r7 = f"{metric.rolling_7d_avg:.1f}" if metric.rolling_7d_avg else "-"
            growth = f"{metric.growth_rate_daily * 100:.2f}%" if metric.growth_rate_daily else "-"
            anomaly = f"**{metric.anomaly_severity}**" if metric.anomaly_flag else "No"
            
            md += f"| {page.page_title} | - | {r7} | {growth} | {anomaly} |\n"
    
    # 3. Anomalies Section
    md += "\n## 2. Recent Anomalies (Last 7 Days)\n"
    start_lookback = today - datetime.timedelta(days=7)
    anomalies = session.query(PageMetric, Page).join(Page).filter(
        PageMetric.anomaly_flag == True,
        PageMetric.date >= start_lookback
    ).all()
    
    if not anomalies:
        md += "No anomalies detected in the last 7 days.\n"
    else:
        for m, p in anomalies:
            md += f"- **{p.page_title}** on {m.date}: Severity {m.anomaly_severity}, Growth {m.growth_rate_daily*100:.2f}%\n"

    # 4. Experiment Results
    md += "\n## 3. Latest Experiment Simulations\n"
    experiments = session.query(ExperimentResult).order_by(ExperimentResult.experiment_date.desc()).limit(5).all()
    
    if not experiments:
        md += "No experiments run yet.\n"
    else:
        for exp in experiments:
            md += f"- **{exp.metric_name}**: {exp.conclusion} (Effect Size: {exp.effect_size:.2f}, p={exp.p_value:.4f})\n"
            
    return md

def run_report_generation():
    print("Starting Report Generation...")
    session = next(get_db())
    try:
        content = generate_markdown_report_content(session)
        
        # Write to file
        with open('reports/executive_report.md', 'w') as f:
            f.write(content)
            
        print("Report generated at reports/executive_report.md")
        
    except Exception as e:
        print(f"Report generation failed: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    run_report_generation()
