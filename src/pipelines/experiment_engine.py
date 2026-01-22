import numpy as np
import scipy.stats as stats
from sqlalchemy.orm import Session
from src.warehouse.db import get_db
from src.warehouse.models import Page, PageMetric, ExperimentResult
import uuid

class ExperimentEngine:
    def __init__(self, session: Session):
        self.session = session

    def simulate_experiment(self, page_id: int, metric_name: str = 'growth_rate_daily', lift: float = 0.05, n_samples: int = 1000):
        """
        Simulate an A/B test based on the historical stats of a page.
        
        Args:
            metric_name: The metric to test (e.g., 'growth_rate_daily')
            lift: The simulated relative effect size (e.g., 0.05 for 5% lift)
            n_samples: Sample size for simulation
        """
        # 1. Get baseline stats from DB
        metrics = self.session.query(getattr(PageMetric, metric_name)).filter_by(page_id=page_id).all()
        values = [m[0] for m in metrics if m[0] is not None]
        
        if len(values) < 10:
            print(f"Not enough data to simulate experiment for page {page_id}")
            return

        baseline_mean = np.mean(values)
        baseline_std = np.std(values)
        
        # 2. Generate Synthetic Control Group
        # Sample directly from historical distribution or normal approx
        control_group = np.random.normal(baseline_mean, baseline_std, n_samples)
        
        # 3. Generate Synthetic Treatment Group (with lift)
        # Treatment mean = baseline * (1 + lift) if positive, else just shift
        treatment_mean = baseline_mean * (1 + lift)
        treatment_group = np.random.normal(treatment_mean, baseline_std, n_samples)
        
        # 4. Perform T-Test
        t_stat, p_value = stats.ttest_ind(treatment_group, control_group)
        
        # 5. Calculate Effect Size (Cohen's d)
        pooled_std = np.sqrt((np.var(control_group) + np.var(treatment_group)) / 2)
        cohens_d = (np.mean(treatment_group) - np.mean(control_group)) / pooled_std
        
        # 6. Confidence Interval (95%)
        ci_low, ci_high = stats.t.interval(0.95, df=len(control_group)+len(treatment_group)-2, loc=(np.mean(treatment_group) - np.mean(control_group)), scale=stats.sem(treatment_group - control_group))
        
        # 7. Record Result
        conclusion = "Significant" if p_value < 0.05 else "Not Significant"
        
        result = ExperimentResult(
            run_id=str(uuid.uuid4()),
            metric_name=f"{metric_name}_simulated_lift_{lift}",
            effect_size=float(cohens_d),
            p_value=float(p_value),
            confidence_interval_lower=float(ci_low),
            confidence_interval_upper=float(ci_high),
            conclusion=conclusion,
            power_analysis=None # Optional
        )
        self.session.add(result)
        self.session.commit()
        
        print(f"Experiment Run: {conclusion} (p={p_value:.4f}, effect={cohens_d:.4f})")

def run_experiments():
    print("Starting Experiment Simulation...")
    session = next(get_db())
    try:
        pages = session.query(Page).all()
        engine = ExperimentEngine(session)
        
        for page in pages:
            print(f"Simulating experiment for {page.page_title}...")
            # Simulate a 10% improvement in growth rate
            engine.simulate_experiment(page.page_id, lift=0.10)
    except Exception as e:
        print(f"Experimentation failed: {e}")
        session.rollback()
    finally:
        session.close()
    print("Experiment Simulation Completed.")

if __name__ == "__main__":
    run_experiments()
