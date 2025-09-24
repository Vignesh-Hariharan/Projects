#!/usr/bin/env python3

import json
import yaml
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from abc import ABC, abstractmethod
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('monitoring/logs/data_quality.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    rule_name: str
    success: bool
    score: float
    details: Dict[str, Any]
    recommendations: List[str]

@dataclass 
class QualityMetrics:
    run_id: str
    timestamp: datetime
    success_rate: float
    total_checks: int
    passed_checks: int
    failed_checks: int
    critical_failures: List[str]
    overall_status: str

class ValidationRule(ABC):
    @abstractmethod
    def validate(self, data: pd.DataFrame) -> ValidationResult:
        pass
    
    @abstractmethod
    def get_recommendations(self, result: ValidationResult) -> List[str]:
        pass

class CompletenessRule(ValidationRule):
    def __init__(self, threshold: float = 2.0):
        self.threshold = threshold
    
    def validate(self, data: pd.DataFrame) -> ValidationResult:
        missing_pct = (data.isnull().sum() / len(data) * 100).max()
        success = missing_pct <= self.threshold
        
        return ValidationResult(
            rule_name="completeness",
            success=success,
            score=max(0, 100 - missing_pct),
            details={"missing_percentage": missing_pct, "threshold": self.threshold},
            recommendations=self.get_recommendations(missing_pct)
        )
    
    def get_recommendations(self, missing_pct: float) -> List[str]:
        if missing_pct > self.threshold:
            return ["Investigate upstream data sources", "Implement data validation at ingestion"]
        return []

class UniquenessRule(ValidationRule):
    def __init__(self, key_columns: List[str]):
        self.key_columns = key_columns

    def validate(self, data: pd.DataFrame) -> ValidationResult:
        duplicates = data.duplicated(subset=self.key_columns).sum()
        success = duplicates == 0

        return ValidationResult(
            rule_name="uniqueness",
            success=success,
            score=100 if success else max(0, 100 - (duplicates / len(data) * 100)),
            details={"duplicate_count": int(duplicates), "key_columns": self.key_columns},
            recommendations=self.get_recommendations(duplicates)
        )

    def get_recommendations(self, duplicates: int) -> List[str]:
        if duplicates > 0:
            return ["Implement deduplication logic", "Review data ingestion process"]
        return []

class DateValidityRule(ValidationRule):
    def __init__(self, date_columns: List[str], max_future_days: int = 1):
        self.date_columns = date_columns
        self.max_future_days = max_future_days

    def validate(self, data: pd.DataFrame) -> ValidationResult:
        invalid_dates = 0
        total_date_cells = 0
        current_date = datetime.now()

        for col in self.date_columns:
            if col in data.columns:
                total_date_cells += len(data)
                try:
                    date_series = pd.to_datetime(data[col], errors='coerce')

                    null_dates = date_series.isnull().sum()
                    invalid_dates += null_dates

                    future_dates = date_series[
                        (date_series > current_date + timedelta(days=self.max_future_days)) &
                        date_series.notnull()
                    ]
                    invalid_dates += len(future_dates)

                    old_dates = date_series[
                        (date_series < pd.Timestamp('2000-01-01')) &
                        date_series.notnull()
                    ]
                    invalid_dates += len(old_dates)

                except Exception:
                    invalid_dates += len(data)

        success = invalid_dates == 0
        validity_rate = ((total_date_cells - invalid_dates) / total_date_cells * 100) if total_date_cells > 0 else 0

        return ValidationResult(
            rule_name="date_validity",
            success=success,
            score=validity_rate,
            details={
                "invalid_dates": int(invalid_dates),
                "total_date_cells": total_date_cells,
                "date_columns": self.date_columns,
                "max_future_days": self.max_future_days
            },
            recommendations=self.get_recommendations(invalid_dates, total_date_cells)
        )

    def get_recommendations(self, invalid_dates: int, total_cells: int) -> List[str]:
        if invalid_dates > 0:
            recommendations = []
            error_rate = invalid_dates / total_cells if total_cells > 0 else 1

            if error_rate > 0.1:
                recommendations.append("Review data source date format standards")
                recommendations.append("Implement date validation at data ingestion")
            else:
                recommendations.append("Clean up invalid date entries")
                recommendations.append("Add date format validation to ETL pipeline")

            recommendations.append("Consider adding date range business rules")
            return recommendations
        return []

class RoiThresholdRule(ValidationRule):
    def __init__(self, roi_column: str, min_threshold: float = 0.0, max_threshold: float = 5.0):
        self.roi_column = roi_column
        self.min_threshold = min_threshold
        self.max_threshold = max_threshold

    def validate(self, data: pd.DataFrame) -> ValidationResult:
        if self.roi_column not in data.columns:
            return ValidationResult(
                rule_name="roi_threshold",
                success=False,
                score=0,
                details={"error": f"Column '{self.roi_column}' not found in dataset"},
                recommendations=["Verify ROI column name in configuration"]
            )

        roi_values = data[self.roi_column].dropna()

        if len(roi_values) == 0:
            return ValidationResult(
                rule_name="roi_threshold",
                success=False,
                score=0,
                details={"error": "No valid ROI values found"},
                recommendations=["Check ROI calculation logic", "Verify spend and sales data"]
            )

        outliers_low = (roi_values < self.min_threshold).sum()
        outliers_high = (roi_values > self.max_threshold).sum()
        total_outliers = outliers_low + outliers_high

        outlier_rate = total_outliers / len(roi_values)
        score = max(0, 100 - (outlier_rate * 100 * 2))

        success = outlier_rate <= 0.05

        return ValidationResult(
            rule_name="roi_threshold",
            success=success,
            score=score,
            details={
                "outliers_below_threshold": int(outliers_low),
                "outliers_above_threshold": int(outliers_high),
                "total_outliers": int(total_outliers),
                "total_valid_values": len(roi_values),
                "min_threshold": self.min_threshold,
                "max_threshold": self.max_threshold,
                "roi_column": self.roi_column
            },
            recommendations=self.get_recommendations(outliers_low, outliers_high, len(roi_values))
        )

    def get_recommendations(self, outliers_low: int, outliers_high: int, total_values: int) -> List[str]:
        recommendations = []

        if outliers_high > 0:
            high_rate = outliers_high / total_values
            if high_rate > 0.1:
                recommendations.append("Review ROI calculation - unusually high values may indicate data errors")
                recommendations.append("Validate advertising spend vs sales attribution")
            else:
                recommendations.append("Investigate campaigns with exceptional ROI performance")

        if outliers_low > 0:
            low_rate = outliers_low / total_values
            if low_rate > 0.1:
                recommendations.append("Review low ROI campaigns for optimization opportunities")
                recommendations.append("Check for data quality issues in spend or sales tracking")
            else:
                recommendations.append("Monitor underperforming campaigns for budget reallocation")

        if not recommendations:
            recommendations.append("ROI values within expected ranges")

        return recommendations

class ValidationRuleFactory:
    _rules = {
        'completeness': CompletenessRule,
        'uniqueness': UniquenessRule,
        'date_validity': DateValidityRule,
        'roi_threshold': RoiThresholdRule
    }
    
    @classmethod
    def create_rule(cls, rule_type: str, **kwargs) -> ValidationRule:
        if rule_type not in cls._rules:
            raise ValueError(f"Unknown rule type: {rule_type}")
        return cls._rules[rule_type](**kwargs)
    
    @classmethod
    def register_rule(cls, name: str, rule_class: ValidationRule):
        cls._rules[name] = rule_class

class AlertManager:
    def __init__(self):
        self.observers = []
    
    def add_observer(self, observer):
        self.observers.append(observer)
    
    def notify_observers(self, metrics: QualityMetrics):
        for observer in self.observers:
            observer.handle_alert(metrics)

class DataQualityMonitor:
    def __init__(self, config_path: str = "config/quality_thresholds.yml"):
        self.config = self._load_config(config_path)
        self.alert_manager = AlertManager()
        self.validation_rules = self._initialize_rules()
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Config file not found: {config_path}, using defaults")
            return self._default_config()
    
    def _default_config(self) -> Dict[str, Any]:
        return {
            "data_quality_rules": {
                "completeness": {"missing_threshold": 2.0},
                "uniqueness": {"duplicate_threshold": 0}
            },
            "alerting": {
                "thresholds": {"warning": 80.0, "critical": 50.0}
            }
        }
    
    def _initialize_rules(self) -> List[ValidationRule]:
        rules = []

        threshold = self.config.get("data_quality_rules", {}).get("completeness", {}).get("missing_threshold", 2.0)
        rules.append(ValidationRuleFactory.create_rule("completeness", threshold=threshold))

        key_columns = ["record_id", "campaign_id"]
        rules.append(ValidationRuleFactory.create_rule("uniqueness", key_columns=key_columns))

        date_columns = ["campaign_date", "ingestion_timestamp"]
        rules.append(ValidationRuleFactory.create_rule("date_validity", date_columns=date_columns, max_future_days=1))

        rules.append(ValidationRuleFactory.create_rule("roi_threshold", roi_column="return_on_ad_spend", min_threshold=0.0, max_threshold=3.0))

        return rules
    
    def validate_data(self, data_path: str) -> List[ValidationResult]:
        try:
            data = pd.read_csv(data_path)
            logger.info(f"Loaded dataset with {len(data)} records, {len(data.columns)} columns")
            
            results = []
            for rule in self.validation_rules:
                try:
                    result = rule.validate(data)
                    results.append(result)
                    logger.info(f"Rule {result.rule_name}: {'PASSED' if result.success else 'FAILED'} (Score: {result.score:.1f})")
                except Exception as e:
                    logger.error(f"Error running rule {rule.__class__.__name__}: {e}")
            
            return results
            
        except Exception as e:
            logger.error(f"Error loading data from {data_path}: {e}")
            return []
    
    def calculate_metrics(self, results: List[ValidationResult]) -> QualityMetrics:
        if not results:
            return None
            
        total_checks = len(results)
        passed_checks = sum(1 for r in results if r.success)
        failed_checks = total_checks - passed_checks
        success_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0
        
        thresholds = self.config.get("alerting", {}).get("thresholds", {})
        if success_rate >= thresholds.get("warning", 80.0):
            status = "✅ PASSING"
        elif success_rate >= thresholds.get("critical", 50.0):
            status = "⚠️  WARNING" 
        else:
            status = "🚨 CRITICAL"
        
        critical_failures = [r.rule_name for r in results if not r.success]
        
        return QualityMetrics(
            run_id=f"quality_check_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            timestamp=datetime.now(),
            success_rate=success_rate,
            total_checks=total_checks,
            passed_checks=passed_checks,
            failed_checks=failed_checks,
            critical_failures=critical_failures,
            overall_status=status
        )
    
    def generate_report(self, metrics: QualityMetrics, results: List[ValidationResult]) -> str:
        if not metrics:
            return "No metrics available"
        
        report = f"""
DATA QUALITY REPORT
==================
Run ID: {metrics.run_id}
Timestamp: {metrics.timestamp.strftime('%Y-%m-%d %H:%M:%S')}
Success Rate: {metrics.success_rate:.1f}% ({metrics.passed_checks}/{metrics.total_checks} checks passed)
Status: {metrics.overall_status}
"""
        
        failed_results = [r for r in results if not r.success]
        if failed_results:
            report += "\nCritical Issues:\n"
            for result in failed_results:
                details = result.details
                if result.rule_name == "completeness":
                    report += f"• Missing values: {details['missing_percentage']:.1f}% (threshold: {details['threshold']}%)\n"
                elif result.rule_name == "uniqueness":
                    report += f"• Duplicate records: {details['duplicate_count']} found (threshold: 0)\n"
                elif result.rule_name == "date_validity":
                    report += f"• Invalid dates: {details['invalid_dates']} found across {len(details['date_columns'])} columns\n"
                elif result.rule_name == "roi_threshold":
                    outliers = details.get('total_outliers', 0)
                    report += f"• ROI outliers: {outliers} values outside thresholds ({details.get('min_threshold', 0.0)} - {details.get('max_threshold', 3.0)})\n"

                for rec in result.recommendations:
                    report += f"  → {rec}\n"
        
        return report
    
    def save_metrics(self, metrics: QualityMetrics) -> Path:
        metrics_dir = Path("monitoring/metrics")
        metrics_dir.mkdir(exist_ok=True, parents=True)
        
        timestamp = metrics.timestamp.strftime("%Y%m%d_%H%M%S")
        metrics_file = metrics_dir / f"quality_metrics_{timestamp}.json"
        
        metrics_dict = {
            "run_id": metrics.run_id,
            "timestamp": metrics.timestamp.isoformat(),
            "success_rate": metrics.success_rate,
            "total_checks": metrics.total_checks,
            "passed_checks": metrics.passed_checks,
            "failed_checks": metrics.failed_checks,
            "critical_failures": metrics.critical_failures,
            "overall_status": metrics.overall_status
        }
        
        with open(metrics_file, 'w') as f:
            json.dump(metrics_dict, f, indent=2)
        
        return metrics_file
    
    def monitor(self, data_path: str = "data/subset/marketing_performance.csv") -> Optional[QualityMetrics]:
        logger.info("Starting data quality monitoring...")
        
        results = self.validate_data(data_path)
        if not results:
            logger.error("No validation results obtained")
            return None
        
        metrics = self.calculate_metrics(results)
        if not metrics:
            logger.error("Failed to calculate metrics")
            return None
        
        report = self.generate_report(metrics, results)
        print(report)
        
        metrics_file = self.save_metrics(metrics)
        logger.info(f"Metrics saved to {metrics_file}")
        
        if metrics.failed_checks > 0:
            if "CRITICAL" in metrics.overall_status:
                logger.error("CRITICAL: Data quality below acceptable thresholds")
            elif "WARNING" in metrics.overall_status:
                logger.warning("WARNING: Data quality issues detected")
        else:
            logger.info("All data quality checks passed")
        
        return metrics

def main():
    try:
        monitor = DataQualityMonitor()
        metrics = monitor.monitor()
        
        if metrics:
            print(f"\nQuality Score: {metrics.success_rate:.1f}%")
        else:
            print("Monitoring failed - check logs for details")
            
    except Exception as e:
        logger.error(f"Monitoring system error: {e}")
        raise

if __name__ == "__main__":
    main()
