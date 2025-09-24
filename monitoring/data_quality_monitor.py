#!/usr/bin/env python3
"""
Enterprise Data Quality Monitoring System
Demonstrates production-ready architecture patterns and best practices.
"""

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

# Configure logging for enterprise-grade observability
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
    """Data class representing validation results - demonstrates modern Python patterns"""
    rule_name: str
    success: bool
    score: float
    details: Dict[str, Any]
    recommendations: List[str]

@dataclass 
class QualityMetrics:
    """Aggregated quality metrics for reporting"""
    run_id: str
    timestamp: datetime
    success_rate: float
    total_checks: int
    passed_checks: int
    failed_checks: int
    critical_failures: List[str]
    overall_status: str

class ValidationRule(ABC):
    """Abstract base class for validation rules - Strategy Pattern"""
    
    @abstractmethod
    def validate(self, data: pd.DataFrame) -> ValidationResult:
        pass
    
    @abstractmethod
    def get_recommendations(self, result: ValidationResult) -> List[str]:
        pass

class CompletenessRule(ValidationRule):
    """Validates data completeness"""
    
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
    """Validates data uniqueness"""
    
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

class ValidationRuleFactory:
    """Factory pattern for creating validation rules"""
    
    _rules = {
        'completeness': CompletenessRule,
        'uniqueness': UniquenessRule
    }
    
    @classmethod
    def create_rule(cls, rule_type: str, **kwargs) -> ValidationRule:
        if rule_type not in cls._rules:
            raise ValueError(f"Unknown rule type: {rule_type}")
        return cls._rules[rule_type](**kwargs)
    
    @classmethod
    def register_rule(cls, name: str, rule_class: ValidationRule):
        """Allows extending with custom rules"""
        cls._rules[name] = rule_class

class AlertManager:
    """Observer pattern for handling alerts"""
    
    def __init__(self):
        self.observers = []
    
    def add_observer(self, observer):
        self.observers.append(observer)
    
    def notify_observers(self, metrics: QualityMetrics):
        for observer in self.observers:
            observer.handle_alert(metrics)

class DataQualityMonitor:
    """Main monitoring class demonstrating enterprise architecture"""
    
    def __init__(self, config_path: str = "config/quality_thresholds.yml"):
        self.config = self._load_config(config_path)
        self.alert_manager = AlertManager()
        self.validation_rules = self._initialize_rules()
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Config file not found: {config_path}, using defaults")
            return self._default_config()
    
    def _default_config(self) -> Dict[str, Any]:
        """Default configuration for demo purposes"""
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
        """Initialize validation rules from config"""
        rules = []
        
        # Completeness rule
        threshold = self.config.get("data_quality_rules", {}).get("completeness", {}).get("missing_threshold", 2.0)
        rules.append(ValidationRuleFactory.create_rule("completeness", threshold=threshold))
        
        # Uniqueness rule  
        key_columns = ["record_id", "campaign_id"]  # Use actual columns from dataset
        rules.append(ValidationRuleFactory.create_rule("uniqueness", key_columns=key_columns))
        
        return rules
    
    def validate_data(self, data_path: str) -> List[ValidationResult]:
        """Run validation rules against dataset"""
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
        """Calculate aggregated quality metrics"""
        if not results:
            return None
            
        total_checks = len(results)
        passed_checks = sum(1 for r in results if r.success)
        failed_checks = total_checks - passed_checks
        success_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0
        
        # Determine status based on thresholds
        thresholds = self.config.get("alerting", {}).get("thresholds", {})
        if success_rate >= thresholds.get("warning", 80.0):
            status = "✅ PASSING"
        elif success_rate >= thresholds.get("critical", 50.0):
            status = "⚠️  WARNING" 
        else:
            status = "🚨 CRITICAL"
        
        # Collect critical failures
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
        """Generate comprehensive quality report"""
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
        
        # Add failed validations with recommendations
        failed_results = [r for r in results if not r.success]
        if failed_results:
            report += "\nCritical Issues:\n"
            for result in failed_results:
                details = result.details
                if result.rule_name == "completeness":
                    report += f"• Missing values: {details['missing_percentage']:.1f}% (threshold: {details['threshold']}%)\n"
                elif result.rule_name == "uniqueness":
                    report += f"• Duplicate records: {details['duplicate_count']} found (threshold: 0)\n"
                
                # Add recommendations
                for rec in result.recommendations:
                    report += f"  → {rec}\n"
        
        return report
    
    def save_metrics(self, metrics: QualityMetrics) -> Path:
        """Save metrics for historical tracking"""
        metrics_dir = Path("monitoring/metrics")
        metrics_dir.mkdir(exist_ok=True, parents=True)
        
        timestamp = metrics.timestamp.strftime("%Y%m%d_%H%M%S")
        metrics_file = metrics_dir / f"quality_metrics_{timestamp}.json"
        
        # Convert dataclass to dict for JSON serialization
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
        """Main monitoring function - orchestrates the entire process"""
        logger.info("🔍 Starting enterprise data quality monitoring...")
        
        # Run validations
        results = self.validate_data(data_path)
        if not results:
            logger.error("No validation results obtained")
            return None
        
        # Calculate metrics
        metrics = self.calculate_metrics(results)
        if not metrics:
            logger.error("Failed to calculate metrics")
            return None
        
        # Generate and display report
        report = self.generate_report(metrics, results)
        print(report)
        
        # Save metrics for historical tracking
        metrics_file = self.save_metrics(metrics)
        logger.info(f"📊 Metrics saved to {metrics_file}")
        
        # Alert based on severity
        if metrics.failed_checks > 0:
            if "CRITICAL" in metrics.overall_status:
                logger.error(f"🚨 CRITICAL: Data quality below acceptable thresholds")
            elif "WARNING" in metrics.overall_status:
                logger.warning(f"⚠️  WARNING: Data quality issues detected")
        else:
            logger.info("✅ All data quality checks passed")
        
        return metrics

def main():
    """Main execution function for demonstration"""
    try:
        # Initialize monitor
        monitor = DataQualityMonitor()
        
        # Run monitoring
        metrics = monitor.monitor()
        
        if metrics:
            print(f"\n🎯 Quality Score: {metrics.success_rate:.1f}%")
            print(f"📈 Trend: {'Improving' if metrics.success_rate > 85 else 'Needs Attention'}")
        else:
            print("❌ Monitoring failed - check logs for details")
            
    except Exception as e:
        logger.error(f"Monitoring system error: {e}")
        raise

if __name__ == "__main__":
    main()
