# Data Quality Framework

Data quality monitoring system with configurable validation rules and Snowflake integration.

## Overview

The framework validates incoming data against configured business rules and writes results to both local storage and Snowflake for reporting.

Key components:
- Validation engine with pluggable rules
- Configurable thresholds via YAML 
- Snowflake integration for enterprise reporting
- JSON metrics storage for historical analysis

## Usage

```bash
pip install -r requirements.txt
python scripts/prepare_data.py
python monitoring/data_quality_monitor.py
```

## Output

```
DATA QUALITY REPORT
==================
Run ID: quality_check_20250924_000943
Success Rate: 50.0% (1/2 checks passed) 
Status: ⚠️ WARNING

Critical Issues:
• Duplicate records: 2 found (threshold: 5)
  → Implement deduplication logic
  → Review data ingestion process

Quality Score: 50.0%
```

## Architecture

Uses strategy pattern for validation rules, factory pattern for rule creation, and observer pattern for alerting. Configuration is externalized to YAML files.

## Structure

```
├── monitoring/data_quality_monitor.py    # Main monitoring logic
├── integrations/snowflake_writer.py      # Snowflake integration
├── config/quality_thresholds.yml         # Business rules configuration
├── data/subset/                           # Sample data with quality issues
└── scripts/prepare_data.py               # Data generation
```

## Technology Stack

- Python 3.11 with pandas/numpy
- YAML configuration
- Snowflake connector
- Strategy/Factory/Observer patterns

## Extending

Add new validation rules by implementing the ValidationRule interface:

```python
class CustomRule(ValidationRule):
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        # validation logic
        pass

ValidationRuleFactory.register('custom', CustomRule)
```

