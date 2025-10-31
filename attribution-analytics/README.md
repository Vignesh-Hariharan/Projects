# Multi-Touch Attribution Analytics

A production-grade attribution modeling system built to quantify the impact of different marketing touchpoints on customer conversions.

## Problem Statement

Traditional last-click attribution gives 100% credit to the final touchpoint before conversion, systematically undervaluing early-stage marketing efforts like prospecting campaigns. This leads to:

- Misallocated marketing budgets
- Underinvestment in awareness-stage channels
- Poor long-term growth strategy

## Solution

This project implements four attribution models (First Touch, Last Touch, Linear, Position-Based) to analyze conversion pathways and quantify the true value of each marketing channel.

### Key Finding

Analysis reveals that prospecting display campaigns are undervalued by approximately 190% under last-click attribution compared to position-based models. These campaigns drive early awareness but receive minimal credit in traditional models.

## Technical Architecture

```
Data Generation (Python)
    ├── GA4 Events (~28K events, 287 purchases)
    ├── Ad Impressions (~14K impressions)
    └── Campaign Metadata (12 campaigns)
           ↓
Snowflake Data Warehouse
    └── Raw Tables
           ↓
dbt Transformations
    ├── Staging Layer (3 models)
    ├── Intermediate Layer (2 models)
    └── Mart Layer (2 models)
           ↓
Attribution Results
    └── 4 attribution models with channel comparison
```

**Stack**: Python 3.10+ | Snowflake | dbt 1.7 | Pandas | NumPy

## Quick Start

### Prerequisites

- Python 3.10+
- Snowflake account
- Git

### Setup

```bash
# Clone the repository
git clone https://github.com/Vignesh-Hariharan/attribution-analytics.git
cd attribution-analytics

# Install dependencies
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

# Configure Snowflake credentials
cp .env.sample .env
# Edit .env with your Snowflake credentials

# Generate data
python src/extract_ga4.py
python src/generate_campaigns.py
python src/generate_impressions.py

# Load to Snowflake
python src/load_snowflake.py

# Run dbt models
cd dbt/attributions
dbt deps
dbt run --profiles-dir .
dbt test --profiles-dir .
```

## Project Structure

```
attribution-analytics/
├── src/
│   ├── config.py              # Configuration management
│   ├── extract_ga4.py         # Generate GA4 events
│   ├── generate_campaigns.py  # Generate campaign data
│   ├── generate_impressions.py # Generate ad impressions
│   └── load_snowflake.py      # Load data to Snowflake
├── dbt/
│   └── attributions/
│       ├── models/
│       │   ├── staging/        # Data cleaning layer
│       │   ├── intermediate/   # Business logic layer
│       │   └── marts/          # Analytics layer
│       ├── tests/              # Data quality tests
│       └── analyses/           # Ad-hoc analyses
├── sql/
│   └── snowflake_ddl.sql      # Database schema
├── data/                       # Generated CSV files
└── tests/                      # Python unit tests
```

## Attribution Models

### 1. First Touch
100% credit to first touchpoint in customer journey.

### 2. Last Touch
100% credit to last touchpoint before conversion.

### 3. Linear
Equal credit distributed across all touchpoints.

### 4. Position-Based (U-Shaped)
- 40% to first touchpoint
- 40% to last touchpoint
- 20% distributed to middle touchpoints

## Data Generation

The system generates realistic synthetic data with:

- **Temporal patterns**: Prospecting ads appear 1-14 days before first session; retargeting ads appear after engagement
- **User overlap**: 60% of web users are targeted with ads
- **Conversion rate**: ~5% of users convert
- **Multiple touchpoints**: Average 3-5 touchpoints per converting user

## Validation

Run tests to ensure attribution logic is correct:

```bash
cd dbt/attributions
dbt test --profiles-dir .
```

Tests validate that:
- Attribution sums equal actual revenue for each conversion
- Data quality metrics pass
- No null values in critical fields

## Results

Query the attribution results:

```sql
USE DATABASE ATTRIBUTION_DEV;
USE SCHEMA analytics;

SELECT
    channel,
    COUNT(DISTINCT conversion_id) AS conversions,
    ROUND(SUM(last_touch_revenue), 2) AS last_touch_revenue,
    ROUND(SUM(position_based_revenue), 2) AS position_based_revenue
FROM fct_attribution
GROUP BY channel
ORDER BY position_based_revenue DESC;
```

## Author

Vignesh Hariharan
