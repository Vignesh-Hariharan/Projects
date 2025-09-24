#!/usr/bin/env python3
"""
Data preparation script for marketing campaign analysis.
Downloads advertising dataset and enhances with realistic business dimensions.
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
import requests
from datetime import datetime, timedelta

def download_base_data():
    """Download classic advertising dataset."""
    
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    url = "https://raw.githubusercontent.com/selva86/datasets/master/Advertising.csv"
    
    try:
        print("Downloading advertising dataset...")
        df = pd.read_csv(url)
        
        # Clean up column names and remove index column if present
        if 'Unnamed: 0' in df.columns:
            df = df.drop('Unnamed: 0', axis=1)
        
        # Standardize column names to title case
        df.columns = df.columns.str.title()
        
        print(f"Downloaded dataset with {len(df)} records")
        print(f"Columns: {list(df.columns)}")
        return df
        
    except Exception as e:
        print(f"Failed to download dataset: {e}")
        raise

def enhance_marketing_data(df):
    """Add business dimensions to create realistic marketing dataset."""
    
    np.random.seed(42)
    n_records = len(df)
    
    # Add campaign metadata
    campaigns = ['Q1_2024_Launch', 'Q2_2024_Summer', 'Q3_2024_BackToSchool', 'Q4_2024_Holiday']
    regions = ['North', 'South', 'East', 'West', 'Central']
    products = ['ProductA', 'ProductB', 'ProductC', 'ProductD']
    channels = ['Digital', 'Traditional', 'Hybrid']
    
    # Generate campaign dates
    start_date = datetime.now() - timedelta(days=365)
    dates = [start_date + timedelta(days=np.random.randint(0, 365)) for _ in range(n_records)]
    
    # Add business dimensions
    enhanced_df = df.copy()
    enhanced_df['campaign_id'] = np.random.choice(campaigns, n_records)
    enhanced_df['region'] = np.random.choice(regions, n_records)
    enhanced_df['product'] = np.random.choice(products, n_records)
    enhanced_df['channel_mix'] = np.random.choice(channels, n_records)
    enhanced_df['campaign_date'] = dates
    
    # Calculate performance metrics (using correct column names)
    enhanced_df['total_spend'] = enhanced_df['Tv'] + enhanced_df['Radio'] + enhanced_df['Newspaper']
    
    # Impressions based on spend
    enhanced_df['impressions'] = (
        enhanced_df['Tv'] * 1000 + 
        enhanced_df['Radio'] * 500 + 
        enhanced_df['Newspaper'] * 200 +
        np.random.normal(0, 1000, n_records)
    ).astype(int)
    enhanced_df['impressions'] = np.maximum(enhanced_df['impressions'], 1000)
    
    # Clicks and conversions
    enhanced_df['clicks'] = (enhanced_df['impressions'] * np.random.uniform(0.01, 0.05, n_records)).astype(int)
    enhanced_df['conversions'] = (enhanced_df['clicks'] * np.random.uniform(0.02, 0.15, n_records)).astype(int)
    
    # Cost metrics
    enhanced_df['cost_per_click'] = np.where(
        enhanced_df['clicks'] > 0, 
        enhanced_df['total_spend'] / enhanced_df['clicks'], 
        0
    )
    enhanced_df['cost_per_acquisition'] = np.where(
        enhanced_df['conversions'] > 0,
        enhanced_df['total_spend'] / enhanced_df['conversions'], 
        0
    )
    
    # Customer and revenue metrics
    enhanced_df['customer_lifetime_value'] = enhanced_df['Sales'] * np.random.uniform(1.5, 3.0, n_records)
    enhanced_df['return_on_ad_spend'] = enhanced_df['Sales'] / enhanced_df['total_spend']
    
    # Add metadata
    enhanced_df['ingestion_timestamp'] = datetime.now()
    enhanced_df['data_source'] = 'marketing_automation_platform'
    enhanced_df['record_id'] = range(1, len(enhanced_df) + 1)
    
    return enhanced_df

def introduce_quality_issues(df):
    """Introduce realistic data quality issues for validation testing."""
    
    n_records = len(df)
    
    # Missing values (5% of conversions)
    missing_conv = np.random.choice(n_records, size=int(n_records * 0.05), replace=False)
    df.loc[missing_conv, 'conversions'] = np.nan
    
    # Missing CLV (3% of records)
    missing_clv = np.random.choice(n_records, size=int(n_records * 0.03), replace=False)
    df.loc[missing_clv, 'customer_lifetime_value'] = np.nan
    
    # Outliers in CPC (2% of records)
    outlier_cpc = np.random.choice(n_records, size=int(n_records * 0.02), replace=False)
    df.loc[outlier_cpc, 'cost_per_click'] = df.loc[outlier_cpc, 'cost_per_click'] * 50
    
    # Duplicate records (1% of records)
    duplicate_idx = np.random.choice(n_records, size=int(n_records * 0.01), replace=False)
    duplicates = df.loc[duplicate_idx].copy()
    df = pd.concat([df, duplicates], ignore_index=True)
    
    return df

def save_datasets(df):
    """Save datasets for different use cases."""
    
    data_dir = Path("data")
    subset_dir = data_dir / "subset"
    subset_dir.mkdir(exist_ok=True)
    
    # Main dataset
    main_path = subset_dir / "marketing_performance.csv"
    df.to_csv(main_path, index=False)
    print(f"Saved dataset with {len(df)} records to {main_path}")
    
    # Create demo subset
    demo_df = df.sample(n=min(1000, len(df)), random_state=42)
    demo_path = subset_dir / "marketing_demo.csv"
    demo_df.to_csv(demo_path, index=False)
    print(f"Saved demo subset with {len(demo_df)} records to {demo_path}")
    
    return str(main_path)

def analyze_data_quality(df):
    """Analyze data quality issues for validation development."""
    
    print("\n=== DATA QUALITY ANALYSIS ===")
    print(f"Dataset shape: {df.shape}")
    
    # Missing values
    missing = df.isnull().sum()
    if missing.any():
        print(f"\nMissing values:")
        for col, count in missing[missing > 0].items():
            pct = (count / len(df)) * 100
            print(f"  {col}: {count} ({pct:.1f}%)")
    
    # Duplicates
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        print(f"\nDuplicate records: {duplicates}")
    
    # Outliers
    outlier_cols = ['cost_per_click', 'cost_per_acquisition', 'return_on_ad_spend']
    for col in outlier_cols:
        if col in df.columns:
            q99 = df[col].quantile(0.99)
            outliers = (df[col] > q99).sum()
            if outliers > 0:
                print(f"Outliers in {col}: {outliers} records")
    
    print(f"\nDate range: {df['campaign_date'].min().date()} to {df['campaign_date'].max().date()}")
    print(f"Campaigns: {df['campaign_id'].nunique()} unique")
    print(f"Regions: {df['region'].nunique()} unique")

def main():
    """Main execution function."""
    
    try:
        print("=== MARKETING DATA PREPARATION ===")
        
        # Download base dataset
        base_df = download_base_data()
        
        print("\nEnhancing with business dimensions...")
        enhanced_df = enhance_marketing_data(base_df)
        
        print("Introducing realistic data quality issues...")
        final_df = introduce_quality_issues(enhanced_df)
        
        print("Saving datasets...")
        dataset_path = save_datasets(final_df)
        
        analyze_data_quality(final_df)
        
        print(f"\n✅ SUCCESS: Marketing dataset prepared!")
        print(f"Main dataset: {dataset_path}")
        print("Ready for data quality validation with Great Expectations")
        
        return dataset_path
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        raise

if __name__ == "__main__":
    main()