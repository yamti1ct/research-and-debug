#!/usr/bin/env python3
"""
Analyze companies by creation date and generate a histogram.

Filters out:
- Empty company names
- Names that are exactly "company", "test", or "0" (case-insensitive)
- Names containing "automation" (case-insensitive)
"""

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime, timedelta

# Configuration
CSV_FILE = Path(__file__).parent / "hubspot-crm-exports-all-companies-2025-12-21.csv"
OUTPUT_FILE = Path(__file__).parent / "companies_by_date_histogram.png"

DAYS_BACK = 200


def load_and_filter_companies(filepath: Path) -> pd.DataFrame:
    """Load CSV and apply filters to exclude test/invalid companies."""
    df = pd.read_csv(filepath)
    
    original_count = len(df)
    print(f"Total companies loaded: {original_count:,}")
    
    # Filter out empty names
    df = df[df["Company name"].notna() & (df["Company name"].str.strip() != "")]
    after_empty = len(df)
    print(f"After removing empty names: {after_empty:,} (removed {original_count - after_empty:,})")
    
    # Filter out exact matches: "company", "test", "0" (case-insensitive)
    excluded_names = {"company", "test", "0"}
    df = df[~df["Company name"].str.lower().str.strip().isin(excluded_names)]
    after_exact = len(df)
    print(f"After removing 'company', 'test', '0': {after_exact:,} (removed {after_empty - after_exact:,})")
    
    # Filter out names containing "automation" (case-insensitive)
    df = df[~df["Company name"].str.lower().str.contains("automation", na=False)]
    after_automation = len(df)
    print(f"After removing 'automation' names: {after_automation:,} (removed {after_exact - after_automation:,})")
    
    # Filter for companies created in the last year only
    df["Create Date"] = pd.to_datetime(df["Create Date"])
    start_date = datetime.now() - timedelta(days=DAYS_BACK)
    df = df[df["Create Date"] >= start_date]
    after_date_filter = len(df)
    print(f"After filtering to last {DAYS_BACK} days: {after_date_filter:,} (removed {after_automation - after_date_filter:,})")
    
    print(f"\nFinal count: {after_date_filter:,} companies ({after_date_filter/original_count*100:.1f}% of original)")
    
    return df


def create_histogram(df: pd.DataFrame, output_path: Path):
    """Create and save histogram of companies by creation date."""
    # Extract just the date part (Create Date already parsed in filter step)
    df["Date"] = df["Create Date"].dt.date
    
    # Count companies per day
    daily_counts = df.groupby("Date").size().reset_index(name="Count")
    daily_counts["Date"] = pd.to_datetime(daily_counts["Date"])
    daily_counts = daily_counts.sort_values("Date")
    
    print(f"\nDate range: {daily_counts['Date'].min().date()} to {daily_counts['Date'].max().date()}")
    print(f"Total days with signups: {len(daily_counts)}")
    print(f"Average companies per day: {daily_counts['Count'].mean():.1f}")
    print(f"Max companies in a day: {daily_counts['Count'].max()} on {daily_counts.loc[daily_counts['Count'].idxmax(), 'Date'].date()}")
    
    # Create histogram
    fig, ax = plt.subplots(figsize=(14, 6))
    
    ax.bar(daily_counts["Date"], daily_counts["Count"], width=0.8, color="#4A90D9", edgecolor="none")
    
    ax.set_xlabel("Create Date", fontsize=12)
    ax.set_ylabel("Number of Companies", fontsize=12)
    ax.set_title("Companies Created Per Day (Filtered)", fontsize=14, fontweight="bold")
    
    # Rotate x-axis labels for readability
    plt.xticks(rotation=45, ha="right")
    
    # Add grid for better readability
    ax.yaxis.grid(True, linestyle="--", alpha=0.7)
    ax.set_axisbelow(True)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"\nHistogram saved to: {output_path}")
    
    plt.show()


def main():
    print("=" * 60)
    print("Company Analysis by Creation Date")
    print("=" * 60 + "\n")
    
    df = load_and_filter_companies(CSV_FILE)
    create_histogram(df, OUTPUT_FILE)


if __name__ == "__main__":
    main()

