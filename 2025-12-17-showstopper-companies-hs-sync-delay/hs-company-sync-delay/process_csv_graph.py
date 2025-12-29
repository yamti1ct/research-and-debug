#!/usr/bin/env python3
"""
Script to process HubSpot CRM export CSV and create a graph showing:
- X-axis: Signup timestamp
- Y-axis: Time difference between Create Date and Signup Timestamp

Requirements:
    pip install pandas matplotlib

Usage:
    python3 process_csv_graph.py [path_to_csv_file]
    
    If no path is provided, uses the default CSV path.
"""

import sys

try:
    import pandas as pd
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from datetime import datetime, timedelta
except ImportError as e:
    print("Error: Required packages not installed.")
    print("Please install them using: pip install -r requirements.txt")
    print(f"Missing package: {e}")
    sys.exit(1)

def parse_csv_and_create_graph(csv_path):
    """
    Process CSV file and create visualization.
    """
    # Read CSV file
    print(f"Reading CSV file: {csv_path}")
    df = pd.read_csv(csv_path)
    
    # Display basic info
    print(f"Total records: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")
    
    # Parse datetime columns
    df['signupTimestamp'] = pd.to_datetime(df['signupTimestamp'], format='%Y-%m-%d %H:%M', errors='coerce')
    df['Create Date'] = pd.to_datetime(df['Create Date'], format='%Y-%m-%d %H:%M', errors='coerce')
    
    # Remove rows with invalid dates
    initial_count = len(df)
    df = df.dropna(subset=['signupTimestamp', 'Create Date'])
    removed_count = initial_count - len(df)
    if removed_count > 0:
        print(f"Removed {removed_count} rows with invalid dates")
    
    # Filter data to specified time range: 2025-12-15 16:00 to 2025-12-17 00:00
    start_time = pd.to_datetime('2025-12-15 16:00', format='%Y-%m-%d %H:%M')
    end_time = pd.to_datetime('2025-12-17 00:00', format='%Y-%m-%d %H:%M')
    
    before_filter = len(df)
    df = df[(df['signupTimestamp'] >= start_time) & (df['signupTimestamp'] <= end_time)]
    after_filter = len(df)
    print(f"Filtered to time range {start_time} to {end_time}")
    print(f"Records after filtering: {after_filter} (removed {before_filter - after_filter} records)")
    
    # Calculate time difference in minutes
    df['time_diff_minutes'] = (df['Create Date'] - df['signupTimestamp']).dt.total_seconds() / 60
    
    # Display statistics
    print(f"\nTime difference statistics:")
    print(f"  Mean: {df['time_diff_minutes'].mean():.2f} minutes")
    print(f"  Median: {df['time_diff_minutes'].median():.2f} minutes")
    print(f"  Min: {df['time_diff_minutes'].min():.2f} minutes")
    print(f"  Max: {df['time_diff_minutes'].max():.2f} minutes")
    
    # Calculate average delay between indicators
    indicator_time1 = pd.to_datetime('2025-12-15 19:40', format='%Y-%m-%d %H:%M')
    indicator_time2 = pd.to_datetime('2025-12-16 20:15', format='%Y-%m-%d %H:%M')
    
    # Filter data between the two indicators
    df_between_indicators = df[(df['signupTimestamp'] >= indicator_time1) & (df['signupTimestamp'] <= indicator_time2)]
    
    if len(df_between_indicators) > 0:
        avg_delay_between = df_between_indicators['time_diff_minutes'].mean()
        print(f"\nAverage delay between indicators ({indicator_time1.strftime('%Y-%m-%d %H:%M')} and {indicator_time2.strftime('%Y-%m-%d %H:%M')}):")
        print(f"  Records: {len(df_between_indicators)}")
        print(f"  Average delay: {avg_delay_between:.2f} minutes")
    else:
        print(f"\nNo records found between indicators ({indicator_time1.strftime('%Y-%m-%d %H:%M')} and {indicator_time2.strftime('%Y-%m-%d %H:%M')})")
    
    # Create the plot
    plt.figure(figsize=(14, 8))
    
    # Scatter plot
    plt.scatter(df['signupTimestamp'], df['time_diff_minutes'], alpha=0.6, s=20)
    
    # Customize the plot
    plt.xlabel('Signup Time (IST)', fontsize=10)
    plt.ylabel('Time Difference (minutes)\n(Create Time (IST) - Signup Time (IST))', fontsize=10)
    plt.title('Company Sync to HS Delay', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    
    # Format x-axis to show dates and hours
    ax = plt.gca()
    
    # Set x-axis limits to start at 2025-12-15 16:00
    ax.set_xlim(left=start_time, right=end_time)
    
    # Set major ticks every 2 hours
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
    
    # Format labels to show date and time (e.g., "2025-12-17 00:00")
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    
    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45, ha='right')
    
    # Auto-format dates to prevent overlap
    plt.gcf().autofmt_xdate()
    
    # Add a horizontal line at y=0 for reference
    plt.axhline(y=0, color='r', linestyle='--', alpha=0.5, label='Zero difference')
    
    # Add vertical indicators at specified times (using the same times as calculated above)
    plt.axvline(x=indicator_time1, color='blue', linestyle='--', alpha=0.7, linewidth=1.5, label=indicator_time1.strftime('%Y-%m-%d %H:%M'))
    plt.axvline(x=indicator_time2, color='green', linestyle='--', alpha=0.7, linewidth=1.5, label=indicator_time2.strftime('%Y-%m-%d %H:%M'))
    
    # Add legend
    plt.legend()
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    # Save the plot
    output_file = csv_path.replace('.csv', '_graph.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\nGraph saved to: {output_file}")
    
    # Close the figure to free memory (comment out plt.show() to avoid displaying)
    plt.close()
    
    return df

if __name__ == "__main__":
    # Default CSV path
    csv_path = "/Users/yamtirosh/Downloads/hubspot-crm-exports-company-sync-2-2025-12-17.csv"
    
    # Allow command line argument for CSV path
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    
    try:
        df = parse_csv_and_create_graph(csv_path)
        print("\nProcessing completed successfully!")
    except FileNotFoundError:
        print(f"Error: File not found: {csv_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Error processing file: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

