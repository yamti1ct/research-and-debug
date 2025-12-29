#!/usr/bin/env python3
"""
Analyze scheduling delay from schedule_pubsub_reader logs.

This script calculates the delay between:
- schedule_timestamp: when the message SHOULD have been processed
- asctime: when the reader ACTUALLY picked it up from Redis

Usage:
    python analyze_schedule_delay.py [--input schedule_reader_logs.json]
"""

import argparse
import json
import re
from datetime import datetime, timezone, timedelta
from collections import defaultdict

import matplotlib.pyplot as plt
import matplotlib.dates as mdates


# Regex to extract message_id and schedule_timestamp from "Got message" logs
GOT_MESSAGE_PATTERN = re.compile(
    r"Got message b?'([^']+)' with schedule_timestamp (\d+\.?\d*)"
)


def parse_asctime(asctime_str: str) -> datetime:
    """Parse asctime format: '2025-12-15 19:30:00,594'"""
    # Handle both comma and dot as millisecond separator
    asctime_str = asctime_str.replace(",", ".")
    try:
        return datetime.strptime(asctime_str, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return datetime.strptime(asctime_str, "%Y-%m-%d %H:%M:%S")


def load_logs(input_file: str) -> list:
    """Load logs from JSON file."""
    with open(input_file, "r") as f:
        return json.load(f)


def extract_got_message_data(logs: list) -> list:
    """
    Extract data from 'Got message' logs.
    
    Returns list of tuples: (message_id, schedule_timestamp, actual_time, delay_seconds)
    """
    results = []
    
    for log in logs:
        message = log.get("message", "")
        asctime = log.get("asctime")
        
        if not asctime:
            continue
        
        match = GOT_MESSAGE_PATTERN.search(message)
        if not match:
            continue
        
        message_id = match.group(1)
        schedule_timestamp = float(match.group(2))
        
        # Parse the actual time the log was written (asctime is in UTC)
        actual_time = parse_asctime(asctime)
        
        # Convert schedule_timestamp to datetime (timestamp is in UTC)
        scheduled_time = datetime.utcfromtimestamp(schedule_timestamp)
        
        # Calculate delay in seconds
        delay_seconds = (actual_time - scheduled_time).total_seconds()
        
        results.append({
            "message_id": message_id,
            "scheduled_time": scheduled_time,
            "actual_time": actual_time,
            "delay_seconds": delay_seconds,
            "delay_minutes": delay_seconds / 60,
        })
    
    return results


def deduplicate_by_message_id(data: list) -> list:
    """
    Keep only the first occurrence of each message_id.
    
    The scheduler may log the same message multiple times if it's still
    in the queue being processed by different instances.
    """
    seen = {}
    for entry in data:
        msg_id = entry["message_id"]
        if msg_id not in seen:
            seen[msg_id] = entry
    
    return list(seen.values())


def filter_valid_delays(data: list) -> list:
    """
    Filter out entries with negative delays (future scheduled messages)
    or unreasonably large delays (data issues).
    """
    valid = []
    skipped_future = 0
    skipped_large = 0
    
    for entry in data:
        delay = entry["delay_seconds"]
        
        # Skip messages scheduled for the future (negative delay)
        if delay < 0:
            skipped_future += 1
            continue
        
        # Skip unreasonably large delays (more than 7 days - likely data issues)
        if delay > 7 * 24 * 60 * 60:
            skipped_large += 1
            continue
        
        valid.append(entry)
    
    if skipped_future > 0:
        print(f"Skipped {skipped_future} messages scheduled for the future")
    if skipped_large > 0:
        print(f"Skipped {skipped_large} messages with unreasonably large delays (>7 days)")
    
    return valid


def print_statistics(data: list):
    """Print summary statistics."""
    if not data:
        print("No data to analyze.")
        return
    
    delays = [d["delay_minutes"] for d in data]
    
    print("\n" + "=" * 60)
    print("SCHEDULING DELAY STATISTICS")
    print("=" * 60)
    print(f"Total messages analyzed: {len(data)}")
    print(f"\nDelay (in minutes):")
    print(f"  Min:    {min(delays):.2f}")
    print(f"  Max:    {max(delays):.2f}")
    print(f"  Mean:   {sum(delays) / len(delays):.2f}")
    
    sorted_delays = sorted(delays)
    p50 = sorted_delays[len(sorted_delays) // 2]
    p90 = sorted_delays[int(len(sorted_delays) * 0.9)]
    p99 = sorted_delays[int(len(sorted_delays) * 0.99)]
    
    print(f"  Median (p50): {p50:.2f}")
    print(f"  p90:    {p90:.2f}")
    print(f"  p99:    {p99:.2f}")
    
    # Time range
    times = [d["actual_time"] for d in data]
    print(f"\nTime range:")
    print(f"  First: {min(times)}")
    print(f"  Last:  {max(times)}")
    
    # Bucket by delay ranges
    buckets = defaultdict(int)
    for delay in delays:
        if delay < 1:
            buckets["< 1 min"] += 1
        elif delay < 5:
            buckets["1-5 min"] += 1
        elif delay < 15:
            buckets["5-15 min"] += 1
        elif delay < 60:
            buckets["15-60 min"] += 1
        elif delay < 360:
            buckets["1-6 hours"] += 1
        else:
            buckets["> 6 hours"] += 1
    
    print("\nDelay distribution:")
    for bucket in ["< 1 min", "1-5 min", "5-15 min", "15-60 min", "1-6 hours", "> 6 hours"]:
        count = buckets.get(bucket, 0)
        pct = count / len(delays) * 100
        bar = "█" * int(pct / 2)
        print(f"  {bucket:12s}: {count:6d} ({pct:5.1f}%) {bar}")


def plot_delay_over_time(data: list, output_file: str = "schedule_delay_plot.png"):
    """Create a plot of delay over time."""
    if not data:
        print("No data to plot.")
        return
    
    # Sort by actual time
    sorted_data = sorted(data, key=lambda x: x["scheduled_time"])
    
    # Convert times to IST (UTC+2) for display
    IST_OFFSET = timedelta(hours=2)
    times = [d["scheduled_time"] + IST_OFFSET for d in sorted_data]
    delays = [d["delay_minutes"] for d in sorted_data]
    
    # Create figure with single plot
    fig, ax = plt.subplots(figsize=(14, 6))
    
    # Scatter plot of delay over time
    ax.scatter(times, delays, alpha=0.5, s=10, c='blue')
    ax.set_xlabel("Scheduled Time (IST)")
    ax.set_ylabel("Delay (minutes)")
    ax.set_title("Scheduling Delay Over Scheduled Time")
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    ax.grid(True, alpha=0.3)
    
    # Add a line connecting the points
    ax.plot(times, delays, color='red', linewidth=1, alpha=0.7)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=150)
    print(f"\nPlot saved to: {output_file}")
    
    # Also show the plot
    plt.show()


def main():
    parser = argparse.ArgumentParser(
        description="Analyze scheduling delay from schedule_pubsub_reader logs"
    )
    parser.add_argument(
        "--input", "-i",
        default="schedule_reader_logs.json",
        help="Input JSON file with logs (default: schedule_reader_logs.json)"
    )
    parser.add_argument(
        "--output", "-o",
        default="schedule_delay_plot.png",
        help="Output plot file (default: schedule_delay_plot.png)"
    )
    parser.add_argument(
        "--no-dedup",
        action="store_true",
        help="Don't deduplicate by message_id (keep all log entries)"
    )
    
    args = parser.parse_args()
    
    print(f"Loading logs from: {args.input}")
    logs = load_logs(args.input)
    print(f"Loaded {len(logs)} log entries")
    
    # Extract data from "Got message" logs
    data = extract_got_message_data(logs)
    print(f"Found {len(data)} 'Got message' entries")
    
    # Deduplicate by message_id
    if not args.no_dedup:
        data = deduplicate_by_message_id(data)
        print(f"After deduplication: {len(data)} unique messages")
    
    # Filter out invalid delays
    data = filter_valid_delays(data)
    print(f"After filtering: {len(data)} valid entries")
    
    # Print statistics
    print_statistics(data)
    
    # Create plot
    plot_delay_over_time(data, args.output)


if __name__ == "__main__":
    main()

