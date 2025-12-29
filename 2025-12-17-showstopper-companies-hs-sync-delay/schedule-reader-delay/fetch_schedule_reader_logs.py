#!/usr/bin/env python3
"""
Fetch schedule_pubsub_reader logs from Elasticsearch.

Usage:
    python fetch_schedule_reader_logs.py --username YOUR_USERNAME --password YOUR_PASSWORD

Output:
    schedule_reader_logs.json - All fetched logs in a clean JSON format
"""

import argparse
import json
import requests
import urllib3
from datetime import datetime, timedelta

# Suppress SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
ELASTICSEARCH_URL = "https://app.prod.connecteam.com:9000/elastic/internal/search/es"
INDEX_PATTERN = "matrix-logs*"
OUTPUT_FILE = "schedule_reader_logs.json"

# Time range (UTC)
START_TIME = "2025-12-15T17:30:00Z"
END_TIME = "2025-12-16T18:30:00Z"

# Service to filter
SERVICE_NAME = "schedule_pubsub_reader"

# Batch size for pagination (keep under 10000 to avoid ES limit)
BATCH_SIZE = 5000

# Sampling configuration
# To get ~5% sample spread over time: fetch 3 minutes every hour or 30 seconds every 10 minutes
SAMPLE_INTERVAL = timedelta(minutes=10)  # How often to sample
SAMPLE_DURATION = timedelta(seconds=30)  # How much to fetch each sample


def parse_time(time_str: str) -> datetime:
    """Parse ISO format time string."""
    # Handle both with and without Z suffix
    time_str = time_str.rstrip("Z")
    try:
        return datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%f")


def format_time(dt: datetime) -> str:
    """Format datetime to ISO string for ES query."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_time_windows(start: datetime, end: datetime, interval: timedelta, duration: timedelta) -> list:
    """
    Generate list of (start, end) time windows for sampling.
    
    Args:
        start: Overall start time
        end: Overall end time
        interval: How often to sample (e.g., every hour)
        duration: How long each sample window is (e.g., 3 minutes)
    
    Returns:
        List of (window_start, window_end) tuples
    """
    windows = []
    current = start
    while current < end:
        # Each sample window is 'duration' long, starting at 'current'
        window_end = min(current + duration, end)
        windows.append((current, window_end))
        # Jump to next sample interval
        current = current + interval
    return windows


def build_query(start_time: str, end_time: str, from_offset: int = 0):
    """Build the Elasticsearch query with time range and pagination offset."""
    return {
        "params": {
            "index": INDEX_PATTERN,
            "body": {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "json.service_name.keyword": SERVICE_NAME
                                }
                            },
                            {
                                "match_phrase": {
                                    "json.message": "Got message"
                                }
                            }
                        ],
                        "filter": [
                            {
                                "range": {
                                    "@timestamp": {
                                        "gte": start_time,
                                        "lt": end_time
                                    }
                                }
                            }
                        ]
                    }
                },
                "sort": [
                    {"@timestamp": {"order": "asc"}}
                ],
                "_source": [
                    "@timestamp",
                    "json.message",
                    "json.asctime",
                    "json.levelname",
                    "json.service_name",
                    "ct_deployment",
                    "json.extra"
                ],
                "from": from_offset,
                "size": BATCH_SIZE
            }
        }
    }


def extract_log_entry(hit: dict) -> dict:
    """Extract clean log entry from ES hit."""
    source = hit.get("_source", {})
    json_field = source.get("json", {})
    
    # Handle both nested and flat field structures
    if isinstance(json_field, dict):
        return {
            "timestamp": source.get("@timestamp"),
            "asctime": json_field.get("asctime"),
            "message": json_field.get("message"),
            "level": json_field.get("levelname"),
            "service": json_field.get("service_name"),
            "deployment": source.get("ct_deployment"),
            "extra": json_field.get("extra"),
        }
    else:
        return {
            "timestamp": source.get("@timestamp"),
            "asctime": source.get("json.asctime"),
            "message": source.get("json.message"),
            "level": source.get("json.levelname"),
            "service": source.get("json.service_name"),
            "deployment": source.get("ct_deployment"),
            "extra": source.get("json.extra"),
        }


def fetch_logs_for_window(
    username: str, 
    password: str, 
    start_time: str, 
    end_time: str,
    headers: dict
) -> list:
    """Fetch all logs for a specific time window using from/size pagination."""
    logs = []
    from_offset = 0
    page = 1
    
    while True:
        query = build_query(start_time, end_time, from_offset)
        
        response = requests.post(
            ELASTICSEARCH_URL,
            auth=(username, password),
            headers=headers,
            json=query,
            verify=False
        )
        
        if response.status_code != 200:
            print(f"    Error: HTTP {response.status_code}")
            print(f"    {response.text[:500]}")
            break
        
        result = response.json()
        
        # Handle the nested response structure from Kibana proxy
        if "rawResponse" in result:
            hits_data = result["rawResponse"]["hits"]
        else:
            hits_data = result.get("hits", {})
        
        hits = hits_data.get("hits", [])
        total = hits_data.get("total", {})
        
        if isinstance(total, dict):
            total_count = total.get("value", 0)
        else:
            total_count = total
        
        if page == 1 and total_count > 0:
            print(f"    Total in window: {total_count}")
        
        if not hits:
            break
        
        # Extract log entries
        for hit in hits:
            logs.append(extract_log_entry(hit))
        
        # Update offset for next page
        from_offset += len(hits)
        page += 1
        
        # Check if we've fetched all logs in this window
        if len(hits) < BATCH_SIZE:
            break
        
        # Safety check: don't exceed total count or ES limit
        if from_offset >= total_count or from_offset >= 10000:
            if from_offset >= 10000 and total_count > 10000:
                print(f"    Warning: Hit ES 10k limit, {total_count - from_offset} logs may be missed in this window")
            break
    
    return logs


def fetch_logs(username: str, password: str) -> list:
    """Fetch all logs using hourly time windows + from/size pagination."""
    all_logs = []
    headers = {
        "Content-Type": "application/json",
        "kbn-xsrf": "true"
    }
    
    # Parse time range
    start_dt = parse_time(START_TIME)
    end_dt = parse_time(END_TIME)
    
    # Generate sampled time windows (e.g., 3 minutes every hour = 5% sample)
    windows = generate_time_windows(start_dt, end_dt, SAMPLE_INTERVAL, SAMPLE_DURATION)
    
    sample_pct = (SAMPLE_DURATION.total_seconds() / SAMPLE_INTERVAL.total_seconds()) * 100
    print(f"Fetching logs from {START_TIME} to {END_TIME}")
    print(f"Service: {SERVICE_NAME}")
    print(f"Sampling: {SAMPLE_DURATION} every {SAMPLE_INTERVAL} (~{sample_pct:.1f}% of data)")
    print(f"Split into {len(windows)} sample windows")
    print("=" * 60)
    
    for i, (window_start, window_end) in enumerate(windows, 1):
        start_str = format_time(window_start)
        end_str = format_time(window_end)
        
        print(f"\n[{i}/{len(windows)}] Window: {start_str} to {end_str}")
        
        window_logs = fetch_logs_for_window(
            username, password, start_str, end_str, headers
        )
        
        all_logs.extend(window_logs)
        print(f"    Fetched {len(window_logs)} logs (total so far: {len(all_logs)})")
    
    return all_logs


def save_logs(logs: list, output_file: str):
    """Save logs to a JSON file."""
    with open(output_file, "w") as f:
        json.dump(logs, f, indent=2)
    print(f"\nSaved {len(logs)} logs to {output_file}")


def print_summary(logs: list):
    """Print a summary of the fetched logs."""
    if not logs:
        print("\nNo logs found.")
        return
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total logs: {len(logs)}")
    
    # Count by level
    levels = {}
    for log in logs:
        level = log.get("level", "UNKNOWN")
        levels[level] = levels.get(level, 0) + 1
    
    print("\nBy log level:")
    for level, count in sorted(levels.items(), key=lambda x: -x[1]):
        print(f"  {level}: {count}")
    
    # Count by deployment
    deployments = {}
    for log in logs:
        deployment = log.get("deployment", "UNKNOWN")
        deployments[deployment] = deployments.get(deployment, 0) + 1
    
    print("\nBy deployment:")
    for deployment, count in sorted(deployments.items(), key=lambda x: -x[1]):
        print(f"  {deployment}: {count}")
    
    # Time range
    timestamps = [log.get("timestamp") for log in logs if log.get("timestamp")]
    if timestamps:
        print(f"\nTime range:")
        print(f"  First log: {min(timestamps)}")
        print(f"  Last log: {max(timestamps)}")


def main():
    parser = argparse.ArgumentParser(description="Fetch schedule_pubsub_reader logs from Elasticsearch")
    parser.add_argument("--username", "-u", required=True, help="Elasticsearch username")
    parser.add_argument("--password", "-p", required=True, help="Elasticsearch password")
    parser.add_argument("--output", "-o", default=OUTPUT_FILE, help=f"Output file (default: {OUTPUT_FILE})")
    
    args = parser.parse_args()
    
    logs = fetch_logs(args.username, args.password)
    save_logs(logs, args.output)
    print_summary(logs)


if __name__ == "__main__":
    main()
