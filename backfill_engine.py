# backfill_engine.py (Final, Optimized)

import asyncio
import aiohttp
from aiohttp_retry import RetryClient, JitterRetry
import os
import pandas as pd
import argparse
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import uuid

# Import shared functions from the main engine
from ingest_engine import (
    fetch_one_location, load_to_bigquery, aggregate_and_publish,
    get_time_period, LOCATIONS_FILE, GOOGLE_API_KEY, GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE, SCHEMA
)
from google.cloud import bigquery

# High-throughput throttling configuration
CHUNK_SIZE = 1000
DELAY_BETWEEN_CHUNKS = 21

async def get_completed_locations(bq_client, period):
    """
    BEST PRACTICE: Pre-flight check.
    Queries BigQuery to find locations that already have complete data for the given period.
    A "complete" location has >150 records for a 7-day period (168 hours).
    """
    print(f"  - Checking for already completed locations in period: {period['startTime']} to {period['endTime']}...")
    table_id = f"`{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`"
    query = f"""
        SELECT latitude, longitude
        FROM {table_id}
        WHERE datetime BETWEEN @start_time AND @end_time
        GROUP BY latitude, longitude
        HAVING COUNT(*) >= 150
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", period["startTime"]),
            bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", period["endTime"]),
        ]
    )
    try:
        results = bq_client.query(query, job_config=job_config).result()
        completed = { (round(row.latitude, 5), round(row.longitude, 5)) for row in results }
        print(f"  - Found {len(completed)} locations with complete data for this period. They will be skipped.")
        return completed
    except Exception as e:
        print(f"  - WARNING: Could not check for existing data. Will fetch all. Error: {e}")
        return set()

async def main(args):
    print(f"--- Starting HIGH-THROUGHPUT Backfill for Month: {args.month} ---")
    
    retry_options = JitterRetry(
        attempts=10, start_timeout=5, max_timeout=120, factor=2.0, statuses=[429, 503, 504]
    )
    retry_client = RetryClient(retry_options=retry_options, raise_for_status=False)
    
    if not os.path.exists(LOCATIONS_FILE):
        print(f"FATAL: Locations file '{LOCATIONS_FILE}' not found."); return

    locations_df = pd.read_csv(LOCATIONS_FILE)
    all_locations = [
        {"latitude": round(row['latitude'], 5), "longitude": round(row['longitude'], 5)} 
        for _, row in locations_df.iterrows()
    ]
    bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    
    year, month = map(int, args.month.split('-'))
    start_of_month = datetime(year, month, 1, tzinfo=timezone.utc)
    end_of_month = start_of_month + relativedelta(months=1) - relativedelta(days=1)
    
    total_days = (end_of_month - start_of_month).days + 1
    num_pages = (total_days + 6) // 7

    all_records_for_month = []

    for page in range(num_pages):
        start_day = page * 7
        end_day = min(start_day + 6, total_days - 1)
        
        start_date = start_of_month + relativedelta(days=start_day)
        end_date = start_of_month + relativedelta(days=end_day)

        period = {
            "startTime": start_date.strftime('%Y-%m-%dT00:00:00Z'),
            "endTime": end_date.strftime('%Y-%m-%dT23:59:59Z')
        }
        print(f"\n--- Processing Page {page + 1}/{num_pages} | Period: {period['startTime']} to {period['endTime']} ---")

        # PRE-FLIGHT CHECK to get locations to skip
        completed_locations = await get_completed_locations(bq_client, period)
        
        # Filter out already completed locations
        locations_to_process = [
            loc for loc in all_locations 
            if (loc['latitude'], loc['longitude']) not in completed_locations
        ]
        
        if not locations_to_process:
            print("  - All locations for this period are already complete. Skipping to next page.")
            continue

        print(f"  - Need to fetch data for {len(locations_to_process)} new locations.")
        
        chunks = [locations_to_process[i:i + CHUNK_SIZE] for i in range(0, len(locations_to_process), CHUNK_SIZE)]
        
        for index, chunk in enumerate(chunks):
            print(f"  - Processing chunk {index + 1} of {len(chunks)}...")
            tasks = [fetch_one_location(retry_client, loc['latitude'], loc['longitude'], period) for loc in chunk]
            results = await asyncio.gather(*tasks)
            for res in results: all_records_for_month.extend(res)
            
            if index < len(chunks) - 1:
                print(f"  - Chunk complete. Throttling for {DELAY_BETWEEN_CHUNKS} seconds...")
                await asyncio.sleep(DELAY_BETWEEN_CHUNKS)

    print(f"\nFetched {len(all_records_for_month)} total new records for the entire month.")
    if all_records_for_month:
        load_to_bigquery(all_records_for_month, bq_client)

    await retry_client.close()
    print("\n--- Final Aggregation Step ---")
    aggregate_and_publish()
    print("--- Backfill Process Complete! ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="High-throughput backfill engine for a single month.")
    parser.add_argument("--month", required=True, help="The month to backfill in YYYY-MM format (e.g., 2025-07).")
    args = parser.parse_args()
    asyncio.run(main(args))