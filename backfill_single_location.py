import asyncio
import aiohttp
import os
import json
import argparse
from google.cloud import bigquery, storage
from datetime import datetime, timezone, timedelta

# --- Configuration from Environment Variables ---
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
BQ_DATASET = "air_quality"
BQ_TABLE = "raw_hourly_aqi"

# --- Import shared functions from the main engine ---
from ingest_engine import SCHEMA, aggregate_and_publish, load_to_bigquery, configure_gcs_cors

def get_date_period(days_to_fetch):
    """Calculates start and end for the last N days in RFC3339 format."""
    end_time = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=int(days_to_fetch))
    return {
        "startTime": start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "endTime": end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    }

def check_if_data_exists(client, lat, lon, period):
    """
    Queries BigQuery to see if any data already exists for this
    location and time period. This is the idempotency check.
    """
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    query = f"""
        SELECT 1
        FROM `{table_id}`
        WHERE latitude = @lat
          AND longitude = @lon
          AND datetime >= @start_time
          AND datetime < @end_time
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("lat", "FLOAT64", lat),
            bigquery.ScalarQueryParameter("lon", "FLOAT64", lon),
            bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", period["startTime"]),
            bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", period["endTime"]),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    results = list(query_job.result())
    return len(results) > 0

async def fetch_one_location(lat, lon, period):
    """Asynchronously fetches historical AQI for the single specified location."""
    api_url = f"https://airquality.googleapis.com/v1/history:lookup?key={GOOGLE_API_KEY}"
    payload = {"location": {"latitude": lat, "longitude": lon}, "period": period}
    
    print(f"Fetching data for {lat}, {lon}...")
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(api_url, json=payload) as response:
                response.raise_for_status()
                data = await response.json()
                records = []
                for hour_info in data.get('hoursInfo', []):
                    dt_str, indexes = hour_info.get('dateTime'), hour_info.get('indexes', [])
                    if not dt_str or not indexes: continue
                    uAQI = next((idx for idx in indexes if idx.get('code') == 'uAQI'), indexes[0])
                    records.append({
                        "latitude": lat, "longitude": lon, "datetime": dt_str,
                        "aqi": uAQI.get('aqi'), "aqi_code": uAQI.get('code'), "category": uAQI.get('category'),
                    })
                return records
        except Exception as e:
            print(f"  - Error fetching {lat},{lon}: {e}")
            return []

async def main(args):
    """Main orchestration function with corrected logic."""
    # Step 0: Ensure cloud infrastructure is correctly configured
    configure_gcs_cors()

    print(f"--- Starting Backfill for Location {args.latitude}, {args.longitude} ---")
    lat, lon = float(args.latitude), float(args.longitude)
    period = get_date_period(args.days)
    print(f"Requesting data for period: {period['startTime']} to {period['endTime']}")

    bq_client = bigquery.Client(project=GCP_PROJECT_ID)

    # 1. Idempotency Check
    if check_if_data_exists(bq_client, lat, lon, period):
        print("Data for this location and period already exists in BigQuery. Skipping API fetch.")
    else:
        # 2. Fetch and Load only if data is missing
        print("No existing data found. Proceeding with API fetch.")
        records_to_load = await fetch_one_location(lat, lon, period)
        if records_to_load:
            print(f"Fetched {len(records_to_load)} new hourly records.")
            load_to_bigquery(records_to_load)
        else:
            print("API fetch returned no records.")

    # 3. Re-aggregate and Publish - THIS STEP NOW RUNS EVERY TIME
    print("\nRefreshing public map data from BigQuery...")
    aggregate_and_publish()

    print("--- Process Complete! ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill AQI data for a single location.")
    parser.add_argument("--latitude", required=True, help="Latitude of the location.")
    parser.add_argument("--longitude", required=True, help="Longitude of the location.")
    parser.add_argument("--days", default=28, help="Number of past days to fetch data for.")
    args = parser.parse_args()
    asyncio.run(main(args))