# ingest_engine.py

import asyncio
import aiohttp
import os
import pandas as pd
import json
import argparse
from google.cloud import bigquery, storage
from datetime import datetime, timezone, timedelta
import uuid

# --- Configuration (Unchanged) ---
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME") 
BQ_DATASET = "air_quality"
BQ_TABLE = "raw_hourly_aqi"
LOCATIONS_FILE = 'nyc_land_points_final.csv'
MAX_CONCURRENT_REQUESTS = 250
SCHEMA = [
    bigquery.SchemaField("latitude", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("longitude", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("datetime", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("aqi", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("aqi_code", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
]

# --- configure_gcs_cors, get_time_period, fetch_one_location (Unchanged) ---

def configure_gcs_cors():
    origin_url = os.environ.get("FIREBASE_ORIGIN")
    if not origin_url: print("WARNING: FIREBASE_ORIGIN not set. Skipping CORS config."); return
    print(f"--- Configuring CORS policy for origin: {origin_url} ---")
    try:
        storage_client = storage.Client(project=GCP_PROJECT_ID)
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        cors_policy = [{"origin": [origin_url], "method": ["GET"], "maxAgeSeconds": 3600}]
        bucket.reload()
        if bucket.cors == cors_policy: print("CORS policy is already up-to-date."); return
        bucket.cors = cors_policy; bucket.patch()
        print(f"Successfully set CORS policy on bucket gs://{GCS_BUCKET_NAME}")
    except Exception as e:
        print(f"WARNING: Could not configure GCS CORS policy. This is non-fatal. Error: {e}")

def get_time_period(days=None):
    end_time = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    if days:
        start_time = end_time - timedelta(days=int(days))
    else:
        first_day_of_current_month = end_time.replace(day=1)
        last_day_of_previous_month = first_day_of_current_month - timedelta(seconds=1)
        start_time = last_day_of_previous_month.replace(day=1)
        end_time = last_day_of_previous_month.replace(hour=23, minute=59, second=59)
    return {
        "startTime": start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "endTime": end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    }

async def fetch_one_location(session, lat, lon, period):
    api_url = f"https://airquality.googleapis.com/v1/history:lookup?key={GOOGLE_API_KEY}"
    payload = {"location": {"latitude": lat, "longitude": lon}, "period": period}
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
                    "aqi": uAQI.get('aqi'), "aqi_code": uAQI.get('code'), "category": uAQI.get('category')
                })
            return records
    except Exception as e:
        print(f"  - Error fetching {lat},{lon}: {e}"); return []


# --- FIX #1: IDEMPOTENT BigQuery Load using MERGE ---
def load_to_bigquery(records, bq_client):
    if not records:
        print("No new records to load. Skipping BigQuery load.")
        return

    print("--- Loading Data into BigQuery using idempotent MERGE ---")
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    temp_table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.temp_{uuid.uuid4().hex}"

    df = pd.DataFrame(records)
    df['datetime'] = pd.to_datetime(df['datetime'])

    # Step 1: Load new data into a temporary table
    print(f"Loading {len(df)} fetched records into temporary table: {temp_table_id}")
    load_job = bq_client.load_table_from_dataframe(df, temp_table_id, job_config=bigquery.LoadJobConfig(schema=SCHEMA))
    load_job.result()

    # Step 2: Merge the temporary table into the main table
    merge_query = f"""
        MERGE `{table_id}` T
        USING `{temp_table_id}` S
        ON T.latitude = S.latitude AND T.longitude = S.longitude AND T.datetime = S.datetime
        WHEN NOT MATCHED THEN
            INSERT (latitude, longitude, datetime, aqi, aqi_code, category)
            VALUES(S.latitude, S.longitude, S.datetime, S.aqi, S.aqi_code, S.category)
    """
    print("Merging new records into the main table...")
    merge_job = bq_client.query(merge_query)
    merge_job.result()
    
    # Step 3: Clean up the temporary table
    bq_client.delete_table(temp_table_id)
    print(f"Successfully merged {merge_job.num_dml_affected_rows} new rows. Temporary table deleted.")

# --- FIX #2: PRE-FLIGHT CHECK to avoid redundant API calls ---
def check_if_data_exists(bq_client, lat, lon, period):
    """Checks if any data already exists for the given location and period."""
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    query = f"""
        SELECT 1 FROM `{table_id}`
        WHERE latitude = @lat AND longitude = @lon AND datetime BETWEEN @start_time AND @end_time
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
    results = bq_client.query(query, job_config=job_config).result()
    return results.total_rows > 0

# --- aggregate_and_publish (Unchanged) ---
def aggregate_and_publish():
    print("--- Aggregating All Data for Map (Multi-Timeframe) ---")
    client = bigquery.Client(project=GCP_PROJECT_ID)
    query = f"""
        WITH all_time_stats AS (
            SELECT 'all_time' AS timeframe, latitude, longitude,
                APPROX_QUANTILES(aqi, 2)[OFFSET(1)] AS median_aqi, MAX(aqi) AS max_aqi, MIN(aqi) AS min_aqi, COUNTIF(aqi IS NOT NULL) AS point_count
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}` WHERE latitude IS NOT NULL AND longitude IS NOT NULL GROUP BY latitude, longitude
        ),
        month_name_stats AS (
            SELECT FORMAT_TIMESTAMP('%B', datetime) AS timeframe, latitude, longitude,
                APPROX_QUANTILES(aqi, 2)[OFFSET(1)] AS median_aqi, MAX(aqi) AS max_aqi, MIN(aqi) AS min_aqi, COUNTIF(aqi IS NOT NULL) AS point_count
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}` WHERE latitude IS NOT NULL AND longitude IS NOT NULL GROUP BY timeframe, latitude, longitude
        )
        SELECT * FROM all_time_stats UNION ALL SELECT * FROM month_name_stats
    """
    job_config = bigquery.QueryJobConfig(use_query_cache=False)
    print("Executing optimized aggregation query...")
    bq_results = client.query(query, job_config=job_config).result()

    locations_dict, all_timeframes = {}, set()
    for row in bq_results:
        lat, lon = round(row["latitude"], 5), round(row["longitude"], 5)
        key = (lat, lon)
        if key not in locations_dict: locations_dict[key] = {"latitude": lat, "longitude": lon, "stats": {}}
        timeframe = row["timeframe"]
        all_timeframes.add(timeframe)
        locations_dict[key]["stats"][timeframe] = { "median_aqi": row["median_aqi"], "max_aqi": row["max_aqi"], "min_aqi": row["min_aqi"], "point_count": row["point_count"] }

    final_json_payload = { "timeframes": sorted(list(all_timeframes), key=lambda x: (x != 'all_time', x)), "locations": list(locations_dict.values()) }
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob("map_data.json")
    json_string = json.dumps(final_json_payload, indent=2)
    blob.cache_control = "no-cache, no-store, must-revalidate"
    blob.upload_from_string(json_string, content_type='application/json')
    blob.make_public()
    print(f"Upload complete. Map data is now live at: {blob.public_url}")


async def main(args):
    configure_gcs_cors()
    
    period = get_time_period(args.days)
    all_records = []
    bq_client = bigquery.Client(project=GCP_PROJECT_ID) # Create one client to reuse

    if args.latitude and args.longitude:
        # --- BACKFILL MODE with PRE-FLIGHT CHECK ---
        print(f"--- Running in Single Location Mode for {args.latitude}, {args.longitude} ---")
        lat, lon = float(args.latitude), float(args.longitude)
        
        if check_if_data_exists(bq_client, lat, lon, period):
            print("Data for this location and period already exists in BigQuery. Skipping API fetch to save costs.")
        else:
            print("No existing data found. Proceeding with API fetch.")
            async with aiohttp.ClientSession() as session:
                records = await fetch_one_location(session, lat, lon, period)
                all_records.extend(records)
    else:
        # --- MONTHLY INGESTION MODE (relies on MERGE for idempotency) ---
        print("--- Running in Monthly Ingestion Mode ---")
        if not os.path.exists(LOCATIONS_FILE):
            print(f"FATAL: Locations file '{LOCATIONS_FILE}' not found for monthly run."); return
        
        locations_df = pd.read_csv(LOCATIONS_FILE)
        print(f"--- Fetching data for period: {period['startTime']} to {period['endTime']} ---")
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        async def limited_fetch(lat, lon, session, semaphore, period):
            async with semaphore: return await fetch_one_location(session, lat, lon, period)
        
        async with aiohttp.ClientSession() as session:
            tasks = [limited_fetch(row['latitude'], row['longitude'], session, semaphore, period) for _, row in locations_df.iterrows()]
            results = await asyncio.gather(*tasks)
            for res in results: all_records.extend(res)

    print(f"Fetched a total of {len(all_records)} new hourly records.")
    
    if all_records:
        load_to_bigquery(all_records, bq_client) # Pass the client
    else:
        print("No new data was fetched or loaded.")
    
    aggregate_and_publish()
    print("--- Process Complete! ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified AQI Ingestion and Backfill Engine.")
    parser.add_argument("--latitude", help="Latitude for single-location backfill.")
    parser.add_argument("--longitude", help="Longitude for single-location backfill.")
    parser.add_argument("--days", type=int, help="Number of past days to fetch. Defaults to last full month if not set.")
    args = parser.parse_args()
    
    if (args.latitude and not args.longitude) or (not args.latitude and args.longitude):
        parser.error("--latitude and --longitude must be used together.")

    asyncio.run(main(args))