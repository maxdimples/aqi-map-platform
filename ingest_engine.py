# ingest_engine.py

import asyncio
import aiohttp
from aiohttp_retry import RetryClient, JitterRetry
import os
import pandas as pd
import json
from google.cloud import bigquery, storage
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
import uuid

# --- Configuration ---
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME") 
BQ_DATASET = "air_quality"
BQ_TABLE = "raw_hourly_aqi"
LOCATIONS_FILE = 'nyc_land_points_final.csv'
MAX_CONCURRENT_REQUESTS = 50
NUM_GROUPS = 4
SCHEMA = [
    bigquery.SchemaField("latitude", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("longitude", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("datetime", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("aqi", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("aqi_code", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
]

# Initialize clients once to be reused
storage_client = storage.Client(project=GCP_PROJECT_ID)

def get_time_period(month_offset=0, days=7):
    today = datetime.now(timezone.utc)
    target_month = today - relativedelta(months=month_offset)
    end_of_month = target_month.replace(day=1) + relativedelta(months=1) - timedelta(days=1)
    end_time = end_of_month.replace(hour=23, minute=59, second=59)
    start_time = end_time - timedelta(days=(days-1))
    return {
        "startTime": start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "endTime": end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    }

async def fetch_one_location(retry_client, lat, lon, period):
    api_url = f"https://airquality.googleapis.com/v1/history:lookup?key={GOOGLE_API_KEY}"
    payload = {"location": {"latitude": lat, "longitude": lon}, "period": period, "pageSize": 168}
    try:
        async with retry_client.post(api_url, json=payload) as response:
            response.raise_for_status()
            data = await response.json()
            
            # --- STAGING UPLOAD ---
            # Save raw response to GCS for durability before processing.
            # This is a critical data preservation step.
            try:
                bucket = storage_client.bucket(GCS_BUCKET_NAME)
                # Sanitize datetime for GCS path
                start_time_safe = period['startTime'].replace(":", "-")
                blob_name = f"raw_responses/{start_time_safe}/{lat}_{lon}.json"
                blob = bucket.blob(blob_name)
                # Run the synchronous GCS upload in a thread pool to avoid blocking asyncio event loop
                await asyncio.to_thread(blob.upload_from_string, json.dumps(data), content_type='application/json')
            except Exception as gcs_e:
                print(f"  - WARNING: GCS upload failed for {lat},{lon}: {gcs_e}")

            # --- PARSING ---
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
        print(f"  - FINAL ERROR after retries for {lat},{lon}: {e}"); return []

def load_to_bigquery(records, bq_client):
    if not records:
        print("No new records to load. Skipping BigQuery load."); return

    print("--- Loading Data into BigQuery using idempotent MERGE ---")
    table_id = f"`{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`"
    temp_table_id = f"`{GCP_PROJECT_ID}.{BQ_DATASET}.temp_{uuid.uuid4().hex}`"

    df = pd.DataFrame(records)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['aqi'] = pd.to_numeric(df['aqi'], errors='coerce').astype('Int64')
    
    df.dropna(subset=['latitude', 'longitude', 'datetime'], inplace=True)
    if df.empty:
        print("DataFrame is empty after cleaning. No data to load.")
        return

    print(f"Loading {len(df)} records into temp table: {temp_table_id}")
    load_job = bq_client.load_table_from_dataframe(df, temp_table_id.strip('`'), job_config=bigquery.LoadJobConfig(schema=SCHEMA))
    load_job.result()

    merge_query = f"""
        MERGE {table_id} T
        USING {temp_table_id} S
        ON T.latitude = S.latitude AND T.longitude = S.longitude AND T.datetime = S.datetime
        WHEN NOT MATCHED THEN
            INSERT (latitude, longitude, datetime, aqi, aqi_code, category)
            VALUES(S.latitude, S.longitude, S.datetime, S.aqi, S.aqi_code, S.category)
    """
    print("Merging new records into the main table...")
    merge_job = bq_client.query(merge_query)
    merge_job.result()
    
    bq_client.delete_table(temp_table_id.strip('`'))
    print(f"Successfully merged {merge_job.num_dml_affected_rows} new rows. Temp table deleted.")

def aggregate_and_publish():
    print("--- Aggregating All Data for Map (Full Precision) ---")
    client = bigquery.Client(project=GCP_PROJECT_ID)
    
    query = f"""
        WITH all_time_stats AS (
            SELECT 
                'All Time' AS timeframe, latitude, longitude,
                ROUND(APPROX_QUANTILES(CAST(aqi AS FLOAT64), 2)[OFFSET(1)], 2) AS median_aqi,
                ROUND(AVG(CAST(aqi AS FLOAT64)), 2) AS avg_aqi,
                CAST(MAX(aqi) AS FLOAT64) AS max_aqi,
                CAST(MIN(aqi) AS FLOAT64) AS min_aqi,
                COUNTIF(aqi IS NOT NULL) AS point_count
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}` WHERE latitude IS NOT NULL GROUP BY latitude, longitude
        ),
        month_stats AS (
            SELECT 
                FORMAT_TIMESTAMP('%Y-%m (%B)', datetime) AS timeframe, latitude, longitude,
                ROUND(APPROX_QUANTILES(CAST(aqi AS FLOAT64), 2)[OFFSET(1)], 2) AS median_aqi,
                ROUND(AVG(CAST(aqi AS FLOAT64)), 2) AS avg_aqi,
                CAST(MAX(aqi) AS FLOAT64) AS max_aqi,
                CAST(MIN(aqi) AS FLOAT64) AS min_aqi,
                COUNTIF(aqi IS NOT NULL) AS point_count
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}` WHERE latitude IS NOT NULL GROUP BY timeframe, latitude, longitude
        )
        SELECT * FROM all_time_stats UNION ALL SELECT * FROM month_stats
    """
    print("Executing float-precision aggregation query...")
    results_df = client.query(query).to_dataframe()
    
    locations_dict, all_timeframes = {}, set()
    for _, row in results_df.iterrows():
        lat, lon = row["latitude"], row["longitude"]
        key = (lat, lon)
        if key not in locations_dict: locations_dict[key] = {"latitude": lat, "longitude": lon, "stats": {}}
        timeframe = row["timeframe"]
        all_timeframes.add(timeframe)
        
        locations_dict[key]["stats"][timeframe] = { 
            "median_aqi": None if pd.isna(row["median_aqi"]) else row["median_aqi"],
            "avg_aqi": None if pd.isna(row["avg_aqi"]) else row["avg_aqi"],
            "max_aqi": None if pd.isna(row["max_aqi"]) else row["max_aqi"], 
            "min_aqi": None if pd.isna(row["min_aqi"]) else row["min_aqi"], 
            "point_count": int(row["point_count"]) 
        }
        
    final_json_payload = { "timeframes": sorted(list(all_timeframes)), "locations": list(locations_dict.values()) }
    
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob("map_data.json")
    blob.cache_control = "no-cache"
    blob.upload_from_string(json.dumps(final_json_payload), content_type='application/json')
    blob.make_public()
    print(f"Upload complete. Map data is now live at: {blob.public_url}")

async def main():
    print("--- Running in Staggered Weekly Ingestion Mode ---")
    retry_options = JitterRetry(attempts=5, start_timeout=1, max_timeout=30, factor=2.0, statuses=[429, 503, 504])
    
    async with RetryClient(retry_options=retry_options, raise_for_status=False) as retry_client:
        locations_df = pd.read_csv(LOCATIONS_FILE)
        bq_client = bigquery.Client(project=GCP_PROJECT_ID)
        
        current_group = datetime.now(timezone.utc).isocalendar()[1] % NUM_GROUPS
        print(f"Processing group #{current_group} of {NUM_GROUPS}.")
        locations_df['group'] = locations_df.index % NUM_GROUPS
        locations_to_process = locations_df[locations_df['group'] == current_group]
        
        period = get_time_period(month_offset=0, days=7)
        print(f"Fetching data for {len(locations_to_process)} locations for period: {period['startTime']} to {period['endTime']}")
        
        all_records = []
        tasks = [fetch_one_location(retry_client, row['latitude'], row['longitude'], period) for _, row in locations_to_process.iterrows()]
        results = await asyncio.gather(*tasks)
        for res in results: all_records.extend(res)

        if all_records:
            load_to_bigquery(all_records, bq_client)

    print("\n--- Final Aggregation Step ---")
    aggregate_and_publish()
    print("--- Process Complete! ---")

if __name__ == "__main__":
    asyncio.run(main())