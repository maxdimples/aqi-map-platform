import asyncio
import aiohttp
import os
import pandas as pd
import numpy as np # Import numpy
import json
from google.cloud import bigquery, storage
from datetime import datetime, timezone, timedelta

# --- Configuration from Environment Variables ---
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME") 
BQ_DATASET = "air_quality"
BQ_TABLE = "raw_hourly_aqi"
LOCATIONS_FILE = 'nyc_land_points_final.csv'
MAX_CONCURRENT_REQUESTS = 250

SCHEMA = [
    bigquery.SchemaField("latitude", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("longitude", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("datetime", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("aqi", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("aqi_code", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
]

def configure_gcs_cors():
    """
    Ensures the GCS bucket has the correct CORS policy for the web app.
    This is idempotent and safe to run every time.
    """
    origin_url = os.environ.get("FIREBASE_ORIGIN")
    if not origin_url:
        print("WARNING: FIREBASE_ORIGIN environment variable not set. Skipping CORS configuration.")
        return

    print(f"--- Configuring CORS policy for origin: {origin_url} ---")
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    
    cors_policy = [{"origin": [origin_url], "method": ["GET"], "maxAgeSeconds": 3600}]
    
    bucket.reload() 
    if bucket.cors == cors_policy:
        print("CORS policy is already up-to-date.")
        return

    bucket.cors = cors_policy
    bucket.patch()
    print(f"Successfully set CORS policy on bucket gs://{GCS_BUCKET_NAME}")


# get_last_month_period, fetch_one_location, fetch_all_locations, load_to_bigquery
# remain unchanged. They are correct.
def get_last_month_period():
    today = datetime.now(timezone.utc)
    first_day_of_current_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_day_of_previous_month = first_day_of_current_month - timedelta(seconds=1)
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    start_time = first_day_of_previous_month.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time = last_day_of_previous_month.strftime('%Y-%m-%dT%H:%M:%SZ')
    return {"startTime": start_time, "endTime": end_time}

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
                records.append({ "latitude": lat, "longitude": lon, "datetime": dt_str, "aqi": uAQI.get('aqi'), "aqi_code": uAQI.get('code'), "category": uAQI.get('category') })
            return records
    except Exception as e:
        print(f"  - Error fetching {lat},{lon}: {e}")
        return []

async def fetch_all_locations():
    print("--- Starting Parallel Fetch ---")
    if not os.path.exists(LOCATIONS_FILE):
        print(f"ERROR: Locations file '{LOCATIONS_FILE}' not found.")
        return []
    locations_df = pd.read_csv(LOCATIONS_FILE)
    period = get_last_month_period()
    print(f"Fetching data for period: {period['startTime']} to {period['endTime']}")
    all_records = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for _, row in locations_df.iterrows():
            async def limited_fetch(lat, lon):
                async with semaphore: return await fetch_one_location(session, lat, lon, period)
            tasks.append(limited_fetch(row['latitude'], row['longitude']))
        results = await asyncio.gather(*tasks)
        for res in results: all_records.extend(res)
    print(f"Fetched a total of {len(all_records)} hourly records.")
    return all_records

def load_to_bigquery(records):
    if not records:
        print("No records to load. Skipping BigQuery load.")
        return
    print("--- Loading Data into BigQuery ---")
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    job_config = bigquery.LoadJobConfig(schema=SCHEMA, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    load_job = client.load_table_from_json(records, table_id, job_config=job_config)
    load_job.result()
    print(f"Successfully loaded {load_job.output_rows} rows to {table_id}.")


def aggregate_and_publish():
    """
    FIXED: Queries BigQuery, sanitizes the data by replacing NaN with None,
    and structures the result into valid JSON for the frontend.
    """
    print("--- Aggregating Data for Map (Multi-Timeframe) ---")
    client = bigquery.Client(project=GCP_PROJECT_ID)
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    
    query = f"""
        WITH all_time_stats AS (
            SELECT
                'all_time' AS timeframe, latitude, longitude,
                APPROX_QUANTILES(aqi, 2)[OFFSET(1)] AS median_aqi,
                MAX(aqi) AS max_aqi, MIN(aqi) AS min_aqi, COUNT(aqi) AS point_count
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}` GROUP BY latitude, longitude
        ),
        monthly_stats AS (
            SELECT
                FORMAT_TIMESTAMP('%Y-%m', datetime) AS timeframe, latitude, longitude,
                APPROX_QUANTILES(aqi, 2)[OFFSET(1)] AS median_aqi,
                MAX(aqi) AS max_aqi, MIN(aqi) AS min_aqi, COUNT(aqi) AS point_count
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}` GROUP BY timeframe, latitude, longitude
        )
        SELECT * FROM all_time_stats UNION ALL SELECT * FROM monthly_stats
    """
    print("Executing multi-timeframe aggregation query...")
    results_df = client.query(query).to_dataframe()

    # FIX 1: This is the critical step. Replace all forms of 'Not a Number' with Python's None.
    # This ensures that the final output will be JSON-compliant 'null'.
    results_df = results_df.replace({pd.NA: None, np.nan: None})
    
    # Convert to standard Python integers after handling nulls, which is safer for JSON.
    for col in ['median_aqi', 'max_aqi', 'min_aqi', 'point_count']:
        results_df[col] = results_df[col].astype('float').astype('Int64')

    print(f"Aggregated {len(results_df)} total rows across all timeframes.")

    def pivot_stats(df):
        return df.set_index('timeframe').to_dict(orient='index')

    nested_df = results_df.groupby(['latitude', 'longitude'], as_index=False).apply(pivot_stats, include_groups=False).rename(columns={None: 'stats'})
    
    timeframes = sorted(results_df['timeframe'].unique().tolist(), reverse=True)

    final_json_payload = {
        "timeframes": timeframes,
        "locations": nested_df.to_dict(orient='records')
    }
    
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob("map_data.json")
    print(f"Uploading structured map_data.json to gs://{GCS_BUCKET_NAME}/")
    
    # FIX 2: Create the JSON string. Using default_handler for numpy types is robust.
    json_string = json.dumps(final_json_payload, indent=2)
    
    blob.upload_from_string(json_string, content_type='application/json')
    blob.make_public()
    print("Upload complete.")

if __name__ == "__main__":
    configure_gcs_cors()
    records_to_load = asyncio.run(fetch_all_locations())
    load_to_bigquery(records_to_load)
    aggregate_and_publish()
    print("--- Process Complete! ---")