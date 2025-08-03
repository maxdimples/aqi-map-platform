import asyncio
import aiohttp
import os
import pandas as pd
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

def get_last_month_period():
    """Calculates start and end for the previous full month in RFC3339 format."""
    today = datetime.now(timezone.utc)
    first_day_of_current_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_day_of_previous_month = first_day_of_current_month - timedelta(seconds=1)
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    start_time = first_day_of_previous_month.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time = last_day_of_previous_month.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    return {"startTime": start_time, "endTime": end_time}

async def fetch_one_location(session, lat, lon, period):
    """Asynchronously fetches historical AQI for one location."""
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
                    "aqi": uAQI.get('aqi'), "aqi_code": uAQI.get('code'), "category": uAQI.get('category'),
                })
            return records
    except Exception as e:
        print(f"  - Error fetching {lat},{lon}: {e}")
        return []

async def fetch_all_locations():
    """Orchestrates parallel fetching for all locations."""
    print("--- Starting Parallel Fetch ---")
    locations_df = pd.read_csv(LOCATIONS_FILE)
    period = get_last_month_period()
    print(f"Fetching data for period: {period['startTime']} to {period['endTime']}")

    all_records = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for _, row in locations_df.iterrows():
            async def limited_fetch(lat, lon):
                async with semaphore:
                    return await fetch_one_location(session, lat, lon, period)
            tasks.append(limited_fetch(row['latitude'], row['longitude']))
        results = await asyncio.gather(*tasks)
        for res in results: all_records.extend(res)
            
    print(f"Fetched a total of {len(all_records)} hourly records.")
    return all_records

def load_to_bigquery(records):
    """Loads a list of dictionary records into BigQuery."""
    if not records:
        print("No records to load. Skipping BigQuery load.")
        return
    print("--- Loading Data into BigQuery ---")
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    load_job = client.load_table_from_json(records, table_id, job_config=job_config)
    load_job.result()
    print(f"Successfully loaded {load_job.output_rows} rows to {table_id}.")

def aggregate_and_publish():
    """Queries BigQuery to aggregate data and uploads the result to GCS."""
    print("--- Aggregating Data for Map ---")
    client = bigquery.Client(project=GCP_PROJECT_ID)
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    
    query = f"""
        SELECT 
            latitude, 
            longitude,
            -- Use APPROX_QUANTILES to get a fast median, more robust than average
            APPROX_QUANTILES(aqi, 2)[OFFSET(1)] AS median_aqi,
            MAX(aqi) as max_aqi,
            MIN(aqi) as min_aqi,
            -- Create a compact timeseries array for mini-graphs on the frontend
            TO_JSON_STRING(ARRAY_AGG(STRUCT(datetime, aqi) ORDER BY datetime)) AS hourly_history
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
        GROUP BY latitude, longitude
    """
    print("Executing aggregation query...")
    query_job = client.query(query)
    results_df = query_job.to_dataframe()
    
    print(f"Aggregated data for {len(results_df)} locations.")
    
    # Convert DataFrame to a list of dicts, which is a standard JSON format
    output_json = results_df.to_dict(orient='records')
    
    # Upload to GCS
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob("map_data.json")
    
    print(f"Uploading map_data.json to gs://{GCS_BUCKET_NAME}/")
    blob.upload_from_string(
        json.dumps(output_json, indent=2),
        content_type='application/json'
    )
    # Make it public just in case default permissions are restrictive
    blob.make_public()
    print("Upload complete.")

if __name__ == "__main__":
    # 1. Fetch
    records_to_load = asyncio.run(fetch_all_locations())
    # 2. Load
    load_to_bigquery(records_to_load)
    # 3. Aggregate & Publish
    aggregate_and_publish()
    print("--- Process Complete! ---")