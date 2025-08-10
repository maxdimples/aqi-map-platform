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
    print("--- Aggregating, Ranking, and Publishing Map Data ---")
    client = bigquery.Client(project=GCP_PROJECT_ID)
    raw_table_id = f"`{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`"
    ranked_table_id = f"`{GCP_PROJECT_ID}.{BQ_DATASET}.map_data_ranked`"
    
    # STEP 1: Calculate all stats and MERGE into the new ranked table.
    merge_query = f"""
        MERGE {ranked_table_id} T
        USING (
            WITH all_stats AS (
              -- All Time Stats
              SELECT
                'All Time' AS timeframe, latitude, longitude,
                ST_GEOHASH(ST_GEOGPOINT(longitude, latitude), 7) as geohash,
                APPROX_QUANTILES(CAST(aqi AS INT64), 100)[OFFSET(50)] AS p50_aqi,
                APPROX_QUANTILES(CAST(aqi AS INT64), 100)[OFFSET(10)] AS p10_aqi,
                APPROX_QUANTILES(CAST(aqi AS INT64), 100)[OFFSET(1)]  AS p01_aqi,
                AVG(CAST(aqi AS FLOAT64)) AS avg_aqi,
                SAFE_DIVIDE(COUNTIF(aqi >= 80), COUNTIF(aqi IS NOT NULL)) AS share_ge80,
                COUNTIF(aqi IS NOT NULL) AS point_count
              FROM {raw_table_id}
              WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND aqi IS NOT NULL
              GROUP BY latitude, longitude

              UNION ALL

              -- Monthly Stats
              SELECT
                FORMAT_TIMESTAMP('%Y-%m (%B)', DATETIME_TRUNC(datetime, MONTH)) AS timeframe,
                latitude, longitude,
                ST_GEOHASH(ST_GEOGPOINT(longitude, latitude), 7) as geohash,
                APPROX_QUANTILES(CAST(aqi AS INT64), 100)[OFFSET(50)] AS p50_aqi,
                APPROX_QUANTILES(CAST(aqi AS INT64), 100)[OFFSET(10)] AS p10_aqi,
                APPROX_QUANTILES(CAST(aqi AS INT64), 100)[OFFSET(1)]  AS p01_aqi,
                AVG(CAST(aqi AS FLOAT64)) AS avg_aqi,
                SAFE_DIVIDE(COUNTIF(aqi >= 80), COUNTIF(aqi IS NOT NULL)) AS share_ge80,
                COUNTIF(aqi IS NOT NULL) AS point_count
              FROM {raw_table_id}
              WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND aqi IS NOT NULL
              GROUP BY timeframe, latitude, longitude
            )
            -- Final selection and sort key calculation
            SELECT
              *,
              -- CORRECTED SYNTAX: The CAST function's type declaration (AS INT64) is now INSIDE the parentheses.
              CAST(
                 (COALESCE(p50_aqi, 0) * 100000000 +
                 COALESCE(p10_aqi, 0) * 1000000 +
                 COALESCE(p01_aqi, 0) * 10000 +
                 ROUND(COALESCE(avg_aqi, 0)) * 100 +
                 LEAST(COALESCE(point_count, 0), 99))
              AS INT64) AS sort_key
            FROM all_stats
        ) S
        ON T.latitude = S.latitude AND T.longitude = S.longitude AND T.timeframe = S.timeframe
        WHEN MATCHED THEN
          UPDATE SET
            p50_aqi = S.p50_aqi, p10_aqi = S.p10_aqi, p01_aqi = S.p01_aqi, avg_aqi = S.avg_aqi,
            share_ge80 = S.share_ge80, point_count = S.point_count, sort_key = S.sort_key, geohash = S.geohash
        WHEN NOT MATCHED THEN
          -- CORRECTED INSERT: Explicitly use the source alias 'S' for all columns to prevent ambiguity.
          INSERT (timeframe, latitude, longitude, geohash, p50_aqi, p10_aqi, p01_aqi, avg_aqi, share_ge80, point_count, sort_key)
          VALUES (S.timeframe, S.latitude, S.longitude, S.geohash, S.p50_aqi, S.p10_aqi, S.p01_aqi, S.avg_aqi, S.share_ge80, S.point_count, S.sort_key)
    """
    print("Executing MERGE to update ranked data table...")
    merge_job = client.query(merge_query)
    merge_job.result()
    print(f"Ranked data table '{ranked_table_id}' is now up-to-date.")

    # STEP 2: Generate the JSON from the new, optimized table.
    print("Querying the ranked table to generate final JSON...")
    results_df = client.query(f"SELECT * FROM {ranked_table_id} ORDER BY sort_key DESC").to_dataframe()

    # Efficiently structure the DataFrame into the nested JSON format
    all_timeframes = sorted(list(results_df['timeframe'].unique()))
    
    def format_stats(group):
        group['avg_aqi'] = group['avg_aqi'].round(2)
        group['share_ge80'] = group['share_ge80'].round(4)
        group.set_index('timeframe', inplace=True)
        return group.drop(columns=['latitude', 'longitude', 'geohash']).to_dict('index')

    locations_series = results_df.groupby(['latitude', 'longitude']).apply(format_stats)
    locations_list = [{"latitude": lat, "longitude": lon, "stats": stats} for (lat, lon), stats in locations_series.items()]
        
    final_json_payload = {
        "timeframes": all_timeframes,
        "locations": locations_list
    }
    
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob("map_data.json")
    blob.cache_control = "no-cache"
    blob.upload_from_string(json.dumps(final_json_payload), content_type='application/json')
    blob.make_public()
    print(f"Upload complete. Enriched map data is now live at: {blob.public_url}")

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