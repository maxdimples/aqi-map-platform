# reprocess_from_gcs.py

import asyncio
import argparse
import json
import pandas as pd
from google.cloud import storage, bigquery
from ingest_engine import (
    load_to_bigquery, aggregate_and_publish, BQ_DATASET, BQ_TABLE, GCP_PROJECT_ID
)

def parse_response(data, lat, lon):
    """Parses a raw JSON response dictionary into BigQuery-ready records."""
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

async def process_blob(blob):
    """Downloads a single JSON blob from GCS, parses it, and returns records."""
    try:
        # Extract lat/lon from the filename, e.g., "40.7128_-74.006.json"
        filename = blob.name.split('/')[-1].replace('.json', '')
        lat_str, lon_str = filename.split('_')
        lat, lon = float(lat_str), float(lon_str)

        json_data = json.loads(await asyncio.to_thread(blob.download_as_string))
        return parse_response(json_data, lat, lon)
    except Exception as e:
        print(f"  - ERROR processing blob {blob.name}: {e}")
        return []

async def main(args):
    """
    Lists all JSON files in a given GCS path, processes them in parallel,
    and loads the resulting records into BigQuery.
    """
    print(f"--- Starting Reprocessing from GCS Path: gs://{args.bucket}/{args.path} ---")
    
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    
    blobs = storage_client.list_blobs(args.bucket, prefix=args.path)
    json_blobs = [blob for blob in blobs if blob.name.endswith('.json')]
    
    if not json_blobs:
        print("No .json files found at the specified path. Exiting.")
        return

    print(f"Found {len(json_blobs)} files to reprocess.")
    
    all_records = []
    tasks = [process_blob(blob) for blob in json_blobs]
    results = await asyncio.gather(*tasks)

    for res in results:
        all_records.extend(res)

    print(f"\nParsed {len(all_records)} total records from GCS.")
    
    if all_records:
        load_to_bigquery(all_records, bq_client)
        print("\n--- Triggering Final Aggregation Step ---")
        aggregate_and_publish()
    else:
        print("No valid records found to load into BigQuery.")
        
    print("--- Reprocessing Complete! ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reprocess raw JSON data from GCS into BigQuery.")
    parser.add_argument("--bucket", required=True, help="The GCS bucket name (e.g., aqi-history-platform).")
    parser.add_argument("--path", required=True, help="The full path/prefix to the JSON files (e.g., 'raw_responses/2025-07-25T23-59-59Z/').")
    args = parser.parse_args()
    asyncio.run(main(args))