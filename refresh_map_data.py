# refresh_map_data.py
# A dedicated script to quickly regenerate the map's JSON data.

from ingest_engine import aggregate_and_publish, GCP_PROJECT_ID, GCS_BUCKET_NAME, BQ_DATASET, BQ_TABLE
import os

# --- Ensure environment is set (if not already) ---
os.environ['GCP_PROJECT_ID'] = GCP_PROJECT_ID
os.environ['GCS_BUCKET_NAME'] = GCS_BUCKET_NAME

if __name__ == "__main__":
    print("--- Triggering Manual Map Data Refresh ---")
    aggregate_and_publish()
    print("--- Refresh Complete ---")