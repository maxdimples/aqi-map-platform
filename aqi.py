import requests
import os
from datetime import datetime, timedelta, timezone

def get_current_aqi(latitude, longitude, api_key):
    """Fetches the current AQI for a single location."""
    api_url = f"https://airquality.googleapis.com/v1/currentConditions:lookup?key={api_key}"
    payload = {"location": {"latitude": latitude, "longitude": longitude}}

    try:
        response = requests.post(api_url, json=payload)
        response.raise_for_status()
        data = response.json()
        
        indexes = data.get('indexes', [])
        if not indexes:
            print("Current AQI: Data not available")
            return

        uAQI = next((idx for idx in indexes if idx.get('code') == 'uAQI'), None)
        index_to_use = uAQI if uAQI else indexes[0]

        aqi_value = index_to_use.get('aqi')
        aqi_code = index_to_use.get('code')
        
        print(f"Current AQI ({aqi_code.upper()}): {aqi_value} ({index_to_use.get('category')})")

    except requests.exceptions.RequestException as e:
        print(f"Could not fetch current AQI: {e}")


def get_28_day_average_aqi(latitude, longitude, api_key):
    """
    Attempts to fetch 28 days of historical data in a single API call.
    Note: This method is less reliable than fetching in smaller chunks.
    """
    api_url = f"https://airquality.googleapis.com/v1/history:lookup?key={api_key}"
    
    end_time = datetime.now(timezone.utc) - timedelta(days=1)
    start_time = end_time - timedelta(days=28)

    end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    payload = {"location": {"latitude": latitude, "longitude": longitude},
               "period": {"startTime": start_time_str, "endTime": end_time_str}}

    print(f"--- Analyzing 28-Day Average for {latitude}, {longitude} ---")
    
    try:
        response = requests.post(api_url, json=payload)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"  - Historical data request failed: {e}")
        return

    all_hourly_data = data.get('hoursInfo', [])
    if not all_hourly_data:
        print("  - No historical data could be found for this period.")
        return

    total_aqi = 0
    hour_count = 0
    aqi_codes_used = set()

    for hour_info in all_hourly_data:
        indexes = hour_info.get('indexes', [])
        if not indexes: continue
        uAQI = next((idx for idx in indexes if idx.get('code') == 'uAQI'), None)
        index_to_use = uAQI if uAQI else indexes[0]
        total_aqi += index_to_use.get('aqi', 0)
        hour_count += 1
        aqi_codes_used.add(index_to_use.get('code'))

    if hour_count == 0:
        print("  - Data found, but it contained no valid AQI records.")
        return
        
    average_aqi = total_aqi / hour_count
    print(f"28-Day Average ({', '.join(aqi_codes_used)}): {average_aqi:.2f}")


if __name__ == "__main__":
    API_KEY = os.environ.get("GOOGLE_API_KEY")
    if not API_KEY:
        print("ERROR: GOOGLE_API_KEY environment variable not set.")
    else:
        REMOTE_LAT = 44.278820
        REMOTE_LON = -71.697578
        NYC_LAT = 40.7128
        NYC_LON = -74.0060

        print("Analyzing remote location in New Hampshire...")
        get_28_day_average_aqi(REMOTE_LAT, REMOTE_LON, API_KEY)
        get_current_aqi(REMOTE_LAT, REMOTE_LON, API_KEY)
        
        print("\n" + "="*50 + "\n")
        
        print("Analyzing populated location (New York City)...")
        get_28_day_average_aqi(NYC_LAT, NYC_LON, API_KEY)
        get_current_aqi(NYC_LAT, NYC_LON, API_KEY)
