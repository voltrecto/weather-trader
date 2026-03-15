import requests
import pandas as pd
from pathlib import Path
from datetime import date, timedelta

# Config
LAT = 40.7128
LON = -74.0060
START_DATE = "2021-01-01"
END_DATE = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
filename = f"nyc_openmeteo_historical_{START_DATE}_{END_DATE}.csv"
OUTPUT_PATH = Path(f"data/raw/{filename}")

def fetch_historical():
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": START_DATE,
        "end_date":  END_DATE,
        "daily": ["temperature_2m_max", "temperature_2m_min"],
        "timezone": "America/New_York",
        "temperature_unit": "fahrenheit"
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    df = pd.DataFrame({
        "date": data["daily"]["time"],
        "temp_max": data["daily"]["temperature_2m_max"],
        "temp_min": data["daily"]["temperature_2m_min"]
    })
    
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved {len(df)} rows to {OUTPUT_PATH}")
    return df

if __name__ == "__main__":
    fetch_historical()