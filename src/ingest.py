import requests
import pandas as pd
from pathlib import Path
from datetime import date, timedelta

# Config
LAT = 40.7826
LON = -73.9656
START_DATE = "2021-01-01"
END_DATE = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
HISTORICAL_PATH = Path(f"data/raw/nyc_openmeteo_historical_{START_DATE}_{END_DATE}.csv")
FORECAST_PATH = Path("data/raw/nyc_nws_forecast.csv")

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
    
    HISTORICAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(HISTORICAL_PATH, index=False)
    print(f"Saved {len(df)} rows to {HISTORICAL_PATH}")
    return df

def fetch_nws_forecast():

    # Point metadata

    points_url = f"https://api.weather.gov/points/{LAT},{LON}"
    headers = {"User-Agent": "weather-trader/1.0"}
    
    points_response = requests.get(points_url, headers=headers)
    points_response.raise_for_status()
    points_data = points_response.json()
    
    # Forecast

    forecast_url = points_data["properties"]["forecast"]
    
    forecast_response = requests.get(forecast_url, headers=headers)
    forecast_response.raise_for_status()
    forecast_data = forecast_response.json()
    
    periods = forecast_data["properties"]["periods"]

    day_periods = [p for p in periods if p["isDaytime"] == True]
    night_periods = [p for p in periods if p["isDaytime"] == False]

    # Drop first night period if forecast starts at night, to prevent date mismatches
    if periods[0]["isDaytime"] == False:
        night_periods = night_periods[1:]

    rows = []
    for day, night in zip(day_periods, night_periods):
        rows.append({
            "date": day["startTime"].split("T")[0],        
            "temp_max": day["temperature"],    
            "temp_min": night["temperature"],    
        })

    df = pd.DataFrame(rows)
    FORECAST_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(FORECAST_PATH, index=False)
    print(f"Saved {len(df)} rows to {FORECAST_PATH}")
    return df

if __name__ == "__main__":
    fetch_historical()
    fetch_nws_forecast()