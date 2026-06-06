import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import requests
import pandas as pd
from config.settings import CITIES

from datetime import date, timedelta

# Config
START_DATE = "2021-01-01"
END_DATE = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

def fetch_historical(city_key):
    url = "https://archive-api.open-meteo.com/v1/archive"
    city = CITIES[city_key]
    path = Path(f"data/raw/{city_key.lower()}_openmeteo_historical.csv")    

    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "start_date": START_DATE,
        "end_date":  END_DATE,
        "daily": ["temperature_2m_max", "temperature_2m_min"],
        "timezone": city["timezone"],
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
    
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Saved {len(df)} rows to {path}")
    return df

def fetch_nws_forecast(city_key):

    city = CITIES[city_key]
    path = Path(f"data/raw/{city_key.lower()}_nws_forecast.csv")

    points_url = f"https://api.weather.gov/points/{city['lat']},{city['lon']}"
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
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Saved {len(df)} rows to {path}")
    return df

if __name__ == "__main__":
    for key in CITIES:
        fetch_historical(key)
        fetch_nws_forecast(key)