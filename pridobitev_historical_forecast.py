import os
import time
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
import urllib3

# --- SSL preverjanje ------------------------------------------------------
VERIFY_SSL = False

if not VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Nastavitev seje s predpomnilnikom (cache)
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)

if not VERIFY_SSL:
    cache_session.verify = False

retry_session = retry(cache_session, retries=8, backoff_factor=1)
openmeteo = openmeteo_requests.Client(session=retry_session)

# --- Zajem koordinat iz CSV datoteke -------------------------------------
coords_df = pd.read_csv("koordinate_kvadratov.csv", sep=";", decimal=".", encoding="utf-8")

# --- Open-Meteo API nastavitev -------------------------------------------
url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
models_list = ["dwd_icon_eu", "dwd_icon_d2", "best_match"]
num_models = len(models_list)

# Ustvarimo izhodno mapo
output_dir = "output_hystorical_api"
os.makedirs(output_dir, exist_ok=True)

# Velikost paketa (50 lokacij naenkrat prepreči URL napako 414)
BATCH_SIZE = 100

# --- Pošiljanje zahtev v paketih ----------------------------------------
for start_idx in range(0, len(coords_df), BATCH_SIZE):
    chunk_df = coords_df.iloc[start_idx : start_idx + BATCH_SIZE]
    
    latitudes = chunk_df["latitude"].tolist()
    longitudes = chunk_df["longitude"].tolist()

    # ODSTRANJENA PARAMETRA "run" IN "forecast_days"
    params = {
        "latitude": ",".join(map(str, latitudes)),
        "longitude": ",".join(map(str, longitudes)),
        "start_date": "2025-01-01",
        "end_date": "2026-07-22",
        "hourly": ["temperature_2m", "rain", "shortwave_radiation"],
        "models": models_list,
    }

    print(f"Pridobivam paket: lokacije {start_idx + 1} do {start_idx + len(chunk_df)} od {len(coords_df)}...")
    

    responses = None
    
    for attempt in range(5):
        try:
            responses = openmeteo.weather_api(url, params=params)
            break
        
        except Exception as e:
            print(f"Poskus {attempt + 1}/5 za paket {start_idx}-{start_idx + len(chunk_df)}:")
            print(e)
    
            if attempt < 4:
                time.sleep(15)
    
    if responses is None:
        print(f"Paket {start_idx}-{start_idx + len(chunk_df)} je bil preskočen.")
        continue

    # --- Obdelava odgovorov ----------------------------------------------
    for i, response in enumerate(responses):
        loc_idx = i // num_models
        model_idx = i % num_models
        
        loc_row = chunk_df.iloc[loc_idx]
        uniq_id = loc_row["WPointID"]
        req_lat = loc_row["latitude"]
        req_lon = loc_row["longitude"]
        model_name = models_list[model_idx]

        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_rain = hourly.Variables(1).ValuesAsNumpy()
        hourly_shortwave_radiation = hourly.Variables(2).ValuesAsNumpy()

        dates = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )

        hourly_dataframe = pd.DataFrame({
            "UniqID": uniq_id,
            "Latitude": req_lat,
            "Longitude": req_lon,
            "date": dates,
            "temperature_2m": hourly_temperature_2m,
            "rain": hourly_rain,
            "shortwave_radiation": hourly_shortwave_radiation
        })

        file_name = f"{output_dir}/hourly_data_ID{uniq_id}_{req_lat}_{req_lon}_{model_name}.csv"
        hourly_dataframe.to_csv(file_name, index=False)
    time.sleep(5)  # Počakamo 1 sekundo, da ne preobremenimo strežnika
print("Vsi paketi so bili uspešno obdelani!")