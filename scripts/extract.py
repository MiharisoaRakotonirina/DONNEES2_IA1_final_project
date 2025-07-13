import requests
import csv
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
BASE_URL = "https://api.openweathermap.org/data/2.5/forecast"
RAW_DATA_DIR = "./data/raw"
os.makedirs(RAW_DATA_DIR, exist_ok=True)

def get_weather_forecast(city_name, api_key):
    url = f"{BASE_URL}?q={city_name}&appid={api_key}&cnt=9&units=metric"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        records = []

        city_name_full = data['city']['name'] # City Name (ex : Paris)
        city_name_full = city_name_full.replace("Arrondissement de ", "").strip().title()
        city_country = data['city']['country'] # City Country Code(ex: FR)

        for item in data['list']:
            date = item['dt_txt']
            main = item['main']
            weather_info = item['weather'][0]
            wind = item['wind']

            record = {
                "datetime": date,
                "temp": main.get("temp"),
                "feels_like": main.get("feels_like"),
                "temp_min": main.get("temp_min"),
                "temp_max": main.get("temp_max"),
                "humidity": main.get("humidity"),
                "wind_speed": wind.get("speed"),
                "rain_prob": item.get("pop", 0) * 100,
                "description": weather_info.get("description"),
                "city_name": city_name_full,
                "city_country": city_country,
            }

            records.append(record)

        file_path = os.path.join(RAW_DATA_DIR, f"{city_name}.csv")
        with open(file_path, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(records[0].keys()))
            writer.writeheader()
            writer.writerows(records)
        print(f"Data saved successfully for {city_name}")
    else:
        print(f"❌ Error for {city_name} : {response.status_code}")

#CITIES = ["Paris","Toulouse","Marseille","Nice","Lyon"]

#for city in CITIES:
#    get_weather_forecast(city)