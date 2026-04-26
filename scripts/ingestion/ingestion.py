import requests
import json
import psycopg2
from datetime import datetime

DB_CONFIG = {
  "dbname": "postgres_practice_gpt",
  "user": "airflow",
  "password": "airflow",
  "host": "localhost",
  "port": "5432"
}

API_URL = "https://api.open-meteo.com/v1/forecast"

try:
  with psycopg2.connect(**DB_CONFIG) as conn:
    with conn.cursor() as cur:

      cur.execute("""
        create schema if not exists raw;

        create table if not exists raw.forecasted_weather
          (
            id serial primary key,
            "Город" varchar(100),
            "Дата" date,
            "Max °C" float,
            "Min °C" float,
            "Количество осадков" float,
            "Дата получения прогноза" date default current_date,
            loaded_at timestamp default now(),
            unique("Город", "Дата", "Дата получения прогноза")
          )
      """)

      cur.execute("select name, lat, lon from raw.city_coords;")
      rows = cur.fetchall()
      
      for name, lat, lon in rows:
        params = {
          "latitude": lat,
          "longitude": lon,
          "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
          "timezone": "auto"
        }
        
        response = requests.get(API_URL, params=params)
        weather_data = response.json()

        if "daily"  in weather_data:
          daily = weather_data["daily"]

          for i in range(len(daily["time"])):
            cur.execute("""
              insert into raw.forecasted_weather("Город", "Дата", "Max °C", "Min °C", "Количество осадков") 
              values (%s, %s, %s, %s, %s)
              on conflict("Город", "Дата", "Дата получения прогноза") do nothing;
            """,(name, daily["time"][i], daily["temperature_2m_max"][i], daily["temperature_2m_min"][i], daily["precipitation_sum"][i]))
except Exception as e:
  print(f"Ошибка {e}")