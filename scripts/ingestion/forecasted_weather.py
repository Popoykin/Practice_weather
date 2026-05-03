import requests
import json
import psycopg2
import time
from psycopg2.extras import execute_batch
from datetime import datetime
from dotenv import load_dotenv
import os
import logging

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

DB_CONFIG = {
  "dbname": os.getenv("dbname"),
  "user": os.getenv("user"),
  "password": os.getenv("password"),
  "host": os.getenv("host"),
  "port": os.getenv("port")
}

API_URL = "https://api.open-meteo.com/v1/forecast"

def get_cities(cur):
    cur.execute("""
      select name, lat, lon
      from raw.city_coords
    """)
    return cur.fetchall()

def fetch_forecast(lat, lon, retries=3):
    params={
       "latitude":lat,
       "longitude":lon,
       "daily":"temperature_2m_max,temperature_2m_min,precipitation_sum"
       }

    for attempt in range(retries):
        try:
            response = requests.get(API_URL, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()

            if "daily" not in data:
                logging.warning(f"Нет daily блока для координат {lat}, {lon}")
                return None
            
            return data["daily"]

        except Exception as e:
            logging.error(f"{lat}, {lon} | попытка {attempt+1} | {e}")
            time.sleep(2 ** attempt)

    return None

def main():
    logging.info(f"START load_forecast")

    date_receipt_of_forecast = datetime.utcnow().date()
    batch_data = []

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                
              cities = get_cities(cur)
              logging.info(f"Городов: {len(cities)}")

              for name, lat, lon in cities:
                  logging.info(f"Обработка: {name}")

                  daily = fetch_forecast(lat, lon)

                  if daily is None:
                      continue
                  
                  dates = daily["time"]
                  max_temp = daily["temperature_2m_max"]
                  min_temp = daily["temperature_2m_min"]
                  precipitation_forecast = daily["precipitation_sum"]

                  for i in range(len(dates)):
                      batch_data.append((
                          name,
                          dates[i],
                          max_temp[i],
                          min_temp[i],
                          precipitation_forecast[i],
                          date_receipt_of_forecast,
                          datetime.utcnow()
                      ))

                  if not batch_data:
                      raise Exception("Нет данных для загрузки forecast")

                  execute_batch(cur, """
                    insert into raw.forecasted_weather (
                        city,
                        "date",
                        max_temp,
                        min_temp,
                        "precipitation forecast",
                        date_receipt_of_forecast,
                        loaded_at
                    )
                    values (%s, %s, %s, %s, %s, %s, %s)
                    on conflict(city, "date", date_receipt_of_forecast)
                    do update set
                        max_temp = excluded.max_temp,
                        min_temp = excluded.min_temp,
                        "precipitation forecast" = excluded."precipitation forecast",
                        loaded_at = excluded.loaded_at;
                  """, batch_data)

                  logging.info(f"Загруженно строк: {len(batch_data)}")

    except Exception as e:
        logging.error(f"CRITICAL: {e}")
        raise

    logging.info("END load_forecast")

if __name__ == "__main__":
    main()
#import requests
#import json
#import psycopg2
#from datetime import datetime
#
#DB_CONFIG = {
#  "dbname": "postgres_practice_gpt",
#  "user": "airflow",
#  "password": "airflow",
#  "host": "localhost",
#  "port": "5432"
#}
#
#API_URL = "https://api.open-meteo.com/v1/forecast"
#
#try:
#  with psycopg2.connect(**DB_CONFIG) as conn:
#    with conn.cursor() as cur:
#
#      cur.execute("""
#        create schema if not exists raw;
#
#        create table if not exists raw.forecasted_weather
#          (
#            id serial primary key,
#            city varchar(100),
#            date date,
#            max_temp float,
#            min_temp float,
#            "Precipitation Amount" float,
#            "Precipitation date" date default current_date,
#            loaded_at timestamp default now(),
#            unique(city, date, "Precipitation date")
#          )
#      """)
#
#      cur.execute("select name, lat, lon from raw.city_coords;")
#      rows = cur.fetchall()
#      
#      for name, lat, lon in rows:
#        params = {
#          "latitude": lat,
#          "longitude": lon,
#          "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
#          "timezone": "auto"
#        }
#        
#        response = requests.get(API_URL, params=params)
#        weather_data = response.json()
#
#        if "daily"  in weather_data:
#          daily = weather_data["daily"]
#
#          for i in range(len(daily["time"])):
#            cur.execute("""
#              insert into raw.forecasted_weather(city, date, max_temp, min_temp, "Precipitation Amount") 
#              values (%s, %s, %s, %s, %s)
#              on conflict(city, date, "Precipitation date") do nothing;
#            """,(name, daily["time"][i], daily["temperature_2m_max"][i], daily["temperature_2m_min"][i], daily["precipitation_sum"][i]))
#except Exception as e:
#  print(f"Ошибка {e}")