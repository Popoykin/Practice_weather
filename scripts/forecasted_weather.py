import requests
import json
import psycopg2
import time
from psycopg2.extras import execute_batch
from datetime import datetime
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

DB_CONFIG = {
  "dbname": os.getenv("DB_NAME"),
  "user": os.getenv("DB_USER"),
  "password": os.getenv("DB_PASSWORD"),
  "host": os.getenv("DB_HOST"),
  "port": os.getenv("DB_PORT")
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
                        precipitation_forecast,
                        date_receipt_of_forecast,
                        loaded_at
                    )
                    values (%s, %s, %s, %s, %s, %s, %s)
                    on conflict(city, "date", date_receipt_of_forecast)
                    do update set
                        max_temp = excluded.max_temp,
                        min_temp = excluded.min_temp,
                        precipitation_forecast = excluded.precipitation_forecast,
                        loaded_at = excluded.loaded_at;
                  """, batch_data)

                  logging.info(f"Загруженно строк: {len(batch_data)}")

    except Exception as e:
        logging.error(f"CRITICAL: {e}")
        raise

    logging.info("END load_forecast")

if __name__ == "__main__":
    main()