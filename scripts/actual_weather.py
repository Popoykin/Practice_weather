import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta, timezone
import time
import logging
import os

DB_CONFIG = {
  "dbname": os.getenv("DB_NAME"),
  "user": os.getenv("DB_USER"),
  "password": os.getenv("DB_PASSWORD"),
  "host": os.getenv("DB_HOST"),
  "port": os.getenv("DB_PORT")
}

HISTORICAL_API_URL = "https://archive-api.open-meteo.com/v1/archive"


def get_cities(cur):
    cur.execute("""
        select name, lat, lon
        from raw.city_coords;
    """)
    return cur.fetchall()

def fetch_actual_weather(lat, lon, end_date, start_date, retries=3):
    params={
        "latitude":lat,
        "longitude":lon,
        "daily":"temperature_2m_max,temperature_2m_min,precipitation_sum",
        "end_date": end_date,
        "start_date": start_date
        }

    for attempt in range(retries):
        try:
            response = requests.get(HISTORICAL_API_URL, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()

            if "daily" not in data:
                logging.warning(f"Нет данных блока для координат {lat}, {lon}")
                return None

            return data["daily"]

        except Exception as e:
            logging.error(f"{lat}, {lon} | попытка {attempt+1} | {e}")
            time.sleep(2 ** attempt)

    return None


def main():
    logging.info(f"START load_actual_weather")

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:

                cities = get_cities(cur)

                if not cities:
                    logging.warning(f"Нет горов для обработки")
                    return

                logging.info(f"Городов: {len(cities)}")

                batch_data = []

                for name, lat, lon in cities:
                    logging.info(f"Обработка: {name}")

                    daily = fetch_actual_weather(lat, lon, end_date, start_date)

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
                            precipitation_forecast[i]
                        ))

                    if not batch_data:
                        raise Exception("Нет данных для загрузки actual_weather")
                    
                    execute_batch(cur, """
                        insert into raw.actual_weather(city, date, max_temp, min_temp, precipitation_forecast) 
                        values (%s, %s, %s, %s, %s)
                        on conflict("city", "date") 
                        do update set 
                          "max_temp" = excluded."max_temp",
                          "min_temp" = excluded."min_temp",
                          precipitation_forecast = excluded.precipitation_forecast,
                          loaded_at = now()
                    """, batch_data)

                    logging.info(f"Загруженно строк: {len(batch_data)}")

    except Exception as e:
        logging.error(f"CRITICAL: {e}")
        raise
    
    logging.info("END load_forecast")


if __name__ == "__main__":
    main()