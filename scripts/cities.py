import requests
import psycopg2
from psycopg2.extras import execute_batch
import logging
import time
from datetime import datetime
import os


DB_CONFIG = {
  "dbname": os.getenv("DB_NAME"),
  "user": os.getenv("DB_USER"),
  "password": os.getenv("DB_PASSWORD"),
  "host": os.getenv("DB_HOST"),
  "port": os.getenv("DB_PORT")
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

API_URL = "https://geocoding-api.open-meteo.com/v1/search"

def get_cities(cur):
    cur.execute("""
        select city, country_code
        from raw.cities
    """)
    return cur.fetchall()

def fetch_city(city_name, country_code, retries=3):
    params = {
        "name": city_name,
        "country_code": country_code,
        "count": 1      
    }

    for attempt in range(retries):
        try:
            response = requests.get(API_URL, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()

            if "results" not in data or not data["results"]:
                logging.warning(f"Не найден: {city_name}")
                return None
            
            return data["results"][0]

        except Exception as e:
            logging.error(f"{city_name} | попытка {attempt+1} | {e}")
            time.sleep(2 ** attempt)

    return None

def main():
    logging.info("START load_cities")

    batch_data = []

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:

                cities = get_cities(cur)

                logging.info(f"Найдено городов: {len(cities)}")

                for city_name, country_code in cities:
                    logging.info(f"Обработка: {city_name}")

                    result = fetch_city(city_name, country_code)

                    if result is None:
                        continue

                    batch_data.append((
                        result["name"],
                        result["latitude"],
                        result["longitude"],
                        result.get("country_code"),
                        datetime.utcnow()
                    ))

                if not batch_data:
                     raise Exception("Нет данных для загрузки city_coords")



                execute_batch(cur, """
                    insert into raw.city_coords (name, lat, lon, country_code, update_at)
                    values (%s, %s, %s, %s, %s)
                    on conflict (name, country_code)
                    do update set
                        lat = excluded.lat,
                        lon = excluded.lon,
                        update_at = excluded.update_at
                """, batch_data)

                logging.info(f"Загружено: {len(batch_data)}")

    except Exception as e:
        logging.error(f"CRITICAL: {e}")
        raise
    
    logging.info(f"END load_cities")

if __name__ == "__main__":
    main()