import requests
import psycopg2
from psycopg2.extras import execute_batch
import logging
import time
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": os.getenv("port"),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

API_URL = "https://geocoding-api.open-meteo.com/v1/search"

def get_cities(cur):
    cur.execute("""
        select city_name, country_code
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

                cur.execute("""
                    create schema if not exists raw;

                    CREATE TABLE if not exists raw.city_coords (
                    "name" varchar(100),
                    lat float8,
                    lon float8,
                    country_code varchar(10),
                    update_at timestamp DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT city_coords_pkey PRIMARY KEY (name, country_code)
                    );
                """)

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

                if batch_data:
                    execute_batch(cur, """
                        insert into raw.city_coords (name, lat, lon, country_code, updated_at)
                        values (%s, %s, %s, %s, %s)
                        on conflict (name, country_code)
                        do update set
                            lat = excluded.lat,
                            lon = excluded.lon,
                            updated_at = excluded.updated_at
                    """, batch_data)

                    logging.info(f"Загружено: {len(batch_data)}")

                else:
                    logging.warning(f"Нет данных для вставки")

    except Exception as e:
        logging.error(f"CRITICAL: {e}")
        raise
    
    logging.info(f"END loaf_cities")

if __name__ == "__main__":
    main()