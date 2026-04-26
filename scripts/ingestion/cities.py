import requests
import psycopg2

DB_CONFIG = {
    "dbname": "postgres_practice_gpt",
    "user": "airflow",
    "password": "airflow",
    "host": "localhost",
    "port": "5432"
}

API_URL = "https://geocoding-api.open-meteo.com/v1/search"
cities = ["Samara", "Moscow", "Saint Petersburg", "Yakutsk"]

try:
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""
        create schema if not exists raw;

        create table if not exists raw.city_coords (
            id serial primary key,
            name varchar(100),
            lat FLOAT,
            lon float,
            country_code varchar(10),
            created_at timestamp default current_timestamp,
            unique(name, country_code)
        );
    """)

    for city in cities:
        params = {
            "name": city,
            "country_code": "RU",
            "count": 1      
        }

        response = requests.get(API_URL, params=params)
        data = response.json()

        if "results" in data:
            res = data["results"][0]

            cur.execute(
                "insert into raw.city_coords (name, lat, lon, country_code) values (%s, %s, %s, %s) on conflict(name, country_code) do nothing",
                (res['name'], res['latitude'], res['longitude'], res.get('country_code'))
            )
            print(f"Город {city} (lat: {res['latitude']}, (lon: {res['longitude']})) сохранён в БД.")
        else:
            print(f"Город {city} не найден.")
    
    cur.close()
    conn.close()

except Exception as e:
    print(f"Ошибка: {e}")