import requests
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
  "dbname": os.getenv("DB_NAME"),
  "user": os.getenv("DB_USER"),
  "password": os.getenv("DB_PASSWORD"),
  "host": os.getenv("DB_HOST"),
  "port": os.getenv("DB_PORT")
}

HISTORICAL_API_URL = "https://archive-api.open-meteo.com/v1/archive"

end_date = datetime.now().strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

def main():
  try:
    with psycopg2.connect(**DB_CONFIG) as conn:
      with conn.cursor() as cur:

        cur.execute("select name, lat, lon from raw.city_coords;")
        rows = cur.fetchall()
        
        for name, lat, lon in rows:
          params = {
            "latitude": lat,
            "longitude": lon,
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
            "end_date": end_date,
            "start_date": start_date
          }
          
          response = requests.get(HISTORICAL_API_URL, params=params)
          weather_data = response.json()

          if "daily"  in weather_data:
            daily = weather_data["daily"]

            for i in range(len(daily["time"])):
              cur.execute("""
                insert into raw.actual_weather("city", "date", "max_temp", "min_temp", precipitation_forecast) 
                values (%s, %s, %s, %s, %s)
                on conflict("city", "date") 
                do update set 
                  "max_temp" = excluded."max_temp",
                  "min_temp" = excluded."min_temp",
                  precipitation_forecast = excluded.precipitation_forecast,
                  loaded_at = now()
              """,(name, daily["time"][i], daily["temperature_2m_max"][i], daily["temperature_2m_min"][i], daily["precipitation_sum"][i]))
  except Exception as e:
    print(f"Ошибка {e}")

if __name__ == "__main__":
    main()