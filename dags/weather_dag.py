from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.cities import main as load_cities
from scripts.actual_weather import main as load_actual_weather
from scripts.forecasted_weather import main as load_forecast_weather

with DAG(
    dag_id="weather_pipeline",
    start_date=datetime(2026, 5, 1),
    schedule="@daily",
    catchup=False
) as dag:

    load_cities_task = PythonOperator(
        task_id="load_cities",
        python_callable=load_cities
    )

    actual_weather_task = PythonOperator(
        task_id="load_actual_weather",
        python_callable=load_actual_weather
    )

    forecast_weather_task = PythonOperator(
        task_id="load_forecast_weather",
        python_callable=load_forecast_weather
    )

    load_cities_task >> actual_weather_task >> forecast_weather_task