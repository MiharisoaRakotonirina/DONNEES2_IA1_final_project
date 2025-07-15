import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from scripts.extract import get_weather_forecast
from scripts.merge import merge_all_csv
from scripts.transform import transform_to_star_schema
from scripts.clean_weather_fact import clean_weather_fact
from scripts.merge_historical_with_actual_data import merge_historical_with_actual_data
from airflow import DAG
from airflow.models import Variable

API_KEY = Variable.get("OPENWEATHER_API_KEY")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 11),
}

CITIES = ["Paris","Toulouse","Marseille","Nice","Lyon"]

with DAG(
    dag_id='weather_etl',
    default_args=default_args,
    description='ETL pipeline for weather data',
    schedule='0 12 * * *',  # Execute every day at 12 PM
    catchup=False,
    max_active_runs=1
) as dag :
    extract_task = [
        PythonOperator(
            task_id=f'extract_{city}',
            python_callable=get_weather_forecast,
            op_kwargs={'city_name': city, 'api_key': API_KEY},
            dag=dag,
        )
        for city in CITIES
    ]
    merge_task = PythonOperator(
        task_id='merge_csv_files',
        python_callable=merge_all_csv,
    )
    transform_task = PythonOperator(
        task_id='transform_to_star_schema',
        python_callable=transform_to_star_schema,
    )
    clean_fact_task = PythonOperator(
        task_id='clean_weather_fact',
        python_callable=clean_weather_fact,
    )
    merge_historical_actual_data = PythonOperator(
        task_id = 'merge_historical_with_actual_data',
        python_callable=merge_historical_with_actual_data
    )
    extract_task >> merge_task >> transform_task >> clean_fact_task >> merge_historical_actual_data