from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import json

def kelvin_to_celsius(temp_in_kelvin):
    temp_in_celsius = temp_in_kelvin - 273.15
    return temp_in_celsius

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]["description"]
    temp_celcius = kelvin_to_celsius(data["main"]["temp"])
    feels_like = kelvin_to_celsius(data["main"]["feels_like"])
    min_temp_celcius = kelvin_to_celsius(data["main"]["temp_min"])
    max_temp_celcius = kelvin_to_celsius(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data["dt"] + data["timezone"])
    sunrise_time = datetime.utcfromtimestamp(data["sys"]['sunrise'] + data["timezone"])
    sunset_time = datetime.utcfromtimestamp(data["sys"]['sunset'] + data["timezone"])

    transformed_data = {"City":city,
                        "Description":weather_description,
                        "Temperature (C)": temp_celcius,
                        "Feels Like (C)": feels_like,
                        "Minimum Temperature (C)": min_temp_celcius,
                        "Maximum Temperature (C)": max_temp_celcius,
                        "Pressure": pressure,
                        "Humidity": humidity,
                        "Wind Speed": wind_speed,
                        "Time of record": time_of_record,
                        "Sunrise (Local Time)": sunrise_time,
                        "Sunset (Local Time)": sunset_time
    }

    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    aws_credentials = {"key": "enter_key", "secret": "enter_secret", "token": "enter_token"}

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_gurgaon_' + dt_string
    df_data.to_csv(f"s3://enter_s3_storage_bucket_name/{dt_string}.csv", index=False, storage_options=aws_credentials)



default_args = {
    'owner':'abheek',
    'depends_on_past':False,
    'start_date': datetime(2024,3,19),
    'email':['abheek.das1605@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 2)
}

with DAG('weather_dag',
        default_args = default_args,
        schedule_interval = '@daily',
        catchup = False) as dag:

        is_weather_api_ready = HttpSensor(
            task_id = 'is_weather_api_ready',
            http_conn_id = 'weathermap_api',
            endpoint = '/data/2.5/weather?q=Gurgaon&appid=de29c2b32a8ab5b31220e08a53ee4212'
        )

        extract_weather_data = SimpleHttpOperator(
            task_id = 'extract_weather_data',
            http_conn_id = 'weathermap_api',
            endpoint = '/data/2.5/weather?q=Gurgaon&appid=de29c2b32a8ab5b31220e08a53ee4212',
            method = 'GET',
            response_filter = lambda r:json.loads(r.text),
            log_response = True
        )

        transform_load_weather_data = PythonOperator(
            task_id = 'transform_load_weather_data',
            python_callable = transform_load_data
        )

        is_weather_api_ready >> extract_weather_data >> transform_load_weather_data