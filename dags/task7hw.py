'''
— Зарегистрируйтесь в ОрепWeatherApi (https://openweathermap.org/api)
— Создайте ETL, который получает температуру в заданной вами локации, и дальше делает ветвление:
• В случае, если температура больше 15 градусов цельсия — идёт на ветку, в которой есть оператор, 
выводящий на экран «тепло»;
• В случае, если температура ниже 15 градусов, идёт на ветку с оператором, который выводит в консоль «холодно».
Оператор ветвления должен выводить в консоль полученную от АРI температуру.
— Приложите скриншот графа и логов работы оператора ветвленния.
'''

from datetime import datetime
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

def get_weather(ti = None):
    weather_url = 'https://api.openweathermap.org/data/2.5/weather?q=London&units=metric&appid=c767982921a9f3980df9e1144840a165'
    resp_weather = requests.get(weather_url)
    cur_weather_total = resp_weather.json()['main']
    cur_temperature_in_cels = cur_weather_total['temp']
    ti.xcom_push(key='weather in London', value=cur_temperature_in_cels)

def choosing_temperature(ti):
    cur_temp = ti.xcom_pull(key='weather in London', task_ids='get_weather')
    # return f'{cur_temp = }, {type(cur_temp) = }'
    if cur_temp > 15.0:
        return 'hot'
    return 'cold'

with DAG ('weather_sem7_hw',
          description='get weather from openweathermap',
          schedule='@daily',
          start_date=datetime(2024, 4, 1),
          catchup=False
) as dag:
    
    weather_operator = PythonOperator(task_id='get_weather', python_callable=get_weather)
    # temp_op = PythonOperator(task_id='choose', python_callable=choosing_temperature)
    choosing_operator = BranchPythonOperator(task_id='choos_temp', python_callable=choosing_temperature)
    hot_operator = BashOperator(task_id='hot', bash_command='echo тепло')
    cold_operator = BashOperator(task_id='warm', bash_command='echo холодно')

weather_operator >> choosing_operator >> [hot_operator, cold_operator]
# weather_operator >> temp_op
