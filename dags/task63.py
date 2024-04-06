'''
Добавьте в граф httpSensor который будет обращаться к сайту gb.ru.
Отправьте в чат скриншот кода и логи работы 
'''

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor

def print_hello():
  return 'Hello world from Airflow DAG!'

def skipp():
  return 99

dag = DAG('task3_sem6', description='Hello world DAG!',
        #   schedule_interval='0 12 * * *',
          schedule='@daily',
          start_date=datetime(2024, 1, 1), catchup=False)
# catchup=False - будут перезапущены все даграны

# BashOperator выводит на экран сообщение “Hello from Airflow”
hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)
skipp_operator = PythonOperator(task_id='skip_task', python_callable=skipp, dag=dag)
# BashOperator берет код из bash файла
hello_file_operator = BashOperator(task_id='hello_file_task', bash_command='./scripts/file1.sh', dag=dag)

task_http_sensor_check = HttpSensor(
    task_id='http_sensor_check',
    http_conn_id='test_connect',
    endpoint='posts',
    # request_params={},
    # response_check=lambda response: 'httpbin' in response.text,
    # poke_interval=5,
    dag=dag
)

hello_operator >> skipp_operator >> hello_file_operator >> task_http_sensor_check
