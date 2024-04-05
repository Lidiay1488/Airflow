'''
В графе из предыдущего задания поменяйте BashOperator на 
pythonOperator функционал измениться не должен.
'''
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def print_hello():
  return 'Hello world from Airflow DAG!'

def skipp():
  return 99

dag = DAG('task2_sem6', description='Hello world DAG!',
          schedule_interval='0 12 * * *',
        #   schedule='@daily'
          start_date=datetime(2024, 1, 1), catchup=False)
# catchup=False - будут перезапущены все даграны

# BashOperator выводит на экран сообщение “Hello from Airflow”
hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)
skipp_operator = PythonOperator(task_id='skip_task', python_callable=skipp, dag=dag)
# BashOperator берет код из bash файла
hello_file_operator = BashOperator(task_id='hello_file_task', bash_command='./scripts/file1.sh', dag=dag)

hello_operator >> [skipp_operator, hello_file_operator]
