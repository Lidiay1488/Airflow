from datetime import datetime
from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator

def print_hello():
    return 'Hello, Liudmila!'

dag = DAG('my_first_dag_changed', description='Hello, name',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

hello_operator
