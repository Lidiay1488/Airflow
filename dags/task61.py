'''
Создайте новый граф, добавьте в него два BashOperator, первый должен 
выводить на экран сообщение “Hello from Airflow”, второй должен брать код 
из bash файла. Создайте bash файл который будет выводить сообщение 
“Hello from Airflow bash script processer.” Создайте последовательную связь 
между первым и вторым оператором. 
Добавьте между первым и вторым операторами еще один BashOperator 
который будет заканчивать свою работу в статусе skipped
'''

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG('task1_sem6', description='Hello World DAG!',
          schedule_interval='0 12 * * *',
        #   schedule='@daily'
          start_date=datetime(2024, 1, 1), catchup=False)
# catchup=False - будут перезапущены все даграны

# BashOperator выводит на экран сообщение “Hello from Airflow”
hello_operator = BashOperator(task_id='hello_task', bash_command='echo Hello from Airflow!', dag=dag)
skipp_operator = BashOperator(task_id='skip_task', bash_command='exit 99', dag=dag)
# BashOperator берет код из bash файла
hello_file_operator = BashOperator(task_id='hello_file_task', bash_command='https://github.com/Lidiay1488/Airflow/blob/main/scripts/file1.sh', dag=dag)

hello_operator >> [skipp_operator, hello_file_operator]
