from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import time

default_args = {
    "owner":"airflow",
    "start_date": datetime(2023,3,9),
    "depends_on_past": False,
    "retries": 0
}

dag = DAG(
    "print_time",
    default_args = default_args,
    schedule_interval = None
)

def print_current_time():
    print('The current time is: {}'.format(time.strftime('%H:%M:%S')))

t1 = PythonOperator(
    task_id='print_time',
    python_callable=print_current_time,
    dag=dag
)
