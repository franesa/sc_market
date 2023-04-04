from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
import random

default_args = {
    'owner': 'airflow',
     'depends_on_past': False,
     'start_date': datetime(2023, 3, 7),
     'retries': 1,
     'retry_delay': timedelta(minutes=5)
}

dag = DAG(
     'branch_ran_num',
     default_args=default_args,
     description='A simple DAG that uses the BranchPythonOperator',
     schedule_interval=None
)

def random_number():
    numbers = random.randint(1,10)
    if numbers > 8:
        return "Good"
    elif numbers >= 6 and numbers <= 7:
        return "Oke"
    else:
        return "Ok"
    
t1 = BranchPythonOperator(
    task_id='random_number',
    python_callable=random_number,
    dag=dag)

good = BashOperator(
    task_id='Good',
    bash_command='echo "Random Number > 8"',
    dag=dag)

oke = BashOperator(
    task_id='Oke',
    bash_command='echo "Random Number >= 6"',
    dag=dag)

ok = BashOperator(
    task_id='Ok',
    bash_command='echo "Random Number <= 6"',
    dag=dag)

t1 >> [good,oke,ok]