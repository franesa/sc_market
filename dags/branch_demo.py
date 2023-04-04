from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
     'depends_on_past': False,
     'start_date': datetime(2023, 3, 7),
     'retries': 1,
     'retry_delay': timedelta(minutes=5)
     }

dag = DAG(
     'branch_demo',
     default_args=default_args,
     description='A simple DAG that uses the BranchPythonOperator',
     schedule_interval=timedelta(days=1)
     )
     
def check_day_of_week():
     today = datetime.today().weekday()
     if today < 5:
         return 'weekday_task'
     else:
         return 'weekend_task'
     
t1 = BranchPythonOperator(
    task_id='check_day_of_week',
    python_callable=check_day_of_week,
    dag=dag)

weekday_task = BashOperator(
    task_id='weekday_task',
    bash_command='echo "Today is a weekday"',
    dag=dag)

weekend_task = BashOperator(
    task_id='weekend_task',
    bash_command='echo "Today is the weekend"',
    dag=dag)

t1 >> [weekday_task, weekend_task]