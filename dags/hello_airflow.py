from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    "name":"airflow",
    "start_date": datetime(2023,3,7),
    "depends_on_past": False,
    "retries": 0
}

dag = DAG(
    "hello_airflow",
    default_args = default_args,
    schedule_interval = None
)

t1 = BashOperator(
    task_id = "print_hello_airflow",
    bash_command = "echo 'Hello World'",
    dag = dag
)

t2 = BashOperator(
    task_id = "perintah_ls",
    bash_command = "ls /",
    dag = dag
)