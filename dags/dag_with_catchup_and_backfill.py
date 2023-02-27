from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'william',
    'retries': 5,
    'retry_delay' : timedelta(minutes = 2)
}

with DAG(
    dag_id = 'our_first_dag',
    default_args=default_args,
    description = 'first dag',
    start_date = datetime(2023,2,10,2),
    schedule_interval = '@daily',
    catchup = False 
    # catch up will run the code for the past days since the start_date
    # catch up = false will only execute the code one time when deployed 

) as dag: 
    task1 = BashOperator(
        task_id = 'first_task_v3',
        bash_command = 'echo hello world, this is the First task!'
    )

    task2 = BashOperator(
        task_id = 'second_task_v2',
        bash_command = 'echo This is the Second task! I will be executed after task 1'
    )

    task3 = BashOperator(
        task_id = 'third_task',
        bash_command = 'echo This is the Third task! I will be executed after task 1'
    )

    # task1.set_downstream(task2)
    # task1.set_downstream(task3)
    # or 
    task1 >> [task2, task3] # task2 and task3 will start after completion of task 1 

    task1