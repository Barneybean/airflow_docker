from datetime import datetime, timedelta

from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'william',
    'retries': 2,
    'retry_delay' : timedelta(seconds = 5) 
}

# never use ti.xcom to share more than 48k of data size
def greet(ti): # has to use task instance ti to get returned variables from another functions
    # ********ti.xcoms can be found in UI -> Admin -> admin 
    first_name = ti.xcom_pull(task_ids='get_name', key ='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key ='last_name')
    age = ti.xcom_pull(task_ids = 'get_age', key = 'age')
    print(f'hello world!, I am {first_name} {last_name}, and I am {age} years old!')

def get_name(ti):
    ti.xcom_push(key='first_name', value = 'Jerry')
    ti.xcom_push(key='last_name', value = 'Fridman')

def get_age(age, ti):
    ti.xcom_push(key='age', value=age)

with DAG(
    dag_id = 'dag_with_python_operator',
    default_args=default_args,
    description = 'dag using python functions',
    start_date = datetime(2023,2,25,2),
    schedule_interval = '@daily',
    catchup=False
):
    task1 = PythonOperator(
        task_id = 'greet',
        # bash_command = 'echo hello world, this is the First task!' uses BashOperator library
        python_callable=greet,
       
    )

    task2 = PythonOperator(
        task_id = 'get_name',
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id = 'get_age',
        python_callable=get_age,
        op_kwargs={'age':20}  # when use ti.xcoms, do not need to pass in to *kwargs
    )

    [task2, task3] >> task1