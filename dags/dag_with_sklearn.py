from datetime import datetime, timedelta

from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'william',
    'retries': 2,
    'retry_delay' : timedelta(seconds = 5) 
}


def get_matplotlib():
    import matplotlib
    print(f"matplotlib version is imported {matplotlib.__version__}")

def get_sklearn():
    import sklearn
    print(f"sklearn version is imported {sklearn.__version__}")


with DAG(
    dag_id = 'dag_with_sklearn',
    default_args=default_args,
    description = 'dag using python functions',
    start_date = datetime(2023,2,25,2),
    schedule_interval = '@daily',
    catchup=False
):
    task1 = PythonOperator(
        task_id = 'sklearn',
        # bash_command = 'echo hello world, this is the First task!' uses BashOperator library
        python_callable=get_sklearn,
    )
   
    task2 = PythonOperator(
        task_id = 'matplotlib',
        python_callable=get_matplotlib,
    )

    task2 >> task1





# def get_matplotlib():
#     import matplotlib
#     print("matplotlib version is imported")


    # task1 = PythonOperator(
    #     task_id = 'matplotlib import',
    #     # bash_command = 'echo hello world, this is the First task!' uses BashOperator library
    #     python_callable=get_matplotlib,
       
    # )
    # task1