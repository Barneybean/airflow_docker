from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'william',
    'retries': 5,
    'retry_delay' : timedelta(minutes = 2)
}

@dag(
    dag_id = 'dag_with_python_library',
    default_args = default_args,
    start_date  = datetime(2023,2,25),
    schedule_interval = '0 0 * * *',
    catchup=False
    )
def ml():
    
    @task()
    def scikit():
        import sklearn
        print(f"sklearn version is {sklearn.__version__}")
    
    scikit()

greet_dag = ml()
