from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'william',
    'retries': 5,
    'retry_delay' : timedelta(minutes = 2)
}

@dag(
    dag_id = 'a_easier_way_dag_with_task_flow_api',
    default_args = default_args,
    start_date  = datetime(2023,2,25),
    schedule_interval = '@daily',
    catchup=False
    )
def hello_world_etl():
# option 1
    # @task()
    # def get_name():
    #     return 'jerry'

    # @task()
    # def get_age():
    #     return 20


    # @task()
    # def greet(name, age):
    #     print(print(f'hello world!, I am {name}, and I am {age} years old!'))

#option 2
    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'Jerry',
            'last_name': 'Fridman'
        }

    @task()
    def get_age():
        return 20
    
    @task()
    def greet(first_name, last_name, age):
        print(print(f'hello world!, I am {first_name} {last_name}, and I am {age} years old!'))

    name_dict = get_name()
    age = get_age()
    greet(first_name = name_dict['first_name'],
          last_name = name_dict['last_name'],
          age=age)

greet_dag = hello_world_etl()
