from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta

@dag(
    default_args= {
        "depends_on_past": False,
        "retries": 2,
    },
    description = 'Testing it out',
    schedule= '@daily',
    start_date = datetime(2024, 7, 29)
)
    
def tutorial_workflow_my_own():

    @task
    def print_shit():
        print('Shit')
        return 'Shit'
    
    @task
    def double_it(received_string: str):
        new_str = '2' + ' ' + received_string
        print(new_str)

        return new_str
    
    @task
    def the_end(received_string: str):
        print(f"The End: {received_string}")    

    string_oi = print_shit()
    double_string = double_it(string_oi)
    the_end(double_string)

tutorial_workflow_my_own()
