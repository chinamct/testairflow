from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019,2,15),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}

# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
#dag = DAG('bnb_dw_dwd_flow_dag', concurrency=20, schedule_interval="0 */1 * * *", default_args=default_args)
dag = DAG('test1_dag', concurrency=20, schedule_interval=None, default_args=default_args)


def print_date(**kwargs):
    ds = kwargs['ds']
    print(ds)


# Define task
start = DummyOperator(
    task_id='start',
    queue='emr',
    dag=dag
)

print_date = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    queue='emr',
    dag=dag)

# Creates the tasks dynamically.
def create_dynamic_task(task):
    return BashOperator(
        task_id= 'task'+str(task),
        bash_command='date',
        queue='emr',
        dag=dag
    )

end = DummyOperator(
    task_id='end',
    queue='emr',
    dag=dag)

for task in range(3):
    created_task = create_dynamic_task(task)
    if task == 1:
        start >> print_date >> created_task
        created_task >> end
    else:
        start >> created_task
        created_task >> end
