from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019,2,17),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}

# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
#dag = DAG('bnb_dw_dwd_flow_dag', concurrency=20, schedule_interval="0 */1 * * *", default_args=default_args)
dag = DAG('test3_2_dag', concurrency=20, schedule_interval='*/10 * * * *', default_args=default_args)


wait_for_test3_1 = ExternalTaskSensor(
	task_id='wait_for_test3_1',
	external_dag_id='test3_1_dag',
	external_task_id='print_date',
	execution_delta=None,
	execution_date_fn=None,
	queue='emr',
	dag=dag)

bash_task = BashOperator(
        task_id= 'bash_task',
        bash_command='date',
        queue='emr',
        dag=dag
    )

wait_for_test3_1 >> bash_task
