from airflow import models
from airflow import DAG
from datetime import datetime, timedelta
from operators import DataSourceToCsv
from operators import CsvToBigquery
from operators import ExternalSensor

transformation_query_sample = """Select 
                        a.user_id,
                        b.country,
                        a.revenue
                        from transactions a 
                        left join users b on
                        a.user_id = b.user_id"""

default_dag_args = {
    'start_date': datetime(2019, 5, 1, 7),
    'email_on_failure': True,
    'email_on_retry': True,
    'project_id' : 'your_project_name',
    'retries': 3,
    'on_failure_callback': notify_email,
    'retry_delay': timedelta(minutes=5),
}
with models.DAG(
    dag_id='your_dag_name',
    schedule_interval = timedelta(days=1),
    catchup = True,
    default_args=default_dag_args) as dag:

# Define Tasks
check_transactions=ExternalSensor.ExternalTaskSensor(
        task_id='check transactions',
        external_dag_id='transactions dag',
        external_task_id= 'final task',
        execution_delta = 'difference in execution times',
        timeout = 5000)

check_users=ExternalSensor.ExternalTaskSensor(
        task_id='check users',
        external_dag_id='users dag',
        external_task_id= 'final task',
        execution_delta = 'difference in execution times',
        timeout = 5000)

transform = TransformInBigquery.TransformInBigquery(
    task_id='Transform in Bigquery',
    transformation_query = transformation_query_sample)

# set dependencies and sequence
transform.set_upstream([check_transactions,check_users])