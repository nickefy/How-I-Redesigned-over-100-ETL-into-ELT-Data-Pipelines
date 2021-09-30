from airflow import models
from airflow import DAG
from datetime import datetime, timedelta
from operators import DataSourceToCsv
from operators import CsvToBigquery

extract_query_source = """Select 
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
Extract_And_Transform = DataSourceToCsv.DataSourceToCsv(
    task_id='Extract from Source',
    table_name = 'source tablename',
    extract_query = extract_query_source,
    connection = 'your defined postgres db connection')

Load = CsvToBigquery.CsvToBigquery(
    task_id='Load into Destination',
    bigquery_table_name = 'destination tablename',
    dataset_name = 'destination dataset name',
    write_mode = 'WRITE_TRUNCATE, WRITE_APPEND OR WRITE_EMPTY')

# set dependencies and sequence
Extract_And_Transform >> Load