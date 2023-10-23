from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

# Define params for Submit Run Operator
notebook_task = {
    'notebook_path': '/Users/laith.jardaneh@gmail.com/Data_Transformation',
}

notebook_params = {
    "Variable":5
}

# Specific start date (adjust as needed)
start_date = datetime(2023, 10, 23, 0, 0)  # 12:00 AM on October 23, 2023

# Default Arguments
default_args = {
    'owner': 'laith.jardaneh@gmail.com',  # You can change this if needed
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  # Set this to the number of desired retries. I've set it to 1 as a placeholder.
    'retry_delay': timedelta(minutes=2)
}

# DAG Definition with the provided name
with DAG('12256357c821_dag',
         start_date=start_date,
         schedule_interval='@daily',
         catchup=False,
         default_args=default_args
         ) as dag:

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',  # Provided cluster ID
        notebook_task=notebook_task
    )

    opr_submit_run
