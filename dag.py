from datetime import datetime
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(__file__))
from pipeline import run

with DAG(
    dag_id="servicenow_to_redshift",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["servicenow", "redshift"],
) as dag:

    PythonOperator(
        task_id="sync_incident_table",
        python_callable=run,
    )
