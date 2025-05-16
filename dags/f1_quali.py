from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipelines.api import _seasons, _circuits, _constructors, _drivers, _races, _quali

with DAG('f1_quali_dag', start_date=datetime(2025, 1, 1), 
    schedule_interval='@monthly', catchup=False) as dag:

    seasons = PythonOperator(
        task_id = 'seasons',
        python_callable = _seasons
    )

    circuits = PythonOperator(
        task_id = 'circuits',
        python_callable = _circuits
    )

    constructors = PythonOperator(
        task_id = 'constructors',
        python_callable = _constructors
    )

    drivers = PythonOperator(
        task_id = 'drivers',
        python_callable = _drivers
    )

    races = PythonOperator(
        task_id = 'races',
        python_callable = _races
    )

    quali = PythonOperator(
        task_id = 'quali',
        python_callable = _quali
    )

    seasons >> circuits >> constructors >> drivers >> races >> quali
