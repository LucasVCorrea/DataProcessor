from functools import partial
from airflow.providers.standard.operators.python import PythonOperator

GdriveLimitedPythonOperator = partial(
    PythonOperator,
    pool='google_drive_pool',
)