import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

from base_worker import ds_error_notifier, notify_to_ds
from google_worker import GdriveLimitedPythonOperator

asterius_dataloader_dag = DAG(
    dag_id="asterius_dataloader_dag",
    start_date=datetime.datetime.now(),
    catchup=False,
    schedule="@hourly",
)

TMP_PATH = "/tmp/asterius/dl"

TICKETS_DATA = f"{TMP_PATH}/tickets_data.parquet"


def fetch_tickets_data(**kwargs):
    import gspread
    import pandas as pd
    from oauth2client.service_account import ServiceAccountCredentials

    # Define scope and authenticate
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('/credentials/credentials.json', scope)
    client = gspread.authorize(creds)

    # Open the sheet by name or ID
    sh = client.open("Actas_formulario")
    worksheet = sh.sheet1

    data = worksheet.get_all_records()
    df = pd.DataFrame(data)

    df.columns = [
        "stamp",
        "ticket_date",
        "ticket_number",
        "license_plate",
        "ticket_type",
        "ticket_type_description",

        "ticket_adress_street",
        "ticket_adress_street_number",
        "ticket_adress_street_direction",

        "ticket_adress_route",
        "ticket_adress_route_km",
        "ticket_adress_route_direction",

        "interno",
        "car_brand",
        "car_model",
        "car_year",
        "car_owner_cuit",
        "car_owner_license",
        "car_owner_license_category",
        "municipio otorgante",
        "provincia otorgante",

        "calle",
        "numero",
        "ciudad",
        "municipio",
        "provincia",
        "codigo postal",
        "zona",
        "dpto",

        "se dio a la fuga",
        "se nego a identificar",
        "no identificacion",

        "law",
        "law_article",
        "law_article_description",

        "legajo",
        "autoridad constatacion",
        "cargo",
        "legajo citacion",

        "retención vehiculo",
        "retención licencia",
        "max_speed",
        "speed_trap_reading",

        "breathalyser_brand",
        "breathalyser_model",
        "breathalyser_series",
        "breathalyser_reading",
        "breathalyser_reading_date",
        "breathalyser_redundant",

        "speed_trap_brand",
        "speed_trap_model",
        "speed_trap_series",
        "speed_trap_inti_valid_date",

        "juzgado",
        "importe",
        "ufs",
        "valor ufs",
        "gastos administrativos",
        "vencimiento voluntarios",
        "vencimiento final"
    ]

    df.to_parquet(TICKETS_DATA)


CUITS_DATA = f"{TMP_PATH}/cuits_data.parquet"


def fetch_people_data(**kwargs):
    import requests
    import os
    import pandas as pd

    tickets_df = pd.read_parquet(TICKETS_DATA)
    tickets_cuits = set(tickets_df.car_owner_cuit)

    headers = {
        "x-token": os.environ.get("EZX-TOKEN",""),
    }

    query = {
        "cuit": tickets_cuits,
    }

    response = requests.get(
        url=os.environ.get("EZX-APIURL",""),
        headers=headers,
        params=query
    )

    df = pd.DataFrame(response.json())

    COLUMNS= [
        "cuit",
        "APYNOM",
        "DESCRIPCION__",
        "TIPO",
        "CALLE",
        "NRO",
        "DEPT",
        "COD_POSTAL",
        "DESCRIPCION",
        "COD_LOC",
        "DESCRIPCION_",
        "COD_PROV",
        "TIPO_DOC",
        "id_number",
        "TELEFONOS",
        "EMAIL"
    ]

    if df.size:
        df.columns = COLUMNS
    else:
        df = pd.DataFrame(columns=COLUMNS)

    df.to_parquet(CUITS_DATA)


def check_missing_cuits():
    import pandas as pd

    people_df = pd.read_parquet(CUITS_DATA)
    tickets_df = pd.read_parquet(TICKETS_DATA)

    people_cuits = set(people_df.cuit)
    tickets_cuits = set(tickets_df.car_owner_cuit)

    missing_cuits = tickets_cuits - people_cuits

    if missing_cuits:
        notify_to_ds(
            "Missing Cuits",
            f"{len(missing_cuits)} cuits are missing from Rafam.\n" + " | ".join(missing_cuits),
            "Asterius Dataloader"
        )

MERGED_DATA = f"{TMP_PATH}/merged_cuit_data.parquet"

def merge_tickets_cuits_data():
    import pandas as pd

    people_df = pd.read_parquet(CUITS_DATA)
    tickets_df = pd.read_parquet(TICKETS_DATA)

    joint_data = people_df.join(tickets_df, on="cuit", how="inner")

    joint_data.to_parquet(MERGED_DATA)


def load_data_into_asterius():
    import pandas as pd

    df = pd.read_parquet(MERGED_DATA)

    if not df.size:
        notify(
                "Nothing to load",
                f"No data to store",
                "Asterius Dataloader"
            )

fetch_tickets_data_task = GdriveLimitedPythonOperator(
    task_id="fetch_tickets_data",
    python_callable=fetch_tickets_data,
    dag=asterius_dataloader_dag,
    on_failure_callback=ds_error_notifier
)

fetch_people_data_task = GdriveLimitedPythonOperator(
    task_id="fetch_people_data",
    python_callable=fetch_people_data,
    dag=asterius_dataloader_dag,
    on_failure_callback=ds_error_notifier
)

check_missing_cuits_task = PythonOperator(
    task_id="check_missing_cuits",
    python_callable=check_missing_cuits,
    dag=asterius_dataloader_dag,
)

merge_tickets_cuits_data_task = PythonOperator(
    task_id="merge_tickets_cuits_data",
    python_callable=merge_tickets_cuits_data,
    dag=asterius_dataloader_dag,
)

load_data_into_asterius_task = PythonOperator(
    task_id="load_data_into_asterius",
    python_callable=load_data_into_asterius,
    dag=asterius_dataloader_dag,
    on_failure_callback=ds_error_notifier
)

fetch_tickets_data_task >> fetch_people_data_task >> check_missing_cuits_task
fetch_people_data_task >> merge_tickets_cuits_data_task >> load_data_into_asterius_task