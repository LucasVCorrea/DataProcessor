import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from google_worker import GdriveLimitedPythonOperator


ezeiza_dag = DAG(
    dag_id="ezeiza_dag",
    start_date=datetime.datetime(2026, 9, 22),
    catchup=False,
    schedule="@hourly",
)


JUICIOS_INICIADOS_RAW = "/tmp/data/ezeiza/juicios_iniciados.parquet"
JUICIOS_INICIADOS_CLEAN = "/tmp/data/ezeiza/juicios_iniciados_clean.parquet"
JUICIOS_INICIADOS_DETAIL = "/data/ezeiza/juicios_iniciados_detail.parquet"
JUICIOS_COBRADOS_RAW = "/tmp/data/ezeiza/juicios_cobrados.parquet"
JUICIOS_COBRADOS_CLEAN = "/tmp/data/ezeiza/juicios_cobrados_clean.parquet"
JUICIOS_COBRADOS_DETAIL = "/data/ezeiza/juicios_cobrados_detail.parquet"

MONTHS = {
    "January": "Enero",
    "February": "Febrero",
    "March": "Marzo",
    "April": "Abril",
    "May": "Mayo",
    "June": "Junio",
    "July": "Julio",
    "August": "Agosto",
    "September": "Septiembre",
    "October": "Octubre",
    "November": "Noviembre",
    "December": "Diciembre",
}


def _parse_dates(series):
    import pandas as pd

    values = series.astype("string").str.strip()
    values = values.str.replace(r"[^0-9/\-: .]", "", regex=True).str.strip()
    values = values.str.replace(
        r"^(\d{1,2})/(\d{1,2})(\d{2})$", r"\1/\2/\3", regex=True
    )

    numeric = pd.to_numeric(values, errors="coerce")
    excel_serial = numeric.between(20000, 80000, inclusive="both").fillna(False)
    parsed_text = pd.to_datetime(
        values.where(~excel_serial),
        format="mixed",
        dayfirst=True,
        errors="coerce",
    )
    parsed_serial = pd.to_datetime(
        numeric.where(excel_serial),
        unit="D",
        origin="1899-12-30",
        errors="coerce",
    )
    return parsed_text.fillna(parsed_serial).dt.normalize()


def _to_numeric(series):
    import pandas as pd

    values = series.astype("string").str.strip()
    values = values.str.replace("$", "", regex=False).str.replace(" ", "", regex=False)
    comma_decimal = values.str.contains(",", na=False)
    values = values.where(
        ~comma_decimal,
        values.str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
    )
    thousands_dot = ~comma_decimal & values.str.match(
        r"^-?\d{1,3}(?:\.\d{3})+$", na=False
    )
    values = values.where(~thousands_dot, values.str.replace(".", "", regex=False))
    return pd.to_numeric(values, errors="coerce")


def _normalize_identifier(series):
    return series.astype("string").str.strip().str.replace(r"\.0$", "", regex=True)


def fetch_juicios_iniciados(**kwargs):
    import gspread
    import pandas as pd
    from oauth2client.service_account import ServiceAccountCredentials

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        "/credentials/credentials.json",
        [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive.readonly",
        ],
    )
    client = gspread.authorize(credentials)
    worksheet = client.open("juicios_iniciados").sheet1
    values = worksheet.get_all_values()
    dataframe = pd.DataFrame(values[1:], columns=values[0]) if values else pd.DataFrame()
    dataframe.to_parquet(JUICIOS_INICIADOS_RAW, index=False)


def transform_juicios_iniciados(**kwargs):
    import pandas as pd

    dataframe = pd.read_parquet(JUICIOS_INICIADOS_RAW)
    dataframe = dataframe.dropna(axis=0, how="all").dropna(axis=1, how="all")
    dataframe.columns = dataframe.columns.astype(str).str.strip()
    drop_columns = [
        column
        for column in dataframe.columns
        if not column or column.lower().startswith("unnamed:")
    ]
    dataframe = dataframe.drop(columns=drop_columns, errors="ignore")

    required_columns = {
        "Nombre",
        "Expediente Juzgado De Paz",
        "Monto De La Demanda",
        "Juzgado De Faltas",
        "Fecha De Inicio De La Demanda",
    }
    missing_columns = required_columns.difference(dataframe.columns)
    if missing_columns:
        raise ValueError(
            "juicios_iniciados no contiene las columnas requeridas: "
            + ", ".join(sorted(missing_columns))
        )

    dataframe["Nombre"] = (
        dataframe["Nombre"]
        .astype("string")
        .str.replace(r"[^a-zA-Z0-9\s]", "", regex=True)
        .str.lower()
        .str.strip()
    )
    dataframe["Expediente Juzgado De Paz"] = _normalize_identifier(
        dataframe["Expediente Juzgado De Paz"]
    )
    dataframe["Juzgado De Faltas"] = _normalize_identifier(
        dataframe["Juzgado De Faltas"]
    )
    dataframe["Monto De La Demanda"] = _to_numeric(dataframe["Monto De La Demanda"])

    fecha = _parse_dates(dataframe["Fecha De Inicio De La Demanda"])
    dataframe["Fecha De Inicio De La Demanda"] = fecha
    dataframe["Fecha_corregida"] = fecha
    dataframe["Fecha_limpia"] = fecha
    dataframe["Mes"] = fecha.dt.month_name().map(MONTHS)
    dataframe["Mes_num"] = fecha.dt.month.astype("Int64")
    dataframe["Año"] = fecha.dt.year.astype("Int64")

    sociedad_pattern = (
        r"\b(?:sa|srl|sociedad|anonima|social|argentina|nacional|nacion|provincia|"
        r"compania|compaia|s a|gobierno|banco|autoridad)\b"
    )
    dataframe["Es sociedad anonima o srl"] = dataframe["Nombre"].str.contains(
        sociedad_pattern,
        regex=True,
        na=False,
    )

    dataframe = dataframe.loc[
        dataframe["Año"].ge(2019)
        & dataframe["Expediente Juzgado De Paz"].ne("")
        & dataframe["Nombre"].ne("")
        & dataframe["Fecha_corregida"].notna()
        & dataframe["Monto De La Demanda"].notna()
    ].copy()
    dataframe["Año"] = dataframe["Año"].astype("int64")
    dataframe["Mes_num"] = dataframe["Mes_num"].astype("int64")
    dataframe = dataframe.sort_values(
        ["Año", "Mes_num", "Fecha_corregida", "Expediente Juzgado De Paz"]
    )
    dataframe.to_parquet(JUICIOS_INICIADOS_CLEAN, index=False)


def build_juicios_iniciados_detail(**kwargs):
    import pandas as pd

    dataframe = pd.read_parquet(JUICIOS_INICIADOS_CLEAN)
    dataframe.to_parquet(JUICIOS_INICIADOS_DETAIL, index=False)


def fetch_juicios_cobrados(**kwargs):
    import gspread
    import pandas as pd
    from oauth2client.service_account import ServiceAccountCredentials

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        "/credentials/credentials.json",
        [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive.readonly",
        ],
    )
    client = gspread.authorize(credentials)
    worksheet = client.open("juicios_cobrados").sheet1
    values = worksheet.get_all_values()
    dataframe = pd.DataFrame(values[1:], columns=values[0]) if values else pd.DataFrame()
    dataframe.to_parquet(JUICIOS_COBRADOS_RAW, index=False)


def transform_juicios_cobrados(**kwargs):
    import pandas as pd

    dataframe = pd.read_parquet(JUICIOS_COBRADOS_RAW)
    dataframe = dataframe.dropna(axis=0, how="all").dropna(axis=1, how="all")
    dataframe.columns = dataframe.columns.astype(str).str.strip()

    required_columns = {
        "Expediente",
        "Nombre",
        "Cuit",
        "Monto Cobrado",
        "Multa Cobrada",
        "Gastos Administrativos",
        "Fecha",
        "Forma De Pago",
        "Juzgado",
    }
    missing_columns = required_columns.difference(dataframe.columns)
    if missing_columns:
        raise ValueError(
            "juicios_cobrados no contiene las columnas requeridas: "
            + ", ".join(sorted(missing_columns))
        )

    shifted_date = _parse_dates(dataframe["Gastos Administrativos"])
    month_marker = dataframe["Fecha"].astype("string").str.strip().str.upper()
    shifted_rows = month_marker.isin(
        {
            "ENERO",
            "FEBRERO",
            "MARZO",
            "ABRIL",
            "MAYO",
            "JUNIO",
            "JULIO",
            "AGOSTO",
            "SEPTIEMBRE",
            "OCTUBRE",
            "NOVIEMBRE",
            "DICIEMBRE",
        }
    ) & shifted_date.notna()
    if shifted_rows.any():
        multa = _to_numeric(dataframe.loc[shifted_rows, "Monto Cobrado"])
        gastos = _to_numeric(dataframe.loc[shifted_rows, "Multa Cobrada"])
        dataframe.loc[shifted_rows, "Fecha"] = shifted_date.loc[shifted_rows]
        dataframe.loc[shifted_rows, "Gastos Administrativos"] = gastos
        dataframe.loc[shifted_rows, "Multa Cobrada"] = multa
        dataframe.loc[shifted_rows, "Monto Cobrado"] = multa + gastos

    dataframe["Nombre"] = (
        dataframe["Nombre"]
        .astype("string")
        .str.replace(r"[^a-zA-Z0-9\s]", "", regex=True)
        .str.lower()
        .str.strip()
    )
    dataframe["Expediente"] = _normalize_identifier(dataframe["Expediente"])
    dataframe["Cuit"] = _normalize_identifier(dataframe["Cuit"])
    dataframe["Juzgado"] = _normalize_identifier(dataframe["Juzgado"])
    dataframe["Forma De Pago"] = (
        dataframe["Forma De Pago"].astype("string").fillna("").str.strip()
    )

    for column in [
        "Monto Dda",
        "Monto Cobrado",
        "Multa Cobrada",
        "Gastos Administrativos",
    ]:
        if column in dataframe.columns:
            dataframe[column] = _to_numeric(dataframe[column])

    dataframe["Fecha"] = _parse_dates(dataframe["Fecha"])
    dataframe["Mes"] = dataframe["Fecha"].dt.month_name().map(MONTHS)
    dataframe["Mes_num"] = dataframe["Fecha"].dt.month.astype("Int64")
    dataframe["Año"] = dataframe["Fecha"].dt.year.astype("Int64")

    primeros_digitos = pd.to_numeric(
        dataframe["Cuit"].str.extract(r"^(\d{2})", expand=False), errors="coerce"
    )
    dataframe["Primeros Digitos Cuit"] = primeros_digitos.astype("Int64")
    dataframe["Tipo Persona"] = (primeros_digitos <= 27).map(
        {True: "Física", False: "Jurídica"}
    )
    dataframe["Forma de pago basica"] = (
        dataframe["Forma De Pago"].str.lower().str.contains("contado", na=False)
    ).map({True: "Contado", False: "Otro"})

    dataframe = dataframe.loc[
        dataframe["Año"].ge(2019)
        & dataframe["Expediente"].ne("")
        & dataframe["Nombre"].ne("")
        & dataframe["Fecha"].notna()
        & dataframe["Monto Cobrado"].notna()
        & dataframe["Primeros Digitos Cuit"].notna()
    ].copy()
    dataframe["Año"] = dataframe["Año"].astype("int64")
    dataframe["Mes_num"] = dataframe["Mes_num"].astype("int64")
    dataframe["Primeros Digitos Cuit"] = dataframe["Primeros Digitos Cuit"].astype(
        "int64"
    )
    dataframe = dataframe.drop_duplicates().sort_values(
        ["Año", "Mes_num", "Fecha", "Expediente"]
    )
    dataframe.to_parquet(JUICIOS_COBRADOS_CLEAN, index=False)


def build_juicios_cobrados_detail(**kwargs):
    import pandas as pd

    dataframe = pd.read_parquet(JUICIOS_COBRADOS_CLEAN)
    dataframe.to_parquet(JUICIOS_COBRADOS_DETAIL, index=False)


fetch_juicios_iniciados_task = GdriveLimitedPythonOperator(
    task_id="fetch_juicios_iniciados",
    python_callable=fetch_juicios_iniciados,
    dag=ezeiza_dag,
)
transform_juicios_iniciados_task = PythonOperator(
    task_id="transform_juicios_iniciados",
    python_callable=transform_juicios_iniciados,
    dag=ezeiza_dag,
)
build_juicios_iniciados_detail_task = PythonOperator(
    task_id="build_juicios_iniciados_detail",
    python_callable=build_juicios_iniciados_detail,
    dag=ezeiza_dag,
)

fetch_juicios_cobrados_task = GdriveLimitedPythonOperator(
    task_id="fetch_juicios_cobrados",
    python_callable=fetch_juicios_cobrados,
    dag=ezeiza_dag,
)
transform_juicios_cobrados_task = PythonOperator(
    task_id="transform_juicios_cobrados",
    python_callable=transform_juicios_cobrados,
    dag=ezeiza_dag,
)
build_juicios_cobrados_detail_task = PythonOperator(
    task_id="build_juicios_cobrados_detail",
    python_callable=build_juicios_cobrados_detail,
    dag=ezeiza_dag,
)

fetch_juicios_iniciados_task >> transform_juicios_iniciados_task
transform_juicios_iniciados_task >> build_juicios_iniciados_detail_task

fetch_juicios_cobrados_task >> transform_juicios_cobrados_task
transform_juicios_cobrados_task >> build_juicios_cobrados_detail_task
