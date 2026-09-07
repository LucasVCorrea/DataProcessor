import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


transito_dag = DAG(
    dag_id="transito_dag",
    start_date=datetime.datetime(2026, 9, 3),
    catchup=False,
    schedule="@daily",
)


# ---- actividad de revisores -----------------------------------------------

ACTIVIDAD_REVISORES_RAW = "/tmp/data/transito/actividad_revisores.parquet"
ACTIVIDAD_REVISORES_CLEAN = "/tmp/data/transito/actividad_revisores_clean.parquet"


def fetch_actividad_revisores(**kwargs):
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
    worksheet = client.open("actividad_revisores").sheet1
    values = worksheet.get_all_values(value_render_option="UNFORMATTED_VALUE")
    dataframe = pd.DataFrame(values[1:], columns=values[0]) if values else pd.DataFrame()
    if not dataframe.empty:
        dataframe = dataframe.astype("string")
    dataframe.to_parquet(ACTIVIDAD_REVISORES_RAW, index=False)


def transform_actividad_revisores(**kwargs):
    import pandas as pd

    dataframe = pd.read_parquet(ACTIVIDAD_REVISORES_RAW).rename(
        columns={"auditor": "Auditor", "Cliente": "Municipio"}
    )

    required_columns = {
        "Auditor",
        "Municipio",
        "Tipo de Revisión",
        "Código de cámara",
        "Fecha",
        "Aceptadas",
        "Rechazadas",
        "Total",
    }
    missing_columns = required_columns.difference(dataframe.columns)
    if missing_columns:
        raise ValueError(
            "actividad_revisores no contiene las columnas requeridas: "
            + ", ".join(sorted(missing_columns))
        )

    dataframe["Municipio"] = (
        dataframe["Municipio"].astype("string").fillna("").str.strip().str.capitalize()
    )
    dataframe["Municipio"] = dataframe["Municipio"].replace({"Ezeiza_ftl": "Ezeiza"})

    dataframe["Tipo de Revisión"] = (
        dataframe["Tipo de Revisión"].astype("string").fillna("").str.strip()
    )
    dataframe["Tipo de Cámara"] = dataframe["Tipo de Revisión"].replace(
        {
            "Single Review": "Luces",
            "Multiple Review": "Semáforo",
            "Plate Finder": "Buscador de patentes",
        }
    )

    dataframe["Auditor"] = dataframe["Auditor"].astype("string").fillna("").map(
        lambda value: " ".join(value.split()).title()
    )
    dataframe["Auditor"] = dataframe["Auditor"].replace(
        {
            "Madelein Sánchez": "Madelein Sanchez",
            "Jerónimo Blanco": "Jeronimo Blanco",
            "Nicolás Azuaga": "Nicolas Azuaga",
            "Sofía Brites": "Sofia Brites",
            "Mabel García": "Mabel Garcia",
        }
    )
    dataframe = dataframe.loc[dataframe["Auditor"] != "Cecilia Zulma Perillo"]

    dataframe["Código de cámara"] = (
        dataframe["Código de cámara"]
        .astype("string")
        .fillna("")
        .str.strip()
        .replace(
            {
                "4-C64 - Av 122 y calle 64": "4C64 - Av 122 y calle 64",
                "3-C59 - Av 122 y calle 59": "3C59 - Av 122 y calle 59",
                "5-C126 - Av 60 y calle 126": "5C126 - Av 60 y calle 126",
            }
        )
    )
    dataframe["Código limpio"] = dataframe["Código de cámara"].map(
        lambda value: value.split("-")[0].strip() if value else None
    )
    dataframe["Código de cámara"] = dataframe["Código de cámara"].map(
        lambda value: value.split("-")[-1].strip() if value else None
    )
    dataframe["Código de cámara"] = dataframe["Código de cámara"].replace(
        {
            "Cámara Móvil 1": "Cámara Móvil",
            "Cámara Móvil 2": "Cámara Móvil",
            "Las Toscas), J. M. Ezeiza": "Castex 950",
            "Pres. N. Kirchner y Sgto. Cabral": "Kirchner y Cabral",
            "Pres. N. Kirchner y Sgto. Cabral, J. M. Ezeiza": "Kirchner y Cabral, J. M. Ezeiza",
            "Pres. N. Kirchner y Zenavilla": "Kirchner y Zenavilla",
            "Pres. Néstor Kirchner & El Trebol (Mano Cañuelas)": "Kirchner & El Trebol (Mano Cañuelas)",
            "Pres. Néstor Kirchner & El Trebol (Mano Ezeiza)": "Kirchner & El Trebol (Mano Ezeiza)",
            "Pres. Néstor Kirchner & Vedia": "Kirchner & Vedia",
            "Presidente Nestor Kirchner y Gaddini": "Kirchner y Gaddini",
            "Presidente Nestor Kirchner y Zenavilla (Mano Esteban Echevarria)": "Kirchner y Zenavilla (Mano Esteban Echevarria)",
            "R58 y Saint Thomas Country club mano a ruta 52": "R58 y St Thomas Country club mano a ruta 52",
        }
    )

    date_values = dataframe["Fecha"].astype("string").str.strip()
    numeric_dates = pd.to_numeric(date_values, errors="coerce")
    excel_serial = numeric_dates.between(20000, 80000, inclusive="both").fillna(False)
    parsed_text = pd.to_datetime(
        date_values.where(~excel_serial),
        format="mixed",
        dayfirst=False,
        errors="coerce",
    )
    parsed_serial = pd.to_datetime(
        numeric_dates.where(excel_serial),
        unit="D",
        origin="1899-12-30",
        errors="coerce",
    )
    dataframe["Fecha"] = parsed_text.fillna(parsed_serial).dt.normalize()

    today = pd.Timestamp.now(tz="America/Argentina/Buenos_Aires").tz_localize(None).normalize()
    dataframe = dataframe.loc[dataframe["Fecha"].notna() & (dataframe["Fecha"] <= today)]
    dataframe["Año"] = dataframe["Fecha"].dt.year.astype("int64")
    dataframe["Mes_num"] = dataframe["Fecha"].dt.month.astype("int64")
    dataframe["Mes"] = dataframe["Fecha"].dt.month_name().map(
        {
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
    )
    dataframe["Mes/Año"] = dataframe["Mes"] + "-" + dataframe["Año"].astype(str)
    dataframe["Semana"] = dataframe["Fecha"].map(
        lambda fecha: 1 if fecha.day <= 7 else 2 if fecha.day <= 15 else 3 if fecha.day <= 23 else 4
    )

    for column in ["Aceptadas", "Rechazadas", "Total", "Nivel de Revisión"]:
        if column in dataframe.columns:
            values = dataframe[column].astype("string").str.strip().str.replace(",", ".", regex=False)
            dataframe[column] = pd.to_numeric(values, errors="coerce")

    dataframe["Aceptadas"] = dataframe["Aceptadas"].fillna(0)
    dataframe["Rechazadas"] = dataframe["Rechazadas"].fillna(0)
    dataframe["Total"] = dataframe["Total"].fillna(dataframe["Aceptadas"] + dataframe["Rechazadas"])

    january_2025_reviewers = dataframe.loc[
        (dataframe["Mes"] == "Enero") & (dataframe["Año"] == 2025), "Auditor"
    ].unique().tolist()
    unlam_reviewers = january_2025_reviewers + [
        "Miguel Gullo",
        "Celeste Donato",
        "Ramiro Odella",
        "Yanina Denise Ybarra",
        "Jeronimo Blanco",
        "Mariana Diaz",
        "Mariana Becerra",
        "Cecilia Solares",
        "Santiago Reyes",
        "Sofia Brites",
        "Nicolas Azuaga",
        "Mabel García",
        "Mariana Maggiolino",
    ]
    dataframe["dominio"] = dataframe["Auditor"].isin(unlam_reviewers).map({True: "Unlam", False: "Otro"})

    dataframe = dataframe.loc[
        dataframe["Tipo de Cámara"].isin(["Semáforo", "Luces", "Buscador de patentes"])
        & (dataframe["dominio"] == "Unlam")
    ]
    dataframe["Mes"] = pd.Categorical(
        dataframe["Mes"],
        categories=[
            "Octubre",
            "Noviembre",
            "Diciembre",
            "Enero",
            "Febrero",
            "Marzo",
            "Abril",
            "Mayo",
            "Junio",
            "Julio",
            "Agosto",
            "Septiembre",
        ],
        ordered=True,
    )
    dataframe = dataframe.drop(
        columns=["Tipo de Revisión", "% aceptadas", "Recorte", "Deshacer", "NoOp"],
        errors="ignore",
    )
    dataframe.drop_duplicates().sort_values("Mes", ascending=False).to_parquet(
        ACTIVIDAD_REVISORES_CLEAN,
        index=False,
    )


fetch_actividad_revisores_task = PythonOperator(
    task_id="fetch_actividad_revisores",
    python_callable=fetch_actividad_revisores,
    dag=transito_dag,
)
transform_actividad_revisores_task = PythonOperator(
    task_id="transform_actividad_revisores",
    python_callable=transform_actividad_revisores,
    dag=transito_dag,
)
fetch_actividad_revisores_task >> transform_actividad_revisores_task


# ---- valores UF -----------------------------------------------------------

VALORES_UF_RAW = "/tmp/data/transito/valores_uf.parquet"
VALORES_UF_CLEAN = "/tmp/data/transito/valores_uf_clean.parquet"


def fetch_valores_uf(**kwargs):
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
    worksheet = client.open("valores_uf").sheet1
    values = worksheet.get_all_values(value_render_option="UNFORMATTED_VALUE")
    dataframe = pd.DataFrame(values[1:], columns=values[0]) if values else pd.DataFrame()
    if not dataframe.empty:
        dataframe = dataframe.astype("string")
    dataframe.to_parquet(VALORES_UF_RAW, index=False)


def transform_valores_uf(**kwargs):
    import pandas as pd

    dataframe = pd.read_parquet(VALORES_UF_RAW)
    dataframe.columns = dataframe.columns.str.strip()

    required_columns = {"Año", "Mes", "Valor UF"}
    missing_columns = required_columns.difference(dataframe.columns)
    if missing_columns:
        raise ValueError(
            "valores_uf no contiene las columnas requeridas: "
            + ", ".join(sorted(missing_columns))
        )

    dataframe["Año"] = pd.to_numeric(
        dataframe["Año"].astype("string").str.strip(),
        errors="coerce",
    ).astype("Int64")
    dataframe["Mes"] = (
        dataframe["Mes"].astype("string").str.strip().str.capitalize()
    )

    values = dataframe["Valor UF"].astype("string").str.strip()
    numeric_values = pd.to_numeric(values, errors="coerce")
    formatted_values = (
        values.str.replace("$", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    dataframe["Valor UF"] = numeric_values.fillna(
        pd.to_numeric(formatted_values, errors="coerce")
    )

    dataframe = dataframe.loc[
        dataframe["Año"].notna()
        & dataframe["Mes"].ne("")
        & dataframe["Valor UF"].notna()
    ]
    dataframe["Año"] = dataframe["Año"].astype("int64")
    dataframe.drop_duplicates(subset=["Año", "Mes"], keep="last").to_parquet(
        VALORES_UF_CLEAN,
        index=False,
    )


fetch_valores_uf_task = PythonOperator(
    task_id="fetch_valores_uf",
    python_callable=fetch_valores_uf,
    dag=transito_dag,
)
transform_valores_uf_task = PythonOperator(
    task_id="transform_valores_uf",
    python_callable=transform_valores_uf,
    dag=transito_dag,
)
fetch_valores_uf_task >> transform_valores_uf_task


# ---- motivos de rechazo ---------------------------------------------------

MOTIVOS_RECHAZOS_RAW = "/tmp/data/transito/motivos_rechazos.parquet"
MOTIVOS_RECHAZOS_CLEAN = "/tmp/data/transito/motivos_rechazos_clean.parquet"


def fetch_motivos_rechazos(**kwargs):
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
    worksheet = client.open("motivos_rechazos").sheet1
    values = worksheet.get_all_values(value_render_option="UNFORMATTED_VALUE")
    dataframe = pd.DataFrame(values[1:], columns=values[0]) if values else pd.DataFrame()
    if not dataframe.empty:
        dataframe = dataframe.astype("string")
    dataframe.to_parquet(MOTIVOS_RECHAZOS_RAW, index=False)


def transform_motivos_rechazos(**kwargs):
    import pandas as pd

    dataframe = pd.read_parquet(MOTIVOS_RECHAZOS_RAW).rename(
        columns={"auditor": "Auditor", "Cliente": "Municipio"}
    )

    required_columns = {
        "Auditor",
        "Municipio",
        "Tipo de Revisión",
        "Código de cámara",
        "Fecha",
        "Motivo de rechazo",
        "Total",
    }
    missing_columns = required_columns.difference(dataframe.columns)
    if missing_columns:
        raise ValueError(
            "motivos_rechazos no contiene las columnas requeridas: "
            + ", ".join(sorted(missing_columns))
        )

    dataframe["Municipio"] = (
        dataframe["Municipio"].astype("string").fillna("").str.strip().str.capitalize()
    )
    dataframe["Municipio"] = dataframe["Municipio"].replace({"Ezeiza_ftl": "Ezeiza"})

    dataframe["Tipo de Revisión"] = (
        dataframe["Tipo de Revisión"].astype("string").fillna("").str.strip()
    )
    dataframe["Tipo de Cámara"] = dataframe["Tipo de Revisión"].replace(
        {
            "Single Review": "Luces",
            "Multiple Review": "Semáforo",
            "Plate Finder": "Buscador de patentes",
        }
    )

    dataframe["Auditor"] = dataframe["Auditor"].astype("string").fillna("").map(
        lambda value: " ".join(value.split()).title()
    )
    dataframe["Auditor"] = dataframe["Auditor"].replace(
        {
            "Madelein Sánchez": "Madelein Sanchez",
            "Jerónimo Blanco": "Jeronimo Blanco",
            "Nicolás Azuaga": "Nicolas Azuaga",
            "Sofía Brites": "Sofia Brites",
            "Mabel García": "Mabel Garcia",
        }
    )
    dataframe = dataframe.loc[dataframe["Auditor"] != "Cecilia Zulma Perillo"]

    dataframe["Código de cámara"] = (
        dataframe["Código de cámara"]
        .astype("string")
        .fillna("")
        .str.strip()
        .replace(
            {
                "4-C64 - Av 122 y calle 64": "4C64 - Av 122 y calle 64",
                "3-C59 - Av 122 y calle 59": "3C59 - Av 122 y calle 59",
                "5-C126 - Av 60 y calle 126": "5C126 - Av 60 y calle 126",
            }
        )
    )
    dataframe["Código limpio"] = dataframe["Código de cámara"].map(
        lambda value: value.split("-")[0].strip() if value else None
    )
    dataframe["Código de cámara"] = dataframe["Código de cámara"].map(
        lambda value: value.split("-")[-1].strip() if value else None
    )
    dataframe["Código de cámara"] = dataframe["Código de cámara"].replace(
        {
            "Cámara Móvil 1": "Cámara Móvil",
            "Cámara Móvil 2": "Cámara Móvil",
            "Las Toscas), J. M. Ezeiza": "Castex 950",
            "Pres. N. Kirchner y Sgto. Cabral": "Kirchner y Cabral",
            "Pres. N. Kirchner y Sgto. Cabral, J. M. Ezeiza": "Kirchner y Cabral, J. M. Ezeiza",
            "Pres. N. Kirchner y Zenavilla": "Kirchner y Zenavilla",
            "Pres. Néstor Kirchner & El Trebol (Mano Cañuelas)": "Kirchner & El Trebol (Mano Cañuelas)",
            "Pres. Néstor Kirchner & El Trebol (Mano Ezeiza)": "Kirchner & El Trebol (Mano Ezeiza)",
            "Pres. Néstor Kirchner & Vedia": "Kirchner & Vedia",
            "Presidente Nestor Kirchner y Gaddini": "Kirchner y Gaddini",
            "Presidente Nestor Kirchner y Zenavilla (Mano Esteban Echevarria)": "Kirchner y Zenavilla (Mano Esteban Echevarria)",
            "R58 y Saint Thomas Country club mano a ruta 52": "R58 y St Thomas Country club mano a ruta 52",
        }
    )

    date_values = dataframe["Fecha"].astype("string").str.strip()
    numeric_dates = pd.to_numeric(date_values, errors="coerce")
    excel_serial = numeric_dates.between(20000, 80000, inclusive="both").fillna(False)
    parsed_text = pd.to_datetime(
        date_values.where(~excel_serial),
        format="mixed",
        dayfirst=False,
        errors="coerce",
    )
    parsed_serial = pd.to_datetime(
        numeric_dates.where(excel_serial),
        unit="D",
        origin="1899-12-30",
        errors="coerce",
    )
    dataframe["Fecha"] = parsed_text.fillna(parsed_serial).dt.normalize()

    today = pd.Timestamp.now(tz="America/Argentina/Buenos_Aires").tz_localize(None).normalize()
    dataframe = dataframe.loc[dataframe["Fecha"].notna() & (dataframe["Fecha"] <= today)]
    dataframe["Año"] = dataframe["Fecha"].dt.year.astype("int64")
    dataframe["Mes_num"] = dataframe["Fecha"].dt.month.astype("int64")
    dataframe["Mes"] = dataframe["Fecha"].dt.month_name().map(
        {
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
    )
    dataframe["Mes/Año"] = dataframe["Mes"] + "-" + dataframe["Año"].astype(str)
    dataframe["Semana"] = dataframe["Fecha"].map(
        lambda fecha: 1 if fecha.day <= 7 else 2 if fecha.day <= 15 else 3 if fecha.day <= 23 else 4
    )
    dataframe["Motivo de rechazo"] = (
        dataframe["Motivo de rechazo"].astype("string").fillna("").str.strip()
    )
    dataframe["Total"] = pd.to_numeric(
        dataframe["Total"].astype("string").str.strip().str.replace(",", ".", regex=False),
        errors="coerce",
    ).fillna(0)

    january_reviewers = dataframe.loc[dataframe["Mes"] == "Enero", "Auditor"].unique().tolist()
    unlam_reviewers = january_reviewers + [
        "Miguel Gullo",
        "Celeste Donato",
        "Ramiro Odella",
        "Yanina Denise Ybarra",
        "Jeronimo Blanco",
        "Mariana Diaz",
        "Mariana Becerra",
        "Sofia Brites",
        "Nicolas Azuaga",
        "Cecilia Solares",
        "Santiago Reyes",
        "Mabel García",
        "Mariana Maggiolino",
    ]

    dataframe = dataframe.loc[
        dataframe["Tipo de Cámara"].isin(["Semáforo", "Luces", "Buscador de patentes"])
        & dataframe["Auditor"].isin(unlam_reviewers)
    ]
    dataframe["Mes"] = pd.Categorical(
        dataframe["Mes"],
        categories=[
            "Octubre",
            "Noviembre",
            "Diciembre",
            "Enero",
            "Febrero",
            "Marzo",
            "Abril",
            "Mayo",
            "Junio",
            "Julio",
            "Agosto",
            "Septiembre",
        ],
        ordered=True,
    )
    dataframe.drop_duplicates().sort_values("Mes", ascending=False).to_parquet(
        MOTIVOS_RECHAZOS_CLEAN,
        index=False,
    )


fetch_motivos_rechazos_task = PythonOperator(
    task_id="fetch_motivos_rechazos",
    python_callable=fetch_motivos_rechazos,
    dag=transito_dag,
)
transform_motivos_rechazos_task = PythonOperator(
    task_id="transform_motivos_rechazos",
    python_callable=transform_motivos_rechazos,
    dag=transito_dag,
)
fetch_motivos_rechazos_task >> transform_motivos_rechazos_task


# ---- tiempo de revision ---------------------------------------------------

TIEMPO_REVISION_RAW = "/tmp/data/transito/tiempo_revision.parquet"
TIEMPO_REVISION_CLEAN = "/tmp/data/transito/tiempo_revision_clean.parquet"


def fetch_tiempo_revision(**kwargs):
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
    worksheet = client.open("tiempo_revision").sheet1
    values = worksheet.get_all_values(value_render_option="UNFORMATTED_VALUE")
    dataframe = pd.DataFrame(values[1:], columns=values[0]) if values else pd.DataFrame()
    if not dataframe.empty:
        dataframe = dataframe.astype("string")
    dataframe.to_parquet(TIEMPO_REVISION_RAW, index=False)


def transform_tiempo_revision(**kwargs):
    import pandas as pd

    dataframe = pd.read_parquet(TIEMPO_REVISION_RAW).rename(
        columns={"auditor": "Auditor", "Cliente": "Municipio"}
    )

    required_columns = {
        "Auditor",
        "Municipio",
        "Tipo de Revisión",
        "Fecha",
        "Total (en horas)",
    }
    missing_columns = required_columns.difference(dataframe.columns)
    if missing_columns:
        raise ValueError(
            "tiempo_revision no contiene las columnas requeridas: "
            + ", ".join(sorted(missing_columns))
        )

    dataframe["Municipio"] = (
        dataframe["Municipio"].astype("string").fillna("").str.strip().str.capitalize()
    )

    dataframe["Tipo de Revisión"] = (
        dataframe["Tipo de Revisión"].astype("string").fillna("").str.strip()
    )
    dataframe["Tipo de Cámara"] = dataframe["Tipo de Revisión"].replace(
        {
            "Single Review": "Luces",
            "Multiple Review": "Semáforo",
            "Plate Finder": "Buscador de patentes",
        }
    )

    dataframe["Auditor"] = dataframe["Auditor"].astype("string").fillna("").map(
        lambda value: " ".join(value.split()).title()
    )
    dataframe["Auditor"] = dataframe["Auditor"].replace(
        {
            "Madelein Sánchez": "Madelein Sanchez",
            "Jerónimo Blanco": "Jeronimo Blanco",
            "Nicolás Azuaga": "Nicolas Azuaga",
            "Sofía Brites": "Sofia Brites",
            "Mabel García": "Mabel Garcia",
        }
    )

    hours = dataframe["Total (en horas)"].astype("string").str.strip().str.replace(",", ".", regex=False)
    dataframe["Total (en horas)"] = pd.to_numeric(hours, errors="coerce").fillna(0.0).astype(float)

    date_values = dataframe["Fecha"].astype("string").str.strip()
    numeric_dates = pd.to_numeric(date_values, errors="coerce")
    excel_serial = numeric_dates.between(20000, 80000, inclusive="both").fillna(False)
    parsed_text = pd.to_datetime(
        date_values.where(~excel_serial),
        format="mixed",
        dayfirst=False,
        errors="coerce",
    )
    parsed_serial = pd.to_datetime(
        numeric_dates.where(excel_serial),
        unit="D",
        origin="1899-12-30",
        errors="coerce",
    )
    dataframe["Fecha"] = parsed_text.fillna(parsed_serial).dt.normalize()
    dataframe["Fecha"] = dataframe["Fecha"].replace({pd.Timestamp("2024-03-17"): pd.Timestamp("2025-03-17")})

    today = pd.Timestamp.now(tz="America/Argentina/Buenos_Aires").tz_localize(None).normalize()
    dataframe = dataframe.loc[dataframe["Fecha"].notna() & (dataframe["Fecha"] <= today)]
    dataframe["Año"] = dataframe["Fecha"].dt.year.astype("int64")
    dataframe["Mes_num"] = dataframe["Fecha"].dt.month.astype("int64")
    dataframe["Mes"] = dataframe["Fecha"].dt.month_name().map(
        {
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
    )
    dataframe["Mes/Año"] = dataframe["Mes"] + "-" + dataframe["Año"].astype(str)

    dataframe = dataframe.loc[
        dataframe["Tipo de Cámara"].isin(["Semáforo", "Luces", "Buscador de patentes"])
    ]
    dataframe = dataframe.drop(columns=["Tipo de Revisión", "Nivel de Revisión"], errors="ignore")
    dataframe.drop_duplicates().to_parquet(
        TIEMPO_REVISION_CLEAN,
        index=False,
    )


fetch_tiempo_revision_task = PythonOperator(
    task_id="fetch_tiempo_revision",
    python_callable=fetch_tiempo_revision,
    dag=transito_dag,
)
transform_tiempo_revision_task = PythonOperator(
    task_id="transform_tiempo_revision",
    python_callable=transform_tiempo_revision,
    dag=transito_dag,
)
fetch_tiempo_revision_task >> transform_tiempo_revision_task


# ---- imagenes pendientes --------------------------------------------------

IMAGENES_PENDIENTES_RAW = "/tmp/data/transito/imagenes_pendientes.parquet"
IMAGENES_PENDIENTES_CLEAN = "/tmp/data/transito/imagenes_pendientes_clean.parquet"


def fetch_imagenes_pendientes(**kwargs):
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
    worksheet = client.open("imagenes_pendientes").sheet1
    values = worksheet.get_all_values(value_render_option="UNFORMATTED_VALUE")
    dataframe = pd.DataFrame(values[1:], columns=values[0]) if values else pd.DataFrame()
    if not dataframe.empty:
        dataframe = dataframe.astype("string")
    dataframe.to_parquet(IMAGENES_PENDIENTES_RAW, index=False)


def transform_imagenes_pendientes(**kwargs):
    import pandas as pd

    dataframe = pd.read_parquet(IMAGENES_PENDIENTES_RAW).rename(
        columns={"client": "Cliente", "status": "Estado", "count": "Cantidad"}
    )
    required_columns = {"Cliente", "Estado", "Cantidad", "tipo"}
    missing_columns = required_columns.difference(dataframe.columns)
    if missing_columns:
        raise ValueError(
            "imagenes_pendientes no contiene las columnas requeridas: "
            + ", ".join(sorted(missing_columns))
        )

    dataframe["Cliente"] = (
        dataframe["Cliente"].astype("string").fillna("").str.strip().str.capitalize()
    )
    dataframe["Estado"] = (
        dataframe["Estado"].astype("string").fillna("").str.strip().str.capitalize()
    )
    dataframe["tipo"] = (
        dataframe["tipo"]
        .astype("string")
        .fillna("")
        .str.strip()
        .str.replace(r"^\[\d+\]\s*", "", regex=True)
    )
    cantidad = dataframe["Cantidad"].astype("string").str.strip()
    cantidad_numerica = pd.to_numeric(cantidad, errors="coerce")
    cantidad_formateada = (
        cantidad.str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    dataframe["Cantidad"] = (
        cantidad_numerica.fillna(
            pd.to_numeric(cantidad_formateada, errors="coerce")
        )
        .fillna(0)
        .round()
        .astype("int64")
    )
    dataframe.drop_duplicates().to_parquet(IMAGENES_PENDIENTES_CLEAN, index=False)


fetch_imagenes_pendientes_task = PythonOperator(
    task_id="fetch_imagenes_pendientes",
    python_callable=fetch_imagenes_pendientes,
    dag=transito_dag,
)
transform_imagenes_pendientes_task = PythonOperator(
    task_id="transform_imagenes_pendientes",
    python_callable=transform_imagenes_pendientes,
    dag=transito_dag,
)
fetch_imagenes_pendientes_task >> transform_imagenes_pendientes_task


# ---- joins ----------------------------------------------------------------

ACTIVITY_UF_CLEAN = "/tmp/data/transito/activity_uf_clean.parquet"
REVIEWER_HOURLY_CLEAN = "/tmp/data/transito/reviewer_hourly_clean.parquet"


def join_activity_uf(**kwargs):
    import pandas as pd

    activity = pd.read_parquet(ACTIVIDAD_REVISORES_CLEAN)
    uf_values = pd.read_parquet(VALORES_UF_CLEAN)[["Año", "Mes", "Valor UF"]]
    activity.merge(
        uf_values,
        on=["Año", "Mes"],
        how="left",
    ).to_parquet(
        ACTIVITY_UF_CLEAN,
        index=False,
    )


def join_activity_review_time(**kwargs):
    import pandas as pd

    activity = pd.read_parquet(ACTIVIDAD_REVISORES_CLEAN)
    review_time = pd.read_parquet(TIEMPO_REVISION_CLEAN)

    activity_daily = activity.groupby(
        ["Fecha", "Auditor", "Tipo de Cámara"],
        as_index=False,
    ).agg(Presunciones=("Total", "sum"))
    time_daily = review_time.groupby(
        ["Fecha", "Auditor", "Tipo de Cámara"],
        as_index=False,
    ).agg(Horas=("Total (en horas)", "sum"))

    dataframe = activity_daily.merge(
        time_daily,
        on=["Fecha", "Auditor", "Tipo de Cámara"],
        how="inner",
    )
    dataframe["Presunciones por hora"] = (
        dataframe["Presunciones"] / dataframe["Horas"].replace(0, pd.NA)
    ).fillna(0)
    dataframe["Año"] = dataframe["Fecha"].dt.year.astype("int64")
    dataframe["Mes_num"] = dataframe["Fecha"].dt.month.astype("int64")
    dataframe["Mes"] = dataframe["Fecha"].dt.month_name().map(
        {
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
    )
    dataframe.to_parquet(REVIEWER_HOURLY_CLEAN, index=False)


join_activity_uf_task = PythonOperator(
    task_id="join_activity_uf",
    python_callable=join_activity_uf,
    dag=transito_dag,
)
join_activity_review_time_task = PythonOperator(
    task_id="join_activity_review_time",
    python_callable=join_activity_review_time,
    dag=transito_dag,
)
transform_actividad_revisores_task >> join_activity_uf_task
transform_valores_uf_task >> join_activity_uf_task
transform_actividad_revisores_task >> join_activity_review_time_task
transform_tiempo_revision_task >> join_activity_review_time_task


# ---- final datasets -------------------------------------------------------


def build_activity_reviewers_detail(**kwargs):
    import pandas as pd

    pd.read_parquet(ACTIVITY_UF_CLEAN).to_parquet(
        "/data/transito/activity_reviewers_detail.parquet",
        index=False,
    )


def build_rejection_reasons_detail(**kwargs):
    import pandas as pd

    pd.read_parquet(MOTIVOS_RECHAZOS_CLEAN).to_parquet(
        "/data/transito/rejection_reasons_detail.parquet",
        index=False,
    )


def build_review_time_detail(**kwargs):
    import pandas as pd

    pd.read_parquet(TIEMPO_REVISION_CLEAN).to_parquet(
        "/data/transito/review_time_detail.parquet",
        index=False,
    )


def build_pending_images_detail(**kwargs):
    import pandas as pd

    pd.read_parquet(IMAGENES_PENDIENTES_CLEAN).to_parquet(
        "/data/transito/pending_images_detail.parquet",
        index=False,
    )


def build_reviewer_hourly_detail(**kwargs):
    import pandas as pd

    dataframe = pd.read_parquet(REVIEWER_HOURLY_CLEAN)
    daily_average = dataframe.groupby(
        ["Fecha", "Tipo de Cámara"],
        as_index=False,
    ).agg(**{"Promedio por hora de la fecha": ("Presunciones por hora", "mean")})
    monthly_average = dataframe.groupby(
        ["Mes", "Tipo de Cámara"],
        as_index=False,
    ).agg(**{"Promedio por hora del mes": ("Presunciones por hora", "mean")})
    dataframe.merge(
        daily_average,
        on=["Fecha", "Tipo de Cámara"],
        how="left",
    ).merge(
        monthly_average,
        on=["Mes", "Tipo de Cámara"],
        how="left",
    ).to_parquet(
        "/data/transito/reviewer_hourly_detail.parquet",
        index=False,
    )


build_activity_reviewers_detail_task = PythonOperator(
    task_id="build_activity_reviewers_detail",
    python_callable=build_activity_reviewers_detail,
    dag=transito_dag,
)
build_rejection_reasons_detail_task = PythonOperator(
    task_id="build_rejection_reasons_detail",
    python_callable=build_rejection_reasons_detail,
    dag=transito_dag,
)
build_review_time_detail_task = PythonOperator(
    task_id="build_review_time_detail",
    python_callable=build_review_time_detail,
    dag=transito_dag,
)
build_pending_images_detail_task = PythonOperator(
    task_id="build_pending_images_detail",
    python_callable=build_pending_images_detail,
    dag=transito_dag,
)
build_reviewer_hourly_detail_task = PythonOperator(
    task_id="build_reviewer_hourly_detail",
    python_callable=build_reviewer_hourly_detail,
    dag=transito_dag,
)

join_activity_uf_task >> build_activity_reviewers_detail_task
transform_motivos_rechazos_task >> build_rejection_reasons_detail_task
transform_tiempo_revision_task >> build_review_time_detail_task
transform_imagenes_pendientes_task >> build_pending_images_detail_task
join_activity_review_time_task >> build_reviewer_hourly_detail_task
