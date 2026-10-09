import datetime
import json
import os
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator


berisso_dag = DAG(
    dag_id="berisso_dag",
    start_date=datetime.datetime(2026, 8, 25),
    catchup=False,
    max_active_runs=1,
    schedule="* * * * *",
)


SOURCE_FILES = [
    "Actas Notificada bajo puerta info extra.csv",
    "Actas por Tipo de Infraccion.csv",
    "Actividad_revisores_nivel_5.csv",
    "Detalle de actas pagas.csv",
    "Fallos.csv",
    "Listado de actas notificadas email.csv",
    "Listado de actas notificadas.csv",
    "detalles_de_infracciones.csv",
]

PUBLISHED_FILES = [
    "camera_by_month_total.parquet",
    "camera_by_month_type.parquet",
    "camera_detail.parquet",
    "camera_map.parquet",
    "dashboard_generated_by_type.parquet",
    "dashboard_infractions_by_type.parquet",
    "dashboard_monthly_metrics.parquet",
    "dashboard_payments_daily.parquet",
    "fallos_daily_juzgado.parquet",
    "fallos_daily_tipo.parquet",
    "fallos_detail.parquet",
    "fallos_tipo_total.parquet",
    "infraction_counts.parquet",
    "lotes_daily_analytics.parquet",
    "lotes_monthly_analytics.parquet",
    "nivel5_daily_camera.parquet",
    "nivel5_daily_metrics.parquet",
    "nivel5_daily_result.parquet",
    "nivel5_daily_result_detail.parquet",
    "nivel5_daily_reviewer.parquet",
    "nivel5_detail.parquet",
    "notification_door_extra_detail.parquet",
    "notification_payments_detail.parquet",
    "notifications_by_date_type.parquet",
    "notifications_by_type_month.parquet",
    "notifications_detail.parquet",
    "payments_by_method_daily.parquet",
    "payments_daily.parquet",
    "payments_detail.parquet",
    "payments_monthly.parquet",
]


def _output_path(file_name):
    output_dir = Path(os.getenv("BERISSO_DATA_DIR", "/data/berisso"))
    output_dir.mkdir(parents=True, exist_ok=True)
    return str(output_dir / file_name)

def _state_key(file_name):
    prefix = os.getenv("BERISSO_S3_PREFIX", "processed/berisso").strip("/")
    return f"{prefix}/{file_name}"


def _load_state(file_name):
    bucket = os.getenv("BERISSO_S3_BUCKET")
    if bucket:
        import boto3
        from botocore.exceptions import ClientError

        try:
            response = boto3.client("s3").get_object(Bucket=bucket, Key=_state_key(file_name))
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
                return None
            raise
        return json.loads(response["Body"].read())

    path = Path(_output_path(file_name))
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _save_state(file_name, payload):
    bucket = os.getenv("BERISSO_S3_BUCKET")
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2)
    if bucket:
        import boto3

        boto3.client("s3").put_object(
            Bucket=bucket,
            Key=_state_key(file_name),
            Body=serialized.encode("utf-8"),
            ContentType="application/json",
        )
        return

    Path(_output_path(file_name)).write_text(serialized, encoding="utf-8")


def _fetch_csv(file_name, target_path, low_memory=False, **kwargs):
    from google_worker.drive_csv import read_csv

    Path(target_path).parent.mkdir(parents=True, exist_ok=True)
    manifest = kwargs["ti"].xcom_pull(task_ids="check_berisso_sources") or {}
    file_id = manifest.get(file_name, {}).get("id")
    dataframe = read_csv(
        file_name,
        low_memory=low_memory,
        file_id=file_id,
    )
    dataframe.to_parquet(target_path, index=False)


def check_berisso_sources(**kwargs):
    from google_worker.drive_csv import folder_manifest

    current = folder_manifest(SOURCE_FILES)
    previous = _load_state("_source_manifest.json")
    if current == previous:
        return False
    return current


def publish_berisso_dataset(**kwargs):
    source_manifest = kwargs["ti"].xcom_pull(task_ids="check_berisso_sources")
    now = datetime.datetime.now(datetime.UTC)
    version = now.strftime("%Y%m%dT%H%M%S%fZ")
    dataset_manifest = {
        "version": version,
        "updated_at": now.isoformat(),
        "source": source_manifest,
    }

    bucket = os.getenv("BERISSO_S3_BUCKET")
    if bucket:
        import boto3

        prefix = os.getenv("BERISSO_S3_PREFIX", "processed/berisso").strip("/")
        version_prefix = f"{prefix}/versions/{version}"
        client = boto3.client("s3")
        output_dir = Path(os.getenv("BERISSO_DATA_DIR", "/data/berisso"))
        for file_name in PUBLISHED_FILES:
            path = output_dir / file_name
            if not path.exists():
                raise FileNotFoundError(f"No se generó el output esperado: {path}")
            client.upload_file(str(path), bucket, f"{version_prefix}/{file_name}")
        dataset_manifest["prefix"] = version_prefix

    _save_state("_dataset_manifest.json", dataset_manifest)
    _save_state("_source_manifest.json", source_manifest)


check_berisso_sources_task = ShortCircuitOperator(
    task_id="check_berisso_sources",
    python_callable=check_berisso_sources,
    dag=berisso_dag,
)


# ---- actas pagadas ---------------------------------------------------------

ACTAS_PAGADAS_RAW = "/tmp/data/berisso/actas_pagadas.parquet"
PAYMENTS_CLEAN = "/tmp/data/berisso/payments_clean.parquet"


def fetch_actas_pagadas(**kwargs):
    _fetch_csv("Detalle de actas pagas.csv", ACTAS_PAGADAS_RAW, **kwargs)

def transform_payments(**kwargs):
    import pandas as pd

    dataframe = pd.read_parquet(ACTAS_PAGADAS_RAW)

    if "numero_acta" in dataframe.columns and "numero" not in dataframe.columns:
        dataframe = dataframe.rename(columns={"numero_acta": "numero"})
    if "Tasa infracción" in dataframe.columns and "Tasa infraccion" not in dataframe.columns:
        dataframe = dataframe.rename(columns={"Tasa infracción": "Tasa infraccion"})

    for column in ["Monto Total", "Tasa administrativa", "Tasa infraccion"]:
        if column not in dataframe.columns:
            dataframe[column] = 0
        values = dataframe[column].astype("string").str.strip()
        values = values.str.replace("$", "", regex=False).str.replace(" ", "", regex=False)
        comma_decimal = values.str.contains(",", na=False)
        values = values.where(
            ~comma_decimal,
            values.str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
        )
        thousands_dot = ~comma_decimal & values.str.match(r"^-?\d{1,3}(?:\.\d{3})+$", na=False)
        values = values.where(~thousands_dot, values.str.replace(".", "", regex=False))
        dataframe[column] = pd.to_numeric(values, errors="coerce").fillna(0)

    for column in ["lote_id", "comprobante_id"]:
        if column in dataframe.columns:
            values = dataframe[column].astype("string").str.strip()
            dataframe[column] = pd.to_numeric(values, errors="coerce").astype("Int64")

    for column in ["fecha_pago", "created_at", "fecha_acreditacion"]:
        values = dataframe[column].astype("string").str.strip()
        numeric_values = pd.to_numeric(values, errors="coerce")
        excel_serial = numeric_values.between(
            20000,
            80000,
            inclusive="both",
        ).fillna(False)

        parsed_text = pd.to_datetime(
            values.where(~excel_serial),
            format="mixed",
            dayfirst=False,
            errors="coerce",
        )
        parsed_serial = pd.to_datetime(
            numeric_values.where(excel_serial),
            unit="D",
            origin="1899-12-30",
            errors="coerce",
        )
        parsed_text = parsed_text.astype("datetime64[us]")
        parsed_serial = parsed_serial.astype("datetime64[us]")
        dataframe[column] = parsed_text.fillna(parsed_serial)

    dataframe["numero"] = dataframe["numero"].astype("string").fillna("").str.strip()
    today = pd.Timestamp.now(
        tz="America/Argentina/Buenos_Aires"
    ).tz_localize(None).normalize()
    dataframe = dataframe.loc[
        dataframe["numero"].ne("")
        & dataframe["fecha_acreditacion"].notna()
        & (dataframe["fecha_acreditacion"] <= today)
    ]

    dataframe["Días transcurridos"] = (dataframe["fecha_pago"] - dataframe["created_at"]).dt.days
    dataframe["Fecha"] = dataframe["fecha_acreditacion"].dt.normalize()
    dataframe["Año"] = dataframe["fecha_acreditacion"].dt.year.astype("int64")
    dataframe["Mes_num"] = dataframe["fecha_acreditacion"].dt.month.astype("int64")
    dataframe["Mes"] = dataframe["fecha_acreditacion"].dt.month_name().map(
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

    if "lote_id" in dataframe.columns:
        dataframe = dataframe.sort_values("lote_id", na_position="last")
    dataframe = dataframe.drop_duplicates(subset="numero", keep="first")
    dataframe = dataframe.loc[dataframe["Año"] >= 2024].sort_values(["Año", "Mes_num", "Fecha"])
    dataframe.to_parquet(PAYMENTS_CLEAN, index=False)

fetch_actas_pagadas_task = PythonOperator(
    task_id="fetch_actas_pagadas",
    python_callable=fetch_actas_pagadas,
    dag=berisso_dag,
)
transform_payments_task = PythonOperator(
    task_id="transform_payments",
    python_callable=transform_payments,
    dag=berisso_dag,
)
fetch_actas_pagadas_task >> transform_payments_task


# ---- detalles infracciones -------------------------------------------------

DETALLES_INFRACCIONES_RAW = "/tmp/data/berisso/detalles_infracciones.parquet"
DETALLES_INFRACCIONES_CLEAN = "/tmp/data/berisso/detalles_infracciones_clean.parquet"


def fetch_detalles_infracciones(**kwargs):
    _fetch_csv("detalles_de_infracciones.csv", DETALLES_INFRACCIONES_RAW, **kwargs)

def transform_detalles_infracciones(**kwargs):
    import pandas as pd

    dataframe = pd.read_parquet(DETALLES_INFRACCIONES_RAW)

    required_columns = ["Año", "Mes", "Tipo de Faltas"]
    for column in required_columns:
        if column not in dataframe.columns:
            dataframe[column] = pd.NA

    dataframe["Año"] = dataframe["Año"].replace("", pd.NA).ffill()
    dataframe["Año"] = pd.to_numeric(dataframe["Año"], errors="coerce").astype("Int64")
    dataframe["Mes"] = dataframe["Mes"].astype("string").fillna("").str.strip().str.title()
    dataframe["Tipo de Faltas"] = (
        dataframe["Tipo de Faltas"].astype("string").fillna("").str.strip()
    )

    numeric_columns = [
        "Con Pago Voluntario",
        "Sin Pago Voluntario",
        "Valor de la UF en 0",
        "Total con pago voluntario",
        "Total sin pago voluntario",
        "Gastos administrativos",
    ]
    for column in numeric_columns:
        if column not in dataframe.columns:
            dataframe[column] = 0
        values = dataframe[column].astype("string").str.strip()
        values = values.str.replace("$", "", regex=False).str.replace(" ", "", regex=False)
        comma_decimal = values.str.contains(",", na=False)
        values = values.where(
            ~comma_decimal,
            values.str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
        )
        thousands_dot = ~comma_decimal & values.str.match(r"^-?\d{1,3}(?:\.\d{3})+$", na=False)
        values = values.where(~thousands_dot, values.str.replace(".", "", regex=False))
        dataframe[column] = pd.to_numeric(values, errors="coerce").fillna(0)

    dataframe = dataframe.loc[
        dataframe["Año"].notna()
        & dataframe["Mes"].ne("")
        & dataframe["Tipo de Faltas"].ne("")
    ]

    key_columns = ["Año", "Mes", "Tipo de Faltas"]
    duplicated = dataframe.loc[dataframe.duplicated(key_columns, keep=False), key_columns]
    if not duplicated.empty:
        rows = duplicated.drop_duplicates().to_dict("records")
        raise ValueError(f"Hay tarifas duplicadas en detalles_de_infracciones.csv: {rows}")

    dataframe.to_parquet(DETALLES_INFRACCIONES_CLEAN, index=False)


fetch_detalles_infracciones_task = PythonOperator(
    task_id="fetch_detalles_infracciones",
    python_callable=fetch_detalles_infracciones,
    dag=berisso_dag,
)
transform_detalles_infracciones_task = PythonOperator(
    task_id="transform_detalles_infracciones",
    python_callable=transform_detalles_infracciones,
    dag=berisso_dag,
)
fetch_detalles_infracciones_task >> transform_detalles_infracciones_task


# ---- actas cantidad --------------------------------------------------------

ACTAS_CANTIDAD_RAW = "/tmp/data/berisso/actas_cantidad.parquet"
ACTAS_CANTIDAD_CLEAN = "/tmp/data/berisso/actas_cantidad_clean.parquet"
INFRACTION_DETAIL_CLEAN = "/tmp/data/berisso/infraction_detail_clean.parquet"


def fetch_actas_cantidad(**kwargs):
    _fetch_csv("Actas por Tipo de Infraccion.csv", ACTAS_CANTIDAD_RAW, **kwargs)

def transform_actas_cantidad(**kwargs):
    import pandas as pd

    dataframe = pd.read_parquet(ACTAS_CANTIDAD_RAW)

    if "ubicacion" not in dataframe.columns:
        dataframe["ubicacion"] = ""

    date_column = "date" if "date" in dataframe.columns else "Fecha"
    if date_column not in dataframe.columns:
        dataframe[date_column] = pd.NA

    dates = pd.to_datetime(
        dataframe[date_column].astype("string").str.strip(),
        format="%Y-%m",
        errors="coerce",
    )
    current_month = pd.Timestamp.now(
        tz="America/Argentina/Buenos_Aires"
    ).tz_localize(None).to_period("M").to_timestamp()
    dataframe = dataframe.loc[dates.notna() & (dates <= current_month)]
    dates = dates.loc[dataframe.index]
    dataframe["Año"] = dates.dt.year.astype("int64")
    dataframe["Mes_num"] = dates.dt.month.astype("int64")
    dataframe["Mes"] = dataframe["Mes_num"].map(
        {
            1: "Enero",
            2: "Febrero",
            3: "Marzo",
            4: "Abril",
            5: "Mayo",
            6: "Junio",
            7: "Julio",
            8: "Agosto",
            9: "Septiembre",
            10: "Octubre",
            11: "Noviembre",
            12: "Diciembre",
        }
    )
    dataframe = dataframe.loc[dataframe["Año"] >= 2025]

    for coordinate in ["latitud", "longitud"]:
        if coordinate not in dataframe.columns:
            dataframe[coordinate] = pd.NA
        dataframe[coordinate] = pd.to_numeric(dataframe[coordinate], errors="coerce")

    if {"Tipo de Faltas", "Cantidad cometidas"}.issubset(dataframe.columns):
        result = pd.DataFrame(
            {
                "ubicacion": dataframe["ubicacion"],
                "Año": dataframe["Año"],
                "Mes": dataframe["Mes"],
                "Mes_num": dataframe["Mes_num"],
                "latitud": dataframe["latitud"],
                "longitud": dataframe["longitud"],
                "Tipo de Faltas": dataframe["Tipo de Faltas"],
                "Cantidad cometidas": dataframe["Cantidad cometidas"],
            }
        )
        result["Tipo de Faltas"] = (
            result["Tipo de Faltas"].astype("string").fillna("").str.strip()
        )
        values = result["Cantidad cometidas"].astype("string").str.strip()
        values = values.str.replace(".", "", regex=False).str.replace(",", "", regex=False)
        result["Cantidad cometidas"] = pd.to_numeric(values, errors="coerce").fillna(0)
        result = result.loc[result["Tipo de Faltas"].ne("")]
    else:
        source_columns = [
            ("CruzarRojo", "Semáforo en rojo"),
            ("Sin Casco", "Manejo sin casco"),
            ("Senda Peatonal", "Invasion de senda"),
            ("Giro Rojo", "Giro a la izquierda"),
            ("Giro Indebido", "Giro a la izquierda"),
            ("Semáforo en rojo", "Semáforo en rojo"),
            ("Manejo sin casco", "Manejo sin casco"),
            ("Invasion de senda", "Invasion de senda"),
            ("Giro a la izquierda", "Giro a la izquierda"),
        ]
        frames = []
        base_columns = ["ubicacion", "Año", "Mes", "Mes_num", "latitud", "longitud"]
        for source_column, infraction_type in source_columns:
            if source_column not in dataframe.columns:
                continue
            frame = pd.DataFrame({column: dataframe[column] for column in base_columns})
            frame["Cantidad cometidas"] = dataframe[source_column]
            values = frame["Cantidad cometidas"].astype("string").str.strip()
            values = values.str.replace(".", "", regex=False).str.replace(",", "", regex=False)
            frame["Cantidad cometidas"] = pd.to_numeric(values, errors="coerce").fillna(0)
            frame["Tipo de Faltas"] = infraction_type
            frames.append(frame)

        if frames:
            result = pd.concat(frames, ignore_index=True)
        else:
            result = pd.DataFrame(
                columns=[
                    "ubicacion",
                    "Año",
                    "Mes",
                    "Mes_num",
                    "latitud",
                    "longitud",
                    "Cantidad cometidas",
                    "Tipo de Faltas",
                ]
            )

    result = (
        result.groupby(
            [
                "ubicacion",
                "Año",
                "Mes",
                "Mes_num",
                "latitud",
                "longitud",
                "Tipo de Faltas",
            ],
            dropna=False,
            as_index=False,
        )
        .agg(**{"Cantidad cometidas": ("Cantidad cometidas", "sum")})
    )
    result.to_parquet(ACTAS_CANTIDAD_CLEAN, index=False)


def join_infraction_data(**kwargs):
    import pandas as pd

    counts = pd.read_parquet(ACTAS_CANTIDAD_CLEAN)
    values = pd.read_parquet(DETALLES_INFRACCIONES_CLEAN)

    counts["Año"] = pd.to_numeric(counts["Año"], errors="coerce").astype("Int64")
    values["Año"] = pd.to_numeric(values["Año"], errors="coerce").astype("Int64")
    counts["Mes"] = counts["Mes"].astype("string").fillna("").str.strip().str.title()
    values["Mes"] = values["Mes"].astype("string").fillna("").str.strip().str.title()
    counts["Tipo de Faltas"] = (
        counts["Tipo de Faltas"].astype("string").fillna("").str.strip()
    )
    values["Tipo de Faltas"] = (
        values["Tipo de Faltas"].astype("string").fillna("").str.strip()
    )

    key_columns = ["Año", "Mes", "Tipo de Faltas"]
    count_keys = counts[key_columns].drop_duplicates()
    value_keys = values[key_columns].drop_duplicates()
    missing_values = count_keys.merge(value_keys, on=key_columns, how="left", indicator=True)
    missing_values = missing_values.loc[missing_values["_merge"] == "left_only", key_columns]
    if not missing_values.empty:
        rows = missing_values.to_dict("records")
        raise ValueError(f"Faltan tarifas para infracciones existentes: {rows}")

    dataframe = counts.merge(
        values,
        on=["Año", "Mes", "Tipo de Faltas"],
        how="inner",
    )
    dataframe["Cantidad cometidas"] = pd.to_numeric(
        dataframe["Cantidad cometidas"], errors="coerce"
    ).fillna(0)
    dataframe["Generado"] = dataframe["Cantidad cometidas"] * (
        pd.to_numeric(dataframe["Valor de la UF en 0"], errors="coerce").fillna(0)
        * pd.to_numeric(dataframe["Sin Pago Voluntario"], errors="coerce").fillna(0)
        + pd.to_numeric(dataframe["Gastos administrativos"], errors="coerce").fillna(0)
    )

    dataframe = (
        dataframe.groupby(
            [
                "ubicacion",
                "Año",
                "Mes",
                "Mes_num",
                "latitud",
                "longitud",
                "Tipo de Faltas",
            ],
            dropna=False,
            as_index=False,
        )
        .agg(
            **{
                "Cantidad cometidas": ("Cantidad cometidas", "sum"),
                "Generado": ("Generado", "sum"),
            }
        )
    )
    dataframe.to_parquet(INFRACTION_DETAIL_CLEAN, index=False)


fetch_actas_cantidad_task = PythonOperator(
    task_id="fetch_actas_cantidad",
    python_callable=fetch_actas_cantidad,
    dag=berisso_dag,
)
transform_actas_cantidad_task = PythonOperator(
    task_id="transform_actas_cantidad",
    python_callable=transform_actas_cantidad,
    dag=berisso_dag,
)
join_infraction_data_task = PythonOperator(
    task_id="join_infraction_data",
    python_callable=join_infraction_data,
    dag=berisso_dag,
)

fetch_actas_cantidad_task >> transform_actas_cantidad_task
transform_actas_cantidad_task >> join_infraction_data_task
transform_detalles_infracciones_task >> join_infraction_data_task


# ---- actas notificadas -----------------------------------------------------

ACTAS_NOTIFICADAS_RAW = "/tmp/data/berisso/actas_notificadas.parquet"
NOTIFICATIONS_DOOR_CLEAN = "/tmp/data/berisso/notifications_door_clean.parquet"


def fetch_actas_notificadas(**kwargs):
    _fetch_csv("Listado de actas notificadas.csv", ACTAS_NOTIFICADAS_RAW, low_memory=False, **kwargs)

def transform_notifications_door(**kwargs):
    import pandas as pd

    dataframe = pd.read_parquet(ACTAS_NOTIFICADAS_RAW)
    dataframe = dataframe.drop(columns=["numero", "piso", "depto", "codigo_postal", "Año/Mes"], errors="ignore")
    dataframe["notific_type"] = "Bajo Puerta"
    if "numero_acta" in dataframe.columns and "acta" not in dataframe.columns:
        dataframe = dataframe.rename(columns={"numero_acta": "acta"})
    elif "acta" not in dataframe.columns and "acta_id" in dataframe.columns:
        dataframe["acta"] = dataframe["acta_id"]

    dataframe["acta"] = dataframe["acta"].astype("string").fillna("").str.strip()
    if "acta_id" in dataframe.columns:
        dataframe["acta_id"] = dataframe["acta_id"].astype("string").fillna("").str.strip()
    if "lote_id" in dataframe.columns:
        values = dataframe["lote_id"].astype("string").str.strip()
        values = values.str.replace(".", "", regex=False).str.replace(",", "", regex=False)
        dataframe["lote_id"] = pd.to_numeric(values, errors="coerce").astype("Int64")

    dataframe["Fecha Lote"] = pd.to_datetime(
        dataframe["Fecha Lote"].astype("string").str.strip(),
        format="mixed",
        dayfirst=False,
        errors="coerce",
    ).dt.normalize()
    today = pd.Timestamp.now(
        tz="America/Argentina/Buenos_Aires"
    ).tz_localize(None).normalize()
    dataframe = dataframe.loc[
        dataframe["acta"].ne("")
        & dataframe["Fecha Lote"].notna()
        & (dataframe["Fecha Lote"] <= today)
    ]
    dataframe["Año"] = dataframe["Fecha Lote"].dt.year.astype("int64")
    dataframe["Mes_num"] = dataframe["Fecha Lote"].dt.month.astype("int64")
    dataframe["Mes"] = dataframe["Fecha Lote"].dt.month_name().map(
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
    dataframe = dataframe.loc[dataframe["Año"] >= 2024].drop_duplicates()
    dataframe.to_parquet(NOTIFICATIONS_DOOR_CLEAN, index=False)

fetch_actas_notificadas_task = PythonOperator(
    task_id="fetch_actas_notificadas",
    python_callable=fetch_actas_notificadas,
    dag=berisso_dag,
)
transform_notifications_door_task = PythonOperator(
    task_id="transform_notifications_door",
    python_callable=transform_notifications_door,
    dag=berisso_dag,
)
fetch_actas_notificadas_task >> transform_notifications_door_task


# ---- info extra bajo puerta ------------------------------------------------

ACTAS_NOTIFICADAS_EXTRA_RAW = "/tmp/data/berisso/actas_notificadas_extra.parquet"
NOTIFICATIONS_DOOR_EXTRA_CLEAN = "/tmp/data/berisso/notifications_door_extra_clean.parquet"


def fetch_actas_notificadas_extra(**kwargs):
    _fetch_csv(
        "Actas Notificada bajo puerta info extra.csv",
        ACTAS_NOTIFICADAS_EXTRA_RAW,
        **kwargs,
    )


def transform_notifications_door_extra(**kwargs):
    import pandas as pd

    dataframe = pd.read_parquet(ACTAS_NOTIFICADAS_EXTRA_RAW)
    dataframe = dataframe.rename(
        columns={
            "fe_vencimiento_pago_total": "Fecha de vencimiento de pago total",
            "actas_notif": "Actas Notificadas",
        }
    )

    dataframe["lote_id"] = pd.to_numeric(dataframe["lote_id"], errors="coerce").astype("Int64")
    dataframe["Fecha de vencimiento de pago total"] = pd.to_datetime(
        dataframe["Fecha de vencimiento de pago total"],
        format="mixed",
        errors="coerce",
    ).dt.normalize()
    dataframe["fecha_impresion"] = pd.to_datetime(
        dataframe["fecha_impresion"],
        format="mixed",
        errors="coerce",
    ).dt.normalize()
    dataframe["Año"] = dataframe["Fecha de vencimiento de pago total"].dt.year.astype("Int64")
    dataframe["Mes_num"] = dataframe["Fecha de vencimiento de pago total"].dt.month.astype("Int64")
    dataframe["Mes"] = dataframe["Mes_num"].map(
        {
            1: "Enero",
            2: "Febrero",
            3: "Marzo",
            4: "Abril",
            5: "Mayo",
            6: "Junio",
            7: "Julio",
            8: "Agosto",
            9: "Septiembre",
            10: "Octubre",
            11: "Noviembre",
            12: "Diciembre",
        }
    )
    dataframe.to_parquet(NOTIFICATIONS_DOOR_EXTRA_CLEAN, index=False)


def build_notifications_door_extra_detail(**kwargs):
    import pandas as pd

    pd.read_parquet(NOTIFICATIONS_DOOR_EXTRA_CLEAN).to_parquet(
        _output_path("notification_door_extra_detail.parquet"),
        index=False,
    )


fetch_actas_notificadas_extra_task = PythonOperator(
    task_id="fetch_actas_notificadas_extra",
    python_callable=fetch_actas_notificadas_extra,
    dag=berisso_dag,
)
transform_notifications_door_extra_task = PythonOperator(
    task_id="transform_notifications_door_extra",
    python_callable=transform_notifications_door_extra,
    dag=berisso_dag,
)
build_notifications_door_extra_detail_task = PythonOperator(
    task_id="build_notifications_door_extra_detail",
    python_callable=build_notifications_door_extra_detail,
    dag=berisso_dag,
)

fetch_actas_notificadas_extra_task >> transform_notifications_door_extra_task
transform_notifications_door_extra_task >> build_notifications_door_extra_detail_task


# ---- actas notificadas email ---------------------------------------------

ACTAS_NOTIFICADAS_EMAIL_RAW = "/tmp/data/berisso/actas_notificadas_email.parquet"
NOTIFICATIONS_EMAIL_CLEAN = "/tmp/data/berisso/notifications_email_clean.parquet"


def fetch_actas_notificadas_email(**kwargs):
    _fetch_csv("Listado de actas notificadas email.csv", ACTAS_NOTIFICADAS_EMAIL_RAW, **kwargs)

def transform_notifications_email(**kwargs):
    import pandas as pd

    dataframe = pd.read_parquet(ACTAS_NOTIFICADAS_EMAIL_RAW)
    dataframe = dataframe.drop(columns=["numero", "piso", "depto", "codigo_postal", "Año/Mes"], errors="ignore")
    dataframe["notific_type"] = "Email"
    if "numero_acta" in dataframe.columns and "acta" not in dataframe.columns:
        dataframe = dataframe.rename(columns={"numero_acta": "acta"})

    dataframe["acta"] = dataframe["acta"].astype("string").fillna("").str.strip()
    if "acta_id" in dataframe.columns:
        dataframe["acta_id"] = dataframe["acta_id"].astype("string").fillna("").str.strip()
    if "lote_id" in dataframe.columns:
        values = dataframe["lote_id"].astype("string").str.strip()
        values = values.str.replace(".", "", regex=False).str.replace(",", "", regex=False)
        dataframe["lote_id"] = pd.to_numeric(values, errors="coerce").astype("Int64")

    dataframe["Fecha Lote"] = pd.to_datetime(
        dataframe["Fecha Lote"].astype("string").str.strip(),
        format="mixed",
        dayfirst=False,
        errors="coerce",
    ).dt.normalize()
    today = pd.Timestamp.now(
        tz="America/Argentina/Buenos_Aires"
    ).tz_localize(None).normalize()
    dataframe = dataframe.loc[
        dataframe["acta"].ne("")
        & dataframe["Fecha Lote"].notna()
        & (dataframe["Fecha Lote"] <= today)
    ]
    dataframe["Año"] = dataframe["Fecha Lote"].dt.year.astype("int64")
    dataframe["Mes_num"] = dataframe["Fecha Lote"].dt.month.astype("int64")
    dataframe["Mes"] = dataframe["Fecha Lote"].dt.month_name().map(
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
    dataframe = dataframe.loc[dataframe["Año"] >= 2024].drop_duplicates()
    dataframe.to_parquet(NOTIFICATIONS_EMAIL_CLEAN, index=False)

fetch_actas_notificadas_email_task = PythonOperator(
    task_id="fetch_actas_notificadas_email",
    python_callable=fetch_actas_notificadas_email,
    dag=berisso_dag,
)
transform_notifications_email_task = PythonOperator(
    task_id="transform_notifications_email",
    python_callable=transform_notifications_email,
    dag=berisso_dag,
)
fetch_actas_notificadas_email_task >> transform_notifications_email_task


# ---- actividad revisores --------------------------------------------------

ACTIVIDAD_REVISORES_RAW = "/tmp/data/berisso/actividad_revisores.parquet"
NIVEL5_FINAL = "/tmp/data/berisso/nivel5_final.parquet"


def fetch_actividad_revisores(**kwargs):
    _fetch_csv("Actividad_revisores_nivel_5.csv", ACTIVIDAD_REVISORES_RAW, **kwargs)

def transform_nivel5(**kwargs):
    import pandas as pd

    dataframe = pd.read_parquet(ACTIVIDAD_REVISORES_RAW).rename(columns={"Cliente": "Municipio"})
    dataframe["Municipio"] = dataframe["Municipio"].astype("string").fillna("").str.strip().str.capitalize()
    dataframe["Tipo de Revisión"] = dataframe["Tipo de Revisión"].astype("string").fillna("").str.strip()
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
    dataframe["Código de cámara"] = dataframe["Código de cámara"].astype("string").fillna("").str.strip().replace(
        {
            "4-C64 - Av 122 y calle 64": "4C64 - Av 122 y calle 64",
            "3-C59 - Av 122 y calle 59": "3C59 - Av 122 y calle 59",
            "5-C126 - Av 60 y calle 126": "5C126 - Av 60 y calle 126",
        }
    )

    dataframe["Fecha"] = pd.to_datetime(
        dataframe["Fecha"].astype("string").str.strip(),
        format="mixed",
        dayfirst=False,
        errors="coerce",
    ).dt.normalize()
    today = pd.Timestamp.now(
        tz="America/Argentina/Buenos_Aires"
    ).tz_localize(None).normalize()
    dataframe = dataframe.loc[
        dataframe["Fecha"].notna() & (dataframe["Fecha"] <= today)
    ]
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

    for column in ["Aceptadas", "Rechazadas", "Total", "Nivel de Revisión"]:
        values = dataframe[column].astype("string").str.strip()
        values = values.str.replace(".", "", regex=False).str.replace(",", "", regex=False)
        dataframe[column] = pd.to_numeric(values, errors="coerce")

    dataframe["Aceptadas"] = dataframe["Aceptadas"].fillna(0)
    dataframe["Rechazadas"] = dataframe["Rechazadas"].fillna(0)
    dataframe["Total"] = dataframe["Total"].fillna(dataframe["Aceptadas"] + dataframe["Rechazadas"])
    dataframe["Semana"] = dataframe["Fecha"].map(
        lambda fecha: 1 if fecha.day <= 7 else 2 if fecha.day <= 15 else 3 if fecha.day <= 23 else 4
    )

    dataframe = dataframe.loc[
        (dataframe["Municipio"] == "Berisso")
        & (dataframe["Nivel de Revisión"] == 5)
        & dataframe["Tipo de Cámara"].isin(["Semáforo", "Luces", "Buscador de patentes"])
        & ~dataframe["Auditor"].isin(["Daniela Zarza", "Lucia Ferrero", "Romina Higa"])
    ]
    dataframe["Código limpio"] = dataframe["Código de cámara"].map(
        lambda value: value.split("-")[0].strip() if value else None
    )
    dataframe["Código de cámara"] = dataframe["Código de cámara"].map(
        lambda value: value.split("-")[-1].strip() if value else None
    )
    dataframe.drop_duplicates().sort_values("Fecha").to_parquet(NIVEL5_FINAL, index=False)

fetch_actividad_revisores_task = PythonOperator(
    task_id="fetch_actividad_revisores",
    python_callable=fetch_actividad_revisores,
    dag=berisso_dag,
)
transform_nivel5_task = PythonOperator(
    task_id="transform_nivel5",
    python_callable=transform_nivel5,
    dag=berisso_dag,
)
fetch_actividad_revisores_task >> transform_nivel5_task


# ---- fallos ---------------------------------------------------------------

FALLOS_RAW = "/tmp/data/berisso/fallos.parquet"
FALLOS_FINAL = "/tmp/data/berisso/fallos_final.parquet"


def fetch_fallos(**kwargs):
    _fetch_csv("Fallos.csv", FALLOS_RAW, **kwargs)

def transform_fallos(**kwargs):
    import pandas as pd

    dataframe = pd.read_parquet(FALLOS_RAW)

    for column in ["valor_inicial", "valor_actual"]:
        values = dataframe[column].astype("string").str.strip()
        values = values.str.replace("$", "", regex=False).str.replace(" ", "", regex=False)
        comma_decimal = values.str.contains(",", na=False)
        values = values.where(
            ~comma_decimal,
            values.str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
        )
        thousands_dot = ~comma_decimal & values.str.match(r"^-?\d{1,3}(?:\.\d{3})+$", na=False)
        values = values.where(~thousands_dot, values.str.replace(".", "", regex=False))
        dataframe[column] = pd.to_numeric(values, errors="coerce")

    if "juzgado_id" in dataframe.columns:
        values = dataframe["juzgado_id"].astype("string").str.strip()
        values = values.str.replace(".", "", regex=False).str.replace(",", "", regex=False)
        dataframe["juzgado_id"] = pd.to_numeric(values, errors="coerce").astype("Int64")

    dataframe["fecha_fallo"] = pd.to_datetime(
        dataframe["fecha_fallo"].astype("string").str.strip(),
        format="mixed",
        dayfirst=False,
        errors="coerce",
    )
    dataframe["acta"] = dataframe["acta"].astype("string").fillna("").str.strip()
    today = pd.Timestamp.now(
        tz="America/Argentina/Buenos_Aires"
    ).tz_localize(None).normalize()
    dataframe = dataframe.loc[
        dataframe["fecha_fallo"].notna()
        & dataframe["acta"].ne("")
        & (dataframe["fecha_fallo"] <= today)
    ]
    dataframe["Fecha"] = dataframe["fecha_fallo"].dt.normalize()
    dataframe["Año"] = dataframe["fecha_fallo"].dt.year.astype("int64")
    dataframe["Mes_num"] = dataframe["fecha_fallo"].dt.month.astype("int64")
    dataframe["Mes"] = dataframe["fecha_fallo"].dt.month_name().map(
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
    dataframe["Semana"] = dataframe["fecha_fallo"].map(
        lambda fecha: 1 if fecha.day <= 7 else 2 if fecha.day <= 15 else 3 if fecha.day <= 23 else 4
    )
    dataframe = dataframe.drop_duplicates(subset=["acta"], keep="first")
    dataframe["Modificado"] = dataframe["valor_inicial"] - dataframe["valor_actual"]
    dataframe["Reducido"] = dataframe["valor_inicial"] - dataframe["valor_actual"]
    dataframe["fallo"] = dataframe["fallo"].astype("string").fillna("").str.strip().replace(
        {
            "1 - RESOLUCION CONDENATORIA COMUN PRESENCIAL": "Condenatoria Comun Presencial",
            "3 - RESOLUCION CONDENATORIA COMUN PRESENCIAL ATENUANTE": "Comun Presencial Atenuante",
        }
    )
    dataframe.to_parquet(FALLOS_FINAL, index=False)

fetch_fallos_task = PythonOperator(
    task_id="fetch_fallos",
    python_callable=fetch_fallos,
    dag=berisso_dag,
)
transform_fallos_task = PythonOperator(
    task_id="transform_fallos",
    python_callable=transform_fallos,
    dag=berisso_dag,
)
fetch_fallos_task >> transform_fallos_task


# ---- joins ----------------------------------------------------------------

NOTIFICATIONS_JOINED = "/tmp/data/berisso/notifications_joined.parquet"
NOTIFICATION_PAYMENTS = "/tmp/data/berisso/notification_payments.parquet"


def join_notifications(**kwargs):
    import pandas as pd

    email = pd.read_parquet(NOTIFICATIONS_EMAIL_CLEAN)
    door = pd.read_parquet(NOTIFICATIONS_DOOR_CLEAN)
    pd.concat([email, door], ignore_index=True).drop_duplicates().to_parquet(NOTIFICATIONS_JOINED, index=False)


def join_notifications_payments(**kwargs):
    import numpy as np
    import pandas as pd

    notifications = pd.read_parquet(NOTIFICATIONS_JOINED)
    payments = pd.read_parquet(PAYMENTS_CLEAN).rename(columns={"numero": "acta"})
    payments["acta"] = payments["acta"].astype("string").fillna("").str.strip()

    for dataframe in [notifications, payments]:
        dataframe["Año"] = pd.to_numeric(dataframe["Año"], errors="coerce").astype("Int64")
        dataframe["Mes_num"] = pd.to_numeric(dataframe["Mes_num"], errors="coerce").astype("Int64")
        dataframe["Mes"] = dataframe["Mes"].astype("string").fillna("").str.strip()

    dataframe = notifications.merge(
        payments,
        on=["acta", "Año", "Mes"],
        how="left",
        suffixes=("", "_pago"),
    ).drop_duplicates()
    dataframe["Estado"] = np.where(dataframe["fecha_acreditacion"].isna(), "No pagada", "Pagada")
    dataframe["Tiempo hasta el pago"] = (dataframe["fecha_acreditacion"] - dataframe["Fecha Lote"]).dt.days
    dataframe.to_parquet(NOTIFICATION_PAYMENTS, index=False)

join_notifications_task = PythonOperator(
    task_id="join_notifications",
    python_callable=join_notifications,
    dag=berisso_dag,
)
join_notifications_payments_task = PythonOperator(
    task_id="join_notifications_payments",
    python_callable=join_notifications_payments,
    dag=berisso_dag,
)
transform_notifications_door_task >> join_notifications_task
transform_notifications_email_task >> join_notifications_task
join_notifications_task >> join_notifications_payments_task
transform_payments_task >> join_notifications_payments_task


# ---- detail outputs -------------------------------------------------------


def build_payments_detail(**kwargs):
    import pandas as pd

    pd.read_parquet(PAYMENTS_CLEAN).to_parquet(_output_path("payments_detail.parquet"), index=False)


def build_fallos_detail(**kwargs):
    import pandas as pd

    pd.read_parquet(FALLOS_FINAL).to_parquet(_output_path("fallos_detail.parquet"), index=False)


def build_nivel5_detail(**kwargs):
    import pandas as pd

    pd.read_parquet(NIVEL5_FINAL).to_parquet(_output_path("nivel5_detail.parquet"), index=False)


def build_notifications_detail(**kwargs):
    import pandas as pd

    pd.read_parquet(NOTIFICATIONS_JOINED).to_parquet(_output_path("notifications_detail.parquet"), index=False)


def build_notification_payments_detail(**kwargs):
    import pandas as pd

    pd.read_parquet(NOTIFICATION_PAYMENTS).to_parquet(
        _output_path("notification_payments_detail.parquet"),
        index=False,
    )


def build_infraction_counts(**kwargs):
    import pandas as pd

    counts = pd.read_parquet(ACTAS_CANTIDAD_CLEAN)
    counts.groupby(
        ["Año", "Mes", "Mes_num", "Tipo de Faltas"],
        as_index=False,
    ).agg(
        **{"Cantidad cometidas": ("Cantidad cometidas", "sum")}
    ).to_parquet(
        _output_path("infraction_counts.parquet"),
        index=False,
    )


def build_camera_detail(**kwargs):
    import pandas as pd

    pd.read_parquet(INFRACTION_DETAIL_CLEAN).to_parquet(
        _output_path("camera_detail.parquet"),
        index=False,
    )


build_payments_detail_task = PythonOperator(
    task_id="build_payments_detail",
    python_callable=build_payments_detail,
    dag=berisso_dag,
)
build_fallos_detail_task = PythonOperator(
    task_id="build_fallos_detail",
    python_callable=build_fallos_detail,
    dag=berisso_dag,
)
build_nivel5_detail_task = PythonOperator(
    task_id="build_nivel5_detail",
    python_callable=build_nivel5_detail,
    dag=berisso_dag,
)
build_notifications_detail_task = PythonOperator(
    task_id="build_notifications_detail",
    python_callable=build_notifications_detail,
    dag=berisso_dag,
)
build_notification_payments_detail_task = PythonOperator(
    task_id="build_notification_payments_detail",
    python_callable=build_notification_payments_detail,
    dag=berisso_dag,
)
build_infraction_counts_task = PythonOperator(
    task_id="build_infraction_counts",
    python_callable=build_infraction_counts,
    dag=berisso_dag,
)
build_camera_detail_task = PythonOperator(
    task_id="build_camera_detail",
    python_callable=build_camera_detail,
    dag=berisso_dag,
)

transform_payments_task >> build_payments_detail_task
transform_fallos_task >> build_fallos_detail_task
transform_nivel5_task >> build_nivel5_detail_task
join_notifications_task >> build_notifications_detail_task
join_notifications_payments_task >> build_notification_payments_detail_task
transform_actas_cantidad_task >> build_infraction_counts_task
join_infraction_data_task >> build_camera_detail_task


# ---- dashboard outputs ----------------------------------------------------


def build_dashboard_payments_daily(**kwargs):
    import pandas as pd

    payments = pd.read_parquet(PAYMENTS_CLEAN)
    payments.groupby(["Fecha", "Año", "Mes", "Mes_num"], as_index=False).agg(
        **{"Actas pagadas": ("numero", "nunique")}
    ).to_parquet(_output_path("dashboard_payments_daily.parquet"), index=False)


def build_dashboard_infractions_by_type(**kwargs):
    import pandas as pd

    counts = pd.read_parquet(ACTAS_CANTIDAD_CLEAN)
    counts.groupby(
        ["Año", "Mes", "Mes_num", "Tipo de Faltas"],
        as_index=False,
    ).agg(
        **{"Cantidad cometidas": ("Cantidad cometidas", "sum")}
    ).to_parquet(
        _output_path("dashboard_infractions_by_type.parquet"),
        index=False,
    )


def build_dashboard_generated_by_type(**kwargs):
    import pandas as pd

    camera = pd.read_parquet(INFRACTION_DETAIL_CLEAN)
    camera.groupby(
        ["Año", "Mes", "Mes_num", "Tipo de Faltas"],
        as_index=False,
    ).agg(
        Generado=("Generado", "sum")
    ).to_parquet(
        _output_path("dashboard_generated_by_type.parquet"),
        index=False,
    )


def build_dashboard_monthly_metrics(**kwargs):
    import pandas as pd

    payments = pd.read_parquet(PAYMENTS_CLEAN)
    counts = pd.read_parquet(ACTAS_CANTIDAD_CLEAN)
    camera = pd.read_parquet(INFRACTION_DETAIL_CLEAN)
    notifications = pd.read_parquet(NOTIFICATIONS_JOINED)

    monthly_payments = payments.groupby(["Año", "Mes", "Mes_num"], as_index=False).agg(
        Recaudado=("Monto Total", "sum"),
        Tasa_infraccion=("Tasa infraccion", "sum"),
        Tasa_administrativa=("Tasa administrativa", "sum"),
        Actas_pagadas=("numero", "nunique"),
    )
    monthly_generated = camera.groupby(["Año", "Mes", "Mes_num"], as_index=False).agg(
        Generado=("Generado", "sum"),
    )
    monthly_generated_count = counts.groupby(
        ["Año", "Mes", "Mes_num"],
        as_index=False,
    ).agg(
        Actas_Generadas=("Cantidad cometidas", "sum"),
    )
    monthly_notifications = notifications.groupby(
        ["Año", "Mes", "Mes_num"],
        as_index=False,
    ).agg(
        Actas_Notificadas=("acta_id", "nunique"),
    )

    dataframe = monthly_payments.merge(
        monthly_generated,
        on=["Año", "Mes", "Mes_num"],
        how="outer",
    ).merge(
        monthly_generated_count,
        on=["Año", "Mes", "Mes_num"],
        how="outer",
    ).merge(
        monthly_notifications,
        on=["Año", "Mes", "Mes_num"],
        how="outer",
    )
    dataframe = dataframe.sort_values(["Año", "Mes_num"])
    columns = [
        "Recaudado",
        "Tasa_infraccion",
        "Tasa_administrativa",
        "Actas_pagadas",
        "Generado",
        "Actas_Generadas",
        "Actas_Notificadas",
    ]
    for column in columns:
        dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce").fillna(0)

    dataframe["_periodo"] = pd.to_datetime(
        {"year": dataframe["Año"], "month": dataframe["Mes_num"], "day": 1}
    )
    previous = dataframe[["_periodo", *columns]].assign(
        _periodo=lambda frame: frame["_periodo"] + pd.offsets.MonthBegin(1)
    )
    previous = previous.rename(columns={column: f"{column}_mes_anterior" for column in columns})
    dataframe.merge(previous, on="_periodo", how="left").drop(columns="_periodo").to_parquet(
        _output_path("dashboard_monthly_metrics.parquet"),
        index=False,
    )


build_dashboard_payments_daily_task = PythonOperator(
    task_id="build_dashboard_payments_daily",
    python_callable=build_dashboard_payments_daily,
    dag=berisso_dag,
)
build_dashboard_infractions_by_type_task = PythonOperator(
    task_id="build_dashboard_infractions_by_type",
    python_callable=build_dashboard_infractions_by_type,
    dag=berisso_dag,
)
build_dashboard_generated_by_type_task = PythonOperator(
    task_id="build_dashboard_generated_by_type",
    python_callable=build_dashboard_generated_by_type,
    dag=berisso_dag,
)
build_dashboard_monthly_metrics_task = PythonOperator(
    task_id="build_dashboard_monthly_metrics",
    python_callable=build_dashboard_monthly_metrics,
    dag=berisso_dag,
)

transform_payments_task >> build_dashboard_payments_daily_task
transform_actas_cantidad_task >> build_dashboard_infractions_by_type_task
join_infraction_data_task >> build_dashboard_generated_by_type_task
transform_payments_task >> build_dashboard_monthly_metrics_task
join_notifications_task >> build_dashboard_monthly_metrics_task
join_infraction_data_task >> build_dashboard_monthly_metrics_task


# ---- payments outputs -----------------------------------------------------


def build_payments_by_method_daily(**kwargs):
    import pandas as pd

    payments = pd.read_parquet(PAYMENTS_CLEAN)
    payments.groupby(["Fecha", "medio_pago"], as_index=False).agg(Actas=("numero", "nunique")).to_parquet(
        _output_path("payments_by_method_daily.parquet"),
        index=False,
    )


def build_payments_daily(**kwargs):
    import pandas as pd

    payments = pd.read_parquet(PAYMENTS_CLEAN)
    payments.groupby(["Fecha"], as_index=False).agg(Pagadas=("numero", "nunique")).to_parquet(
        _output_path("payments_daily.parquet"),
        index=False,
    )


def build_payments_monthly(**kwargs):
    import pandas as pd

    payments = pd.read_parquet(PAYMENTS_CLEAN)
    payments.groupby(["Año", "Mes", "Mes_num"], as_index=False).agg(
        Actas_pagadas=("numero", "nunique"),
        Recaudado=("Monto Total", "sum"),
    ).to_parquet(_output_path("payments_monthly.parquet"), index=False)


def build_camera_by_month_type(**kwargs):
    import pandas as pd

    camera = pd.read_parquet(INFRACTION_DETAIL_CLEAN)
    camera.groupby(
        ["Año", "Mes", "Mes_num", "ubicacion", "Tipo de Faltas"],
        as_index=False,
    ).agg(
        **{
            "Cantidad cometidas": ("Cantidad cometidas", "sum"),
            "Generado": ("Generado", "sum"),
        }
    ).to_parquet(_output_path("camera_by_month_type.parquet"), index=False)


def build_camera_by_month_total(**kwargs):
    import pandas as pd

    camera = pd.read_parquet(INFRACTION_DETAIL_CLEAN)
    camera.groupby(
        ["Año", "Mes", "Mes_num", "ubicacion", "latitud", "longitud"],
        dropna=False,
        as_index=False,
    ).agg(
        **{
            "Cantidad cometidas": ("Cantidad cometidas", "sum"),
            "Generado": ("Generado", "sum"),
        }
    ).to_parquet(_output_path("camera_by_month_total.parquet"), index=False)


def build_camera_map(**kwargs):
    import pandas as pd

    camera = pd.read_parquet(INFRACTION_DETAIL_CLEAN)
    camera.groupby(
        ["Año", "Mes", "Mes_num", "ubicacion", "latitud", "longitud"],
        dropna=False,
        as_index=False,
    ).agg(
        **{
            "Cantidad cometidas": ("Cantidad cometidas", "sum"),
            "Generado": ("Generado", "sum"),
        }
    ).to_parquet(_output_path("camera_map.parquet"), index=False)


build_payments_by_method_daily_task = PythonOperator(
    task_id="build_payments_by_method_daily",
    python_callable=build_payments_by_method_daily,
    dag=berisso_dag,
)
build_payments_daily_task = PythonOperator(
    task_id="build_payments_daily",
    python_callable=build_payments_daily,
    dag=berisso_dag,
)
build_payments_monthly_task = PythonOperator(
    task_id="build_payments_monthly",
    python_callable=build_payments_monthly,
    dag=berisso_dag,
)
build_camera_by_month_type_task = PythonOperator(
    task_id="build_camera_by_month_type",
    python_callable=build_camera_by_month_type,
    dag=berisso_dag,
)
build_camera_by_month_total_task = PythonOperator(
    task_id="build_camera_by_month_total",
    python_callable=build_camera_by_month_total,
    dag=berisso_dag,
)
build_camera_map_task = PythonOperator(
    task_id="build_camera_map",
    python_callable=build_camera_map,
    dag=berisso_dag,
)

transform_payments_task >> build_payments_by_method_daily_task
transform_payments_task >> build_payments_daily_task
transform_payments_task >> build_payments_monthly_task
join_infraction_data_task >> build_camera_by_month_type_task
join_infraction_data_task >> build_camera_by_month_total_task
join_infraction_data_task >> build_camera_map_task


# ---- lotes outputs --------------------------------------------------------


def build_notifications_by_date_type(**kwargs):
    import pandas as pd

    notifications = pd.read_parquet(NOTIFICATIONS_JOINED)
    notifications.groupby(["Año", "Mes", "Mes_num", "Fecha Lote", "notific_type"], as_index=False).agg(
        Actas_notificadas=("acta", "nunique")
    ).rename(columns={"notific_type": "Tipo de Notificacion"}).to_parquet(
        _output_path("notifications_by_date_type.parquet"),
        index=False,
    )


def build_notifications_by_type_month(**kwargs):
    import pandas as pd

    notifications = pd.read_parquet(NOTIFICATIONS_JOINED)
    notifications.groupby(["Año", "Mes", "Mes_num", "notific_type"], as_index=False).agg(
        Actas_notificadas=("acta", "nunique")
    ).rename(columns={"notific_type": "Tipo de Notificacion"}).to_parquet(
        _output_path("notifications_by_type_month.parquet"),
        index=False,
    )


def build_lotes_daily_analytics(**kwargs):
    import numpy as np
    import pandas as pd

    notification_payments = pd.read_parquet(NOTIFICATION_PAYMENTS)
    daily = notification_payments.groupby(
        ["Año", "Mes", "Mes_num", "Fecha Lote", "notific_type"],
        as_index=False,
    ).agg(
        Cantidad_de_Actas_Enviadas=("acta_id", "nunique"),
        Cantidad_de_Actas_Pagadas=("fecha_pago", "count"),
    )
    daily["Porcentaje_Pagadas"] = np.where(
        daily["Cantidad_de_Actas_Enviadas"] > 0,
        daily["Cantidad_de_Actas_Pagadas"] / daily["Cantidad_de_Actas_Enviadas"] * 100,
        0,
    )
    daily.to_parquet(_output_path("lotes_daily_analytics.parquet"), index=False)


def build_lotes_monthly_analytics(**kwargs):
    import pandas as pd

    notification_payments = pd.read_parquet(NOTIFICATION_PAYMENTS)
    rows = []
    for (year, month, month_number), group in notification_payments.groupby(
        ["Año", "Mes", "Mes_num"],
        dropna=False,
    ):
        total = group["acta_id"].nunique()
        row = {"Año": year, "Mes": month, "Mes_num": month_number, "Actas_notificadas": total}
        for notification_type, prefix in [("Email", "Email"), ("Bajo Puerta", "Bajo_Puerta")]:
            subset = group.loc[group["notific_type"] == notification_type]
            notified = subset["acta_id"].nunique()
            paid = subset.loc[subset["fecha_pago"].notna(), "acta_id"].nunique()
            locations = subset.groupby("localidad")["acta_id"].nunique().sort_values(ascending=False)
            row[f"{prefix}_notificadas"] = notified
            row[f"{prefix}_pagadas"] = paid
            row[f"{prefix}_recaudado"] = subset.loc[subset["fecha_pago"].notna(), "Monto Total"].sum()
            row[f"{prefix}_porcentaje"] = notified / total * 100 if total else 0
            row[f"{prefix}_efectividad"] = paid / notified * 100 if notified else 0
            row[f"{prefix}_promedio_dias"] = subset.loc[subset["fecha_pago"].notna(), "Días transcurridos"].mean()
            row[f"{prefix}_top_localidad"] = locations.index[0] if not locations.empty else ""
            row[f"{prefix}_top_localidad_cantidad"] = int(locations.iloc[0]) if not locations.empty else 0
        rows.append(row)

    columns = [
        "Año",
        "Mes",
        "Mes_num",
        "Actas_notificadas",
        "Email_notificadas",
        "Email_pagadas",
        "Email_recaudado",
        "Email_porcentaje",
        "Email_efectividad",
        "Email_promedio_dias",
        "Email_top_localidad",
        "Email_top_localidad_cantidad",
        "Bajo_Puerta_notificadas",
        "Bajo_Puerta_pagadas",
        "Bajo_Puerta_recaudado",
        "Bajo_Puerta_porcentaje",
        "Bajo_Puerta_efectividad",
        "Bajo_Puerta_promedio_dias",
        "Bajo_Puerta_top_localidad",
        "Bajo_Puerta_top_localidad_cantidad",
    ]
    monthly = pd.DataFrame(rows, columns=columns)

    if not monthly.empty:
        monthly = monthly.sort_values(["Año", "Mes_num"])
        previous_columns = [
            "Actas_notificadas",
            "Email_notificadas",
            "Bajo_Puerta_notificadas",
            "Email_efectividad",
            "Email_promedio_dias",
            "Bajo_Puerta_efectividad",
            "Bajo_Puerta_promedio_dias",
        ]
        monthly["_periodo"] = pd.to_datetime(
            {"year": monthly["Año"], "month": monthly["Mes_num"], "day": 1},
            errors="coerce",
        )
        previous = monthly[["_periodo", *previous_columns]].assign(
            _periodo=lambda frame: frame["_periodo"] + pd.offsets.MonthBegin(1)
        )
        previous = previous.rename(columns={column: f"{column}_anterior" for column in previous_columns})
        monthly = monthly.merge(previous, on="_periodo", how="left").drop(columns="_periodo")

    monthly.to_parquet(_output_path("lotes_monthly_analytics.parquet"), index=False)

build_notifications_by_date_type_task = PythonOperator(
    task_id="build_notifications_by_date_type",
    python_callable=build_notifications_by_date_type,
    dag=berisso_dag,
)
build_notifications_by_type_month_task = PythonOperator(
    task_id="build_notifications_by_type_month",
    python_callable=build_notifications_by_type_month,
    dag=berisso_dag,
)
build_lotes_daily_analytics_task = PythonOperator(
    task_id="build_lotes_daily_analytics",
    python_callable=build_lotes_daily_analytics,
    dag=berisso_dag,
)
build_lotes_monthly_analytics_task = PythonOperator(
    task_id="build_lotes_monthly_analytics",
    python_callable=build_lotes_monthly_analytics,
    dag=berisso_dag,
)

join_notifications_task >> build_notifications_by_date_type_task
join_notifications_task >> build_notifications_by_type_month_task
join_notifications_payments_task >> build_lotes_daily_analytics_task
join_notifications_payments_task >> build_lotes_monthly_analytics_task


# ---- juzgados outputs -----------------------------------------------------


def build_fallos_daily_juzgado(**kwargs):
    import pandas as pd

    fallos = pd.read_parquet(FALLOS_FINAL)
    fallos.groupby(["Fecha", "juzgado_id"], as_index=False).agg(Fallos_emitidos=("acta", "count")).to_parquet(
        _output_path("fallos_daily_juzgado.parquet"),
        index=False,
    )


def build_fallos_daily_tipo(**kwargs):
    import pandas as pd

    fallos = pd.read_parquet(FALLOS_FINAL)
    fallos.groupby(["Fecha", "juzgado_id", "fallo"], as_index=False).agg(
        Emitidos=("acta", "nunique"),
        Reducido=("Reducido", "sum"),
    ).to_parquet(_output_path("fallos_daily_tipo.parquet"), index=False)


def build_fallos_tipo_total(**kwargs):
    import pandas as pd

    fallos = pd.read_parquet(FALLOS_FINAL)
    fallos.groupby(["fallo"], as_index=False).agg(Cantidad=("acta", "count")).to_parquet(
        _output_path("fallos_tipo_total.parquet"),
        index=False,
    )


build_fallos_daily_juzgado_task = PythonOperator(
    task_id="build_fallos_daily_juzgado",
    python_callable=build_fallos_daily_juzgado,
    dag=berisso_dag,
)
build_fallos_daily_tipo_task = PythonOperator(
    task_id="build_fallos_daily_tipo",
    python_callable=build_fallos_daily_tipo,
    dag=berisso_dag,
)
build_fallos_tipo_total_task = PythonOperator(
    task_id="build_fallos_tipo_total",
    python_callable=build_fallos_tipo_total,
    dag=berisso_dag,
)

transform_fallos_task >> build_fallos_daily_juzgado_task
transform_fallos_task >> build_fallos_daily_tipo_task
transform_fallos_task >> build_fallos_tipo_total_task


# ---- nivel 5 outputs ------------------------------------------------------


def build_nivel5_daily_reviewer(**kwargs):
    import pandas as pd

    nivel5 = pd.read_parquet(NIVEL5_FINAL)
    nivel5.groupby(["Fecha", "Auditor"], as_index=False).agg(
        {"Aceptadas": "sum", "Rechazadas": "sum", "Total": "sum"}
    ).to_parquet(_output_path("nivel5_daily_reviewer.parquet"), index=False)


def build_nivel5_daily_camera(**kwargs):
    import pandas as pd

    nivel5 = pd.read_parquet(NIVEL5_FINAL)
    camera = nivel5.groupby(["Fecha", "Código de cámara"], as_index=False).agg(
        {"Aceptadas": "sum", "Rechazadas": "sum"}
    ).rename(columns={"Código de cámara": "Ubicacion"})
    camera["Total"] = camera["Aceptadas"] + camera["Rechazadas"]
    camera.to_parquet(_output_path("nivel5_daily_camera.parquet"), index=False)


def build_nivel5_daily_result_detail(**kwargs):
    import pandas as pd

    nivel5 = pd.read_parquet(NIVEL5_FINAL)
    accepted = nivel5.groupby(["Fecha", "Auditor"], as_index=False)["Aceptadas"].sum().rename(
        columns={"Aceptadas": "Presunciones de Nivel 5"}
    )
    accepted["Resultado"] = "Aceptadas"
    rejected = nivel5.groupby(["Fecha", "Auditor"], as_index=False)["Rechazadas"].sum().rename(
        columns={"Rechazadas": "Presunciones de Nivel 5"}
    )
    rejected["Resultado"] = "Rechazadas"
    pd.concat([accepted, rejected], ignore_index=True).to_parquet(
        _output_path("nivel5_daily_result_detail.parquet"),
        index=False,
    )


def build_nivel5_daily_result(**kwargs):
    import pandas as pd

    detail = pd.read_parquet(_output_path("nivel5_daily_result_detail.parquet"))
    if detail.empty:
        result = pd.DataFrame(
            columns=["Fecha", "Resultado", "Presunciones de Nivel 5", "hover_text", "Total de la fecha"]
        )
    else:
        hover_rows = []
        for (fecha, resultado), group in detail.groupby(["Fecha", "Resultado"]):
            hover_rows.append(
                {
                    "Fecha": fecha,
                    "Resultado": resultado,
                    "hover_text": "<br>".join(
                        f"- {resultado} {row['Auditor']}: {row['Presunciones de Nivel 5']}"
                        for _, row in group.iterrows()
                    ),
                }
            )

        hover = pd.DataFrame(hover_rows)
        result = detail.groupby(["Fecha", "Resultado"], as_index=False)["Presunciones de Nivel 5"].sum()
        result = result.merge(hover, on=["Fecha", "Resultado"], how="left")
        totals = result.groupby("Fecha", as_index=False)["Presunciones de Nivel 5"].sum().rename(
            columns={"Presunciones de Nivel 5": "Total de la fecha"}
        )
        result = result.merge(totals, on="Fecha", how="left")

    result.to_parquet(_output_path("nivel5_daily_result.parquet"), index=False)

def build_nivel5_daily_metrics(**kwargs):
    import pandas as pd

    nivel5 = pd.read_parquet(NIVEL5_FINAL)
    nivel5.groupby(["Fecha"], as_index=False).agg(
        {"Total": "sum", "Aceptadas": "sum", "Rechazadas": "sum"}
    ).to_parquet(_output_path("nivel5_daily_metrics.parquet"), index=False)


build_nivel5_daily_reviewer_task = PythonOperator(
    task_id="build_nivel5_daily_reviewer",
    python_callable=build_nivel5_daily_reviewer,
    dag=berisso_dag,
)
build_nivel5_daily_camera_task = PythonOperator(
    task_id="build_nivel5_daily_camera",
    python_callable=build_nivel5_daily_camera,
    dag=berisso_dag,
)
build_nivel5_daily_result_detail_task = PythonOperator(
    task_id="build_nivel5_daily_result_detail",
    python_callable=build_nivel5_daily_result_detail,
    dag=berisso_dag,
)
build_nivel5_daily_result_task = PythonOperator(
    task_id="build_nivel5_daily_result",
    python_callable=build_nivel5_daily_result,
    dag=berisso_dag,
)
build_nivel5_daily_metrics_task = PythonOperator(
    task_id="build_nivel5_daily_metrics",
    python_callable=build_nivel5_daily_metrics,
    dag=berisso_dag,
)

transform_nivel5_task >> build_nivel5_daily_reviewer_task
transform_nivel5_task >> build_nivel5_daily_camera_task
transform_nivel5_task >> build_nivel5_daily_result_detail_task
build_nivel5_daily_result_detail_task >> build_nivel5_daily_result_task
transform_nivel5_task >> build_nivel5_daily_metrics_task

check_berisso_sources_task >> [
    fetch_actas_pagadas_task,
    fetch_detalles_infracciones_task,
    fetch_actas_cantidad_task,
    fetch_actas_notificadas_task,
    fetch_actas_notificadas_extra_task,
    fetch_actas_notificadas_email_task,
    fetch_actividad_revisores_task,
    fetch_fallos_task,
]


publish_berisso_dataset_task = PythonOperator(
    task_id="publish_berisso_dataset",
    python_callable=publish_berisso_dataset,
    dag=berisso_dag,
)

[
    build_payments_detail_task,
    build_fallos_detail_task,
    build_nivel5_detail_task,
    build_notifications_detail_task,
    build_notification_payments_detail_task,
    build_infraction_counts_task,
    build_camera_detail_task,
    build_notifications_door_extra_detail_task,
    build_dashboard_payments_daily_task,
    build_dashboard_infractions_by_type_task,
    build_dashboard_generated_by_type_task,
    build_dashboard_monthly_metrics_task,
    build_payments_by_method_daily_task,
    build_payments_daily_task,
    build_payments_monthly_task,
    build_camera_by_month_type_task,
    build_camera_by_month_total_task,
    build_camera_map_task,
    build_notifications_by_date_type_task,
    build_notifications_by_type_month_task,
    build_lotes_daily_analytics_task,
    build_lotes_monthly_analytics_task,
    build_fallos_daily_juzgado_task,
    build_fallos_daily_tipo_task,
    build_fallos_tipo_total_task,
    build_nivel5_daily_reviewer_task,
    build_nivel5_daily_camera_task,
    build_nivel5_daily_result_task,
    build_nivel5_daily_metrics_task,
] >> publish_berisso_dataset_task

