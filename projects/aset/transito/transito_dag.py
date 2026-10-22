import datetime
import json
import os
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from google_worker import GdriveLimitedPythonOperator


transito_dag = DAG(
    dag_id="transito_dag",
    start_date=datetime.datetime(2026, 8, 25),
    catchup=False,
    max_active_runs=1,
    schedule="* * * * *",
)


# Tessa conserva el nombre interno ``transito`` para no romper imports ni rutas
# históricas, pero publica su dataset en processed/tessa y se sirve en /tessa/.
DEFAULT_TESSA_DRIVE_FOLDER_ID = "1w-r6aZAX5oGqyPjSHO1SMS4XunAbIFEu"

SOURCE_ALIASES = {
    "actividad_revisores": [
        "01-Actividad de revisores.xlsx",
        "actividad_revisores_t.xlsx",
        "actividad_revisores.xlsx",
        "actividad_revisores_t",
        "actividad_revisores",
    ],
    "valores_uf": [
        "valores ufs (1).xlsx",
        "valores_uf.xlsx",
        "valores_ufs.xlsx",
        "valores_uf",
    ],
    "motivos_rechazos": [
        "02-Motivos de rechazo.xlsx",
        "motivos_rechazos.xlsx",
        "motivos_rechazos",
    ],
    "tiempo_revision": [
        "03-Tiempo de revisión.xlsx",
        "03-Tiempo de revision.xlsx",
        "tiempo_revision.xlsx",
        "tiempo_revision",
    ],
    "imagenes_pendientes": [
        "Pendientes.csv",
        "Pendientes.xlsx",
        "imagenes_pendientes.xlsx",
        "imagenes_pendientes",
    ],
    "reportes_camaras": [
        "Reportes_camaras.csv",
        "reportes_camaras.csv",
        "Reportes_camaras.xlsx",
        "reportes_camaras.xlsx",
        "reportes_camaras",
    ],
    "historial_pendientes": [
        "historial_completo.csv",
        "historial_completo.xlsx",
    ],
}

SOURCE_ENV_VARS = {
    "actividad_revisores": "TESSA_SOURCE_ACTIVIDAD_REVISORES",
    "valores_uf": "TESSA_SOURCE_VALORES_UF",
    "motivos_rechazos": "TESSA_SOURCE_MOTIVOS_RECHAZOS",
    "tiempo_revision": "TESSA_SOURCE_TIEMPO_REVISION",
    "imagenes_pendientes": "TESSA_SOURCE_IMAGENES_PENDIENTES",
    "reportes_camaras": "TESSA_SOURCE_REPORTES_CAMARAS",
    "historial_pendientes": "TESSA_SOURCE_HISTORIAL_PENDIENTES",
}

PUBLISHED_FILES = [
    "activity_reviewers_detail.parquet",
    "rejection_reasons_detail.parquet",
    "review_time_detail.parquet",
    "pending_images_detail.parquet",
    "pending_images_history_detail.parquet",
    "camera_reports_detail.parquet",
    "reviewer_hourly_detail.parquet",
]

def _bucket():
    # Reutiliza el bucket ya configurado para Berisso salvo que Tessa indique otro.
    return os.getenv("TESSA_S3_BUCKET") or os.getenv("BERISSO_S3_BUCKET")


def _output_path(file_name):
    output_dir = Path(os.getenv("TESSA_DATA_DIR", "/data/transito"))
    output_dir.mkdir(parents=True, exist_ok=True)
    return str(output_dir / file_name)


def _state_key(file_name):
    prefix = os.getenv("TESSA_S3_PREFIX", "processed/tessa").strip("/")
    return f"{prefix}/{file_name}"


def _load_state(file_name):
    bucket = _bucket()
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
    bucket = _bucket()
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



def _load_previous_published_parquet(file_name):
    import pandas as pd

    bucket = _bucket()
    if bucket:
        import boto3
        from botocore.exceptions import ClientError

        manifest = _load_state("_dataset_manifest.json") or {}
        prefix = (manifest.get("prefix") or "").strip("/")
        if not prefix:
            return pd.DataFrame()
        try:
            response = boto3.client("s3").get_object(
                Bucket=bucket,
                Key=f"{prefix}/{file_name}",
            )
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
                return pd.DataFrame()
            raise
        from io import BytesIO

        return pd.read_parquet(BytesIO(response["Body"].read()))

    path = Path(_output_path(file_name))
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)

def _tessa_data_folder_id():
    from google_worker.drive_csv import list_folder, resolve_folder_path

    root_folder_id = os.getenv("TESSA_DRIVE_FOLDER_ID", DEFAULT_TESSA_DRIVE_FOLDER_ID)
    relative_path = os.getenv("TESSA_DRIVE_DATA_PATH", "").strip("/\\")
    if not relative_path:
        return root_folder_id

    # El dashboard histórico usaba directamente la carpeta raíz. El fallback permite
    # seguir funcionando si data_files todavía no existe o si el ID ya apunta a ella.
    try:
        return resolve_folder_path(root_folder_id, relative_path)
    except FileNotFoundError:
        list_folder(root_folder_id)  # valida acceso y conserva un error útil si no existe
        return root_folder_id


def _resolve_source_manifest():
    from google_worker.drive_csv import list_folder

    folder_id = _tessa_data_folder_id()
    files = list_folder(folder_id)
    manifest = {}

    for logical_name, aliases in SOURCE_ALIASES.items():
        override = os.getenv(SOURCE_ENV_VARS[logical_name])
        candidates = [override] if override else []
        candidates.extend(name for name in aliases if name not in candidates)
        selected = next((name for name in candidates if name in files), None)
        if selected is None:
            raise FileNotFoundError(
                f"No se encontró la fuente {logical_name!r} en Drive. "
                f"Nombres aceptados: {', '.join(candidates)}"
            )

        item = files[selected]
        manifest[logical_name] = {
            "name": selected,
            "id": item["id"],
            "mimeType": item.get("mimeType"),
            "modifiedTime": item.get("modifiedTime"),
            "md5Checksum": item.get("md5Checksum"),
            "size": item.get("size"),
            "folder_id": folder_id,
        }

    return manifest


def check_tessa_sources(**kwargs):
    current = _resolve_source_manifest()
    previous = _load_state("_source_manifest.json")
    if current == previous:
        return False
    return current


def _fetch_source(logical_name, target_path, **kwargs):
    from google_worker.drive_csv import read_csv, read_excel

    Path(target_path).parent.mkdir(parents=True, exist_ok=True)
    manifest = kwargs["ti"].xcom_pull(task_ids="check_tessa_sources") or {}
    source = manifest.get(logical_name)
    if not source:
        raise RuntimeError(f"No se encontró {logical_name!r} en el manifest de Tessa")

    file_name = source["name"]
    if file_name.lower().endswith(".csv"):
        dataframe = read_csv(
            file_name,
            file_id=source["id"],
            dtype="string",
        )
    else:
        dataframe = read_excel(
            file_name,
            file_id=source["id"],
            mime_type=source.get("mimeType"),
            dtype="string",
        )

    dataframe.to_parquet(target_path, index=False)


def publish_tessa_dataset(**kwargs):
    source_manifest = kwargs["ti"].xcom_pull(task_ids="check_tessa_sources")
    now = datetime.datetime.now(datetime.UTC)
    version = now.strftime("%Y%m%dT%H%M%S%fZ")
    dataset_manifest = {
        "version": version,
        "updated_at": now.isoformat(),
        "source": source_manifest,
    }

    bucket = _bucket()
    if bucket:
        import boto3

        prefix = os.getenv("TESSA_S3_PREFIX", "processed/tessa").strip("/")
        version_prefix = f"{prefix}/versions/{version}"
        client = boto3.client("s3")
        output_dir = Path(os.getenv("TESSA_DATA_DIR", "/data/transito"))
        for file_name in PUBLISHED_FILES:
            path = output_dir / file_name
            if not path.exists():
                raise FileNotFoundError(f"No se generó el output esperado: {path}")
            client.upload_file(str(path), bucket, f"{version_prefix}/{file_name}")
        dataset_manifest["prefix"] = version_prefix

    _save_state("_dataset_manifest.json", dataset_manifest)
    _save_state("_source_manifest.json", source_manifest)


check_tessa_sources_task = ShortCircuitOperator(
    task_id="check_tessa_sources",
    python_callable=check_tessa_sources,
    dag=transito_dag,
)


# ---- actividad de revisores -----------------------------------------------

ACTIVIDAD_REVISORES_RAW = "/tmp/data/transito/actividad_revisores.parquet"
ACTIVIDAD_REVISORES_CLEAN = "/tmp/data/transito/actividad_revisores_clean.parquet"


def fetch_actividad_revisores(**kwargs):
    _fetch_source("actividad_revisores", ACTIVIDAD_REVISORES_RAW, **kwargs)


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

    # El dominio UNLaM conserva la referencia histórica original del dashboard:
    # auditores activos en enero de 2025 más las incorporaciones posteriores.
    # Usar cualquier enero mezcla auditores pertenecientes a otros dominios.
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
        "Mabel Garcia",
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
    auditor_order = sorted(
        dataframe["Auditor"].dropna().astype(str).unique(),
        reverse=True,
    )
    dataframe["Auditor"] = pd.Categorical(
        dataframe["Auditor"],
        categories=auditor_order,
        ordered=True,
    )
    dataframe.drop_duplicates().sort_values(
        ["Mes", "Auditor"],
        ascending=[False, True],
    ).to_parquet(
        ACTIVIDAD_REVISORES_CLEAN,
        index=False,
    )


fetch_actividad_revisores_task = GdriveLimitedPythonOperator(
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
    _fetch_source("valores_uf", VALORES_UF_RAW, **kwargs)


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


fetch_valores_uf_task = GdriveLimitedPythonOperator(
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
    _fetch_source("motivos_rechazos", MOTIVOS_RECHAZOS_RAW, **kwargs)


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

    # Motivos de rechazo usa exactamente el mismo dominio ya validado por
    # actividad de revisores, evitando mantener una segunda regla distinta.
    activity_reviewers = pd.read_parquet(ACTIVIDAD_REVISORES_CLEAN)
    unlam_reviewers = set(activity_reviewers["Auditor"].dropna().astype(str).unique())

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


fetch_motivos_rechazos_task = GdriveLimitedPythonOperator(
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
transform_actividad_revisores_task >> transform_motivos_rechazos_task


# ---- tiempo de revision ---------------------------------------------------

TIEMPO_REVISION_RAW = "/tmp/data/transito/tiempo_revision.parquet"
TIEMPO_REVISION_CLEAN = "/tmp/data/transito/tiempo_revision_clean.parquet"


def fetch_tiempo_revision(**kwargs):
    _fetch_source("tiempo_revision", TIEMPO_REVISION_RAW, **kwargs)


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

    # Tiempo de revisión usa el mismo conjunto de auditores UNLaM validado
    # por actividad de revisores.
    activity_reviewers = pd.read_parquet(ACTIVIDAD_REVISORES_CLEAN)
    unlam_reviewers = set(activity_reviewers["Auditor"].dropna().astype(str).unique())

    dataframe = dataframe.loc[
        dataframe["Tipo de Cámara"].isin(["Semáforo", "Luces", "Buscador de patentes"])
        & dataframe["Auditor"].isin(unlam_reviewers)
    ]
    dataframe = dataframe.drop(columns=["Tipo de Revisión", "Nivel de Revisión"], errors="ignore")
    auditor_order = sorted(dataframe["Auditor"].dropna().astype(str).unique())
    dataframe["Auditor"] = pd.Categorical(
        dataframe["Auditor"],
        categories=auditor_order,
        ordered=True,
    )
    dataframe.drop_duplicates().sort_values(
        ["Fecha", "Auditor"],
        ascending=[True, True],
    ).to_parquet(
        TIEMPO_REVISION_CLEAN,
        index=False,
    )


fetch_tiempo_revision_task = GdriveLimitedPythonOperator(
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
transform_actividad_revisores_task >> transform_tiempo_revision_task


# ---- imagenes pendientes --------------------------------------------------

IMAGENES_PENDIENTES_RAW = "/tmp/data/transito/imagenes_pendientes.parquet"
IMAGENES_PENDIENTES_CLEAN = "/tmp/data/transito/imagenes_pendientes_clean.parquet"


def fetch_imagenes_pendientes(**kwargs):
    _fetch_source("imagenes_pendientes", IMAGENES_PENDIENTES_RAW, **kwargs)


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


fetch_imagenes_pendientes_task = GdriveLimitedPythonOperator(
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


# ---- historial de imagenes pendientes -------------------------------------

HISTORIAL_PENDIENTES_RAW = "/tmp/data/transito/historial_pendientes.parquet"
HISTORIAL_PENDIENTES_CLEAN = "/tmp/data/transito/historial_pendientes_clean.parquet"


def fetch_historial_pendientes(**kwargs):
    _fetch_source("historial_pendientes", HISTORIAL_PENDIENTES_RAW, **kwargs)


def transform_historial_pendientes(**kwargs):
    import pandas as pd

    dataframe = pd.read_parquet(HISTORIAL_PENDIENTES_RAW).rename(
        columns={"client": "Cliente", "status": "Estado", "count": "Cantidad"}
    )
    required_columns = {"Cliente", "Estado", "Cantidad", "tipo", "Tiempo"}
    missing_columns = required_columns.difference(dataframe.columns)
    if missing_columns:
        raise ValueError(
            "historial_pendientes no contiene las columnas requeridas: "
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
    dataframe["Cantidad"] = pd.to_numeric(
        dataframe["Cantidad"].astype("string").str.replace(",", ".", regex=False),
        errors="coerce",
    ).fillna(0).round().astype("int64")
    dataframe["Tiempo"] = pd.to_datetime(
        dataframe["Tiempo"],
        format="mixed",
        errors="coerce",
    )
    dataframe = dataframe.loc[dataframe["Tiempo"].notna()]
    dataframe[["Cliente", "tipo", "Estado", "Cantidad", "Tiempo"]].drop_duplicates().sort_values(
        ["Tiempo", "Cliente", "Estado", "tipo"]
    ).to_parquet(HISTORIAL_PENDIENTES_CLEAN, index=False)


fetch_historial_pendientes_task = GdriveLimitedPythonOperator(
    task_id="fetch_historial_pendientes",
    python_callable=fetch_historial_pendientes,
    dag=transito_dag,
)
transform_historial_pendientes_task = PythonOperator(
    task_id="transform_historial_pendientes",
    python_callable=transform_historial_pendientes,
    dag=transito_dag,
)
fetch_historial_pendientes_task >> transform_historial_pendientes_task


# ---- reportes de camaras ---------------------------------------------------

REPORTES_CAMARAS_RAW = "/tmp/data/transito/reportes_camaras.parquet"
REPORTES_CAMARAS_CLEAN = "/tmp/data/transito/reportes_camaras_clean.parquet"


def fetch_reportes_camaras(**kwargs):
    _fetch_source("reportes_camaras", REPORTES_CAMARAS_RAW, **kwargs)


def transform_reportes_camaras(**kwargs):
    import pandas as pd

    dataframe = pd.read_parquet(REPORTES_CAMARAS_RAW)
    dataframe.columns = dataframe.columns.str.strip()
    dataframe = dataframe.rename(
        columns={
            "Fecha": "Fecha del reporte",
            "Motivo de rechazo": "Motivo de Rechazo",
            "Observación": "Observacion",
            "Resuelto": "Fue resuelto",
        }
    )

    required_columns = {
        "Cliente",
        "Fecha del reporte",
        "Fecha del video",
        "Cámara",
        "Motivo de Rechazo",
        "Observacion",
        "Fue resuelto",
    }
    missing_columns = required_columns.difference(dataframe.columns)
    if missing_columns:
        raise ValueError(
            "reportes_camaras no contiene las columnas requeridas: "
            + ", ".join(sorted(missing_columns))
        )

    if "Usuario" not in dataframe.columns:
        dataframe["Usuario"] = ""

    for column in ["Fecha del reporte", "Fecha del video"]:
        values = dataframe[column].astype("string").str.strip()
        numeric_values = pd.to_numeric(values, errors="coerce")
        excel_serial = numeric_values.between(
            20000, 80000, inclusive="both"
        ).fillna(False)
        parsed_text = pd.to_datetime(
            values.where(~excel_serial),
            format="mixed",
            dayfirst=True,
            errors="coerce",
        )
        parsed_serial = pd.to_datetime(
            numeric_values.where(excel_serial),
            unit="D",
            origin="1899-12-30",
            errors="coerce",
        )
        dataframe[column] = parsed_text.fillna(parsed_serial).dt.normalize()

    for column in [
        "Usuario",
        "Cliente",
        "Cámara",
        "Motivo de Rechazo",
        "Observacion",
        "Fue resuelto",
    ]:
        dataframe[column] = (
            dataframe[column].astype("string").fillna("").str.strip()
        )

    dataframe[
        [
            "Usuario",
            "Cliente",
            "Fecha del reporte",
            "Fecha del video",
            "Cámara",
            "Motivo de Rechazo",
            "Observacion",
            "Fue resuelto",
        ]
    ].drop_duplicates().to_parquet(REPORTES_CAMARAS_CLEAN, index=False)


fetch_reportes_camaras_task = GdriveLimitedPythonOperator(
    task_id="fetch_reportes_camaras",
    python_callable=fetch_reportes_camaras,
    dag=transito_dag,
)
transform_reportes_camaras_task = PythonOperator(
    task_id="transform_reportes_camaras",
    python_callable=transform_reportes_camaras,
    dag=transito_dag,
)
fetch_reportes_camaras_task >> transform_reportes_camaras_task


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




def _prepare_for_publish(dataframe):
    """Persist stable scalar dtypes; ordering categories belong in the UI layer."""
    import pandas as pd

    dataframe = dataframe.copy()
    for column in dataframe.columns:
        if isinstance(dataframe[column].dtype, pd.CategoricalDtype):
            dataframe[column] = dataframe[column].astype("string")
    return dataframe


def build_activity_reviewers_detail(**kwargs):
    import pandas as pd

    _prepare_for_publish(pd.read_parquet(ACTIVITY_UF_CLEAN)).to_parquet(
        _output_path("activity_reviewers_detail.parquet"),
        index=False,
    )


def build_rejection_reasons_detail(**kwargs):
    import pandas as pd

    _prepare_for_publish(pd.read_parquet(MOTIVOS_RECHAZOS_CLEAN)).to_parquet(
        _output_path("rejection_reasons_detail.parquet"),
        index=False,
    )


def build_review_time_detail(**kwargs):
    import pandas as pd

    _prepare_for_publish(pd.read_parquet(TIEMPO_REVISION_CLEAN)).to_parquet(
        _output_path("review_time_detail.parquet"),
        index=False,
    )


def build_pending_images_detail(**kwargs):
    import pandas as pd

    _prepare_for_publish(pd.read_parquet(IMAGENES_PENDIENTES_CLEAN)).to_parquet(
        _output_path("pending_images_detail.parquet"),
        index=False,
    )


def build_pending_images_history_detail(**kwargs):
    import pandas as pd

    seed = pd.read_parquet(HISTORIAL_PENDIENTES_CLEAN)
    previous = _load_previous_published_parquet("pending_images_history_detail.parquet")
    history = pd.concat([seed, previous], ignore_index=True).drop_duplicates()

    current = pd.read_parquet(IMAGENES_PENDIENTES_CLEAN)[
        ["Cliente", "tipo", "Estado", "Cantidad"]
    ].drop_duplicates()
    current_cmp = current.sort_values(["Cliente", "tipo", "Estado", "Cantidad"]).reset_index(drop=True)

    append_snapshot = True
    if not history.empty and "Tiempo" in history.columns:
        history["Tiempo"] = pd.to_datetime(history["Tiempo"], errors="coerce")
        last_time = history["Tiempo"].max()
        last = history.loc[history["Tiempo"] == last_time, ["Cliente", "tipo", "Estado", "Cantidad"]]
        last_cmp = last.sort_values(["Cliente", "tipo", "Estado", "Cantidad"]).reset_index(drop=True)
        append_snapshot = not last_cmp.equals(current_cmp)

    if append_snapshot:
        now = pd.Timestamp.now(tz="America/Argentina/Buenos_Aires").tz_localize(None).floor("s")
        snapshot = current.copy()
        snapshot["Tiempo"] = now
        history = pd.concat([history, snapshot], ignore_index=True)

    history = history.drop_duplicates().sort_values(
        ["Tiempo", "Cliente", "Estado", "tipo"]
    )
    _prepare_for_publish(history).to_parquet(
        _output_path("pending_images_history_detail.parquet"),
        index=False,
    )


def build_camera_reports_detail(**kwargs):
    import pandas as pd

    _prepare_for_publish(pd.read_parquet(REPORTES_CAMARAS_CLEAN)).to_parquet(
        _output_path("camera_reports_detail.parquet"),
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
    hourly = dataframe.merge(
        daily_average,
        on=["Fecha", "Tipo de Cámara"],
        how="left",
    ).merge(
        monthly_average,
        on=["Mes", "Tipo de Cámara"],
        how="left",
    )
    _prepare_for_publish(hourly).to_parquet(
        _output_path("reviewer_hourly_detail.parquet"),
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
build_pending_images_history_detail_task = PythonOperator(
    task_id="build_pending_images_history_detail",
    python_callable=build_pending_images_history_detail,
    dag=transito_dag,
)
build_camera_reports_detail_task = PythonOperator(
    task_id="build_camera_reports_detail",
    python_callable=build_camera_reports_detail,
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
[transform_imagenes_pendientes_task, transform_historial_pendientes_task] >> build_pending_images_history_detail_task
transform_reportes_camaras_task >> build_camera_reports_detail_task
join_activity_review_time_task >> build_reviewer_hourly_detail_task

# La consulta de metadata de Drive corta todo el DAG cuando no cambió ninguna fuente.
check_tessa_sources_task >> [
    fetch_actividad_revisores_task,
    fetch_valores_uf_task,
    fetch_motivos_rechazos_task,
    fetch_tiempo_revision_task,
    fetch_imagenes_pendientes_task,
    fetch_historial_pendientes_task,
    fetch_reportes_camaras_task,
]

publish_tessa_dataset_task = PythonOperator(
    task_id="publish_tessa_dataset",
    python_callable=publish_tessa_dataset,
    dag=transito_dag,
)

[
    build_activity_reviewers_detail_task,
    build_rejection_reasons_detail_task,
    build_review_time_detail_task,
    build_pending_images_detail_task,
    build_pending_images_history_detail_task,
    build_camera_reports_detail_task,
    build_reviewer_hourly_detail_task,
] >> publish_tessa_dataset_task

