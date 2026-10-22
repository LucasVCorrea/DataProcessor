# Pipeline Tessa

El DAG mantiene el nombre interno `transito_dag`, pero publica el dataset de Tessa en `processed/tessa`.

## Fuente en Google Drive

Se reutiliza la misma service account de Berisso. La carpeta real de Tessa es:

```text
data_files/tessa
ID: 1w-r6aZAX5oGqyPjSHO1SMS4XunAbIFEu
```

El DAG espera estas fuentes dentro de esa carpeta:

- `01-Actividad de revisores.xlsx`
- `02-Motivos de rechazo.xlsx`
- `03-Tiempo de revisión.xlsx`
- `valores ufs (1).xlsx`
- `Pendientes.csv`
- `Reportes_camaras.csv`
- `historial_completo.csv`

`historial_accesos.csv` ya no forma parte del pipeline. Los accesos se registran desde Aset en DynamoDB, igual que Berisso.

Variables principales:

```env
TESSA_DRIVE_FOLDER_ID=1w-r6aZAX5oGqyPjSHO1SMS4XunAbIFEu
TESSA_DRIVE_DATA_PATH=
TESSA_DATA_DIR=/data/transito
TESSA_S3_BUCKET=
TESSA_S3_PREFIX=processed/tessa
```

Si `TESSA_S3_BUCKET` queda vacío se reutiliza `BERISSO_S3_BUCKET`.

## Imágenes pendientes

La vista de imágenes pendientes se migró tomando como referencia la versión actual de `Tessa2.0`.

`Pendientes.csv` contiene el estado actual. `historial_completo.csv` se usa como semilla del historial existente. En cada ejecución, DataProcessor compara el estado actual con el último snapshot publicado y agrega una nueva fotografía únicamente cuando hay un cambio real. El historial resultante se publica como:

```text
pending_images_history_detail.parquet
```

De esta forma el historial no depende del filesystem efímero del contenedor de Streamlit.

## Flujo

```text
Google Drive / data_files/tessa
        ↓
check_tessa_sources
        ↓
lectura Excel/CSV
        ↓
transformaciones
        ↓
/data/transito/*.parquet
        ↓
processed/tessa/versions/<version>/*.parquet
        ↓
processed/tessa/_dataset_manifest.json
```

## Outputs publicados

- `activity_reviewers_detail.parquet`
- `rejection_reasons_detail.parquet`
- `review_time_detail.parquet`
- `pending_images_detail.parquet`
- `pending_images_history_detail.parquet`
- `camera_reports_detail.parquet`
- `reviewer_hourly_detail.parquet`
