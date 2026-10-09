# Migración Berisso

## Responsabilidades

- `Aset/berisso`: interfaz Streamlit. No lee CSV de origen ni hace limpieza pesada.
- `DataProcessor/projects/aset/berisso`: lee los CSV de una única carpeta de Google Drive, limpia, valida y genera Parquet.
- AWS S3: almacenamiento de los Parquet publicados. El dashboard lee únicamente este dataset procesado.
- DynamoDB: historial de accesos cuando `BERISSO_AUDIT_TABLE` está configurada.

## Archivos obligatorios en Drive

La carpeta indicada por `BERISSO_DRIVE_FOLDER_ID` debe contener exactamente estos nombres:

- `Detalle de actas pagas.csv`
- `detalles_de_infracciones.csv`
- `Actas por Tipo de Infraccion.csv`
- `Fallos.csv`
- `Actas Notificada bajo puerta info extra.csv`
- `Actividad_revisores_nivel_5.csv`
- `Listado de actas notificadas.csv`
- `Listado de actas notificadas email.csv`

El service account de Google debe tener acceso de lector a esa carpeta.

`Actas enviadas al correo.csv` ya no forma parte del pipeline. Las actas generadas del dashboard se calculan desde `Actas por Tipo de Infraccion.csv` y las actas notificadas desde los listados de notificaciones por email y bajo puerta.

## Actualización de datos

`berisso_dag` consulta la metadata de Drive una vez por minuto. Si no cambió ningún archivo, el DAG corta y no vuelve a procesar los datos. Si cambió algo, procesa el dataset, genera los Parquet y, si `BERISSO_S3_BUCKET` está configurado, publica el conjunto en S3. El manifest se actualiza al final del procesamiento.

Aset consulta el manifest cada pocos segundos. Cuando detecta una versión nueva invalida su cache y vuelve a cargar los Parquet.

En S3 cada publicación se escribe en un prefijo versionado (`processed/berisso/versions/<version>/`). El manifest estable se reemplaza recién cuando todos los Parquet de esa versión terminaron de subir. De esa forma Aset nunca mezcla archivos de dos ejecuciones distintas.

## Variables de DataProcessor

Copiar `.env.example` a `.env` para desarrollo local y completar como mínimo:

```env
BERISSO_DRIVE_FOLDER_ID=<folder-id>
GOOGLE_CREDENTIALS_PATH=/credentials/credentials.json
BERISSO_DATA_DIR=/data/berisso
```

Para AWS se agregan:

```env
BERISSO_S3_BUCKET=<bucket>
BERISSO_S3_PREFIX=processed/berisso
AWS_REGION=<region>
```

En AWS puede usarse `GOOGLE_SERVICE_ACCOUNT_JSON` desde Secrets Manager en lugar del archivo de credenciales.

## Variables de Aset

Para trabajar contra los Parquet locales:

```env
BERISSO_DATA_DIR=/data/berisso
```

Para AWS:

```env
BERISSO_S3_BUCKET=<bucket>
BERISSO_S3_PREFIX=processed/berisso
BERISSO_AUDIT_TABLE=<dynamodb-table>
AWS_REGION=<region>
```

## Validaciones de datos

El pipeline falla intencionalmente si `detalles_de_infracciones.csv` contiene dos tarifas para la misma combinación de año, mes y tipo de falta, o si existen infracciones sin una tarifa correspondiente. Esto evita publicar montos `Generado` incorrectos.

En el archivo recibido para esta migración se detectaron dos problemas que deben corregirse en el CSV de Drive antes de la primera ejecución:

1. Falta `Invasion de senda` para enero de 2025. El dashboard anterior agregaba esa fila manualmente desde Python; ahora debe vivir en la fuente.
2. Febrero de 2026 tiene dos juegos de tarifas para los cuatro tipos de infracción, uno con UF 1807 y otro con UF 1896. Hay que conservar únicamente el juego que corresponda según la fuente oficial.

## Dependencias

Se agregó `boto3` para S3/DynamoDB y `google-auth` para leer archivos binarios de Drive. Como los ZIP recibidos traen un `uv.lock` anterior, después de copiar los cambios hay que regenerarlo en cada repo:

```powershell
uv lock
```

Luego:

```powershell
uv sync
```

## GitHub

Los workflows anteriores dependían de `Asterius-Dev/CICD` y de un runner `unlam`. Se dejaron deshabilitados y se agregaron workflows propios para CI y plantillas de deploy AWS.

Los deploys necesitan configurar en GitHub un `AWS_ROLE_ARN` para OIDC y las variables AWS indicadas por los workflows. Esos valores se completan después de crear la infraestructura.

## Cambio de repositorio GitHub

Conviene crear los repositorios nuevos como privados al principio. Para conservar el remoto anterior como referencia y pasar a trabajar contra el repositorio propio:

```powershell
cd C:\Users\Usuario\PycharmProjects\Aset
git remote -v
git remote rename origin upstream
git remote add origin https://github.com/<TU_USUARIO_O_ORG>/<NUEVO_REPO_ASET>.git
git push -u origin main
git push origin --all
git push origin --tags
```

Si no se quiere conservar el remoto anterior:

```powershell
git remote set-url origin https://github.com/<TU_USUARIO_O_ORG>/<NUEVO_REPO_ASET>.git
git push -u origin main
```

El mismo patrón puede aplicarse a DataProcessor. No hay que copiar `.git` desde los ZIP entregados: los paquetes de migración contienen solamente el árbol de trabajo.

`berisso/src/logs/config.yaml` queda ignorado porque contiene la autenticación local actual. Se incluye `berisso/src/logs/config.example.yaml` únicamente como plantilla sin credenciales. No debe subirse al repositorio nuevo. El código también acepta `BERISSO_AUTH_CONFIG_JSON`, pensado para que ECS reciba la configuración desde AWS Secrets Manager sin guardar credenciales en Git. Si ya estaba trackeado en un repositorio local, conservar el archivo local y sacarlo del índice con:

```powershell
git rm --cached berisso/src/logs/config.yaml
git commit -m "chore: stop tracking local auth config"
```

En AWS la autenticación local se reemplazará por Cognito. El historial local `historial_accesos.csv` tampoco se versiona; en AWS se usa DynamoDB.

## Flujo de Pull Request

Para cada cambio de código:

```powershell
git switch main
git pull origin main
git switch -c nombre-del-cambio
# editar archivos
git add .
git commit -m "tipo: descripcion"
git push -u origin nombre-del-cambio
```

Luego se abre un Pull Request en GitHub hacia `main`. El workflow `CI` valida el código. Cuando el PR se mergea, el workflow de deploy correspondiente construye la imagen y actualiza AWS.

Los workflows de deploy incluidos son plantillas listas para completar una vez creados ECR/ECS/EC2 y el rol OIDC de GitHub. No crean por sí solos la infraestructura AWS.

## Orden recomendado de puesta en marcha

1. Corregir los dos problemas de `detalles_de_infracciones.csv` indicados arriba y completar los ocho CSV en Drive.
2. Crear los repositorios propios y cambiar los remotos.
3. Regenerar `uv.lock` en Aset y DataProcessor con Python 3.14.
4. Probar localmente Drive -> DataProcessor -> Parquet -> Aset usando `BERISSO_DATA_DIR`.
5. Crear S3 y probar DataProcessor -> S3 -> Aset.
6. Crear DynamoDB para auditoría.
7. Crear ECR/ECS/ALB/ACM/Route53 para Aset y la instancia de DataProcessor administrada por SSM.
8. Configurar OIDC y variables de GitHub; recién entonces habilitar los deploys automáticos.
9. Sustituir la autenticación local de Streamlit por Cognito para producción.
