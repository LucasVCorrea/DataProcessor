import json
import os
from io import BytesIO, StringIO

import pandas as pd
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.readonly"
DRIVE_API = "https://www.googleapis.com/drive/v3"
GOOGLE_SHEETS_MIME = "application/vnd.google-apps.spreadsheet"
GOOGLE_FOLDER_MIME = "application/vnd.google-apps.folder"
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _credentials():
    raw_credentials = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if raw_credentials:
        info = json.loads(raw_credentials)
        return service_account.Credentials.from_service_account_info(
            info,
            scopes=[DRIVE_SCOPE],
        )

    path = os.getenv("GOOGLE_CREDENTIALS_PATH", "/credentials/credentials.json")
    return service_account.Credentials.from_service_account_file(
        path,
        scopes=[DRIVE_SCOPE],
    )


def _session():
    return AuthorizedSession(_credentials())


def _default_folder_id():
    folder_id = os.getenv("BERISSO_DRIVE_FOLDER_ID")
    if not folder_id:
        raise RuntimeError(
            "No se indicó folder_id y BERISSO_DRIVE_FOLDER_ID no está configurado"
        )
    return folder_id


def list_folder(folder_id=None):
    folder_id = folder_id or _default_folder_id()
    session = _session()
    query = f"'{folder_id}' in parents and trashed = false"
    fields = "nextPageToken,files(id,name,mimeType,modifiedTime,md5Checksum,size)"

    files = []
    page_token = None
    while True:
        params = {
            "q": query,
            "fields": fields,
            "pageSize": 1000,
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
        }
        if page_token:
            params["pageToken"] = page_token

        response = session.get(f"{DRIVE_API}/files", params=params, timeout=60)
        response.raise_for_status()
        payload = response.json()
        files.extend(payload.get("files", []))
        page_token = payload.get("nextPageToken")
        if not page_token:
            break

    result = {}
    for item in files:
        name = item["name"]
        if name in result:
            raise ValueError(f"Hay más de un archivo llamado {name!r} en la carpeta de Drive")
        result[name] = item
    return result


def resolve_folder_path(root_folder_id, relative_path):
    """Resuelve una ruta de carpetas relativa a un folder id de Google Drive."""
    current_id = root_folder_id
    parts = [part for part in str(relative_path or "").replace("\\", "/").split("/") if part]
    for part in parts:
        items = list_folder(current_id)
        item = items.get(part)
        if not item or item.get("mimeType") != GOOGLE_FOLDER_MIME:
            raise FileNotFoundError(
                f"No se encontró la carpeta {part!r} dentro de la ruta {relative_path!r}"
            )
        current_id = item["id"]
    return current_id


def folder_manifest(expected_files, folder_id=None):
    files = list_folder(folder_id)
    missing = [name for name in expected_files if name not in files]
    if missing:
        raise FileNotFoundError(
            "Faltan archivos en la carpeta de Drive: " + ", ".join(sorted(missing))
        )

    return {
        name: {
            "id": files[name]["id"],
            "mimeType": files[name].get("mimeType"),
            "modifiedTime": files[name].get("modifiedTime"),
            "md5Checksum": files[name].get("md5Checksum"),
            "size": files[name].get("size"),
        }
        for name in sorted(expected_files)
    }


def _decode_csv_content(content):
    # Google Drive entrega los bytes tal como fueron subidos. Los CSV exportados
    # desde Excel/Windows pueden venir en UTF-8, UTF-16 o Windows-1252.
    if content.startswith((b"\xff\xfe", b"\xfe\xff")):
        return content.decode("utf-16")

    try:
        return content.decode("utf-8-sig")
    except UnicodeDecodeError:
        return content.decode("cp1252")


def _download_bytes(file_id, mime_type=None):
    session = _session()

    if mime_type == GOOGLE_SHEETS_MIME:
        response = session.get(
            f"{DRIVE_API}/files/{file_id}/export",
            params={"mimeType": XLSX_MIME},
            timeout=120,
        )
    else:
        response = session.get(
            f"{DRIVE_API}/files/{file_id}",
            params={"alt": "media", "supportsAllDrives": "true"},
            timeout=120,
        )

    response.raise_for_status()
    return response.content


def read_csv(file_name, folder_id=None, low_memory=False, file_id=None, dtype="string"):
    mime_type = None
    if file_id is None:
        files = list_folder(folder_id)
        if file_name not in files:
            raise FileNotFoundError(f"No se encontró {file_name!r} en la carpeta de Drive")
        file_id = files[file_name]["id"]
        mime_type = files[file_name].get("mimeType")

    content = _download_bytes(file_id, mime_type=mime_type)
    text = _decode_csv_content(content)
    return pd.read_csv(StringIO(text), low_memory=low_memory, dtype=dtype)


def read_excel(
    file_name,
    folder_id=None,
    file_id=None,
    mime_type=None,
    sheet_name=0,
    dtype="string",
):
    """Lee un Excel de Drive; también exporta Google Sheets a XLSX si hiciera falta."""
    if file_id is None:
        files = list_folder(folder_id)
        if file_name not in files:
            raise FileNotFoundError(f"No se encontró {file_name!r} en la carpeta de Drive")
        item = files[file_name]
        file_id = item["id"]
        mime_type = item.get("mimeType")

    content = _download_bytes(file_id, mime_type=mime_type)
    return pd.read_excel(BytesIO(content), sheet_name=sheet_name, dtype=dtype)
