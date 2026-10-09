import json
import os
from io import BytesIO

import pandas as pd
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.readonly"
DRIVE_API = "https://www.googleapis.com/drive/v3"


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


def list_folder(folder_id=None):
    folder_id = folder_id or os.environ["BERISSO_DRIVE_FOLDER_ID"]
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
            "modifiedTime": files[name].get("modifiedTime"),
            "md5Checksum": files[name].get("md5Checksum"),
            "size": files[name].get("size"),
        }
        for name in sorted(expected_files)
    }


def read_csv(file_name, folder_id=None, low_memory=False, file_id=None, dtype="string"):
    if file_id is None:
        files = list_folder(folder_id)
        if file_name not in files:
            raise FileNotFoundError(f"No se encontró {file_name!r} en la carpeta de Drive")
        file_id = files[file_name]["id"]

    session = _session()
    response = session.get(
        f"{DRIVE_API}/files/{file_id}",
        params={"alt": "media", "supportsAllDrives": "true"},
        timeout=120,
    )
    response.raise_for_status()
    return pd.read_csv(BytesIO(response.content), low_memory=low_memory, dtype=dtype)
