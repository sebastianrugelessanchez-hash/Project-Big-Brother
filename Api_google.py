# --- api_google.py (PATCH) -----------------------------------
from __future__ import annotations
from typing import Optional, Dict, Tuple, List
import os, io, time, mimetypes
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive.readonly"
]

# ========== (NUEVO) caches ==========
_CHUNK_SIZE = 2 * 1024 * 1024  # 2 MB para subidas y descargas
_SERVICE_CACHE: dict = {}      # cache del cliente Drive
class FolderIdCache(dict):
    """Cache simple de folderIds. Clave: tuple(path) o ('id', some_id)."""
    pass

def build_service(
    auth_mode: str = "service_account",
    sa_path: str = "service_account.json",
    credentials_path: str = "credentials.json",
    token_path: str = "token.json",
):
    """Devuelve un cliente Drive y LO CACHEA por parámetros de auth (re-uso)."""
    key = (auth_mode, os.path.abspath(sa_path), os.path.abspath(credentials_path), os.path.abspath(token_path))
    if key in _SERVICE_CACHE:
        return _SERVICE_CACHE[key]

    if auth_mode == "service_account":
        creds = service_account.Credentials.from_service_account_file(sa_path, scopes=SCOPES)
        svc = build("drive", "v3", credentials=creds)
    else:
        creds = None
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        if not creds or not creds.valid:
            from google.auth.transport.requests import Request
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
                creds = flow.run_local_server(port=0)
            with open(token_path, "w") as f:
                f.write(creds.to_json())
        svc = build("drive", "v3", credentials=creds)

    _SERVICE_CACHE[key] = svc
    return svc

def _with_retries(fn, *args, **kwargs):
    max_tries = kwargs.pop("_max_tries", 5)
    base_sleep = kwargs.pop("_base_sleep", 0.8)
    for attempt in range(1, max_tries + 1):
        try:
            return fn(*args, **kwargs)
        except HttpError as e:
            status = getattr(e, "status_code", None) or (e.resp.status if hasattr(e, "resp") else None)
            if status in (429, 500, 502, 503, 504) and attempt < max_tries:
                time.sleep(base_sleep * (2 ** (attempt - 1)))
                continue
            raise

# ====== (ACTUALIZADO) find_folder_id con cache opcional ======
def find_folder_id(
    service,
    *,
    folder_id: Optional[str] = None,
    name: Optional[str] = None,
    parent_id: Optional[str] = None,
    shared_drive_id: Optional[str] = None,
    cache: Optional[FolderIdCache] = None,
) -> str:
    """Busca/valida folderId. Usa cache si se provee."""
    if folder_id:
        if cache is not None:
            cache[('id', folder_id)] = folder_id
        return folder_id

    if not name:
        raise ValueError("Debes pasar folder_id o name para resolver la carpeta.")

    # clave de cache por (parent_id, name)
    cache_key = ('path', parent_id or 'ROOT', name)
    if cache is not None and cache_key in cache:
        return cache[cache_key]

    q = "mimeType = 'application/vnd.google-apps.folder' and trashed = false and name = '{}'".format(
        name.replace("'", r"\'")
    )
    if parent_id:
        q += f" and '{parent_id}' in parents"
    kwargs = {"q": q, "fields": "files(id, name, parents)", "pageSize": 50}
    if shared_drive_id:
        kwargs.update({
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
            "corpora": "drive",
            "driveId": shared_drive_id,
        })
    res = _with_retries(service.files().list, **kwargs).execute()
    files = res.get("files", [])
    if not files:
        raise FileNotFoundError(f"No se encontró la carpeta '{name}' (parent={parent_id}).")

    fid = files[0]["id"]
    if cache is not None:
        cache[cache_key] = fid
    return fid

# ====== (ACTUALIZADO) resolve_path usa cache ======
def resolve_path(
    service,
    *,
    shared_drive_id: str,
    path_segments: List[str],
    cache: Optional[FolderIdCache] = None,
) -> str:
    if not path_segments:
        raise ValueError("path_segments no puede ser vacío.")
    parent = None
    for seg in path_segments:
        parent = find_folder_id(service, name=seg, parent_id=parent, shared_drive_id=shared_drive_id, cache=cache)
    return parent

# ====== (ACTUALIZADO) get_region_folders usa cache ======
def get_region_folders(
    service,
    *,
    shared_drive_id: str,
    regions_root_id: Optional[str] = None,
    region: str = "",
    cache: Optional[FolderIdCache] = None,
) -> Dict[str, str]:
    region = region.strip().upper()
    valid = {"LMR", "MAM", "MAR", "WCR", "NER", "GLR"}
    if region not in valid:
        raise ValueError(f"Región inválida '{region}'. Debe ser una de {sorted(valid)}")

    if regions_root_id:
        regions_id = find_folder_id(service, folder_id=regions_root_id, shared_drive_id=shared_drive_id, cache=cache)
    else:
        regions_id = resolve_path(service, shared_drive_id=shared_drive_id, path_segments=["Regions"], cache=cache)

    region_id = find_folder_id(service, name=region, parent_id=regions_id, shared_drive_id=shared_drive_id, cache=cache)
    in_id     = find_folder_id(service, name="Entradas", parent_id=region_id, shared_drive_id=shared_drive_id, cache=cache)
    out_id    = find_folder_id(service, name="Resultados", parent_id=region_id, shared_drive_id=shared_drive_id, cache=cache)
    return {"region": region_id, "entradas": in_id, "resultados": out_id}

def _guess_mime(path: str) -> str:
    if path.lower().endswith(".xlsx"):
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    t, _ = mimetypes.guess_type(path)
    return t or "application/octet-stream"

def find_latest_file_by_prefix(
    service,
    *,
    parent_id: str,
    prefix: str,
    extension: str = ".xlsx",
    shared_drive_id: Optional[str] = None
) -> Optional[Dict]:
    q = f"'{parent_id}' in parents and trashed = false"
    kwargs = {
        "q": q,
        "fields": "files(id, name, modifiedTime)",
        "pageSize": 1000,
        "orderBy": "modifiedTime desc, name",
    }
    if shared_drive_id:
        kwargs.update({
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
            "corpora": "drive",
            "driveId": shared_drive_id,
        })
    res = _with_retries(service.files().list, **kwargs).execute()
    files = res.get("files", [])
    candidates = [f for f in files if f["name"].lower().startswith(prefix.lower())
                  and f["name"].lower().endswith(extension.lower())]
    return candidates[0] if candidates else None

def download_file(service, *, file_id: str, local_path: str, chunk_size: int = _CHUNK_SIZE) -> str:
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    req = service.files().get_media(fileId=file_id)
    fh = io.FileIO(local_path, "wb")
    downloader = MediaIoBaseDownload(fh, req, chunksize=chunk_size)
    done = False
    while not done:
        status, done = _with_retries(downloader.next_chunk, _max_tries=5)
    fh.close()
    return local_path

def _search_file_in_folder(service, *, filename: str, parent_id: str, shared_drive_id: Optional[str]):
    # Escape single quotes for use inside the Drive query string
    safe_filename = filename.replace("'", "\\'")
    safe_parent_id = parent_id.replace("'", "\\'") if parent_id is not None else parent_id
    q = f"name = '{safe_filename}' and '{safe_parent_id}' in parents and trashed = false"
    kwargs = {"q": q, "fields": "files(id, name)", "pageSize": 10}
    if shared_drive_id:
        kwargs.update({
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
            "corpora": "drive",
            "driveId": shared_drive_id,
        })
    res = _with_retries(service.files().list, **kwargs).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None

# ====== (ACTUALIZADO) subidas con chunksize =========
def upload_or_update_file(
    service,
    *,
    local_path: str,
    parent_folder_id: str,
    shared_drive_id: Optional[str] = None,
    mime_type: Optional[str] = None
) -> Tuple[str, str]:
    filename = os.path.basename(local_path)
    mime_type = mime_type or _guess_mime(local_path)
    media = MediaFileUpload(local_path, mimetype=mime_type, resumable=True, chunksize=_CHUNK_SIZE)
    common = {"supportsAllDrives": True} if shared_drive_id else {}

    file_id = _search_file_in_folder(service, filename=filename, parent_id=parent_folder_id, shared_drive_id=shared_drive_id)
    if file_id:
        updated = _with_retries(service.files().update, fileId=file_id, media_body=media, **common).execute()
        return updated["id"], "updated"
    else:
        meta = {"name": filename, "parents": [parent_folder_id]}
        created = _with_retries(service.files().create, body=meta, media_body=media, fields="id", **common).execute()
        return created["id"], "created"

def pull_input_from_drive(
    service,
    *,
    shared_drive_id: str,
    regions_root_id: Optional[str],
    region: str,
    fecha_tag: Optional[str],
    local_dir: str = "./tmp",
    cache: Optional[FolderIdCache] = None,
) -> Tuple[Optional[str], Optional[str]]:
    folders = get_region_folders(service, shared_drive_id=shared_drive_id, regions_root_id=regions_root_id, region=region, cache=cache)
    entradas_id = folders["entradas"]

    if fecha_tag:
        expected_name = f"ticket_reconciliation_{region}_{fecha_tag}.xlsx"
        found_id = _search_file_in_folder(service, filename=expected_name, parent_id=entradas_id, shared_drive_id=shared_drive_id)
        if not found_id:
            return None, None
        lp = os.path.join(local_dir, region, expected_name)
        return download_file(service, file_id=found_id, local_path=lp), expected_name
    else:
        prefix = f"ticket_reconciliation_{region}_"
        f = find_latest_file_by_prefix(service, parent_id=entradas_id, prefix=prefix, extension=".xlsx", shared_drive_id=shared_drive_id)
        if not f:
            return None, None
        lp = os.path.join(local_dir, region, f["name"])
        return download_file(service, file_id=f["id"], local_path=lp), f["name"]

def push_output_to_drive(
    service,
    *,
    shared_drive_id: str,
    regions_root_id: Optional[str],
    region: str,
    fecha_tag: str,
    local_output_path: str,
    cache: Optional[FolderIdCache] = None,
) -> Tuple[str, str]:
    folders = get_region_folders(service, shared_drive_id=shared_drive_id, regions_root_id=regions_root_id, region=region, cache=cache)
    resultados_id = folders["resultados"]
    target_name = f"resultado_reconciliation_{fecha_tag}{region}.xlsx"
    tmp_dir = os.path.dirname(local_output_path)
    desired_path = os.path.join(tmp_dir, target_name)
    if os.path.abspath(local_output_path) != os.path.abspath(desired_path):
        import shutil
        shutil.copy2(local_output_path, desired_path)
        local_output_path = desired_path

    return upload_or_update_file(
        service,
        local_path=local_output_path,
        parent_folder_id=resultados_id,
        shared_drive_id=shared_drive_id
    )
# --- fin PATCH ----------------------------------------------
