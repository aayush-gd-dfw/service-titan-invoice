import os, sys, time, json, logging, io, pickle, re
from typing import Dict, List

import requests
import pandas as pd
from dateutil.parser import isoparse
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload


CLIENT_ID    = "cid.lz91bsv6oyhzq29ceb0r9g80z"
CLIENT_SECRET= "cs1.dzmosw0zu9jlhl5e0ymqkqpd04adtbc0y1am5tpugzfglcom47"
#APP_KEY      = "ak1.nb1udeer5otcqp6yz34f50dq9"
#TENANT_ID    = "875946535"
#ENV = "production"
APP_KEY      = "ak1.nb1udeer5otcqp6yz34f50dq9"
TENANT_ID    = "875946535"
TOKEN_URL = "https://auth.servicetitan.io/connect/token"
# Google Drive service‑account JSON file (downloaded from GCP Console)
GDRIVE_SA_PATH    = r"C:\Users\Aayush Patil\Desktop\Asset-Panda-GoLive-Dashboard\ap-asset-action-report-234928d69ebd.json"

# Google Drive folder to hold the CSV  (copy the ID from the URL)
GDRIVE_FOLDER_ID  = "1t-vJ8IV2b4ebHA1T2SCoPLUMT26JgzjM"

CSV_FILENAME      = "invoice_data_v2.csv"
SINCE_ISO          = "2000-01-01T00:00:00Z"  
# ── Constants ─────────────────────────────────────────────────────────────────
PAGE_SIZE   = 500
PAGE_BATCH  = 100  
TOKEN_URL         = "https://auth.servicetitan.io/connect/token"
#EXPORT_URL         = f"https://api.servicetitan.io/telecom/v3/tenant/{TENANT_ID}/calls"
CLIENT_SECRET_FILE = r"C:\Users\Aayush Patil\Downloads\client_secret_740594783744-bgsb4ditlgb5u4b7d63sosj8ku7l50ba.apps.googleusercontent.com.json"
TOKEN_PICKLE       = "token.pkl"              # cached user credentials (auto‑created)
SCOPES             = ["https://www.googleapis.com/auth/drive.file"]

BASE_URL            = f"https://api.servicetitan.io/accounting/v2/tenant/{TENANT_ID}/invoices"
START_DATE         = "2000-01-01"

LAST_PAGE_FILE     = "last_page.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%H:%M:%S",
)

def camel_to_snake(name: str) -> str:
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
    return s2.replace(".", "_").lower()

def get_access_token() -> str:
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
        timeout=30
    )
    response.raise_for_status()
    return response.json()["access_token"]

def drive_service():
    creds = None
    if os.path.exists(TOKEN_PICKLE):
        with open(TOKEN_PICKLE, "rb") as f:
            creds = pickle.load(f)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_PICKLE, "wb") as f:
            pickle.dump(creds, f)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def find_file_id(svc, name, folder_id):
    q = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    files = svc.files().list(q=q, spaces="drive", fields="files(id)").execute().get("files", [])
    return files[0]["id"] if files else None

def read_drive_csv(svc, fid):
    buf = io.BytesIO()
    req = svc.files().get_media(fileId=fid)
    downloader = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buf.seek(0)
    try:
        return pd.read_csv(buf, low_memory=False)
    except pd.errors.EmptyDataError:
        logging.warning("CSV file is empty or malformed. Returning empty DataFrame.")
        return pd.DataFrame()

def append_drive_csv(svc, df, fid):
    existing_df = read_drive_csv(svc, fid)
    df = df[~df['id'].isin(existing_df['id'])] if 'id' in existing_df.columns else df
    updated_df = pd.concat([existing_df, df], ignore_index=True)
    buf = io.BytesIO()
    updated_df.to_csv(buf, index=False)
    buf.seek(0)
    media = MediaIoBaseUpload(buf, mimetype="text/csv", resumable=True)
    for _ in range(5):
        try:
            svc.files().update(fileId=fid, media_body=media).execute()
            return
        except Exception as e:
            logging.warning("Write attempt failed: %s", e)
            time.sleep(5)
    raise Exception("All retry attempts to update Drive file failed.")

def load_last_page():
    if os.path.exists(LAST_PAGE_FILE):
        with open(LAST_PAGE_FILE) as f:
            return int(f.read().strip())
    return 1

def save_last_page(page: int):
    with open(LAST_PAGE_FILE, "w") as f:
        f.write(str(page))

def fetch_and_store_customers():
    svc = drive_service()
    fid = find_file_id(svc, CSV_FILENAME, GDRIVE_FOLDER_ID)
    if not fid:
        logging.warning("File %s not found in Drive – creating new one.", CSV_FILENAME)
        df_empty = pd.DataFrame()
        buf = io.BytesIO(); df_empty.to_csv(buf, index=False); buf.seek(0)
        media = MediaIoBaseUpload(buf, mimetype="text/csv", resumable=True)
        meta = {
            "name": CSV_FILENAME,
            "parents": [GDRIVE_FOLDER_ID],
            "mimeType": "text/csv"
        }
        file = svc.files().create(body=meta, media_body=media).execute()
        fid = file["id"]

    page_size = 500
    page = load_last_page()
    all_batches = []

    while True:
        if page % 10 == 1:
            access_token = get_access_token()
            headers = {"Authorization": f"Bearer {access_token}", "ST-App-Key": APP_KEY}

        url = f"{BASE_URL}?page={page}&pageSize={page_size}"
        try:
            logging.info("Fetching page %s", page)
            response = requests.get(url, headers=headers, timeout=60)
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error("✖ Failed to fetch page %s: %s", page, e)
            break

        page_data = response.json().get("data", [])
        if not page_data:
            logging.info("Page %s returned no data. Stopping.", page)
            break

        df = pd.json_normalize(page_data)
        df.columns = [camel_to_snake(c) for c in df.columns]
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

        all_batches.append(df)

        if page % 10 == 0:
            if all_batches:
                new_data = pd.concat(all_batches, ignore_index=True)
                append_drive_csv(svc, new_data, fid)
                save_last_page(page + 1)
                logging.info("✓ Appended %s records from pages %s–%s to Google Drive.", len(new_data), page - 9, page)
                all_batches.clear()
        page += 1
        time.sleep(0.3)

    if all_batches:
        new_data = pd.concat(all_batches, ignore_index=True)
        append_drive_csv(svc, new_data, fid)
        save_last_page(page)
        logging.info("✓ Appended final %s records from pages ending at %s.", len(new_data), page - 1)

if __name__ == "__main__":
    try:
        fetch_and_store_customers()
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")