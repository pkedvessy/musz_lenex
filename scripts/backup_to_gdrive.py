#!/usr/bin/env python3
import os
import psycopg2
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import sys

sys.stdout.reconfigure(line_buffering=True)

# -----------------------------
# DB config
# -----------------------------
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']

# -----------------------------
# Google Drive config
# -----------------------------
GDRIVE_CREDENTIAL_FILE = '/secrets/gdrive.json'  # will mount Jenkins secret here
GDRIVE_FOLDER_ID = '15XguFYYvif-iu9sLwKUcTgbetOgFRB2o'  # folder to put files

# -----------------------------
# Connect DB
# -----------------------------
conn = psycopg2.connect(
    host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
)
cur = conn.cursor()

# -----------------------------
# Get newly imported LENEX files
# -----------------------------
cur.execute("SELECT filename FROM importedlenexfile WHERE uploaded_to_drive IS DISTINCT FROM TRUE")
rows = cur.fetchall()

if not rows:
    print("[INFO] No new LENEX files to upload")
    sys.exit(0)

files_to_upload = [r[0] for r in rows]

print(f"[INFO] {len(files_to_upload)} files to upload to Google Drive", flush=True)

# -----------------------------
# Authenticate Google Drive
# -----------------------------
creds = service_account.Credentials.from_service_account_file(
    GDRIVE_CREDENTIAL_FILE,
    scopes=["https://www.googleapis.com/auth/drive.file"]
)
drive_service = build('drive', 'v3', credentials=creds)

# -----------------------------
# Upload files
# -----------------------------
for fname in files_to_upload:
    if not os.path.exists(fname):
        print(f"[WARN] File {fname} does not exist locally, skipping", flush=True)
        continue

    media = MediaFileUpload(fname, mimetype='application/octet-stream')
    file_metadata = {'name': os.path.basename(fname)}
    if GDRIVE_FOLDER_ID:
        file_metadata['parents'] = [GDRIVE_FOLDER_ID]

    try:
        drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"[UPLOADED] {fname} to Google Drive", flush=True)
        # mark as uploaded in DB
        cur.execute("UPDATE importedlenexfile SET uploaded_to_drive=TRUE WHERE filename=%s", (fname,))
        conn.commit()
    except Exception as e:
        print(f"[ERROR] Failed to upload {fname}: {e}", flush=True)
        conn.rollback()

cur.close()
conn.close()
print("[DONE] Google Drive backup finished", flush=True)