#!/usr/bin/env python3
import os
import psycopg2
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import sys

sys.stdout.reconfigure(line_buffering=True)

DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
FOLDER_ID = '15XguFYYvif-iu9sLwKUcTgbetOgFRB2o'

DOWNLOAD_DIR = 'lenex_files'

print("[INIT] Starting Google Drive backup...", flush=True)

# -----------------------------
# DB CONNECTION
# -----------------------------
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    print("[DB] Connected to PostgreSQL", flush=True)
except Exception as e:
    print("[ERROR] Cannot connect to DB:", e, flush=True)
    sys.exit(1)

# -----------------------------
# GOOGLE DRIVE AUTH (OAUTH)
# -----------------------------
try:
    creds = Credentials.from_authorized_user_file(
        '/secrets/token.json',
        ['https://www.googleapis.com/auth/drive.file']
    )

    service = build(
        'drive',
        'v3',
        credentials=creds,
        cache_discovery=False
    )

    print("[AUTH] Google Drive OAuth authentication successful", flush=True)
except Exception as e:
    print("[ERROR] Google Drive auth failed:", e, flush=True)
    sys.exit(1)

# -----------------------------
# QUERY FILES NOT UPLOADED
# -----------------------------
try:
    cur.execute("""
        SELECT filename
        FROM importedlenexfile
        WHERE filename IS NOT NULL AND gdrive_uploaded IS DISTINCT FROM TRUE
    """)
    files = cur.fetchall()
    print(f"[INFO] {len(files)} files to upload", flush=True)
except Exception as e:
    print("[ERROR] Cannot query DB:", e, flush=True)
    sys.exit(1)

# -----------------------------
# UPLOAD LOOP
# -----------------------------
for row in files:
    fname = row[0]
    file_path = os.path.join(DOWNLOAD_DIR, fname)

    if not os.path.exists(file_path):
        print(f"[WARN] {file_path} does not exist locally, skipping", flush=True)
        continue

    print(f"[UPLOAD] Uploading {fname}", flush=True)

    try:
        file_metadata = {
            'name': fname,
            'parents': [FOLDER_ID]
        }

        media = MediaFileUpload(
            file_path,
            mimetype='application/octet-stream'
        )

        uploaded = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()

        print(f"[OK] Uploaded {fname} with ID {uploaded.get('id')}", flush=True)

        cur.execute("""
            UPDATE importedlenexfile
            SET gdrive_uploaded = TRUE
            WHERE filename = %s
        """, (fname,))
        conn.commit()

    except Exception as e:
        print(f"[ERROR] Failed to upload {fname}: {e}", flush=True)
        conn.rollback()

cur.close()
conn.close()

print("[DONE] Google Drive backup finished", flush=True)