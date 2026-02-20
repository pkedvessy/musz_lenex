#!/usr/bin/env python3
import os
import hashlib
import requests
import psycopg2
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import sys

# -----------------------------
# Enable unbuffered output for Jenkins
# -----------------------------
sys.stdout.reconfigure(line_buffering=True)

# -----------------------------
# Configuration from env
# -----------------------------
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']

BASE_URL = 'https://live.musz.hu'

# -----------------------------
# Helpers
# -----------------------------
def file_hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()

def get_lenex_file_url(event_page_html, event_id):
    """Parse event page to find the LENEX file link with the /file/{random}?event={eventId} pattern"""
    soup = BeautifulSoup(event_page_html, 'html.parser')
    
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.startswith('/file/') and f'?event={event_id}' in href:
            full_url = urljoin(BASE_URL, href)
            return full_url
    return None

# -----------------------------
# Connect to PostgreSQL
# -----------------------------
try:
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    cur = conn.cursor()
    print("[DB] Connected to PostgreSQL", flush=True)
except Exception as e:
    print("[ERROR] Cannot connect to DB:", e, flush=True)
    sys.exit(1)

# -----------------------------
# Fetch index page
# -----------------------------
try:
    resp = requests.get(BASE_URL)
    resp.raise_for_status()
    print("[HTTP] Fetched index page", flush=True)
except Exception as e:
    print("[ERROR] Cannot fetch index page:", e, flush=True)
    sys.exit(1)

soup = BeautifulSoup(resp.text, 'html.parser')

# -----------------------------
# Find ready events (checkmark)
# -----------------------------
events = []
for row in soup.find_all('tr'):
    first_td = row.find('td')
    if not first_td:
        continue

    icon = first_td.find('i', class_='fas fa-check')
    if not icon:
        continue  # Skip events without checkmark

    tds = row.find_all('td')
    if len(tds) < 2:
        continue

    a_tag = tds[1].find('a', href=True)
    if not a_tag:
        continue

    href = a_tag['href']
    if 'OnlineEventId=' in href:
        event_id = href.split('OnlineEventId=')[1]
        event_name = a_tag.get_text(strip=True)
        events.append({'id': event_id, 'name': event_name})

print(f"[INFO] Found {len(events)} ready events", flush=True)

# -----------------------------
# Process each event
# -----------------------------
for ev in events:
    event_id = ev['id']
    event_name = ev['name']
    event_url = f"{BASE_URL}/event/program?OnlineEventId={event_id}"
    print(f"[EVENT] Processing {event_name} ({event_id}) at {event_url}", flush=True)

    try:
        r = requests.get(event_url)
        r.raise_for_status()
    except Exception as e:
        print(f"[ERROR] Cannot fetch event page for {event_id}: {e}", flush=True)
        continue

    lenex_url = get_lenex_file_url(r.text, event_id)
    if not lenex_url:
        print(f"[SKIP] No LENEX file found for {event_name} ({event_id})", flush=True)
        continue

    print(f"[DOWNLOAD] LENEX file URL: {lenex_url}", flush=True)

    try:
        file_data = requests.get(lenex_url).content
        file_name = f"event_{event_id}.lxf"
        hash_digest = file_hash(file_data)
    except Exception as e:
        print(f"[ERROR] Cannot download LENEX file for {event_id}: {e}", flush=True)
        continue

    # -----------------------------
    # Check if already imported
    # -----------------------------
    cur.execute("SELECT 1 FROM importedlenexfile WHERE filehash=%s", (hash_digest,))
    if cur.fetchone():
        print(f"[SKIP] {file_name} already imported", flush=True)
        continue

    # -----------------------------
    # Store imported file info
    # -----------------------------
    try:
        cur.execute(
            "INSERT INTO importedlenexfile(filename, filehash) VALUES (%s,%s)",
            (file_name, hash_digest)
        )
        conn.commit()
        print(f"[IMPORTED] {file_name} ({event_name})", flush=True)
    except Exception as e:
        print(f"[ERROR] Cannot insert {file_name} into DB: {e}", flush=True)
        conn.rollback()

cur.close()
conn.close()
print("[DONE] LENEX import finished", flush=True)