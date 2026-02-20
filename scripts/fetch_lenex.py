#!/usr/bin/env python3
import os
from datetime import datetime
import requests
import psycopg2
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import sys

sys.stdout.reconfigure(line_buffering=True)

DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']

BASE_URL = 'https://live.musz.hu'

DOWNLOAD_DIR = 'lenex_files'
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def get_lenex_file_url(event_page_html, event_id):
    soup = BeautifulSoup(event_page_html, 'html.parser')
    for link in soup.find_all('a', href=True):
        if link.get('target') != '_blank':
            continue
        if link.get_text(strip=True) != 'LENEX':
            continue
        href = link['href']
        if href.startswith('/file/') and f'?event={event_id}' in href:
            return urljoin(BASE_URL, href)
    return None

try:
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    cur = conn.cursor()
    print("[DB] Connected to PostgreSQL", flush=True)
except Exception as e:
    print("[ERROR] Cannot connect to DB:", e, flush=True)
    sys.exit(1)

try:
    print("[HTTP] Fetched index page", flush=True)
    resp = requests.get(BASE_URL)
    resp.raise_for_status()
except Exception as e:
    print("[ERROR] Cannot fetch index page:", e, flush=True)
    sys.exit(1)

soup = BeautifulSoup(resp.text, 'html.parser')

events = []
for row in soup.find_all('tr'):
    first_td = row.find('td')
    if not first_td:
        continue

    icon = first_td.find('i', class_='fas fa-check')
    if not icon:
        continue

    tds = row.find_all('td')
    if len(tds) < 3:
        continue

    a_tag = tds[1].find('a', href=True)
    if not a_tag:
        continue

    href = a_tag['href']
    if 'OnlineEventId=' not in href:
        continue

    event_id = href.split('OnlineEventId=')[1].split('&')[0]
    event_name = a_tag.get_text(strip=True)

    # Parse dates from third column: "2026-06-13 - 2026-06-13" or "YYYY-MM-DD - YYYY-MM-DD"
    event_datefrom = None
    event_dateto = None
    dates_td = tds[2].get_text(strip=True)
    if dates_td and ' - ' in dates_td:
        parts = dates_td.split(' - ', 1)
        if len(parts) == 2:
            try:
                event_datefrom = datetime.strptime(parts[0].strip()[:10], '%Y-%m-%d').date()
                event_dateto = datetime.strptime(parts[1].strip()[:10], '%Y-%m-%d').date()
            except ValueError:
                pass

    events.append({
        'id': event_id,
        'name': event_name,
        'datefrom': event_datefrom,
        'dateto': event_dateto
    })

print(f"[INFO] Found {len(events)} ready events on the index page", flush=True)

# -----------------------------
# Phase 1: Create events in DB if not exists
# -----------------------------
for ev in events:
    event_id = ev['id']
    event_name = ev['name']
    event_datefrom = ev.get('datefrom')
    event_dateto = ev.get('dateto')
    try:
        cur.execute(
            """INSERT INTO importedlenexfile(eventid, eventname, eventdatefrom, eventdateto)
               VALUES (%s,%s,%s,%s)
               ON CONFLICT (eventid) DO UPDATE SET
                 eventname = EXCLUDED.eventname,
                 eventdatefrom = EXCLUDED.eventdatefrom,
                 eventdateto = EXCLUDED.eventdateto""",
            (event_id, event_name, event_datefrom, event_dateto)
        )
        conn.commit()
    except Exception as e:
        print(f"[ERROR] Cannot insert event {event_id} into DB: {e}", flush=True)
        conn.rollback()

# -----------------------------
# Phase 2: Lookup events from DB where not downloaded and lenex_not_found not set
# -----------------------------
cur.execute(
    """SELECT eventid, eventname FROM importedlenexfile
       WHERE downloaded IS NOT TRUE AND (lenex_not_found IS NOT TRUE)
       ORDER BY eventid"""
)
pending = cur.fetchall()
print(f"[INFO] Found {len(pending)} events pending download", flush=True)

# -----------------------------
# Phase 3: Process pending events
# -----------------------------
for event_id, event_name in pending:
    event_name = event_name or event_id
    file_name = f"event_{event_id}.lef"
    file_path = os.path.join(DOWNLOAD_DIR, file_name)

    print(f"[EVENT] Processing {event_name} ({event_id})", flush=True)

    if os.path.exists(file_path):
        print(f"[SKIP] {file_name} already exists, skipping fetch and download", flush=True)
        try:
            cur.execute(
                "UPDATE importedlenexfile SET filename=%s, downloaded=TRUE WHERE eventid=%s",
                (file_name, event_id)
            )
            conn.commit()
            print(f"[IMPORTED] {file_name} ({event_name})", flush=True)
        except Exception as e:
            print(f"[ERROR] Cannot update {event_id}: {e}", flush=True)
            conn.rollback()
        continue

    event_url = f"{BASE_URL}/event/program?OnlineEventId={event_id}"

    try:
        r = requests.get(event_url)
        r.raise_for_status()
        print(f"[HTTP] Fetched event page for {event_id}", flush=True)
    except Exception as e:
        print(f"[ERROR] Cannot fetch event page for {event_id}: {e}", flush=True)
        continue

    lenex_url = get_lenex_file_url(r.text, event_id)
    if not lenex_url:
        print(f"[SKIP] No LENEX file found for {event_name} ({event_id})", flush=True)
        try:
            cur.execute(
                "UPDATE importedlenexfile SET lenex_not_found=TRUE WHERE eventid=%s",
                (event_id,)
            )
            conn.commit()
        except Exception as e:
            print(f"[ERROR] Cannot update lenex_not_found for {event_id}: {e}", flush=True)
            conn.rollback()
        continue

    print(f"[DOWNLOAD] {lenex_url}", flush=True)
    temp_path = file_path + '.tmp'
    try:
        file_data = requests.get(lenex_url).content
        with open(temp_path, 'wb') as f:
            f.write(file_data)
        os.rename(temp_path, file_path)
        print(f"[SAVED] {file_name}", flush=True)
    except Exception as e:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        print(f"[ERROR] Cannot download LENEX file for {event_id}: {e}", flush=True)
        continue

    try:
        cur.execute(
            "UPDATE importedlenexfile SET filename=%s, url=%s, downloaded=TRUE WHERE eventid=%s",
            (file_name, lenex_url, event_id)
        )
        conn.commit()
        print(f"[IMPORTED] {file_name} ({event_name})", flush=True)
    except Exception as e:
        print(f"[ERROR] Cannot update downloaded status for {event_id}: {e}", flush=True)
        conn.rollback()

cur.close()
conn.close()
print("[DONE] LENEX fetch finished", flush=True)