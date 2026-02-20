#!/usr/bin/env python3
import os
import hashlib
import requests
import psycopg2
from bs4 import BeautifulSoup

# -----------------------------
# Config
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
    """Parse event page to find the LENEX file link"""
    soup = BeautifulSoup(event_page_html, 'html.parser')
    # Find <a href="/file/{randomNumber}?event={event_id}"> with .lxf
    link = soup.find('a', href=True, text=lambda t: t and t.lower().endswith('.lxf'))
    if link:
        return BASE_URL + link['href']
    return None

# -----------------------------
# Connect to DB
# -----------------------------
conn = psycopg2.connect(
    host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
)
cur = conn.cursor()

# -----------------------------
# Fetch index page
# -----------------------------
resp = requests.get(BASE_URL)
resp.raise_for_status()
soup = BeautifulSoup(resp.text, 'html.parser')

# -----------------------------
# Find all events with checkmark
# -----------------------------
events = []
for ev in soup.select('.fas.fa-check'):
    # The checkmark is usually inside a row with a link to event page
    parent = ev.find_parent('a', href=True)
    if parent:
        href = parent['href']
        # Example href: /event/program?OnlineEventId=12345
        if 'OnlineEventId=' in href:
            event_id = href.split('OnlineEventId=')[1]
            events.append(event_id)

print(f"Found {len(events)} ready events.")

# -----------------------------
# Iterate events
# -----------------------------
for event_id in events:
    event_url = f"{BASE_URL}/event/program?OnlineEventId={event_id}"
    print(f"Processing event {event_id} ...")
    r = requests.get(event_url)
    r.raise_for_status()

    lenex_url = get_lenex_file_url(r.text, event_id)
    if not lenex_url:
        print(f"No LENEX file found for event {event_id}, skipping.")
        continue

    # Download file
    file_data = requests.get(lenex_url).content
    file_name = f"event_{event_id}.lxf"
    hash_digest = file_hash(file_data)

    # Skip if already imported
    cur.execute("SELECT 1 FROM lx_importedlenexfile WHERE filehash=%s", (hash_digest,))
    if cur.fetchone():
        print(f"{file_name} already imported, skipping.")
        continue

    # Save imported file record
    cur.execute(
        "INSERT INTO lx_importedlenexfile(filename, filehash) VALUES (%s,%s)",
        (file_name, hash_digest)
    )
    conn.commit()
    print(f"Imported {file_name}")

cur.close()
conn.close()
