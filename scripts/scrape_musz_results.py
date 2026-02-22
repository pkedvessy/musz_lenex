#!/usr/bin/env python3
"""
Scrape swimming results from live.musz.hu when LENEX file is not available.

Looks up event IDs from importedlenexfile where filename IS NULL and status
= 'lenex_not_found', then scrapes each. Sets status='scraped' on success,
status='scrape_failed' on failure (no retry).

Parses:
- event/program - session and event structure
- event/eventdata - meet metadata
- event/startlist - heat numbers (HeatId in links) for result heatnumber
- event/summary - all results per event, per CategoryId (includes non-ranked from age categories)

Hungarian mappings: pillangó=FLY, hát=BACK, mell=BREAST, gyors=FREE, vegyes=IM
                   férfi=M, női=F
"""
import os
import re
import traceback
from datetime import datetime, date
from urllib.parse import urljoin, urlparse, parse_qs
import requests
from bs4 import BeautifulSoup
import psycopg2
import sys

sys.stdout.reconfigure(line_buffering=True)

BASE_URL = 'https://live.musz.hu'
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']

STROKE_MAP = {
    'pillangó': 'FLY', 'hát': 'BACK', 'mell': 'BREAST', 'gyors': 'FREE',
    'vegyes': 'IM', 'gyorsváltó': 'FREE', 'vegyesváltó': 'IM',
}
GENDER_MAP = {'férfi': 'M', 'női': 'F', 'mix': 'X'}


def _parse_swimtime(s: str) -> int | None:
    """Parse time (e.g. 04:51.71, 1:23.45) to hundredths."""
    if not s or s.upper() in ('NT', 'DNS', 'DSQ', 'DQ', 'VL', '-', ''):
        return None
    s = s.replace(',', '.').strip()
    parts = s.split(':')
    try:
        if len(parts) == 1:
            m, sec = 0, parts[0]
        else:
            m, sec = int(parts[0]), parts[1]
        if '.' in sec:
            sec_i, hund = sec.split('.')
            hund = int((hund + '00')[:2])
        else:
            sec_i, hund = sec, 0
        return m * 6000 + int(sec_i) * 100 + hund
    except (ValueError, IndexError):
        return None


def _parse_event_title(title: str) -> tuple[str, int, str]:
    """Parse e.g. '1.- 200 m férfi pillangó' -> (stroke, distance, gender)."""
    title = title.lower()
    distance = 0
    for m in re.finditer(r'(\d+)\s*m', title):
        distance = int(m.group(1))
        break
    stroke = 'FREE'
    for hu, en in STROKE_MAP.items():
        if hu in title:
            stroke = en
            break
    gender = 'X'
    for hu, g in GENDER_MAP.items():
        if hu in title:
            gender = g
            break
    return stroke, distance, gender


def _parse_session_date(s: str) -> datetime | None:
    """Parse e.g. '2023.01.28.' or '2026.06.13.'."""
    if not s:
        return None
    m = re.search(r'(\d{4})\.(\d{2})\.(\d{2})', s)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    return None


def _extract_umk(href: str) -> int | None:
    """Extract UMK (athlete id) from /event/swimmer?UMK=123."""
    href = href.replace('&amp;', '&')
    if 'UMK=' in href:
        qs = parse_qs(urlparse(href).query)
        val = qs.get('UMK', [None])[0]
        return int(val) if val and str(val).isdigit() else None
    return None


def _parse_athlete_from_link(a_tag, club_from_text: str = '') -> tuple[int | None, str, str, str, int | None]:
    """Parse athlete link: [Name](url?UMK=X)(YYYY)Club -> (umk, firstname, lastname, club, birth_year)."""
    href = a_tag.get('href', '')
    umk = _extract_umk(href)
    text = a_tag.get_text(strip=True)
    birth_year = None
    # Text can be "Zombori Nóra (2011) FTC" or "Mihály Viktória Hanna"
    name_match = re.match(r'^(.+?)\s*\((\d{4})\)\s*(.*)$', text)
    if name_match:
        full_name = name_match.group(1).strip()
        birth_year = int(name_match.group(2))
        club = (name_match.group(3) or club_from_text).strip()
    else:
        full_name = text
        club = club_from_text
    parts = full_name.split(None, 1)
    firstname = parts[1] if len(parts) > 1 else ''
    lastname = parts[0] if parts else ''
    return umk, firstname, lastname, club, birth_year


def _fetch_swimmer_birthyear(onlineeventid: int, umk: int) -> int | None:
    """Fetch swimmer subpage and parse birth year from (YYYY) display. Returns None on failure."""
    url = f"{BASE_URL}/event/swimmer?OnlineEventId={onlineeventid}&UMK={umk}"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        m = re.search(r'\((\d{4})\)', r.text)
        if m:
            y = int(m.group(1))
            if 1900 <= y <= 2030:
                return y
    except (requests.RequestException, ValueError):
        pass
    return None


def scrape_and_import(onlineeventid: int) -> bool:
    """Scrape MUSZ pages and import into lx_* tables. Returns True on success."""
    print(f"[SCRAPE] Starting for onlineeventid={onlineeventid}", flush=True)
    meet_id = int(onlineeventid)
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    cur = conn.cursor()
    print(f"[SCRAPE] DB connected, fetching eventdata...", flush=True)

    # 1. Fetch eventdata for meet info
    eventdata_url = f"{BASE_URL}/event/eventdata?OnlineEventId={onlineeventid}"
    print(f"[SCRAPE] GET {eventdata_url}", flush=True)
    r = requests.get(eventdata_url)
    r.raise_for_status()
    soup_ed = BeautifulSoup(r.text, 'html.parser')
    meet_name = 'Unknown'
    for h in soup_ed.find_all(['h4', 'h5', 'h6']):
        t = h.get_text(strip=True)
        if t and ' - ' in t and len(t) < 200:
            meet_name = t.split(' - ')[0].strip()
            break
    # Parse date range
    startdate, enddate = None, None
    for h in soup_ed.find_all('h6'):
        t = h.get_text(strip=True)
        if re.match(r'\d{4}-\d{2}-\d{2}\s*-\s*\d{4}-\d{2}-\d{2}', t):
            parts = t.split(' - ')
            try:
                startdate = datetime.strptime(parts[0].strip()[:10], '%Y-%m-%d').date()
                enddate = datetime.strptime(parts[1].strip()[:10], '%Y-%m-%d').date()
            except ValueError:
                pass
            break
    course = 'LCM'
    if '50m' in r.text or '50 m' in r.text:
        course = 'SCM'

    cur.execute(
        """INSERT INTO lx_meet(id, name, startdate, enddate, course, datasource)
           VALUES (%s,%s,%s,%s,%s,'scraped')
           ON CONFLICT (id) DO UPDATE SET name=COALESCE(EXCLUDED.name,lx_meet.name),
             startdate=COALESCE(EXCLUDED.startdate,lx_meet.startdate),
             enddate=COALESCE(EXCLUDED.enddate,lx_meet.enddate),
             course=COALESCE(EXCLUDED.course,lx_meet.course),
             datasource=CASE WHEN lx_meet.datasource='lenex' THEN lx_meet.datasource ELSE 'scraped' END""",
        (meet_id, meet_name, startdate, enddate, course)
    )
    conn.commit()

    # 2. Fetch program page
    program_url = f"{BASE_URL}/event/program?OnlineEventId={onlineeventid}"
    print(f"[SCRAPE] GET {program_url}", flush=True)
    r = requests.get(program_url)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, 'html.parser')

    # Parse sessions and events from summary links
    events_to_scrape = []  # (session_id, event_id, event_title)
    session_dates = {}  # session_id -> date from nearby text

    for a in soup.find_all('a', href=True):
        href = a['href'].replace('&amp;', '&')
        if 'event/summary' in href and 'SessionId=' in href and 'EventId=' in href:
            qs = parse_qs(urlparse(href).query)
            sid = qs.get('SessionId', [None])[0]
            eid = qs.get('EventId', [None])[0]
            if sid and eid:
                try:
                    session_id = int(sid)
                    event_id = int(eid)
                except (ValueError, TypeError):
                    continue
                title = a.get_text(strip=True)
                # Look for session date in preceding elements (nearest first)
                for prev in a.find_all_previous(string=True):
                    prev = str(prev).strip()
                    if re.search(r'\d{4}\.\d{2}\.\d{2}', prev) and 'SESSION' in prev.upper():
                        session_dates[session_id] = _parse_session_date(prev)
                        break
                events_to_scrape.append((session_id, event_id, title))

    # Dedupe events
    seen = set()
    unique_events = []
    for sid, eid, title in events_to_scrape:
        if (sid, eid) not in seen:
            seen.add((sid, eid))
            unique_events.append((sid, eid, title))
    print(f"[SCRAPE] Found {len(unique_events)} events to scrape", flush=True)

    # Create sessions (one per unique SessionId)
    session_ids = {}
    for sid, _, _ in unique_events:
        if sid not in session_ids:
            sess_num = len(session_ids) + 1
            sess_date = session_dates.get(sid)
            cur.execute(
                """INSERT INTO lx_session(meetid, sessionnumber, sessiondate)
                   VALUES (%s,%s,%s) RETURNING id""",
                (meet_id, sess_num, sess_date.date() if sess_date else None)
            )
            session_ids[sid] = cur.fetchone()[0]
    conn.commit()

    # 3. For each event, fetch summary to get heat links, then result pages
    for session_id, event_id, event_title in unique_events:
        stroke, distance, gender = _parse_event_title(event_title)
        lx_session_id = session_ids.get(session_id)
        if not lx_session_id:
            continue

        cur.execute(
            """INSERT INTO lx_event(meetid, stroke, distance, round, gender)
               VALUES (%s,%s,%s,'TIM',%s) RETURNING id""",
            (meet_id, stroke, distance, gender)
        )
        lx_event_id = cur.fetchone()[0]

        # Discover CategoryIds from summary page (tabs) and program page (ALL, D1, D2, etc.)
        # First category is not necessarily 1 - only use IDs found in links
        category_ids = set()
        summary_base = f"{BASE_URL}/event/summary?OnlineEventId={onlineeventid}&SessionId={session_id}&EventId={event_id}"
        r_sum = requests.get(summary_base)
        r_sum.raise_for_status()
        soup_sum = BeautifulSoup(r_sum.text, 'html.parser')
        for a in soup_sum.find_all('a', href=True):
            href = a['href'].replace('&amp;', '&')
            if 'event/summary' in href and f'EventId={event_id}' in href:
                qs = parse_qs(urlparse(href).query)
                cid = qs.get('CategoryId', [None])[0]
                if cid is not None and str(cid).isdigit():
                    category_ids.add(int(cid))
        for a in soup.find_all('a', href=True):  # program page
            href = a['href'].replace('&amp;', '&')
            if 'event/summary' in href and f'EventId={event_id}' in href:
                qs = parse_qs(urlparse(href).query)
                cid = qs.get('CategoryId', [None])[0]
                if cid is not None and str(cid).isdigit():
                    category_ids.add(int(cid))

        # If no category links found, use initial response (single-category event)
        if not category_ids:
            category_ids = {None}
            fetch_for_category = {None: soup_sum}
        else:
            fetch_for_category = {}
            for cid in sorted(category_ids):
                summary_url = f"{summary_base}&CategoryId={cid}"
                print(f"[SCRAPE] GET {summary_url} (category {cid})", flush=True)
                r_cat = requests.get(summary_url)
                r_cat.raise_for_status()
                fetch_for_category[cid] = BeautifulSoup(r_cat.text, 'html.parser')

        result_count_total = 0
        seen_heat_athlete = set()  # (lx_event_id, heatnumber, umk) to avoid duplicates

        print(f"[SCRAPE] {summary_base} ({event_title}) categories={sorted(c for c in category_ids if c is not None) or ['default']}", flush=True)

        for category_id in sorted(category_ids, key=lambda x: (x is None, x)):
            soup_cat = fetch_for_category[category_id]

            for table in soup_cat.find_all('table'):
                headers = [th.get_text(strip=True).upper()[:20] for th in table.find_all('th')]
                if not any('RK' in h or 'HELY' in h for h in headers):
                    continue
                if not any('NAME' in h or 'NEV' in h for h in headers):
                    continue
                if not any('TIME' in h or 'IDŐ' in h for h in headers):
                    continue

                col_idx = {}
                for i, h in enumerate(headers):
                    if 'RK' in h or 'HELY' in h:
                        col_idx['rk'] = i
                    elif 'NAME' in h or 'NEV' in h:
                        col_idx['name'] = i
                    elif 'TIME' in h or 'IDŐ' in h:
                        col_idx['time'] = i
                rk_idx = col_idx.get('rk', 0)
                name_idx = col_idx.get('name', 1)
                time_idx = col_idx.get('time', 3)

                rows = table.find_all('tr')[1:]
                print(f"[SCRAPE]   -> summary table: {len(rows)} rows (cat {category_id or 'default'})", flush=True)

                for tr in rows:
                    tds = tr.find_all('td')
                    if len(tds) <= max(rk_idx, name_idx, time_idx):
                        continue
                    rank_s = tds[rk_idx].get_text(strip=True).replace('*', '').strip()
                    rank = int(rank_s) if rank_s and rank_s.isdigit() else None  # non-ranked: use NULL
                    time_s = tds[time_idx].get_text(strip=True).replace('*', '').split()[0] if tds[time_idx].get_text(strip=True) else ''
                    time_hund = _parse_swimtime(time_s)

                    name_cell = tds[name_idx]
                    a_tag = name_cell.find('a', href=True)
                    if not a_tag:
                        continue
                    umk, firstname, lastname, club_name, birth_year = _parse_athlete_from_link(a_tag)
                    if umk is None:
                        continue

                    # Get heatnumber and lane from H/L column (link HeatId= or plain text "3/4" = heat 3, lane 4)
                    heatnumber = 0  # 0 = not found, use 1 as default
                    lane = None
                    for cell in tds:
                        for a in cell.find_all('a', href=True):
                            href = a['href'].replace('&amp;', '&')
                            if ('event/result' in href or 'event/startlist' in href) and 'HeatId=' in href:
                                qs = parse_qs(urlparse(href).query)
                                hid = qs.get('HeatId', [None])[0]
                                if hid and str(hid).isdigit() and int(hid) > 0:
                                    heatnumber = int(hid)
                                # Parse H/L from link text e.g. "3/4" or "[3/4]" -> lane = 4
                                hl_text = a.get_text(strip=True).strip('[]')
                                hl_match = re.match(r'(\d+)/(\d+)', hl_text)
                                if hl_match:
                                    lane = int(hl_match.group(2))
                                break
                        if heatnumber > 0:
                            break
                    # Fallback: parse heat/lane from plain text "X/Y" in any cell
                    if heatnumber == 0:
                        for cell in tds:
                            text = cell.get_text(strip=True).strip('[]')
                            hl_match = re.search(r'(\d+)/(\d+)', text)
                            if hl_match:
                                heatnumber = int(hl_match.group(1))
                                lane = int(hl_match.group(2))
                                break
                    if heatnumber == 0:
                        heatnumber = 1  # default for combined/unknown results

                    if (lx_event_id, heatnumber, umk) in seen_heat_athlete:
                        continue
                    seen_heat_athlete.add((lx_event_id, heatnumber, umk))

                    # Upsert club
                    club_code = (club_name or '')[:32] or 'UNK'
                    cur.execute("SELECT id FROM lx_club WHERE lenexclubcode = %s", (club_code,))
                    row = cur.fetchone()
                    if row:
                        club_id = row[0]
                    else:
                        cur.execute(
                            "INSERT INTO lx_club(lenexclubcode, name, nation) VALUES (%s,%s,'HUN') RETURNING id",
                            (club_code, club_name or club_code)
                        )
                        club_id = cur.fetchone()[0]

                    # Athlete: skip swimmer fetch if already in DB
                    cur.execute("SELECT 1 FROM lx_athlete WHERE id = %s", (umk,))
                    if cur.fetchone():
                        pass
                    else:
                        if birth_year is None:
                            birth_year = _fetch_swimmer_birthyear(onlineeventid, umk)
                        birthdate = date(birth_year, 1, 1) if birth_year else None
                        cur.execute(
                            """INSERT INTO lx_athlete(id, firstname, lastname, birthdate, gender)
                               VALUES (%s,%s,%s,%s,'X')
                               ON CONFLICT (id) DO UPDATE SET
                                 firstname=COALESCE(EXCLUDED.firstname,lx_athlete.firstname),
                                 lastname=COALESCE(EXCLUDED.lastname,lx_athlete.lastname),
                                 birthdate=COALESCE(EXCLUDED.birthdate,lx_athlete.birthdate)""",
                            (umk, firstname, lastname, birthdate)
                        )

                    cur.execute(
                        """INSERT INTO lx_result(athleteid, eventid, heatnumber, clubid, lane, timehundredths, rank)
                           VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                        (umk, lx_event_id, heatnumber, club_id, lane, time_hund, rank)
                    )
                    result_count_total += 1
                break  # one result table per category page

        heats_count = len({(e, h) for e, h, _ in seen_heat_athlete})
        print(f"[SCRAPE]   -> {heats_count} heat(s), {result_count_total} result(s)", flush=True)
        conn.commit()

    cur.close()
    conn.close()
    return True


def _get_events_without_lenex(conn) -> list[tuple[int, str]]:
    """Return (onlineeventid, eventname) for events where LENEX file is missing."""
    cur = conn.cursor()
    cur.execute(
        """SELECT onlineeventid, eventname FROM importedlenexfile
           WHERE filename IS NULL AND status = 'lenex_not_found'
           ORDER BY onlineeventid"""
    )
    rows = cur.fetchall()
    cur.close()
    return rows


if __name__ == '__main__':
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        events = _get_events_without_lenex(conn)
        if not events:
            conn.close()
            print("[INFO] No events with missing LENEX found in DB", flush=True)
        else:
            print(f"[INFO] Found {len(events)} events with missing LENEX", flush=True)
            failed = 0
            for oid, name in events:
                name = name or oid
                print(f"[EVENT] Scraping {name} ({oid})...", flush=True)
                try:
                    if scrape_and_import(oid):
                        cur = conn.cursor()
                        cur.execute(
                            "UPDATE importedlenexfile SET status='scraped' WHERE onlineeventid=%s",
                            (oid,)
                        )
                        conn.commit()
                        print(f"[OK] Scraped {name} ({oid})", flush=True)
                        break  # TEST: stop after first successful scrape
                    else:
                        failed += 1
                        cur = conn.cursor()
                        cur.execute(
                            "UPDATE importedlenexfile SET status='scrape_failed' WHERE onlineeventid=%s",
                            (oid,)
                        )
                        conn.commit()
                        print(f"[ERROR] Scrape failed for {oid}", flush=True)
                except Exception as e:
                    failed += 1
                    cur = conn.cursor()
                    cur.execute(
                        "UPDATE importedlenexfile SET status='scrape_failed' WHERE onlineeventid=%s",
                        (oid,)
                    )
                    conn.commit()
                    print(f"[ERROR] Scrape failed for {oid}: {e}", flush=True)
                    traceback.print_exc()
            conn.close()
            if failed:
                sys.exit(1)
    except Exception as e:
        print(f"[ERROR] {e}", flush=True)
        sys.exit(1)
