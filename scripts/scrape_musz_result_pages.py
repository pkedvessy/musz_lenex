#!/usr/bin/env python3
"""
Alternative scraper: uses event/result pages (per heat) to scrape results and splits.

Uses event/result page structure:
- Event selector (select, value=EventId) for eventIds per session
- heatSelect (value=HeatId) for heatIds per event
- Loop: SessionId -> EventId -> HeatId; fetch each result page for results + splits
- EventIds continue across sessions (1,2..14 for sess1; 15,16.. for sess2)
- Stop when SessionId+1 yields no heats for events

Run: DB_HOST=... DB_PORT=... python scripts/scrape_musz_result_pages.py
     --online-event-id ID   Test a single event (dry run, no DB writes)
"""
import argparse
import os
import re
import traceback
from datetime import datetime, date
from urllib.parse import urlparse, parse_qs
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
    m = re.search(r'(\d{1,2}):(\d{2})\.(\d{2})', s)
    if m:
        try:
            mins, sec, hund = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return mins * 6000 + sec * 100 + hund
        except (ValueError, IndexError):
            pass
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
    """Fetch swimmer subpage and parse birth year from (YYYY) display."""
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


def _parse_splits_from_row(row_html) -> list[tuple[int, int]]:
    """Parse split distance+time from row. E.g. 100m**01:14.41**150m**01:55.64**."""
    text = row_html.get_text() if hasattr(row_html, 'get_text') else str(row_html)
    splits = []
    for m in re.finditer(r'(\d+)\s*m\**\s*(\d{1,2}):(\d{2})\.(\d{2})', text):
        dist = int(m.group(1))
        mins, sec, hund = int(m.group(2)), int(m.group(3)), int(m.group(4))
        th = mins * 6000 + sec * 100 + hund
        splits.append((dist, th))
    return splits


def _parse_splits_from_splittimes_div(splittimes_div) -> list[tuple[int, int]]:
    """Parse splits from MUSZ splittimes structure: div.col-3 with span(distance) + span strong(time)."""
    splits = []
    for col in splittimes_div.find_all('div', class_=lambda c: c and 'col-3' in str(c)):
        text = col.get_text()
        m = re.search(r'(\d+)\s*m\s*(\d{1,2}):(\d{2})\.(\d{2})', text)
        if m:
            dist = int(m.group(1))
            mins, sec, hund = int(m.group(2)), int(m.group(3)), int(m.group(4))
            th = mins * 6000 + sec * 100 + hund
            splits.append((dist, th))
    return splits


class _DryRunCursor:
    """No-op cursor for dry run: logs executes, returns fake ids for fetchone after INSERT."""

    def __init__(self):
        self._fake_id = 0
        self._last_was_insert = False

    def execute(self, sql, params=None):
        self._last_was_insert = "INSERT" in sql.upper() or "UPDATE" in sql.upper()
        msg = sql.strip()[:100] + "..." if len(sql) > 100 else sql.strip()
        print(f"[DRY] Would execute: {msg}", flush=True)

    def fetchone(self):
        if self._last_was_insert:
            self._fake_id += 1
            return (self._fake_id,)
        return None  # SELECTs "find nothing" in dry run

    def close(self):
        pass


class _DryRunConn:
    def cursor(self):
        return _DryRunCursor()

    def commit(self):
        print("[DRY] Would commit", flush=True)

    def close(self):
        pass


def scrape_and_import(onlineeventid: int, dry_run: bool = False) -> None:
    """Scrape MUSZ event/result pages and import into lx_* tables."""
    print(f"[SCRAPE] Starting for onlineeventid={onlineeventid}" + (" (dry run)" if dry_run else ""), flush=True)
    meet_id = int(onlineeventid)
    if dry_run:
        conn = _DryRunConn()
        cur = conn.cursor()
    else:
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
            )
            cur = conn.cursor()
        except Exception as e:
            print(f"[SCRAPE] ERROR: DB connect failed: {e}", flush=True)
            traceback.print_exc()
            return

    try:
        # 1. Fetch eventdata for meet info
        eventdata_url = f"{BASE_URL}/event/eventdata?OnlineEventId={onlineeventid}"
        print(f"[SCRAPE] GET {eventdata_url}", flush=True)
        r = requests.get(eventdata_url, timeout=30)
        if not r.ok:
            print(f"[SCRAPE] ERROR: eventdata {r.status_code}", flush=True)
            return
        soup_ed = BeautifulSoup(r.text, 'html.parser')
        meet_name = 'Unknown'
        for h in soup_ed.find_all(['h4', 'h5', 'h6']):
            t = h.get_text(strip=True)
            if t and ' - ' in t and len(t) < 200:
                meet_name = t.split(' - ')[0].strip()
                break
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

        # 2. Fetch program for event titles
        program_url = f"{BASE_URL}/event/program?OnlineEventId={onlineeventid}"
        print(f"[SCRAPE] GET {program_url}", flush=True)
        r = requests.get(program_url, timeout=30)
        if not r.ok:
            print(f"[SCRAPE] ERROR: program {r.status_code}", flush=True)
            return
        soup_prog = BeautifulSoup(r.text, 'html.parser')
        event_titles = {}
        session_dates = {}
        for a in soup_prog.find_all('a', href=True):
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
                    event_titles[(session_id, event_id)] = title
                    for prev in a.find_all_previous(string=True):
                        prev = str(prev).strip()
                        if re.search(r'\d{4}\.\d{2}\.\d{2}', prev) and 'SESSION' in prev.upper():
                            session_dates[session_id] = _parse_session_date(prev)
                            break
        print(f"[SCRAPE] Program: {len(event_titles)} events", flush=True)

        session_ids = {}
        seen_events = set()
        result_count_total = 0
        split_count_total = 0
        seen_heat_athlete = set()

        session_id = 1
        next_event_id = 1
        result_base = f"{BASE_URL}/event/result?OnlineEventId={onlineeventid}"

        while True:
            url = f"{result_base}&SessionId={session_id}&EventId={next_event_id}"
            print(f"[SCRAPE] GET {url} (discover session {session_id})", flush=True)
            try:
                r = requests.get(url, timeout=30)
                if not r.ok:
                    print(f"[SCRAPE] Session {session_id} {r.status_code}, stopping", flush=True)
                    break
            except Exception as e:
                print(f"[SCRAPE] Session {session_id} fetch failed: {e}, stopping", flush=True)
                break

            soup = BeautifulSoup(r.text, 'html.parser')

            event_options = []
            for sel in soup.find_all('select'):
                if 'heatSelect' in (sel.get('class') or []):
                    continue
                for opt in sel.find_all('option', value=True):
                    val = opt.get('value', '').strip()
                    if val and val.isdigit():
                        event_options.append((int(val), opt.get_text(strip=True) or ''))
                if event_options:
                    break

            heat_options = []
            for sel in soup.select('select.heatSelect'):
                for opt in sel.find_all('option', value=True):
                    val = opt.get('value', '').strip()
                    if val and val.isdigit():
                        heat_options.append(int(val))
                break

            if not heat_options:
                next_ev = min((eid for (sid, eid) in event_titles if sid == session_id + 1), default=next_event_id + 1)
                next_session_url = f"{result_base}&SessionId={session_id + 1}&EventId={next_ev}"
                print(f"[SCRAPE] No heats in session {session_id}, trying session {session_id + 1}", flush=True)
                try:
                    r_next = requests.get(next_session_url, timeout=30)
                    if r_next.ok:
                        soup_next = BeautifulSoup(r_next.text, 'html.parser')
                        heat_options_next = []
                        for sel in soup_next.select('select.heatSelect'):
                            for opt in sel.find_all('option', value=True):
                                val = opt.get('value', '').strip()
                                if val and val.isdigit():
                                    heat_options_next.append(int(val))
                            break
                        if not heat_options_next:
                            print(f"[SCRAPE] Session {session_id + 1} has no heats, done", flush=True)
                            break
                except Exception:
                    pass
                break

            if not event_options:
                event_options = sorted([(eid, event_titles.get((session_id, eid), ''))
                                       for (sid, eid) in event_titles if sid == session_id])
            else:
                event_options = sorted(event_options)
            if not event_options:
                print(f"[SCRAPE] No events for session {session_id}, stopping", flush=True)
                break

            if session_id not in session_ids:
                sess_date = session_dates.get(session_id)
                cur.execute(
                    """INSERT INTO lx_session(meetid, sessionnumber, sessiondate)
                       VALUES (%s,%s,%s) RETURNING id""",
                    (meet_id, session_id, sess_date.date() if sess_date else None)
                )
                session_ids[session_id] = cur.fetchone()[0]
            conn.commit()

            for event_id, event_title in event_options:
                if (session_id, event_id) in seen_events:
                    continue
                stroke, distance, gender = _parse_event_title(event_titles.get((session_id, event_id), event_title))
                cur.execute(
                    """INSERT INTO lx_event(meetid, stroke, distance, round, gender)
                       VALUES (%s,%s,%s,'TIM',%s) RETURNING id""",
                    (meet_id, stroke, distance, gender)
                )
                lx_event_id = cur.fetchone()[0]
                seen_events.add((session_id, event_id))

                url_ev = f"{result_base}&SessionId={session_id}&EventId={event_id}"
                try:
                    r_ev = requests.get(url_ev, timeout=30)
                    if not r_ev.ok:
                        continue
                except Exception as e:
                    print(f"[SCRAPE] Event {event_id} fetch failed: {e}", flush=True)
                    continue
                soup_ev = BeautifulSoup(r_ev.text, 'html.parser')
                heat_ids = []
                for sel in soup_ev.select('select.heatSelect'):
                    for opt in sel.find_all('option', value=True):
                        val = opt.get('value', '').strip()
                        if val and val.isdigit():
                            heat_ids.append(int(val))
                    break

                for heat_id in heat_ids:
                    heatnumber = heat_ids.index(heat_id) + 1 if heat_id else 1
                    url_heat = f"{result_base}&SessionId={session_id}&EventId={event_id}&HeatId={heat_id}"
                    print(f"[SCRAPE] GET {url_heat} (heat {heatnumber})", flush=True)
                    try:
                        r_heat = requests.get(url_heat, timeout=30)
                        if not r_heat.ok:
                            continue
                    except Exception as e:
                        print(f"[SCRAPE] Heat {heat_id} fetch failed: {e}", flush=True)
                        continue
                    soup_heat = BeautifulSoup(r_heat.text, 'html.parser')

                    for table in soup_heat.find_all('table'):
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
                            elif 'LN' in h:
                                col_idx['ln'] = i
                            elif 'NAME' in h or 'NEV' in h:
                                col_idx['name'] = i
                            elif 'TIME' in h or 'IDŐ' in h:
                                col_idx['time'] = i
                            elif 'FINA' in h:
                                col_idx['fina'] = i
                        rk_idx = col_idx.get('rk', 0)
                        name_idx = col_idx.get('name', 1)
                        time_idx = col_idx.get('time', 3)
                        fina_idx = col_idx.get('fina')
                        ln_idx = col_idx.get('ln')

                        rows = table.find_all('tr')[1:]
                        for tr in rows:
                            tds = tr.find_all('td')
                            req_cols = [rk_idx, name_idx, time_idx]
                            if fina_idx is not None:
                                req_cols.append(fina_idx)
                            if len(tds) <= max(req_cols):
                                continue

                            rank_s = tds[rk_idx].get_text(strip=True).replace('*', '').strip()
                            rank = int(rank_s) if rank_s and rank_s.isdigit() else None
                            time_raw = tds[time_idx].get_text(strip=True).replace('*', '').strip()
                            time_match = re.search(r'\d{1,2}:\d{2}\.\d{2}', time_raw)
                            time_s = time_match.group(0) if time_match else (time_raw.split()[0] if time_raw else '')
                            time_hund = _parse_swimtime(time_s) if time_s else None
                            rt_match = re.search(r'R:(\d+)\.(\d{2})', time_raw, re.IGNORECASE)
                            reactiontime_hund = int(rt_match.group(1)) * 100 + int(rt_match.group(2)) if rt_match else None
                            finapoints = None
                            if fina_idx is not None and fina_idx < len(tds):
                                pts_raw = tds[fina_idx].get_text(strip=True)
                                try:
                                    finapoints = int(float(pts_raw)) if pts_raw else None
                                except (ValueError, TypeError):
                                    pass
                            status = None
                            for st in ('DNS', 'DSQ', 'DQ', 'DNF', 'SCR', 'NT'):
                                if st in time_raw.upper():
                                    status = st
                                    break

                            name_cell = tds[name_idx]
                            a_tag = name_cell.find('a', href=True)
                            if not a_tag:
                                continue
                            umk, firstname, lastname, club_name, birth_year = _parse_athlete_from_link(a_tag)
                            if umk is None:
                                continue

                            lane = None
                            if ln_idx is not None and ln_idx < len(tds):
                                ln_s = tds[ln_idx].get_text(strip=True)
                                if ln_s and ln_s.replace('-', '').isdigit():
                                    lane = int(ln_s)

                            if (lx_event_id, heatnumber, umk) in seen_heat_athlete:
                                continue
                            if time_hund is None and status is None:
                                continue
                            seen_heat_athlete.add((lx_event_id, heatnumber, umk))

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

                            cur.execute("SELECT 1 FROM lx_athlete WHERE id = %s", (umk,))
                            if not cur.fetchone():
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
                                """INSERT INTO lx_result(athleteid, eventid, heatnumber, clubid, lane, timehundredths, status, rank, reactiontimehundredths, finapoints)
                                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                                (umk, lx_event_id, heatnumber, club_id, lane, time_hund, status, rank, reactiontime_hund, finapoints)
                            )
                            result_id = cur.fetchone()[0]
                            result_count_total += 1

                            splits = []
                            next_tr = tr.find_next_sibling('tr')
                            if next_tr:
                                splittimes_div = next_tr.find('div', class_=lambda c: c and 'splittimes' in str(c))
                                if splittimes_div:
                                    splits = _parse_splits_from_splittimes_div(splittimes_div)
                            if not splits:
                                splits = _parse_splits_from_row(tr)
                            for dist, th in splits:
                                cur.execute(
                                    "INSERT INTO lx_split(resultid, distance, timehundredths) VALUES (%s,%s,%s)",
                                    (result_id, dist, th)
                                )
                                split_count_total += 1
                        break

            if event_options:
                next_event_id = max(eid for eid, _ in event_options) + 1
            session_id += 1
            conn.commit()

        print(f"[SCRAPE] Done: {result_count_total} results, {split_count_total} splits", flush=True)

    except Exception as e:
        print(f"[SCRAPE] ERROR: {e}", flush=True)
        traceback.print_exc()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def _get_events_without_lenex(conn) -> list[tuple[int, str]]:
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
    parser = argparse.ArgumentParser(description='Scrape MUSZ result pages (per heat) with splits')
    parser.add_argument('--online-event-id', type=int, metavar='ID',
                        help='Test this event only (dry run, no DB writes)')
    args = parser.parse_args()

    events_to_scrape = []
    conn = None
    if args.online_event_id:
        events_to_scrape = [(args.online_event_id, None)]
        print(f"[INFO] Testing event {args.online_event_id} (dry run, no DB writes)", flush=True)
    else:
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
            )
            events_to_scrape = _get_events_without_lenex(conn)
            if conn:
                conn.close()
        except Exception as e:
            print(f"[ERROR] DB connect failed: {e}", flush=True)
            sys.exit(1)
        if not events_to_scrape:
            print("[INFO] No events with missing LENEX found in DB", flush=True)
        else:
            print(f"[INFO] Found {len(events_to_scrape)} events with missing LENEX", flush=True)

    if events_to_scrape:
        for oid, name in events_to_scrape:
            name = name or oid
            print(f"[EVENT] Scraping {name} ({oid})...", flush=True)
            dry_run = args.online_event_id is not None
            scrape_and_import(oid, dry_run=dry_run)
            if not dry_run:
                upd_conn = None
                try:
                    upd_conn = psycopg2.connect(
                        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
                    )
                    cur = upd_conn.cursor()
                    cur.execute(
                        "UPDATE importedlenexfile SET status='scraped' WHERE onlineeventid=%s",
                        (oid,)
                    )
                    upd_conn.commit()
                    print(f"[OK] Scraped {name} ({oid})", flush=True)
                except Exception as e:
                    print(f"[EVENT] ERROR updating status for {oid}: {e}", flush=True)
                    traceback.print_exc()
                finally:
                    if upd_conn:
                        upd_conn.close()
