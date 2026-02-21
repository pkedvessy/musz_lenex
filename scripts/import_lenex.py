#!/usr/bin/env python3
"""
Import LENEX XML files into the PostgreSQL schema.

LENEX is an XML-based swimming data format. Structure:
  LENEX -> MEETS -> MEET -> SESSIONS -> SESSION -> EVENTS -> EVENT -> SWIMSTYLE, HEATS -> HEAT
        -> CLUBS -> CLUB -> ATHLETES -> ATHLETE -> RESULTS -> RESULT -> SPLITS -> SPLIT

Processes files from importedlenexfile where status is downloaded or backed_up.
"""
import os
from datetime import date, datetime
import xml.etree.ElementTree as ET
import psycopg2
import sys

sys.stdout.reconfigure(line_buffering=True)

DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']

DOWNLOAD_DIR = os.environ.get('LENEX_DIR', 'lenex_files')


def _attr(el: ET.Element, key: str, default=None):
    return el.get(key) if el is not None else default


def _parse_swimtime(swimtime: str) -> int | None:
    """Convert LENEX swimtime to hundredths of seconds.
    Supports: '40.09', '1:23.45' (MM:SS.hh), '00:00:40.09' (HH:MM:SS.hh)
    """
    if not swimtime or swimtime.upper() in ('NT', 'DNS', 'DSQ', 'DQ', 'SCR', ''):
        return None
    parts = swimtime.replace(',', '.').strip().split(':')
    try:
        if len(parts) == 1:
            sec_part = parts[0]
            hours, minutes = 0, 0
        elif len(parts) == 2:
            minutes = int(parts[0])
            sec_part = parts[1]
            hours = 0
        else:
            hours = int(parts[0])
            minutes = int(parts[1])
            sec_part = parts[2]
        # sec_part can be "40.09" or "40"
        if '.' in sec_part:
            sec, hund = sec_part.split('.')
            sec = int(sec)
            hund = int((hund + '00')[:2])  # pad/truncate to 2 digits
        else:
            sec = int(sec_part)
            hund = 0
        return hours * 360000 + minutes * 6000 + sec * 100 + hund
    except (ValueError, IndexError):
        return None


def _parse_date(s: str) -> date | None:
    if not s:
        return None
    for fmt in ('%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y'):
        try:
            return datetime.strptime(s.strip()[:10], fmt).date()
        except ValueError:
            continue
    return None


def import_lenex_file(cur, conn, filepath: str, onlineeventid) -> bool:
    """Parse LENEX XML and import into DB. Returns True on success."""
    # onlineeventid (meet id) must be parseable as integer
    if onlineeventid is None:
        raise ValueError(f"Meet onlineeventid is missing (file: {filepath})")
    try:
        meet_id = int(onlineeventid)
    except (ValueError, TypeError) as e:
        raise ValueError(
            f"Meet onlineeventid must be numeric, got {onlineeventid!r} (file: {filepath})"
        ) from e

    try:
        tree = ET.parse(filepath)
    except ET.ParseError as e:
        print(f"[ERROR] Invalid XML in {filepath}: {e}", flush=True)
        return False

    root = tree.getroot()
    # Handle optional XML namespace (e.g. {http://...}LENEX)
    root_tag = root.tag.split('}')[-1] if '}' in root.tag else root.tag
    if root_tag.upper() != 'LENEX':
        print(f"[ERROR] Root element is not LENEX in {filepath}", flush=True)
        return False

    meets = root.find('MEETS')
    if meets is None:
        print(f"[WARN] No MEETS in {filepath}", flush=True)
        return False

    meet_elem = meets.find('MEET')
    if meet_elem is None:
        print(f"[WARN] No MEET in {filepath}", flush=True)
        return False

    meet_name = _attr(meet_elem, 'name') or 'Unknown'
    meet_course = _attr(meet_elem, 'course') or 'LCM'

    # --- Meet ---
    cur.execute(
        """INSERT INTO lx_meet(id, name, startdate, enddate, course, datasource)
           VALUES (%s,%s,NULL,NULL,%s,'lenex')
           ON CONFLICT (id) DO UPDATE SET
             name = COALESCE(EXCLUDED.name, lx_meet.name),
             course = COALESCE(EXCLUDED.course, lx_meet.course),
             datasource = EXCLUDED.datasource""",
        (meet_id, meet_name, meet_course)
    )
    conn.commit()

    # Maps for LENEX ids -> our DB ids
    eventid_to_lx_event = {}
    heatid_to_lx_heat = {}
    sessionid_to_lx_session = {}

    # --- Sessions and Events ---
    sessions_elem = meet_elem.find('SESSIONS')
    if sessions_elem is not None:
        for sess in sessions_elem.findall('SESSION'):
            sess_num = _attr(sess, 'number')
            sess_date = _parse_date(_attr(sess, 'date'))
            cur.execute(
                """INSERT INTO lx_session(meetid, sessionnumber, sessiondate)
                   VALUES (%s,%s,%s) RETURNING id""",
                (meet_id, int(sess_num) if sess_num and sess_num.isdigit() else None, sess_date)
            )
            session_id = cur.fetchone()[0]
            sessionid_to_lx_session[_attr(sess, 'number')] = session_id

            events_elem = sess.find('EVENTS')
            if events_elem is None:
                continue
            for ev in events_elem.findall('EVENT'):
                ev_id = _attr(ev, 'eventid') or _attr(ev, 'number')
                gender = (_attr(ev, 'gender') or 'X')[:1].upper()
                round_ = _attr(ev, 'round') or None

                swimstyle = ev.find('SWIMSTYLE')
                stroke = 'FREE'
                distance = 0
                if swimstyle is not None:
                    stroke = (_attr(swimstyle, 'stroke') or 'FREE').upper()
                    dist_s = _attr(swimstyle, 'distance')
                    distance = int(dist_s) if dist_s and dist_s.isdigit() else 0

                cur.execute(
                    """INSERT INTO lx_event(meetid, stroke, distance, round, gender)
                       VALUES (%s,%s,%s,%s,%s) RETURNING id""",
                    (meet_id, stroke, distance, round_, gender)
                )
                lx_event_id = cur.fetchone()[0]
                eventid_to_lx_event[ev_id] = lx_event_id

                heats_elem = ev.find('HEATS')
                if heats_elem is not None:
                    for heat in heats_elem.findall('HEAT'):
                        heat_id = _attr(heat, 'heatid') or _attr(heat, 'number')
                        heat_num = _attr(heat, 'number')
                        cur.execute(
                            """INSERT INTO lx_heat(eventid, sessionid, heatnumber)
                               VALUES (%s,%s,%s) RETURNING id""",
                            (lx_event_id, session_id, int(heat_num) if heat_num and heat_num.isdigit() else None)
                        )
                        lx_heat_id = cur.fetchone()[0]
                        heatid_to_lx_heat[heat_id] = lx_heat_id

    # Update meet start/end from sessions
    cur.execute(
        """UPDATE lx_meet SET startdate = (SELECT MIN(sessiondate) FROM lx_session WHERE meetid = %s),
           enddate = (SELECT MAX(sessiondate) FROM lx_session WHERE meetid = %s) WHERE id = %s""",
        (meet_id, meet_id, meet_id)
    )

    # --- Clubs and Athletes ---
    clubs_elem = meet_elem.find('CLUBS')
    if clubs_elem is None:
        conn.commit()
        return True

    club_code_to_id = {}

    for club_elem in clubs_elem.findall('CLUB'):
        club_code = _attr(club_elem, 'code') or _attr(club_elem, 'name', '')[:32]
        club_name = _attr(club_elem, 'name') or club_code
        nation = (_attr(club_elem, 'nation') or '')[:3]

        cur.execute("SELECT id FROM lx_club WHERE lenexclubcode = %s", (club_code,))
        row = cur.fetchone()
        if row:
            club_id = row[0]
        else:
            cur.execute(
                """INSERT INTO lx_club(lenexclubcode, name, nation)
                   VALUES (%s,%s,%s) RETURNING id""",
                (club_code, club_name, nation or None)
            )
            club_id = cur.fetchone()[0]
        club_code_to_id[club_code] = club_id

        athletes_elem = club_elem.find('ATHLETES')
        if athletes_elem is None:
            continue

        for athlete_elem in athletes_elem.findall('ATHLETE'):
            ath_id = _attr(athlete_elem, 'athleteid')
            firstname = _attr(athlete_elem, 'firstname') or ''
            lastname = _attr(athlete_elem, 'lastname') or ''
            birthdate_s = _attr(athlete_elem, 'birthdate')
            birth = _parse_date(birthdate_s)
            gender = (_attr(athlete_elem, 'gender') or 'X')[:1].upper()

            # athleteid must be parseable as integer
            if not ath_id:
                raise ValueError(
                    f"Athlete missing athleteid: firstname={firstname!r}, lastname={lastname!r}, "
                    f"birthdate={birthdate_s!r} (file: {filepath})"
                )
            try:
                athlete_id = int(ath_id)
            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"Athlete athleteid must be numeric, got {ath_id!r} for "
                    f"firstname={firstname!r}, lastname={lastname!r} (file: {filepath})"
                ) from e

            cur.execute(
                """INSERT INTO lx_athlete(id, firstname, lastname, birthdate, gender)
                   VALUES (%s,%s,%s,%s,%s)
                   ON CONFLICT (id) DO UPDATE SET
                     firstname = COALESCE(EXCLUDED.firstname, lx_athlete.firstname),
                     lastname = COALESCE(EXCLUDED.lastname, lx_athlete.lastname),
                     birthdate = COALESCE(EXCLUDED.birthdate, lx_athlete.birthdate),
                     gender = COALESCE(EXCLUDED.gender, lx_athlete.gender)
                   RETURNING id""",
                (athlete_id, firstname, lastname, birth, gender)
            )
            athlete_id = cur.fetchone()[0]

            # Club affiliation at this meet
            if club_id and meet_id:
                cur.execute(
                    """INSERT INTO lx_athleteclubaffiliation(athleteid, clubid, validfrom, validto, sourcemeetid)
                       SELECT %s,%s,%s,%s,%s
                       WHERE NOT EXISTS (
                         SELECT 1 FROM lx_athleteclubaffiliation
                         WHERE athleteid = %s AND clubid = %s AND sourcemeetid = %s
                       )""",
                    (athlete_id, club_id, birth, None, meet_id, athlete_id, club_id, meet_id)
                )

            results_elem = athlete_elem.find('RESULTS')
            if results_elem is None:
                continue

            for res in results_elem.findall('RESULT'):
                res_eventid = _attr(res, 'eventid')
                res_heatid = _attr(res, 'heatid')
                if not res_eventid or not res_heatid:
                    continue
                lx_event_id = eventid_to_lx_event.get(res_eventid)
                lx_heat_id = heatid_to_lx_heat.get(res_heatid)
                if not lx_event_id or not lx_heat_id:
                    continue

                lane_s = _attr(res, 'lane')
                lane = int(lane_s) if lane_s and lane_s.isdigit() else None
                swimtime = _attr(res, 'swimtime')
                time_hund = _parse_swimtime(swimtime)
                status = (_attr(res, 'status') or _attr(res, 'disqualified') or '')[:10]
                if not status and swimtime and swimtime.upper() in ('DQ', 'DSQ', 'DNS', 'SCR'):
                    status = swimtime.upper()[:10]
                rank_s = _attr(res, 'place') or _attr(res, 'rank')
                rank = int(rank_s) if rank_s and rank_s.isdigit() else None
                react_s = _attr(res, 'reactiontime')
                reactiontime = _parse_swimtime(react_s) if react_s else None

                cur.execute(
                    """INSERT INTO lx_result(athleteid, heatid, clubid, lane, timehundredths, status, rank, reactiontime)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                    (athlete_id, lx_heat_id, club_id, lane, time_hund, status or None, rank, reactiontime)
                )
                result_id = cur.fetchone()[0]

                splits_elem = res.find('SPLITS')
                if splits_elem is not None:
                    for sp in splits_elem.findall('SPLIT'):
                        dist_s = _attr(sp, 'distance')
                        dist = int(dist_s) if dist_s and dist_s.isdigit() else None
                        st = _attr(sp, 'swimtime')
                        th = _parse_swimtime(st)
                        if dist is not None and th is not None and result_id:
                            cur.execute(
                                """INSERT INTO lx_split(resultid, distance, timehundredths)
                                   VALUES (%s,%s,%s)""",
                                (result_id, dist, th)
                            )

    conn.commit()
    return True


def main():
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        cur = conn.cursor()
        print("[DB] Connected to PostgreSQL", flush=True)
    except Exception as e:
        print("[ERROR] Cannot connect to DB:", e, flush=True)
        sys.exit(1)

    cur.execute("""
        SELECT onlineeventid, filename FROM importedlenexfile
        WHERE status IN ('downloaded', 'backed_up') AND filename IS NOT NULL
        ORDER BY eventdatefrom
    """)
    rows = cur.fetchall()
    print(f"[INFO] {len(rows)} files to process", flush=True)

    for onlineeventid, filename in rows:
        filepath = os.path.join(DOWNLOAD_DIR, filename)
        if not os.path.isfile(filepath):
            print(f"[WARN] File not found: {filepath}", flush=True)
            continue

        print(f"[IMPORT] Processing {filename} (event {onlineeventid})", flush=True)
        try:
            if import_lenex_file(cur, conn, filepath, onlineeventid):
                cur.execute("UPDATE importedlenexfile SET status = 'processed' WHERE onlineeventid = %s", (onlineeventid,))
                conn.commit()
                print(f"[OK] Imported {filename}", flush=True)
            else:
                cur.execute("UPDATE importedlenexfile SET status = 'processing_failed' WHERE onlineeventid = %s", (onlineeventid,))
                conn.commit()
                print(f"[ERROR] Failed to import {filename}", flush=True)
        except ValueError as e:
            cur.execute("UPDATE importedlenexfile SET status = 'processing_failed' WHERE onlineeventid = %s", (onlineeventid,))
            conn.commit()
            print(f"[ERROR] Import failed for {filename}: {e}", flush=True)
        except Exception as e:
            try:
                cur.execute("UPDATE importedlenexfile SET status = 'processing_failed' WHERE onlineeventid = %s", (onlineeventid,))
                conn.commit()
            except Exception:
                conn.rollback()
            print(f"[ERROR] Failed to import {filename}: {e}", flush=True)

    cur.close()
    conn.close()
    print("[DONE] LENEX import finished", flush=True)


if __name__ == '__main__':
    main()
