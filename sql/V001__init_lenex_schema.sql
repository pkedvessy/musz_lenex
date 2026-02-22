-- =============================================
-- LENEX ANALYTICS BASELINE SCHEMA (PostgreSQL)
-- Flyway Version: V001
-- =============================================

-- =====================
-- ATHLETE
-- =====================
CREATE TABLE lx_athlete (
    id BIGINT PRIMARY KEY,
    firstname VARCHAR(128),
    lastname VARCHAR(128),
    birthdate DATE,
    gender CHAR(1),
    createdat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================
-- CLUB
-- =====================
CREATE TABLE lx_club (
    id BIGSERIAL PRIMARY KEY,
    lenexclubcode VARCHAR(32),
    name VARCHAR(255),
    nation CHAR(3)
);

CREATE INDEX ix_lx_club_code
ON lx_club(lenexclubcode);

-- =====================
-- MEET
-- =====================
CREATE TABLE lx_meet (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    startdate DATE,
    enddate DATE,
    course VARCHAR(10),
    datasource VARCHAR(20) NOT NULL DEFAULT 'lenex'
        CHECK (datasource IN ('lenex', 'scraped'))
);

CREATE INDEX ix_lx_meet_date
ON lx_meet(startdate);

CREATE INDEX ix_lx_meet_datasource
ON lx_meet(datasource);

COMMENT ON COLUMN lx_meet.datasource IS 'lenex=official LENEX file import, scraped=parsed from MUSZ HTTP pages (may be less reliable)';

-- =====================
-- SESSION
-- =====================
CREATE TABLE lx_session (
    id BIGSERIAL PRIMARY KEY,
    meetid BIGINT NOT NULL REFERENCES lx_meet(id),
    sessionnumber INT,
    sessiondate DATE
);

CREATE INDEX ix_lx_session_meet
ON lx_session(meetid);

-- =====================
-- EVENT
-- =====================
CREATE TABLE lx_event (
    id BIGSERIAL PRIMARY KEY,
    meetid BIGINT NOT NULL REFERENCES lx_meet(id),
    stroke VARCHAR(50),
    distance INT,
    round VARCHAR(50),
    gender CHAR(1)
);

CREATE INDEX ix_lx_event_meet
ON lx_event(meetid);

CREATE INDEX ix_lx_event_query
ON lx_event(meetid, stroke, distance, gender);

-- =====================
-- RESULT (heat as counter: eventid + heatnumber)
-- =====================
CREATE TABLE lx_result (
    id BIGSERIAL PRIMARY KEY,
    athleteid BIGINT NOT NULL REFERENCES lx_athlete(id),
    eventid BIGINT NOT NULL REFERENCES lx_event(id),
    heatnumber INT,
    clubid BIGINT REFERENCES lx_club(id),
    lane INT,
    timehundredths INT,
    status VARCHAR(10),
    rank INT,
    reactiontime INT
);

CREATE INDEX ix_lx_result_athlete
ON lx_result(athleteid);

CREATE INDEX ix_lx_result_event
ON lx_result(eventid);

CREATE INDEX ix_lx_result_event_heat
ON lx_result(eventid, heatnumber);

CREATE INDEX ix_lx_result_club
ON lx_result(clubid);

CREATE INDEX ix_lx_result_time
ON lx_result(timehundredths);

CREATE INDEX ix_lx_result_query
ON lx_result(athleteid, timehundredths);

CREATE INDEX ix_lx_result_clubhist
ON lx_result(athleteid, clubid);

-- =====================
-- SPLIT
-- =====================
CREATE TABLE lx_split (
    id BIGSERIAL PRIMARY KEY,
    resultid BIGINT NOT NULL REFERENCES lx_result(id),
    distance INT,
    timehundredths INT
);

CREATE INDEX ix_lx_split_result
ON lx_split(resultid);

-- =====================
-- RELAY RESULT (heat as counter: eventid + heatnumber)
-- =====================
CREATE TABLE lx_relayresult (
    id BIGSERIAL PRIMARY KEY,
    eventid BIGINT REFERENCES lx_event(id),
    heatnumber INT,
    clubid BIGINT REFERENCES lx_club(id),
    timehundredths INT,
    status VARCHAR(10),
    rank INT
);

CREATE INDEX ix_lx_relayresult_event
ON lx_relayresult(eventid);

CREATE INDEX ix_lx_relayresult_event_heat
ON lx_relayresult(eventid, heatnumber);

CREATE INDEX ix_lx_relayresult_club
ON lx_relayresult(clubid);

-- =====================
-- RELAY SWIMMER
-- =====================
CREATE TABLE lx_relayswimmer (
    id BIGSERIAL PRIMARY KEY,
    relayresultid BIGINT NOT NULL REFERENCES lx_relayresult(id),
    athleteid BIGINT NOT NULL REFERENCES lx_athlete(id),
    swimorder INT
);

CREATE INDEX ix_lx_relayswimmer_result
ON lx_relayswimmer(relayresultid);

CREATE INDEX ix_lx_relayswimmer_athlete
ON lx_relayswimmer(athleteid);

-- =====================
-- CLUB AFFILIATION
-- =====================
CREATE TABLE lx_athleteclubaffiliation (
    id BIGSERIAL PRIMARY KEY,
    athleteid BIGINT NOT NULL REFERENCES lx_athlete(id),
    clubid BIGINT NOT NULL REFERENCES lx_club(id),
    validfrom DATE,
    validto DATE,
    sourcemeetid BIGINT REFERENCES lx_meet(id)
);

CREATE INDEX ix_lx_aff_athlete
ON lx_athleteclubaffiliation(athleteid);

CREATE INDEX ix_lx_aff_timeline
ON lx_athleteclubaffiliation(athleteid, validfrom);

-- =====================
-- IMPORTED FILE TRACKING
-- =====================
CREATE TABLE importedlenexfile (
    onlineeventid BIGINT PRIMARY KEY,
    eventname VARCHAR(255) NULL,
    eventdatefrom DATE NULL,
    eventdateto DATE NULL,
    filename VARCHAR(255) NULL,
    url VARCHAR(511) NULL,
    createdat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(32) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'lenex_not_found', 'downloaded', 'backed_up', 'processed', 'processing_failed', 'scraped', 'scrape_failed'))
);

COMMENT ON COLUMN importedlenexfile.status IS 'pending=awaiting fetch, lenex_not_found=no file, downloaded=on disk, backed_up=on gdrive, processed=imported from LENEX, processing_failed=import failed, scraped=parsed from MUSZ web pages, scrape_failed=scraper attempted but failed';
