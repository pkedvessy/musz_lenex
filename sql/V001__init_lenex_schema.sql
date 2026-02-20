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
    course VARCHAR(10)
);

CREATE INDEX ix_lx_meet_date
ON lx_meet(startdate);

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
-- HEAT
-- =====================
CREATE TABLE lx_heat (
    id BIGSERIAL PRIMARY KEY,
    eventid BIGINT NOT NULL REFERENCES lx_event(id),
    sessionid BIGINT REFERENCES lx_session(id),
    heatnumber INT
);

CREATE INDEX ix_lx_heat_event
ON lx_heat(eventid);

CREATE INDEX ix_lx_heat_session
ON lx_heat(sessionid);

-- =====================
-- RESULT
-- =====================
CREATE TABLE lx_result (
    id BIGSERIAL PRIMARY KEY,
    athleteid BIGINT NOT NULL REFERENCES lx_athlete(id),
    heatid BIGINT NOT NULL REFERENCES lx_heat(id),
    clubid BIGINT REFERENCES lx_club(id),
    lane INT,
    timehundredths INT,
    status VARCHAR(10),
    rank INT,
    reactiontime INT
);

CREATE INDEX ix_lx_result_athlete
ON lx_result(athleteid);

CREATE INDEX ix_lx_result_heat
ON lx_result(heatid);

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
-- RELAY RESULT
-- =====================
CREATE TABLE lx_relayresult (
    id BIGSERIAL PRIMARY KEY,
    heatid BIGINT NOT NULL REFERENCES lx_heat(id),
    clubid BIGINT REFERENCES lx_club(id),
    timehundredths INT,
    status VARCHAR(10),
    rank INT
);

CREATE INDEX ix_lx_relayresult_heat
ON lx_relayresult(heatid);

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
        CHECK (status IN ('pending', 'lenex_not_found', 'downloaded', 'backed_up', 'processed', 'processing_failed'))
);

COMMENT ON COLUMN importedlenexfile.status IS 'pending=awaiting fetch, lenex_not_found=no file, downloaded=on disk, backed_up=on gdrive, processed=imported to lx_*, processing_failed=import failed';
