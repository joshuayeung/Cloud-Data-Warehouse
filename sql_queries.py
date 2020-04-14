import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist          VARCHAR,
    auth            VARCHAR,
    firstName       VARCHAR,
    gender          CHAR(1),
    itemInSession   INTEGER,
    lastName        VARCHAR,
    length          DECIMAL,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    DECIMAL,
    sessionId       INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              VARCHAR,
    userAgent       VARCHAR,
    userId          INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           INTEGER,
    artist_id           VARCHAR,
    artist_latitude     DECIMAL,
    artist_longitude    DECIMAL,
    artist_location     VARCHAR,
    artist_name         VARCHAR,
    song_id             VARCHAR,
    title               VARCHAR,
    duration            DECIMAL,
    year                INTEGER
)
""")

# Fact Table
# songplays - records in event data associated with song plays i.e. records with page NextSong
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
    songplay_id INTEGER     IDENTITY (0,1)  PRIMARY KEY, 
    start_time  TIMESTAMP      NOT NULL, 
    user_id     INTEGER     NOT NULL, 
    level       VARCHAR, 
    song_id     VARCHAR, 
    artist_id   VARCHAR, 
    session_id  INTEGER, 
    location    VARCHAR, 
    user_agent  VARCHAR
)
""")

# Dimension Tables
# users - users in the app
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id     INTEGER PRIMARY KEY, 
    first_name  VARCHAR, 
    last_name   VARCHAR, 
    gender      VARCHAR, 
    level       VARCHAR
)
""")

# songs - songs in music database
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id     VARCHAR PRIMARY KEY, 
    title       VARCHAR, 
    artist_id   VARCHAR, 
    year        INTEGER, 
    duration    DECIMAL
)
""")

# artists - artists in music database
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id   VARCHAR PRIMARY KEY, 
    name        VARCHAR, 
    location    VARCHAR, 
    latitude    DECIMAL, 
    longitude   DECIMAL
)
""")

# time - timestamps of records in songplays broken down into specific units
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time  TIMESTAMP   PRIMARY KEY, 
    hour        INTEGER, 
    day         INTEGER, 
    week        INTEGER, 
    month       INTEGER, 
    year        INTEGER, 
    weekday     INTEGER
)
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    TIMEFORMAT AS 'auto'
    FORMAT AS JSON {}
""").format(config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON 'auto'
""").format(config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT  TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, 
        userId AS user_id, 
        level, 
        song_id, 
        artist_id, 
        sessionId AS session_id, 
        location, 
        userAgent
FROM staging_events
INNER JOIN staging_songs
ON staging_events.artist = staging_songs.artist_name AND staging_events.song = staging_songs.title
WHERE page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT  DISTINCT userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
FROM staging_events
WHERE userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (song_id,title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT  DISTINCT artist_id, 
        artist_name AS name, 
        artist_location AS location, 
        artist_latitude AS latitude, 
        artist_longitude AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT  DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
        EXTRACT(hour from TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS hour,
        EXTRACT(day from TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS day,
        EXTRACT(week from TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS week,
        EXTRACT(month from TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS month,
        EXTRACT(year from TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS year,
        EXTRACT(weekday from TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS weekday
FROM staging_events
WHERE ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
