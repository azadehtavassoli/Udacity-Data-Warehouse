import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

LOG_DATA = config.get("S3", "LOG_DATA")
SONG_DATA = config.get("S3", "SONG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
ARN = config.get("IAM_ROLE", "ARN")


# DROP TABLES
staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
time_table_drop = "DROP table IF EXISTS time"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"

# CREATE TABLES

staging_events_table_create = """CREATE TABLE IF NOT EXISTS staging_events (
                                artist          VARCHAR,
                                auth            VARCHAR,
                                firstName       VARCHAR,
                                gender          VARCHAR,
                                itemInSession   int,
                                lastName        VARCHAR,
                                length          NUMERIC,
                                level           VARCHAR,
                                location        VARCHAR,
                                method          VARCHAR,
                                page            VARCHAR,
                                registration    VARCHAR,
                                sessionId       int,
                                song            VARCHAR,
                                status          int,
                                ts              BIGINT,
                                userAgent       VARCHAR,
                                userId          int);"""

staging_songs_table_create = """CREATE TABLE IF NOT EXISTS staging_songs (
                                num_songs        int,
                                artist_id        VARCHAR,
                                artist_latitude  numeric,
                                artist_longitude numeric,
                                artist_location  VARCHAR,
                                artist_name      VARCHAR,
                                song_id          VARCHAR,
                                title            VARCHAR,
                                duration         float,
                                year             int);"""


songplay_table_create = """CREATE TABLE IF NOT EXISTS songplays (songplay_id int IDENTITY(1,1) NOT NULL PRIMARY KEY,\
                                                               start_time timestamp NOT NULL,\
                                                               user_id int NOT NULL sortkey,\
                                                               song_id varchar,\
                                                               artist_id varchar distkey,\
                                                               session_id int,\
                                                               level varchar NOT NULL,\
                                                               location varchar,\
                                                               user_agent varchar);"""

user_table_create = """CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY,\
                                                       first_name varchar NOT NULL,\
                                                       last_name varchar NOT NULL,\
                                                       gender varchar,\
                                                       level varchar);"""

song_table_create = """CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY sortkey,\
                                                       title varchar NOT NULL,\
                                                       artist_id varchar NOT NULL,\
                                                       year int,\
                                                       duration float NOT NULL);"""

artist_table_create = """CREATE TABLE IF NOT EXISTS artists (artist_id varchar NOT NULL PRIMARY KEY sortkey,\
                                                           artist_name varchar NOT NULL,\
                                                           location varchar,\
                                                           latitude float,\
                                                           longitude float);"""

time_table_create = """CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY,\
                                                      hour int,\
                                                      day int,\
                                                      week int,\
                                                      month int,\
                                                      year int,\
                                                      weekday int);"""

# STAGING TABLES

staging_events_copy = (
    """
    copy staging_events from {0}
    credentials 'aws_iam_role={1}'
    format as JSON {2}
    timeformat as 'epochmillisecs'
    region 'us-west-2';
"""
).format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = (
    """
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    format as JSON 'auto'
    timeformat as 'epochmillisecs'
    region 'us-west-2';
"""
).format(SONG_DATA, ARN)


# FINAL TABLES


songplay_table_insert = """ INSERT INTO songplays (
                            start_time, 
                            user_id, 
                            level, 
                            song_id,
                            artist_id, 
                            session_id, 
                            location, 
                            user_agent)
                            SELECT timestamp 'epoch' + ev.ts/1000 * interval '1 second' as start_time, 
                            ev.userId, ev.level, so.song_id, so.artist_id,
                            ev.sessionId, ev.location, ev.userAgent
                            FROM staging_events ev
                            JOIN staging_songs so
                            ON ev.artist = so.artist_name
                            AND ev.song = so.title
                            AND ev.length = so.duration
                            WHERE ev.page = 'NextSong'; """


user_table_insert = """ INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT userId, firstName, lastName, gender, level
                        FROM staging_events WHERE page = 'NextSong' """

song_table_insert = """ INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM staging_songs"""


artist_table_insert = """ INSERT INTO artists (artist_id, artist_name, location, latitude, longitude)
                          SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude,
                          artist_longitude FROM staging_songs"""

time_table_insert = """ INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT start_time,
                        EXTRACT(hour from start_time),
                        EXTRACT(day from start_time),
                        EXTRACT(week from start_time),
                        EXTRACT(month from start_time),
                        EXTRACT(year from start_time),
                        EXTRACT(dayofweek from start_time)
                        FROM songplays"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]