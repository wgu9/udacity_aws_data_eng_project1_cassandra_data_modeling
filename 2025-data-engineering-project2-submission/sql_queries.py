import configparser

# Load configuration
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INTEGER
)
""")

## The fact table
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR
)
DISTSTYLE KEY
DISTKEY (start_time)
SORTKEY (start_time);                           
""")

## The dimension tables
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
)
DISTSTYLE ALL
SORTKEY (user_Id);                     
""")

## The dimension tables
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INTEGER,
    duration FLOAT
)
DISTSTYLE ALL
SORTKEY(song_id);                  
""")

## The dimension tables
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
)
DISTSTYLE ALL
SORTKEY (artist_id);                  
""")

## The dimension tables
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
)
DISTSTYLE KEY
DISTKEY (start_time)
SORTKEY (start_time);   
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, 
                        staging_songs_table_create, 
                        songplay_table_create, 
                        user_table_create, 
                        song_table_create, 
                        artist_table_create, 
                        time_table_create]
drop_table_queries = [staging_events_table_drop, 
                      staging_songs_table_drop, 
                      songplay_table_drop, 
                      user_table_drop, 
                      song_table_drop, 
                      artist_table_drop, 
                      time_table_drop]

## Step 2.1 Review the queries for staging tables

## note: Added .strip("'") to remove the single quotes from the ARN value in your config file

## log data (partitioned by year/month)
staging_events_copy = """
    COPY staging_events FROM 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    json {}
    region 'us-west-2'
    maxerror 10000
    compupdate off
    statupdate off;
""".format(config['IAM_ROLE']['ARN'].strip("'"), config['S3']['LOG_JSONPATH'])


## song data (partitioned by first 3 letters of track ID)
# COPY staging_songs FROM 's3://udacity-dend/song-data/A/A'
# COPY staging_songs FROM 's3://udacity-dend/song-data'
staging_songs_copy = """
    
    COPY staging_songs FROM 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2'
    maxerror 10000
    compupdate off
    statupdate off;
""".format(config['IAM_ROLE']['ARN'].strip("'"))

copy_table_queries = [staging_events_copy, staging_songs_copy]

# FINAL TABLES

## fact and dimension tables

## songplay table is a fact table
## checked
## input: staging_events, staging_songs
## join logic: song_id, artist_id
## the table describes the plays of songs


# Fact Table
# songplays - records in event data associated with song plays i.e. records with page NextSong
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
       se.userId AS user_id,
       se.level,
       ss.song_id,
       ss.artist_id,
       se.sessionId AS session_id,
       se.location,
       se.userAgent AS user_agent
FROM staging_events se
JOIN staging_songs ss
ON se.song = ss.title AND se.artist = ss.artist_name
WHERE se.page = 'NextSong'
      and se.userId IS NOT NULL
      and ss.song_id IS NOT NULL
      and ss.artist_id IS NOT NULL
      and start_time IS NOT NULL
""")

## user table is a dimension table
## checked
user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId AS user_id,
       firstName AS first_name,
       lastName AS last_name,
       gender,
       level
FROM staging_events
WHERE userId IS NOT NULL
""")

## song table is a dimension table
song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,
       title,
       artist_id,
       year,
       duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

## artist table is a dimension table

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id,
       artist_name AS name,
       artist_location AS location,
       artist_latitude AS latitude,
       artist_longitude AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

## time table is a dimension table
## checked.
time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT start_time,
       EXTRACT(hour FROM start_time),
       EXTRACT(day FROM start_time),
       EXTRACT(week FROM start_time),
       EXTRACT(month FROM start_time),
       EXTRACT(year FROM start_time),
       EXTRACT(weekday FROM start_time)
FROM (SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time
      FROM staging_events)
""")


insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

