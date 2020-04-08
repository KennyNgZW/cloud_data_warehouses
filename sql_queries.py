import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# get the following info to configure the cluster
# CLUSTER_REGION = config.get('CLUSTER', 'CLUSTER_REGION')
S3_LOG_DATA = config.get('S3', 'LOG_DATA')
S3_SONG_DATA = config.get('S3', 'SONG_DATA')
S3_LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
IAM_ROLE_ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# the staging table is for the OLTP step to get the data from the S3) 
# staging table for events (log) 
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession integer,
    lastName varchar,
    length float,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    sessionId integer,
    song varchar,
    status varchar,
    ts varchar,
    userAgent varchar,
    userId varchar
    )
    diststyle auto;""")

# staging table for songs records
staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
    num_songs integer,
    artist_id varchar, 
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year integer
    )
    diststyle auto;""")


# below tables in the star schema extract data from the staging tables

# user dimension table
# primary key: user_id
# distribute style: all
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
    user_id integer NOT NULL PRIMARY KEY,
    firstName varchar,
    lastName varchar,
    gender varchar, 
    level varchar
    )
    diststyle all;""")

# song dimension table
# primary key: song_id
# distribute style: key (song_id)
song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song
    (
    song_id varchar NOT NULL PRIMARY KEY,
    title varchar, 
    artist_id varchar NOT NULL, 
    year integer, 
    duration float
    )
    distkey(song_id);""")

# artist dimension table
# primary key: artist_id
# distribute style: all
artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist
    (
    artist_id varchar NOT NULL PRIMARY KEY,
    name varchar, 
    location varchar,
    latitude float,
    longitude float
    )
    diststyle even;""")

# time dimension table
# primary key: start_time
# distribute style: all
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
    start_time timestamp NOT NULL PRIMARY KEY,
    hour smallint,
    day smallint,
    week smallint, 
    month smallint, 
    year smallint, 
    weekday smallint
    )
    diststyle even;""")

# songplay fact table
# primary key: songplay_id
# distribute this table based on the song_id and sort the table by start_time
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay
    (
    songplay_id integer identity(0,1) NOT NULL PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id integer NOT NULL,
    level varchar,
    song_id varchar NOT NULL,
    artist_id varchar NOT NULL,
    session_id integer,
    location varchar,
    user_agent varchar
    )
    distkey(song_id)
    sortkey(start_time);""")

# STAGING TABLES
staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    JSON {}
    """).format(S3_LOG_DATA, IAM_ROLE_ARN, S3_LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    json 'auto';
    """).format(S3_SONG_DATA, IAM_ROLE_ARN)


# INSERT DATA TO DIMENSION AND FACT TABLES
user_table_insert = ("""
INSERT INTO users (user_id, firstName, lastName, gender, level)
SELECT
    DISTINCT cast(userId as integer) as userId,
    firstName,
    lastName,
    gender,
    max(level) as level
FROM staging_events
where nvl(trim(userId), '') <> ''
group by userId, firstName, lastName, gender;
""")

song_table_insert = ("""
INSERT INTO song (song_id, title, artist_id, year, duration)
SELECT
    DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
where song_id <> '';
""")

artist_table_insert = ("""
INSERT INTO artist (artist_id, name, location, latitude, longitude)
SELECT
    DISTINCT artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM 
(
    select
        DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude,
        row_number() over(partition by artist_id order by year desc, artist_name) rno
    from staging_songs
) AS a 
where rno = 1;
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT
    DISTINCT timestamp 'epoch' + se.ts::bigint / 1000 * interval '1 second' as start_time,
    extract('hour' from start_time) as hour,
    extract('day' from start_time) as day,
    extract('week' from start_time) as week,
    extract('month' from start_time) as month,
    extract('year' from start_time) as year,
    extract('weekday' from start_time) as weekday
FROM staging_events AS se
where se.ts<> '';
""")

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
    DISTINCT timestamp 'epoch' + se.ts::bigint / 1000 * interval '1 second' as start_time,
    case when nvl(userId, '') = '' then NULL else cast(se.userId as integer) end as userId,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.userAgent
FROM staging_events AS se
left join staging_songs AS ss
on se.song = ss.title
and se.length = ss.duration
and se.artist = ss.artist_name
where se.page = 'NextSong'
and ss.song_id <> '';
""")

# COUNT THE RECORD IN EVERY TABLE
count_songplay = ("""
SELECT COUNT(songplay_id) AS songplay_num
FROM songplay
""")

count_song = ("""
SELECT COUNT(song_id) AS song_num
FROM song
""")

count_artist = ("""
SELECT COUNT(artist_id) AS artist_num
FROM artist
""")

count_user = ("""
SELECT COUNT(user_id) AS user_num
FROM users
""")

count_time = ("""
SELECT COUNT(*) AS time_record
FROM time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

count_table_queries = [count_songplay, count_song, count_artist, count_user, count_time]
