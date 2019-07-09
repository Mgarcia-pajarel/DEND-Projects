import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# GLOBAL VARIABLES

LOG_DATA = config.get("S3","LOG_DATA")
LOG_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays;"
user_table_drop = "DROP TABLE IF EXISTS dim_users;"
song_table_drop = "DROP TABLE IF EXISTS dim_songs;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events 
                        (artist varchar(100),
                         auth varchar(100),
                         firstName varchar(50),
                         gender varchar(10),  
                         itemInSession int,
                         lastName varchar(50),
                         length numeric,
                         level varchar(25),
                         location varchar(100),
                         method varchar(3),
                         page varchar(25),
                         registration DOUBLE PRECISION,
                         sessionId int,
                         song varchar,
                         status int,
                         ts timestamp,
                         userAgent varchar(150),
                         userId int)
                         ;""")
                                
staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs 
                        (song_id varchar,
                         num_songs int,
                         title varchar,
                         artist_name varchar,  
                         artist_latitude numeric,
                         artist_longitude numeric,
                         year int,
                         duration numeric,
                         artist_id varchar,
                         artist_location varchar(100))
                         ;""")                 

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS fact_songplays 
                        (songplay_id int IDENTITY(0,1) PRIMARY KEY sortkey,
                        start_time timestamp NOT NULL,
                        user_id int NOT NULL REFERENCES dim_users(user_id),
                        level varchar,
                        song_id varchar,
                        artist_id varchar REFERENCES dim_songs(song_id),
                        session_id int, 
                        location varchar,
                        user_agent varchar)
                        ;""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS dim_users 
                    (user_id int PRIMARY KEY distkey, 
                    first_name varchar, 
                    last_name varchar, 
                    gender varchar, 
                    level varchar)
                    ;""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS dim_songs 
                    (song_id varchar PRIMARY KEY,
                    title varchar, 
                    artist_id varchar distkey,
                    year numeric,
                    duration numeric)
                    ;""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS dim_artists 
                     (artist_id varchar PRIMARY KEY distkey, 
                     artist_name varchar, 
                     artist_location varchar, 
                     artist_latitude numeric, 
                     artist_longitude numeric)
                     ;""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS dim_time
                    (start_time timestamp PRIMARY KEY sortkey distkey, 
                    hour smallint, 
                    day smallint, 
                    week smallint, 
                    month smallint, 
                    year int, 
                    weekday smallint)
                    ;""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2' 
    FORMAT AS JSON {}
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(LOG_DATA, IAM_ROLE, LOG_PATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, IAM_ROLE)      
        

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO fact_songplays 
                        (start_time,
                        user_id,
                        level,
                        song_id,
                        artist_id,
                        session_id,
                        location,
                        user_agent)
                        SELECT DISTINCT
                        to_timestamp(to_char(ev.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                        ev.userId as user_id,
                        ev.level as level,
                        so.song_id as song_id,
                        so.artist_id as artist_id,
                        ev.sessionId as session_id,
                        so.artist_location as location,
                        ev.userAgent as user_agent
                        FROM staging_events ev
                        JOIN staging_songs so
                        ON ev.song = so.title
                        AND ev.artist = so.artist_name
                        AND ev.length = so.duration 
                        ;""")


user_table_insert = ("""INSERT INTO dim_users 
                    (user_id,
                    first_name,
                    last_name,
                    gender,
                    level)
                    SELECT DISTINCT
                    userId,
                    firstName,
                    lastname,
                    gender,
                    level
                    FROM staging_events
                    WHERE userid IS NOT NULL
                    ;""")

song_table_insert = ("""INSERT INTO dim_songs
                    (song_id,
                    title,
                    artist_id,
                    year,
                    duration)
                    SELECT DISTINCT
                    song_id,
                    title,
                    artist_id,
                    year,
                    duration
                    FROM staging_songs
                    WHERE song_id IS NOT NULL
                    ;""")

artist_table_insert = ("""INSERT INTO dim_artists
                      (artist_id,
                      artist_name,
                      artist_location,
                      artist_latitude,
                      artist_longitude)
                      SELECT DISTINCT
                      artist_id,
                      artist_name,
                      artist_location,
                      artist_latitude,
                      artist_longitude
                      FROM staging_songs
                      WHERE artist_id IS NOT NULL
                      ;""")

time_table_insert = ("""INSERT INTO dim_time 
                    (start_time,
                    hour,
                    day,
                    week,
                    month,
                    year,
                    weekday)
                    SELECT DISTINCT
                    ts,
                    extract(hour from ts),
                    extract(day from ts),
                    extract(week from ts),
                    extract(month from ts),
                    extract(year from ts),
                    extract(weekday from ts)
                    FROM staging_events
                    WHERE ts IS NOT NULL
                    ;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, 
                        staging_songs_table_create, 
                        user_table_create, 
                        song_table_create,
                        artist_table_create, 
                        time_table_create,
                        songplay_table_create]

drop_table_queries = [staging_events_table_drop, 
                      staging_songs_table_drop, 
                      songplay_table_drop, 
                      user_table_drop, 
                      song_table_drop, 
                      artist_table_drop, 
                      time_table_drop]

copy_table_queries = [staging_events_copy, 
                      staging_songs_copy]

insert_table_queries = [songplay_table_insert, 
                        user_table_insert, 
                        song_table_insert, 
                        artist_table_insert, 
                        time_table_insert]
