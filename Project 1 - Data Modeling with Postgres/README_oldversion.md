# DATA MODELING WITH POSTGRES

## Purpose of this database

The purpose of the database is to provide to Sparkify with information in a friendly format that allows them to query and obtain answers to their questions. The information gathered by Sparify is in the format of json files that contains songs information as well as the logs of the activities performed by users regarding themselves as well as about what they listen to, when, etc.

Processing this log files and include this information in a well designed database can provide Sparkify with ways to understand better their customers and their behaviors. That way they can implement solutions that address both organizational as well as customer needs.

## Justification of database schema and ETL pipeline

The database created is a denormalized database (star schema) that has four dimensional tables (users, songs, artists and time) which are descriptive attributes of the fact data. The database contains a fact table that (songplays) and some relevant associated information. This is an effective way for handling simpler queries and can provide simplified business answers.  A start schema like the one in Sparkify database produces query performance gains since it is a read-only. Executing aggregation operations are easy and efficient.

It can be used to build OLAP cubes for analytical purposes.

The dimensional tables are USERS, SONGS, ARTISTS and TIME and are created as follows:

 - users (user_id int PRIMARY KEY, first_name varchar, last_name varchar, gender varchar, level varchar)

- songs (song_id varchar PRIMARY KEY, title varchar, artist_id varchar, year int, duration numeric)

 - artists (artist_id varchar PRIMARY KEY, name varchar, location varchar, latitude numeric, longitude numeric)

- time (start_time timestamp PRIMARY KEY, hour smallint, day smallint, week smallint, month smallint, year int, weekday smallint)

The fact table (SONGPLAYS) is defined as follows:

- songplays (songplay_id serial PRIMARY KEY, start_time timestamp, user_id int, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar)

