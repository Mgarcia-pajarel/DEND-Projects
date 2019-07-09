
# Import necessary libraries

import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):  
    """
    Description: This function is used to read the files in the filepath (data/song_data) in order
    to get the user and time info and with them populate the songs and artists dimensional tables.
    
    Arguments:
        cur: the cursor object. 
        filepath: log data file path. 
    
    Returns:
        None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
     
    song_data=[]
    tmp_songs=df[['song_id', 'title', 'artist_id', 'year', 'duration']].values
    for item in tmp_songs[0]:
        song_data.append(item)
    
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    
    artist_data=[]
    tmp_artists=df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values
    for item in tmp_artists[0]:
        artist_data.append(item)
    
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    """
    Description: This function is used to read the files in the filepath (data/log_data) in order
    to get the user and time info and with them used to populate the users and time dimensional tables.
    
    Arguments:
        cur: the cursor object. 
        filepath: log data file path. 
    
    Returns:
        None
    """
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action and assigns back to the same data frame. The main dataframe gets overwritten
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    
    timestamp=t
    hour=t.dt.hour
    day=t.dt.day
    week=t.dt.week
    month=t.dt.month
    year=t.dt.year
    weekday=t.dt.weekday

    time_data = list(zip(timestamp, hour, day, week, month, year, weekday)) 
    column_labels = columns_labels=('timestamp','hour','day','week','month','year','weekday')
    
    # We create a new data frame based on time_data and the defined column_labels
    time_df=pd.DataFrame(time_data, columns=columns_labels)
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        
    # load user table
    user_df = pd.DataFrame(df, columns=['userId','firstName', 'lastName', 'gender', 'level'])

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables based on the match of song, artist and length
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):    
    """
    Description: This function is used to collect all JSON files and call functions defined 
    above (process_song_file, process_log_file) to process logs.
    
    Arguments:
        cur: the cursor object
        conn: connection to database
        filepath: log data file path 
        func: function, in which we  processed log data 
    
    Returns:
        None
    """  
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

def main():
    
    """
    Description: main function is the first when the script etl.py is ran. It establishes and closes the 
    connection to the database, calls define above functions process_data and associated process_song_file and 
    process_log_file in order to process the logs and populate the database.
    
    Arguments:
        None
    
    Returns:
        None
    """ 
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()