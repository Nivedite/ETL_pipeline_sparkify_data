#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import datetime

def process_song_file(cur, filepath):
    '''
    Extracts song relevant data and artist relevant data from the song data and insert into
     the song and artist tables.

    Args:
        cur (psycopg2.extensions.cursor): connection cursor used to execute statements in the Sparkify database
        filepath (str): this argument points to the filepath location of the songplay data
    '''
    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0]
    
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Extracts timestamp data and Sparkify user relevant data from the songplay log data and inserts the data into the 
    time and user tables. Extracts the songplay data from the log data and returns song_id and artist_id matches from the song and 
    artist tables, combines the sonplay data with the song_id and artist_id, then loads into the songplay table in the Sparkify database.
    
    Args:
        cur (psycopg2.extensions.cursor): connection cursor used to execute statements in the Sparkify database
        filepath (str): this argument points to the filepath location of the songplay data
    '''
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = df.ts.apply(lambda x: datetime.datetime.fromtimestamp(x/1000))
    df.ts = t
    
    # insert time data records
    #time_data = 
    column_labels = ['start_time', 'hour',' day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday])))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        
    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
                             row.ts,
                             row.userId,
                             row.level,
                             songid,
                             artistid,
                             row.sessionId,
                             row.location,
                             row.userAgent)

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Executes the process_log_file and process_song_file functions on all the JSON files found in the filepath.

    Args:
        cur (psycopg2.extensions.cursor): connection cursor used to execute statements in the Sparkify database
        conn (psycopg2.extensions.connection): database connection object
        filepath (str): this argument points to the filepath location of the songplay data
        func (user-defined function): function to execute on data extracted in the filepath
    '''
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=Nive1999")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()


# In[ ]:




