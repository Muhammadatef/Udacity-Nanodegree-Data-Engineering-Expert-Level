#Importing_Libraries
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


    
def songs_file_processing(cur, path):

    """
    Process songs files and insert rows into the database.
    
    paramaters:
             cur: cursor object
             filepath: complete file path for the file to load
             
     Returns:
             None        
     """
    

    dataframe = pd.read_json(path, dtype={'year': int}, lines=True)

    song_data = dataframe[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    artist_data = dataframe[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)



    

def log_file_processing(cur, path):
    
    """
    Process event log files and insert rows into the database
    
    paramaters 
           cur: cursor object
           filepath: complete file path for the file to load
           
    Returns:
           None       
    """
    
    
    dataframe = pd.read_json(path, lines=True)

    dataframe = dataframe[dataframe["page"] == "NextSong"]
    

    time = pd.to_datetime(dataframe["ts"], unit='ms')
    
    time_data = (time, time.dt.hour, time.dt.day, time.dt.week, time.dt.month, time.dt.year, time.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_dataframe = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, rows in time_dataframe.iterrows():
        cur.execute(time_table_insert, list(rows))

    user_dataframe = dataframe[["userId", "firstName", "lastName", "gender", "level"]]

    for i, rows in user_dataframe.iterrows():
        cur.execute(user_table_insert, rows)

    dataframe["ts"] = pd.to_datetime(dataframe["ts"], unit='ms')
    
    for i, rows in dataframe.iterrows():
        
        cur.execute(song_select, (rows.song, rows.artist, rows.length))
        result = cur.fetchone()
        
        if result:
            songid, artistid = result
        else:
            songid, artistid = None, None

        songplay_data = [i+1, rows.ts, rows.userId, rows.level, songid, artistid, rows.sessionId, rows.location, rows.userAgent]
        cur.execute(songplay_table_insert, songplay_data)

        

def data_processing(cur, conn, path, function):
    """
    This function is responsible for listing the files in a directory,
    and then executing the ingest process for each file according to the function
    that performs the transformation to save it to the database.
    
    Parameters: 
              cur : cursor object
              conn : connection to the postgresql database
              path : path of the file 
              function: function to transform the data and insert it into the database
    Returns: 
              None
    """
    
    

    
    files_list = []
    for root, dirs, files in os.walk(path):
        files = glob.glob(os.path.join(root,'*.json'))
        for file in files:
            files_list.append(os.path.abspath(file))

    number_of_files = len(files_list)
    print(f"{number_of_files} files found in {path}")

    for i, datafile in enumerate(files_list, 1):
        function(cur, datafile)
        conn.commit()
        print(f"{i}/{number_of_files} processed.")
        
        


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    data_processing(cur, conn, path='data/song_data', function =songs_file_processing)
    data_processing(cur, conn, path='data/log_data', function=log_file_processing)

    conn.close()


if __name__ == "__main__":
    main()