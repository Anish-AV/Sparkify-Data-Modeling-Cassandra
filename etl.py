import csv

# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster(['127.0.0.1'],port=9042)

# To establish connection and begin executing queries, need a session
session = cluster.connect()

#create namespace

try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS newkey 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)


try:
    session.set_keyspace('newkey')
except Exception as e:
    print(e)


#Delete and create the sessions table to match query 1
delete_sessions_table_query = "DROP TABLE IF EXISTS sessions"
session.execute(delete_sessions_table_query)

create_sessions_table_query = "CREATE TABLE IF NOT EXISTS sessions (artist text, item_in_session int, \
length float, session_id int, song_title text, PRIMARY KEY (session_id, item_in_session))"
session.execute(create_sessions_table_query)


#Insert all sessions from the csv data into the sessions table
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO sessions (artist, item_in_session, length, session_id, song_title)"
        query = query + " VALUES (%s, %s, %s, %s, %s)"
        session.execute(query, (line[0], int(line[3]), float(line[5]), int(line[8]), line[9]))


#Do a SELECT to verify that the data have been inserted into the table
#Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
select_session_quert = "select artist, song_title, length from sessions WHERE session_id = 338 and item_in_session = 4"
rows = session.execute(select_session_quert)
for row in rows:
    print (row.artist, row.song_title, row.length)

delete_sessions_table_query = "DROP TABLE IF EXISTS sessions"
session.execute(delete_sessions_table_query)

session.shutdown()
cluster.shutdown()