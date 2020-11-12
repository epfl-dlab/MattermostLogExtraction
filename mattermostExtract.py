import psycopg2
import sys
from config import config
from csv_parser import write_csv
from datetime import datetime

def query_message_from_to(cur, channelid_to_username):
    """Generator function that query who sent which message to whom at which date

    Parameters
    ----------
    cur : The cursor to write query to the database
    channelid_to_username : A dictionnary that maps channelid to username

    Returns
    -------
    List((string, string, string, list<string>, datetime))
        A list of tuples containing a string for the sender, a string for the message, the channel
        on which the message was sent, a list of string for the receivers and a datetime at which
        the message was sent
    """

    query = """
        SELECT U.username, P.message, C.name, C.id AS channelid, P.createat FROM posts P
        INNER JOIN users U ON P.userid = U.id
        INNER JOIN channels C ON P.channelid = C.id
        WHERE P.message!=''
        ORDER BY createat DESC
        """
    cur.execute(query)
    rows = cur.fetchall()

    f = lambda (sender, message, channel_name, channelid, unix_time) : (sender, message, channel_name, channelid_to_username[channelid], datetime.fromtimestamp(unix_time/1000))

    result = map(f, rows)

    row_definition = ("Sender", "Message", "Channel", "Receivers", "Time")
    result.insert(0, row_definition)

    return result

#    yield row_definition
#    for sender, message, channel_name, channelid, unix_time in rows:
#        date = datetime.fromtimestamp(unix_time/1000)
#        yield sender, message, channel_name, channelid_to_username[channelid], date

    
def create_dictionnary_channelid_to_username(cur):
    """Create a dictionnary that maps channelid to username

    Parameters
    ----------
    cur : The cursor to write query to the database

    Returns
    -------
    dictionnary(str, list[str])
        A dictionnary where the key is the channelid and the value a list of all username
        that are on the channel
    """

    query = """
        SELECT CM.channelid, U.username FROM channelmembers CM
        INNER JOIN users U ON CM.userid = U.id
        """

    cur.execute(query)
    rows = cur.fetchall()
    channelid_to_username = {}
    for channelid, username in rows:
        channelid_to_username.setdefault(channelid, []).append(username) 
    return channelid_to_username

def main():
    
    conn = None
    cur = None
    print("Connecting to the PostgresSQL database...")

    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        print("Succesfully connected to the database. Will start writing queries.")

        channelid_to_username = create_dictionnary_channelid_to_username(cur)
        query_result = query_message_from_to(cur, channelid_to_username)
        write_csv(data=query_result, filename='from_message_to_at.csv')

    except(Exception, psycopg2.DatabaseError) as error:
        print("Error: " + str(error))
        print("Programm exits.")
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    main()