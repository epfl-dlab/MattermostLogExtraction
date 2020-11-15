import psycopg2
import sys
from collections import OrderedDict
from config import config
from csv_parser import write_csv
from datetime import datetime


def query_message_from_to(cur):
    """Generator function that queries who sent which message to whom at which date. Only people who were on the channel at the
    time the message was sent are taken in account, using the channelmemberhistory table from the database. Only other people are
    considered as receivers, not the one sending the message.

    Parameters
    ----------
    cur : The cursor to write query to the database

    Returns
    -------
    Generator((string, string, string, list<string>, datetime))
        A list of tuples containing a string for the sender, a string for the message, the channel
        on which the message was sent, a list of string for the receivers (in the order in which they joined the channel) and a datetime at which
        the message was sent
    """

    query = """
        SELECT U.username, P.message,  C.name, (SELECT username from users where users.id = CMH.userid) as receiver, P.createat FROM posts P
        INNER JOIN users U ON P.userid = U.id
        INNER JOIN channelmemberhistory CMH ON P.channelid = CMH.channelid
        INNER JOIN channels C ON P.channelid = C.id
        WHERE P.message!='' AND P.createat > CMH.jointime AND (CMH.leavetime IS NULL OR CMH.leavetime>P.createat)
        ORDER BY P.createat DESC, U.username ASC, P.message ASC
        """

    cur.execute(query)
    rows = cur.fetchall()

    row_definition = ("Sender", "Message", "Channel", "Receivers", "Time")
    yield row_definition

    message_to_list_receivers = OrderedDict()
    for sender, message, channel, receiver, unix_time in rows:
        receivers = message_to_list_receivers.setdefault((sender, message, channel, unix_time), [])
        if(sender!=receiver):
            receivers.append(receiver)
    
    for (sender, message, channel, unix_time), receivers in message_to_list_receivers.iteritems():
        yield sender, message, channel, receivers, datetime.fromtimestamp(unix_time/1000)


def query_message_from_toNumber(cur):
    """Function that queries who sent which message to how many persons at which date. Only people who were on the channel at the
    time the message was sent are taken in account, using the channelmemberhistory table from the database. Only other people are
    counted as receivers, not the one sending the message. This method runs faster than query_message_from_to which generates a list
    of the receivers instead of the numbers.

    Parameters
    ----------
    cur : The cursor to write query to the database

    Returns
    -------
    List((string, string, string, int, datetime))
        A list of tuples containing a string for the sender, a string for the message, the channel
        on which the message was sent, the number of receivers and a datetime at which
        the message was sent
    """

    query = """
        SELECT U.username, P.message,  C.name, COUNT(CMH.userid)-1 as number_receivers, P.createat FROM posts P
        INNER JOIN users U ON P.userid = U.id
        INNER JOIN channelmemberhistory CMH ON P.channelid = CMH.channelid
        INNER JOIN channels C ON P.channelid = C.id
        WHERE P.message!='' AND P.createat > CMH.jointime AND (CMH.leavetime IS NULL OR CMH.leavetime>P.createat)
        GROUP BY U.username, P.message, C.name, P.createat
        ORDER BY P.createat DESC, U.username ASC, P.message ASC
        """
    
    cur.execute(query)
    rows = cur.fetchall()
    
    f = lambda (sender, message, channel_name, number_receivers, unix_time) : (sender, message, channel_name, number_receivers, datetime.fromtimestamp(unix_time/1000))

    result = map(f, rows)

    row_definition = ("Sender", "Message", "Channel", "Number_Receivers", "Time")
    result.insert(0, row_definition)

    return result


def main():
    
    conn = None
    cur = None
    print("Connecting to the PostgresSQL database...")

    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        print("Succesfully connected to the database. Will start writing queries.")

        #query_result = query_message_from_toNumber(cur)
        query_result = query_message_from_to(cur)

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