import psycopg2
import sys
from collections import OrderedDict
from config import config
from csv_parser import write_csv
from datetime import datetime
import hashlib
import message_processing as mp
import base64


def query_message_from_to(cur):
    """Generator function that queries who sent which message to whom at which date on which channel with additional informations
    about the message such as the id of the post, its parent id (if it was a reply to another post) to be able to construct a tree,
    the extension of the file if it was sent with a file joined and the number of words/char in the message and a list of emojis and 
    a set of mentions (with raw data for the moment)
    The sender and receivers are anonymised using MD5 hashing.
    Only people who were on the channel at the time the message was sent are taken in account as receivers, using the channelmemberhistory
    table from the database. 
    Only other people are considered as receivers, not the one sending the message.

    Parameters
    ----------
    cur : The cursor to write query to the database

    Returns
    -------
    Generator((string, string, int, int, List<String>, Set<String>, string, char, list<string>, datetime, string, string, string))
        A generator of tuples containing 
        - A string for the md5 hash of the mail of the sender
        - A string for the message
        - An int for the number of words in the message (emojis are not counted as words)
        - An int for the number of chars in the message
        - An list of string with every emojis in the message
        - A set of strings with all the mentions (as raw text for now on)
        - A string for the name of the channel on which the message was sent
        - A char for the type of channel on wichh the message was sent (O for public, P for private or D for direct messages)
        - A list of string for the md5 hash of the mail of the receivers (in the order in which they joined the channel)
        - A datetime at which the message was sent
        - A string for the id of the post
        - A string for the parent id of the post (if it a response to another post) or NULL if it not a response
        - A string for the extension of the file if the message was sent with a document (picture for example) or NULL otherwise
    """

    query = """
        SELECT U.email AS sender, P.message,  C.name as channel_name, C.type AS channel_type, (SELECT email from users where users.id = CMH.userid) as receiver,
        P.createat, P.id AS postid, P.parentid as post_parent_id, F.extension as file_extension FROM posts P
        INNER JOIN users U ON P.userid = U.id
        INNER JOIN channelmemberhistory CMH ON P.channelid = CMH.channelid
        INNER JOIN channels C ON P.channelid = C.id
        LEFT JOIN fileinfo F ON P.id = F.postid
        WHERE P.message!='' AND P.createat > CMH.jointime AND (CMH.leavetime IS NULL OR CMH.leavetime>P.createat)
        AND C.id NOT IN (SELECT channelid from channelmemberhistory where userid = (SELECT id from users where username='surveybot'))
        AND P.type!='system_join_channel' AND P.type!='system_add_to_channel' AND P.type!='system_join_team'
        ORDER BY P.createat DESC, P.message ASC
        """

    cur.execute(query)
    rows = cur.fetchall()

    row_definition = ("Sender", "Message", "NumberWords", "NumberChars","Emojis", "Mentions", "Channel", "ChannelType", "Receivers", "Time", "PostId", "PostParentId", "FileExtension")
    yield row_definition

    #Here we just do a "groupby" to have a list of receivers for the same message and we hash them with md5. We store it in a dictionnary for simplicity.
    message_to_list_receivers = OrderedDict()
    for sender, message, channel, channel_type, receiver, unix_time, post_id, post_parent_id, file_extension in rows:
        hash_receivers = message_to_list_receivers.setdefault((sender, message, channel, channel_type, unix_time, post_id, post_parent_id, file_extension), [])
        if(sender!=receiver):
            hash_receivers.append(hashlib.md5(receiver.encode()).hexdigest())

    
    for (sender, message, channel, channel_type, unix_time, post_id, post_parent_id, file_extension), hash_receivers in message_to_list_receivers.iteritems():
        md5_sender = hashlib.md5(sender.encode()).hexdigest()
        date = datetime.fromtimestamp(unix_time/1000)
        no_words, emojis, mentions = mp.count_words_emojis_mentions(message)
        no_char = len(message)
        yield md5_sender, message, no_words, no_char, emojis, mentions, channel, channel_type, hash_receivers, date, post_id, post_parent_id, file_extension


def query_message_from_toNumber(cur):
    """Function that queries who sent which message to how many persons at which date on which channel with additional informations
    about the message such as the id of the post, its parent id (if it was a reply to another post) to be able to construct a tree,
    the extension of the file if it was sent with a file joined and the number of words/emojis/char in the message.
    The sender is anonymised using MD5 hashing.
    Only people who were on the channel at the time the message was sent are taken in account as receivers, using the channelmemberhistory table
    from the database. 
    Only other people are considered as receivers, not the one sending the message.
    This method runs faster than query_message_from_to because it used the groupby method inside the database to group the number of receivers.

    Parameters
    ----------
    cur : The cursor to write query to the database

    Returns
    -------
    List((string, string, int, int, list<string>, set<string>, string, char, int, datetime, string, string, string))
        A list of tuples containing 
        - A string for the md5 hash of the mail of the sender
        - A string for the message
        - An int for the number of words in the message (emojis are not counted as words)
        - An int for the number of chars in the message
        - An list of string with every emojis in the message
        - A set of strings with all the mentions (as raw text for now on)
        - A string for the name of the channel on which the message was sent
        - A char for the type of channel on wichh the message was sent (O for public, P for private or D for direct messages)
        - An int for the number of receivers.
        - A datetime at which the message was sent
        - A string for the id of the post
        - A string for the parent id of the post (if it a response to another post) or NULL if it not a response
        - A string for the extension of the file if the message was sent with a document (picture for example) or NULL otherwise
    """

    query = """SELECT U.email AS sender, P.message,  C.name as channel_name, C.type AS channel_type, COUNT(CMH.userid)-1 as number_receivers,
        P.createat, P.id AS postid, P.parentid as post_parent_id, F.extension as file_extension FROM posts P
        INNER JOIN users U ON P.userid = U.id
        INNER JOIN channelmemberhistory CMH ON P.channelid = CMH.channelid
        INNER JOIN channels C ON P.channelid = C.id
        LEFT JOIN fileinfo F ON P.id = F.postid
        WHERE P.message!='' AND P.createat > CMH.jointime AND (CMH.leavetime IS NULL OR CMH.leavetime>P.createat)
        AND C.id NOT IN (SELECT channelid from channelmemberhistory where userid = (SELECT id from users where username='surveybot'))
        AND P.type!='system_join_channel' AND P.type!='system_add_to_channel' AND P.type!='system_join_team'
        GROUP BY U.email, P.message, C.name, C.type, P.createat, P.id, F.extension
        ORDER BY P.createat DESC, P.message ASC"""
    
    cur.execute(query)
    rows = cur.fetchall()
    
    def process((sender, message, channel, channel_type, number_receivers, unix_time, post_id, post_parent_id, file_extension)):
        md5_sender = hashlib.md5(sender.encode()).hexdigest()
        date = datetime.fromtimestamp(unix_time/1000)
        no_words, emojis, mentions = mp.count_words_emojis_mentions(message)
        no_char = len(message)
        return md5_sender, message, no_words, no_char, emojis, mentions, channel, channel_type, number_receivers, date, post_id, post_parent_id, file_extension

    result = map(process, rows)

    row_definition = ("Sender", "Message", "NumberWords", "NumberChars","Emojis", "Mentions", "Channel", "ChannelType", "NumberReceivers", "Time", "PostId", "PostParentId", "FileExtension")
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

        write_csv(data=query_result, filename='csv/from_message_to_at_message_count.csv')

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