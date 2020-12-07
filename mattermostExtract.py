# -*- coding: utf-8 -*-
import psycopg2
import sys
from collections import OrderedDict
from config import config
from csv_parser import write_csv
from datetime import datetime
import hashlib
import message_processing as mp
import traceback


def anonymise_non_public_channel(channel_name, channel_type):
    """Functions that anonymise (using md5 hashing of the channel_name) the channel if it is not public.

    Parameters
    ----------
    channel_name : The name of the channel.
    channel_type: The type of the channel.

    Returns
    -------
    String:
        The channel name if it is public and the md5 hashing of its name otherwise.

    """
    return hashlib.md5(channel_name.encode()).hexdigest() if (not channel_type == 'O') else channel_name

def create_map_users_hashed_mail(cur):
    """Functions that creates a dictionnary with the username as key and the md5 hashing of their mail as value.

    Parameters
    ----------
    cur : The cursor to write query to the database.

    Returns
    -------
    Dictionnary(String, String): 
        A dictionnary with the username of the users as key and the md5 hashing of their mail as value.
    """

    query = "SELECT username, email FROM users"
    
    cur.execute(query)
    rows = cur.fetchall()

    users_to_hashed_mail = dict()

    for row in rows:
        username = row[0]
        email = row[1]
        users_to_hashed_mail[username] = hashlib.md5(email.encode()).hexdigest()    

    return users_to_hashed_mail


def hashed_mails_from_mentions(mentions, users_to_hashed_mail, hashed_receivers):
    """Functions that processes the mentions to extract the md5 of the mail of the people who were mentionned.

    Parameters
    ----------
    mentions : The list of all the mentions found in the message.
    users_to_hashed_mail: A dictionnary from the username of the users to the md5 hashing of their emails.
    hashed_receivers: The list of all the md5 hash of the email of the people on the channel when the message was sent.

    Returns
    -------
    Set(String): 
        A set with the md5 hashing of the mail of the people who were mentionned in the message or an empty set if nobody was mentionned.
    """
    hashed_mails = set()
    for mention in mentions:
        if(mention == "all" or mention == "channel" or mention == "here"):
            #Put all the members present on the channel
            hashed_mails.update(hashed_receivers)
        else:
            hashed_mail = users_to_hashed_mail.get(mention)
            if(hashed_mail != None):
                hashed_mails.add(hashed_mail)
    return hashed_mails


def process_data(raw_data, users_to_mail):
    """Function that processes the raw_data to extract additional features or tranform some formats.
    Transform the unix timestamp to a date.
    Clean all the messages by removing the emojis and the useless spaces and extracts the following features from the messages:
    - The number of words in the message (mentions are counted as words, not emojis)
    - The number of chars in the message (after the message was cleaned)
    - A list of all the emojis in the message
    - A set of all the mentions

    Parameters
    ----------
    raw_data : The list with the data returned by the query
    users_to_mail: A dictionnary from the username of the users to their emails.

    Returns
    -------
    Generator((string, string, String, String, (String, String) int, int, List<String>, Set<String>, string, char, list<string>, datetime, string, string, string))
        A generator of tuples containing 
        - A string for the md5 hash of the mail of the sender
        - A string for the message
        - A string for the message cleaned
        - A String for the language
        - A (String, String) tuple for the entities
        - An int for the number of words in the message (emojis are not counted as words)
        - An int for the number of chars in the message after cleaning
        - A list of string with every emojis in the message
        - A set of strings with all the md5 hash of the mail of the people who were mentioned (@)
        - A string for the name of the channel on which the message was sent
        - A char for the type of channel on wichh the message was sent (O for public, P for private or D for direct messages)
        - A list of string for the md5 hash of the mail of the receivers (in the order in which they joined the channel)
        - A datetime at which the message was sent
        - A string for the id of the post
        - A string for the parent id of the post (if it a response to another post) or None if it not a response
        - A string for the extension of the file if the message was sent with a document (picture for example) or None otherwise
    """
    anonymised_channel_to_messages = dict()
    data_first_traversal = list()

    #First traversal, we clean all the messages, anonymise the channels and regroup the cleaned messages by channel to later on analyse the language by channel
    for (hashed_sender, message, channel, channel_type, unix_time, post_id, post_parent_id, file_extension, hash_receivers) in raw_data:
        date = datetime.fromtimestamp(unix_time/1000)
        no_words, emojis, mentions, message_cleaned = mp.clean_message_extract_emojis_mentions(message)
        anonymised_channel = anonymise_non_public_channel(channel, channel_type)

        if(message_cleaned != ""):
            channelMessage = anonymised_channel_to_messages.get(anonymised_channel)
            channelMessage = message_cleaned if channelMessage == None else channelMessage + '\n' + message_cleaned
            anonymised_channel_to_messages[anonymised_channel] = channelMessage

        data_first_traversal.append((hashed_sender, message, message_cleaned, no_words, emojis, mentions, anonymised_channel, channel_type, hash_receivers, date, post_id, post_parent_id, file_extension))  

    #Detect the language of each channel and load the models
    channel_to_language = mp.detect_channel_language(anonymised_channel_to_messages)
    language_to_nlp_model = mp.create_language_to_nlp_model()

    #yield the definitions of the columns as first row
    definitions = ("Sender", "Message", "MessageCleaned", "Language", "Tags", "NamedEntities", "SentimentScores", "NumberWords", "NumberChars","Emojis", "Mentions", "Channel", "ChannelType", "Receivers", "Time", "PostId", "PostParentId", "FileExtension")
    yield definitions

    #Second traversal, we process the messages by getting the language and loading the corresponding nlp model
    for (hashed_sender, message, message_cleaned, no_words, emojis, mentions, anonymised_channel, channel_type, hash_receivers, date, post_id, post_parent_id, file_extension) in data_first_traversal:
        
        no_char = len(message_cleaned)
        hashed_mails_mentions = list(hashed_mails_from_mentions(mentions, users_to_mail, hash_receivers))
        language = channel_to_language.get(anonymised_channel)
        nlp = language_to_nlp_model.get(language, language_to_nlp_model.get("default"))

        pos_tagged, named_entities = mp.entity_processing(message_cleaned, nlp)

        sentiment_analysis = mp.sentimentAnalysis(message, language)

        yield hashed_sender, message, message_cleaned, language, pos_tagged, named_entities, sentiment_analysis, no_words, no_char, emojis, hashed_mails_mentions, anonymised_channel, channel_type, hash_receivers, date, post_id, post_parent_id, file_extension


def query_message_from_to(cur):
    """Function that queries in the database who sent which message to whom with additional informations such as the channel name and the type of the
    channel, the id of the post and its parent id (if it was a reply to another post) to be able to construct a tree, the extension of
    the file if it was sent with a file joined.
    The sender and receivers are anonymised using MD5 hashing.
    Only people who were on the channel at the time the message was sent are taken in account as receivers, using the channelmemberhistory
    table from the database. 
    Only other people are considered as receivers, not the one sending the message.

    Parameters
    ----------
    cur : The cursor to write query to the database.

    Returns
    -------
    List((string, string, int, int, List<String>, Set<String>, string, char, list<string>, datetime, string, string, string))
        A generator of tuples containing 
        - A string for the md5 hash of the mail of the sender
        - A string for the message
        - A string for the name of the channel on which the message was sent
        - A char for the type of channel on wichh the message was sent (O for public, P for private or D for direct messages)
        - A list of string for the md5 hash of the mail of the receivers (in the order in which they joined the channel)
        - An int for the unix timestamp at which the message was sent
        - A string for the id of the post
        - A string for the parent id of the post (if it a response to another post) or None if it not a response
        - A string for the extension of the file if the message was sent with a document (picture for example) or None otherwise
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

    #Here we just do a "groupby" to have a list of receivers for the same message and we hash them and the senders directly with md5.
    message_to_list_receivers = OrderedDict()
    for sender, message, channel, channel_type, receiver, unix_time, post_id, post_parent_id, file_extension in rows:
        hashed_sender = hashlib.md5(sender.encode()).hexdigest()
        hash_receivers = message_to_list_receivers.setdefault((hashed_sender, message, channel, channel_type, unix_time, post_id, post_parent_id, file_extension), [])
        if(sender!=receiver):
            hash_receivers.append(hashlib.md5(receiver.encode()).hexdigest())

    #Flatten the tuples before returning
    return [(hashed_sender, message, channel, channel_type, unix_time, post_id, post_parent_id, file_extension, hash_receivers) for (hashed_sender, message, channel, channel_type, unix_time, post_id, post_parent_id, file_extension), hash_receivers in message_to_list_receivers.items()]


def main():
    
    conn = None
    cur = None
    print("Connecting to the PostgresSQL database...")
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        print("Succesfully connected to the database. Will start writing queries.")
        raw_data = query_message_from_to(cur)
        users_to_hashed_mail = create_map_users_hashed_mail(cur)

        print("Queries ran succesfully.")

        print("Start processing the data.")
        data_processed = process_data(raw_data, users_to_hashed_mail)
        write_csv(data=data_processed, filename='csv/from_message_to_sentiment.csv')

    except(Exception, psycopg2.DatabaseError) as error:
        print("Error: " + str(error))
        traceback.print_exc()
        print("Programm exits.")
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    main()