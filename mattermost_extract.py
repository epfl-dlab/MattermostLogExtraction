import psycopg2
import sys
from config import config
from csv_parser import write_csv
from datetime import datetime
from query import create_map_users_hashed_mail, query_message_from_to
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
    """Generator function that processes the raw_data to extract additional features or tranform some formats.
    -Transform the unix timestamp to a date.
    -Anonymize the channel that are not public
    Clean all the messages by removing the emojis and the useless spaces and extracts the following features from the messages:
    - The number of words in the message (mentions and emojis are not counted as words)
    - The number of chars in the message (after the message was cleaned)
    - A list of all the emojis in the message
    - A set of all the people (anonymised with md5) mentionned 
    - The language of the message (by associating a language to a whole channel instead of individually per message)
    - The tag and the entities of the message
    - A sentiment score for the message
    - A vector with the occurences of liwc categories

    Parameters
    ----------
    raw_data : The list with the data returned by the query
    users_to_mail: A dictionnary from the username of the users to their emails.

    Returns
    -------
    Generator((String, String, List(String, String, String), List((Tuple(int), String, String)), List(int), Dict(String, int), int, int, List(String), Set(String), String, char, List(String), Datetime, String, String, String))
        A generator of tuples containing 
        - A String for the md5 hash of the mail of the sender
        - A String for the language
        - A List((String, String)) for the tags (pos and tag).
        - A List((Tuple(int), String)) for the entities (index(es) in the tag and label)
        - A List(int) for the vector of liwc categories (the occurence for each category)
        - A Dict(String, int) for the sentiment score with the definition of the sentiment and the corresponding value (between 0 and 1)
        - An int for the number of words in the message (emojis and mentionned are not counted as words). Here, a word is some alpha or numerical char separeated with spaces and doesn't correspond to the number of tokens, which can be dervied from the tags
        - An int for the number of chars in the message after cleaning
        - A List(String) with every emojis in the message
        - A Set(String) with all the md5 hash of the mail of the people who were mentioned (@)
        - A String for the name of the channel on which the message was sent (anonymised for non public channel)
        - A char for the type of channel on wichh the message was sent (O for public, P for private or D for direct messages)
        - A List(String) for the md5 hash of the mail of the receivers (in the order in which they joined the channel)
        - A datetime at which the message was sent
        - A String for the id of the post
        - A String for the parent id of the post (if it a response to another post) or None if it not a response
        - A String for the extension of the file if the message was sent with a document (picture for example) or None otherwise
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

    #Detect the language of each channel and load the models for spacy and liwc
    channel_to_language = mp.detect_channel_language(anonymised_channel_to_messages)
    language_to_nlp_model = mp.create_language_to_nlp_model()
    language_to_liwc_model = mp.create_language_to_liwc_model()

    #yield the definitions of the columns as first row
    definitions = ("Sender", "Language", "Tags", "NamedEntities", "LIWCCategories", "SentimentScores", "NumberWords", "NumberChars","Emojis", "Mentions", "Channel", "ChannelType", "Receivers", "Time", "PostId", "PostParentId", "FileExtension")
    yield definitions

    #Second traversal, we process the messages by getting the language and loading the corresponding nlp model
    for (hashed_sender, message, message_cleaned, no_words, emojis, mentions, anonymised_channel, channel_type, hash_receivers, date, post_id, post_parent_id, file_extension) in data_first_traversal:
        
        no_char = len(message_cleaned)
        hashed_mails_mentions = list(hashed_mails_from_mentions(mentions, users_to_mail, hash_receivers))
        
        language = channel_to_language.get(anonymised_channel)
        nlp = language_to_nlp_model.get(language)
        pos_tagged, named_entities = mp.entity_processing(message_cleaned, nlp)

        sentiment_analysis = mp.sentiment_analysis(message, language)

        liwc_model = language_to_liwc_model.get(language)
        categories = mp.categories_analysis(message_cleaned, liwc_model)

        yield hashed_sender, language, pos_tagged, named_entities, categories, sentiment_analysis, no_words, no_char, emojis, hashed_mails_mentions, anonymised_channel, channel_type, hash_receivers, date, post_id, post_parent_id, file_extension

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
        write_csv(data=data_processed, filename='mattermost_log_extraction.csv')

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