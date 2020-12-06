# -*- coding: utf-8 -*-
from langdetect import detect, detect_langs, DetectorFactory
import spacy


def clean_message_extract_emojis_mentions(message):
    """Function that goes through the message, clean it by removing useless spaces and emojis and extracts how many words 
    the message contains, all the mentions and all the emojis that are in the message

    Parameters
    ----------
    message : The raw message

    Returns
    -------
    (int, list(string), set(string), string):
        - An int for the number of words (mentions are counted as words, not emojis)
        - A list with all the emojis
        - A set with all the mentions
        - A string with the message cleaned (without emojis and useless spaces)
    """
    no_words = 0
    
    emojis = []
    mentions = set()

    already_space = True #To remove useless spaces in the sentence
    start_word = False
    start_emoji = False
    start_mention = False
    mention=""
    emoji=""

    message_cleaned = ""

    for ch in message:

        if(ch.isspace()):
            if(not already_space):
                message_cleaned += ch
                already_space=True
            if(start_word):
                start_word = False
                no_words += 1
            if(start_mention):
                start_mention = False
                mentions.add(mention)
                mention = ""  
            start_emoji = False
        else:
            message_cleaned += ch
            already_space = False

            if((ch.isalpha() or ch.isdigit()) and not start_emoji):
                start_word = True
            
        if(start_emoji):
            emoji += ch
        if(start_mention):
            mention += ch

        if(ch == '@' and not start_emoji and not start_word):
            start_mention = True
        elif(ch == ':'):
            if(start_emoji):
                start_emoji = False
                emojis.append(emoji[:-1])
                message_cleaned = message_cleaned[:-(len(emoji)+3)] if (len(emoji)+3<=len(message_cleaned) and message_cleaned[len(message_cleaned)- len(emoji)-3].isspace()) else message_cleaned[:-(len(emoji)+2)]
                emoji=""
            else:
                start_emoji = True

    #Count the last word and add it to the mention if it was one
    if(start_word):
        no_words += 1
    if(start_mention):
        mentions.add(mention)
    #Remove the last char if it is a space
    if(message_cleaned[len(message_cleaned)-1].isspace()):
        message_cleaned = message_cleaned[:-1]
    
    return no_words, emojis, mentions, message_cleaned


def detect_channel_language(anonymised_channel_to_messages):
    """Function that detect the language of each channel (set to None if the message if empty).

    Parameters
    ----------
    anonymised_channel_to_messages : A dictionnary with the anonymised channel as keys and the concatenation of all messages sent on the channel as value.

    Returns
    -------
    dict(String, String):
        A dictionnary with the anonymised channel as key and the abreviation (string of 2 chars) of the language.
    """
    #Set the seed such that the result is always the same
    DetectorFactory.seed = 0

    channel_to_language = dict()

    for anonymised_channel, messages in  anonymised_channel_to_messages.items():

        if(messages == None or messages==""):#Should never happen
            channel_to_language[anonymised_channel] = None
        else:
            unicode_messages = messages.decode("utf-8")
            language = detect(unicode_messages)
            channel_to_language[anonymised_channel] = language
    
    return channel_to_language


def entity_processing(message, nlp):
    """Function that retrieves the entities using the correspondong spacy model.

    Parameters
    ----------
    message : The message cleaned
    nlp: The nlp model corresponding to the language of the channel of the message

    Returns
    -------
    list(String, String):
        The entities of the message
    """
    if(message == ""):
        return None

    if(nlp == None):
        raise Exception("nlp model shouldn't be None")

    #Spacy need to work with unicode, not by default in python 2
    unicode_message = message.decode("utf-8")

    doc = nlp(unicode_message)
    pos_tagged = [(token.text.encode("utf-8"), token.pos_.encode("utf-8")) for token in doc]

    return pos_tagged



def create_language_to_nlp_model():
    """Function that loads the different spacy model used to analyse the data in a dictionnary with the abbreviation of the language.
    The recognised languages are English, German, French and Italian, for the other case (key="default"), the multilingual model is used.

    Returns
    -------
    dict(String, spacy.lang.):
        A dictionnary with the abrbreviation of the languages as key and the corresponding NLP model.
    """

    print("Start loading the models.")
    language_to_nlp_model = dict()

    language_to_nlp_model["en"] = spacy.load("en_core_web_sm")
    language_to_nlp_model["fr"] = spacy.load("fr_core_news_sm")
    language_to_nlp_model["de"] = spacy.load("de_core_news_sm")
    language_to_nlp_model["it"] = spacy.load("it_core_news_sm")
    multi_lingual_nlp = spacy.load("xx_ent_wiki_sm")
    multi_lingual_nlp.add_pipe(multi_lingual_nlp.create_pipe('sentencizer')) #Must add it in order to work
    language_to_nlp_model["default"] = multi_lingual_nlp

    print("Done loading the models.")

    return language_to_nlp_model
