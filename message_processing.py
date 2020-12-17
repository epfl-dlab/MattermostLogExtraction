import re #Used in liwc_parsing
from langdetect import detect, detect_langs, DetectorFactory
import spacy
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import liwc_parsing as liwc


def clean_message_extract_emojis_mentions(message):
    """Function that goes through the message, clean it by removing useless spaces, emojis and mentions and extracts how many words 
    the message contains, all the mentions and all the emojis that are in the message

    Parameters
    ----------
    message : The raw message

    Returns
    -------
    (int, List(String), Set(String), String):
        - An int for the number of words (mentions and emojis are not counted)
        - A list with all the emojis
        - A set with all the mentions
        - A string with the message cleaned (without emojis, mentions and useless spaces)
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
            if(start_word):
                start_word = False
                no_words += 1
            if(start_mention):
                start_mention = False
                mentions.add(mention)
                message_cleaned = message_cleaned[:-(len(mention)+2)] if (len(mention)+2<=len(message_cleaned) and message_cleaned[len(message_cleaned)- len(mention)-2].isspace()) else message_cleaned[:-(len(mention)+1)]
                no_words-=1
                mention = ""  
            if(not already_space):
                message_cleaned += ch
                already_space=True
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
        message_cleaned = message_cleaned[:-(len(mention)+2)] if (len(mention)+2<=len(message_cleaned)) else message_cleaned[:-(len(mention)+1)]
        no_words-=1
    #Remove the last char if it is a space
    if(message_cleaned[len(message_cleaned)-1].isspace()):
        message_cleaned = message_cleaned[:-1]
    
    return no_words, emojis, mentions, message_cleaned


def detect_channel_language(anonymised_channel_to_messages):
    """Function that detect the language of each channel (set to None if the message is empty).

    Parameters
    ----------
    anonymised_channel_to_messages : A dictionnary with the anonymised channel as keys and the concatenation of all messages sent on the channel as value.

    Returns
    -------
    dict(String, String):
        A dictionnary with the anonymised channel as key and the abreviation (string of 2 chars) of the language as value.
    """
    #Set the seed such that the result is always the same
    DetectorFactory.seed = 0

    channel_to_language = dict()

    for anonymised_channel, messages in  anonymised_channel_to_messages.items():

        if(messages == None or messages==""):#Should never happen
            channel_to_language[anonymised_channel] = None
        else:
            language = detect(messages)
            channel_to_language[anonymised_channel] = language
    
    return channel_to_language


def entity_processing(message, nlp):
    """Function that retrieves the tags and entities using the correspondong spacy model.

    Parameters
    ----------
    message : The message cleaned
    nlp: The nlp model corresponding to the language of the channel of the message

    Returns
    -------
    (List((String, String)), List(tuple(int), String):
        - The tags of the messages
        - The named entities of the message with their corresponding index(es) in the tags (or None in some special case where it is badly split).
          The indexes are there to retrieve to which tag the entity makes reference to, since we cannot have plain text due to anonymity.
    """
    if(message == "" or nlp == None):
        return None, None

    doc = nlp(message)
    
    tokens = [token.text for token in doc]
    pos_tagged = [(token.pos_, token.tag_) for token in doc]
    named_entities = [(ent.text, ent.label_) for ent in doc.ents]

    index_and_entities = list()

    for (text, label) in named_entities:
        
        entity_tokens = text.split(" ")
        
        indexes = ()
        try:
            for entity_token in entity_tokens:
                indexes += (tokens.index(entity_token),)
        except ValueError as err:
            indexes = None

        index_and_entities.append((indexes, label))


    return pos_tagged, index_and_entities



def create_language_to_nlp_model():
    """Function that loads the different spacy model used to analyse the data in a dictionnary with the abbreviation of the language.
    The recognised languages are English, German, French and Italian.

    Returns
    -------
    dict(String, spacy.lang.):
        A dictionnary with the abbreviation of the languages as key and the corresponding NLP model.
    """

    print("Start loading the models.")
    language_to_nlp_model = dict()

    language_to_nlp_model["en"] = spacy.load("en_core_web_sm")
    language_to_nlp_model["fr"] = spacy.load("fr_core_news_sm")
    language_to_nlp_model["de"] = spacy.load("de_core_news_sm")
    language_to_nlp_model["it"] = spacy.load("it_core_news_sm")

    print("Done loading the models.")

    return language_to_nlp_model

def sentiment_analysis(message, language):
    """Function that gives score for the positivity/neutrality/negativity of sentiment in the message. 
    It only analyses english messages and return None for other languages.

    Parameters
    ----------
    message : The message (not cleaned, vader can analyse emojis).
    language: The language of the message.

    Returns
    -------
    dict(String, Int):
        A dictionnary with the sentiment as key (neg, neu, pos and compound) and a value between 0 and 1 which is the intensity of that emotion.
    """
    if language != "en" or message == "":
        return None
    
    analyzer = SentimentIntensityAnalyzer()
    vs = analyzer.polarity_scores(message)
    
    return vs


def create_language_to_liwc_model(path_to_directory="liwc_dict/", extension="_liwc.txt"):
    """Function that loads the different liwc model used to analyse the data in a dictionnary with the abbreviation of the language.
    The recognised languages are English, German, French and Italian.

    Parameters
    ----------
    path_to_directory : relative path to directory where the models are stored. Default is liwc_dict/
    extension: the extension name of the file after the language. Default is _liwc.txt

    Returns
    -------
    dict(String, liwc_model):
        A dictionnary with the abbreviation of the languages as key and the corresponding liwc model as value (see get_liwc_groups).
    """
    language_to_liwc_model = dict()

    recognised_language = {"en", "fr", "de", "it"}

    for language in recognised_language:
        path_file = path_to_directory + language + extension
        language_to_liwc_model[language] = liwc.get_liwc_groups(path_file)

    return language_to_liwc_model


def categories_analysis(message, liwc_model):
    """Function that searches the occurence of the category in liwc.
    For English, the categories are the 64 original LIWC categories + ADA (Applied data analysis course related term) as last one.
    For French, German and Italian, the categories are only those with pronouns and ADA, ie: [Ppron, You, We, They, I, Pron, HeShe, ADA]

    Parameters
    ----------
    message : The message cleaned.
    liwc_model: A tuple with the star_words, all_words and liwc_names of the liwc_model corresponding to the language of the message.

    Returns
    -------
    list(int):
        A vector with the occurences of each categories of the corresponding liwc vector.
    """
    if(liwc_model == None or message == ""):
        return None
        
    return liwc.get_liwc_features(message, *liwc_model)