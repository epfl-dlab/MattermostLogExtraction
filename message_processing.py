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