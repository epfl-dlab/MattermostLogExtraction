def count_words_emojis_mentions(message):
    
    no_words = 0
    
    emojis = []
    mentions = set()

    start_word = False
    start_emoji = False
    start_mention = False
    mention=""
    emoji=""

    for ch in message:

        if((ch.isalpha() or ch.isdigit()) and not start_emoji):
            start_word = True
        elif(ch.isspace()):
            if(start_word):
                start_word = False
                no_words += 1
            else:
                start_emoji = False
            if(start_mention):
                start_mention = False
                mentions.add(mention)
                mention = ""
            
            
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
                emoji=""
            else:
                start_emoji = True

    if(start_word):
        no_words += 1
    if(start_mention):
        mentions.add(mention)
    
    return no_words, emojis, mentions