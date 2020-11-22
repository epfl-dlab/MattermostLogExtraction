def count_number_words_and_smiley(message):
    
    no_words = 0
    no_smiley = 0

    start_word = False
    start_smiley = False

    for ch in message:

        if((ch.isalpha() or ch.isdigit()) and not start_smiley):
            start_word = True
        elif(ch == ':'):
            if(start_smiley):
                start_smiley = False
                no_smiley+=1
            else:
                start_smiley = True
        elif(ch.isspace() and start_word):
            start_word = False
            no_words += 1
    
    if(start_word):
        no_words += 1
    
    return no_words, no_smiley

#print("Interesting {}".format(count_number_words_and_smiley("Interesting")))
#print("Fisrt message on public channel n. 1 {}".format(count_number_words_and_smiley("Fisrt message on public channel n. 1")))
#print(":flushed:  :innocent: :smiley:  {}".format(count_number_words_and_smiley(":flushed:  :innocent: :smiley: ")))



