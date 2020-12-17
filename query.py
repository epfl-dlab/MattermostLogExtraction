from collections import OrderedDict
import hashlib


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
    List((String, String, int, int, List(String), Set(String), String, char, List(String), Datetime, String, String, String))
        A generator of tuples containing 
        - A String for the md5 hash of the mail of the sender
        - A String for the message
        - A String for the name of the channel on which the message was sent
        - A char for the type of channel on wichh the message was sent (O for public, P for private or D for direct messages)
        - A List(String) for the md5 hash of the mail of the receivers (in the order in which they joined the channel)
        - An int for the unix timestamp at which the message was sent
        - A String for the id of the post
        - A String for the parent id of the post (if it a response to another post) or None if it not a response
        - A String for the extension of the file if the message was sent with a document (picture for example) or None otherwise
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
