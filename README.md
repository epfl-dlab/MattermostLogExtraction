# MattermostLogExtraction

A script to extract features about messages sent on an instance of Mattermost (from a Mattermost database) and store it in a csv file.



## Docker


If you want to make the script run on our own instance of Mattermost (that we used to test the script and develop it), you should first check that the three containers (mattermost-docker_web, mattermost-docker_db and mattermost-docker_app) are running on computer iccluster111.iccluster.epfl.ch. If they are not, you can make them run with the following command (once you are on the computer physically or via ssh):

```
cd /scratch/lohoumar/mattermost-docker
docker-compose up -d
```

You should also be inside EPFL network or use the VPN to execute the script.



## Database setup


The script will first try to connect to the database using the **database.ini** file in the databaseSetup directory. This file contains the name of the host, the port, the name of the database, the user and the password to connect to it. For the moment, it contains the right value to connect to our own instance of Mattermost, but if you want to use another one, you should put the right values there (if you use Mattermost with docker, the values for the port, database name, user and password should be in the docker-compose.yml file).


## Version and libraries


The script uses **python3**. 

To make it run properly, you should have the following libraries installed for python3:

* [psycopg2](https://pypi.org/project/psycopg2/) to run database queries
* [langdetect](https://pypi.org/project/langdetect/) to detect the language
* [vaderSentiment](https://pypi.org/project/vaderSentiment/) to analyse sentiment
* [spacy](https://spacy.io/) to analyse tags. Spacy requires some other libraries such as [numpy](https://numpy.org/). You might also have to install [Cython](https://cython.org/) in order to install spacy.


Once you have installed the libraries above, you also need to download spacy models for English, French, German and Italian languages. The models that you need can be found on [spacy website](https://spacy.io/usage) and can be downloaded with the following commands:

```
python3 -m spacy download en_core_web_sm
python3 -m spacy download fr_core_news_sm
python3 -m spacy download de_core_news_sm
python3 -m spacy download it_core_news_sm
```



## Script


The script is in the file **mattermost_extract.py**. You can run it using:

`python3 mattermost_extract.py`

It will connect to the database (see [Database setup](#database-setup)), write queries to the database to extract NLP features, process them and store them in a csv file called 'mattermost_log_extraction.csv'. If you want to change the name of the csv file or give another path, you can modify line 240 of mattermost_extract.py and give another filename.
