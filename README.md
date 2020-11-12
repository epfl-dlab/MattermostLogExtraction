# MattermostLogExtraction

A script to extract informations from a Mattermost database and store it in a csv file.


## Docker
---

To make the script run properly, you should first check that the three containers (mattermost-docker_web, mattermost-docker_db and mattermost-docker_app) are running on computer iccluster111.iccluster.epfl.ch. If they are not, you can make them run with the following command:

`Inline code`cd /scratch/lohoumar/mattermost-docker
`Inline code`docker-compose up -d

You should also be inside EPFL network or use the VPN to execute the script.


## Database setup
---

The script will first try to connect to the database using the database.ini file in directory databaseSetup. This file contains the name of the host, the port, the name of the database, the user and password to connect to it. If you use another instance of Mattermost, you should put the right values there (if you use Mattermost with docker, the values for the port, database name, user and password should be in the docker-compose.yml file).


## Script
---

The script is in file mattermostExtract.py. You can run it using:

`Inline code`python mattermostExtract.py

It will connect to the database (see Database setup), write queries to the database to extract NLP features and stores it in a csv file.
