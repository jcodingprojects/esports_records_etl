# Esports_records_etl
Library providing functionality to extract, transform and load professional League of Legends Esports game data from lol Fandom.
Uses asyncio and asyncpg to asynchronously query the api for the raw data, process that data, and then insert that clean data into a PostgreSQL database for later use. 

## Overview:

- postgres_conf.py provides functionality to create local postgres schema and tables for storing extracted data in.
- ETL.py provides standard asyncronous worker factory implementation, and uses DBInterface to query the lol fandom API then load into the local database.
- postgres_conf.py and ETL.py can be ran as standalone scripts to create postgreSQL tables then to extract individual player game records as well as team records from lol fandom. Rate limiting is set to 2 queries per second. 
