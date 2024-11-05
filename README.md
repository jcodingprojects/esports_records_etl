# Esports_records_etl
Library providing functionality to extract, transform and load professional League of Legends Esports game data from lol Fandom.
Uses asyncio and asyncpg to asynchronously query the api for the raw data, process that data, and then insert that clean data into a PostgreSQL database for later use. 
