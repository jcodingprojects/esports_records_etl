from mwrogue.esports_client import EsportsClient
from mwrogue.auth_credentials import AuthCredentials

from sqlalchemy import create_engine
from psycopg2 import connect
import asyncpg
import asyncio

from api_config import api_username, api_password
from local_config import local_username, local_password, local_dbname, local_ip, local_port


class DBConnection:
    db_username = local_username
    db_password = local_password
    db_host = local_ip
    db_name = local_dbname
    db_port = local_port

    @classmethod
    def get_psycop_conn(cls):
        """Return a psycopg2 connection object to the local database."""
        return connect(
            database=cls.db_name,
            user=cls.db_username,
            password=cls.db_password,
            host=cls.db_host,
            port=cls.db_port
        )


    @classmethod
    async def get_async_con(cls) -> asyncpg.Connection:
        return await asyncpg.connect(
            host=cls.db_host,
            port=cls.db_host,
            user=cls.db_username,
            password=cls.db_password,
            database=cls.db_name
        )
    

    @classmethod
    def get_alchemy_engine(cls):
        """Return an sql alchemy connection object to the local database."""
        database_url = f'postgresql://{cls.db_username}:{cls.db_password}@{cls.db_host}:{cls.db_port}/{cls.db_name}'
        alchemy_engine = create_engine(database_url)
        return alchemy_engine


    @classmethod
    def print_conn_info(cls):
        """Print summary of the connection parameters."""
        print(
            f"Local connection info: \n\
                name = {cls.db_name} \n\
                username = {cls.db_username}\n\
                ip = {cls.db_host}\n\
                port = {cls.db_port}"
        )


class ApiConnection:
    api_username = api_username
    api_password = api_password

    @classmethod
    def get_client(cls):
        credentials = AuthCredentials(username=cls.api_username, password=cls.api_password)
        client = EsportsClient('lol', credentials=credentials)
        return client

    @classmethod
    def print_conn_info(cls):
        print(f"Api connection info: \n\
              api username = {cls.api_username}")
