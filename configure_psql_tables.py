from connection_info import DBConnection
from asyncpg import Connection
import asyncio
from fields import FieldHolder, DTypeEnum, dtype_wiki2psql, SPFields, SPDTypes, SGFields

"""
GRANT SCHEMA fandom_schema;
GRANT USAGE, CREATE ON SCHEMA fandom_schema to base_user;
"""

class TableCreator:
    def __init__(
            self,
            schema_name,
            table_name: str,
            fields: FieldHolder,
            field_dtypes: DTypeEnum 
    ) -> None:
        self.table_name = table_name
        self.fields = fields
        self.field_wikidtypes = field_dtypes
        self.schema_name = schema_name


    async def __call__(self) -> str:
        """
        Creates table of name self.table_name with columns self.fields
        of datatypes self
        """
        conn = await DBConnection.get_async_con()
        table_exists = await self.get_exists(conn)

        if not table_exists:
            create_query = self.get_create_query()
            await conn.execute(create_query)
            return 'Table Created'
        
        return 'Table already found in database'

    async def get_exists(self, conn: Connection) -> bool:
        """
        Returns Bool as to whether table already exists
        """
        my_str = await conn.execute(f"""
            select table_name from information_schema.tables 
            where table_schema = 'public' and table_name = '{self.table_name}';
        """)

        return True if my_str == 'SELECT 1' else False


    def get_create_query(self) -> str:
        """
        Returns query for creating postgreSQL table with columns from fields with
        datatypes from field_datatypes
        """
        col_fields = self.get_field_query()
        id_str = 'id SERIAL PRIMARY KEY, \n'
        created_str = 'created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n'
        
        col_fields = id_str + col_fields + created_str

        create_query = f"""
            CREATE TABLE {self.schema_name}.{self.table_name} (
            {col_fields});
        """

        return create_query


    def get_field_query(self) -> str:
        """
        Returns query string of format '{column} {column_dataype},' for each member in self.fields
        """

        res = ''

        for name, value in self.fields.__members__.items():
            res += f'{name} {dtype_wiki2psql[self.field_wikidtypes[value]]},\n'

        return res
    

async def main() -> None:
    SGCreator = TableCreator('fandom_schema', 'test', SPFields, SPDTypes)
    print(await SGCreator())


if __name__ == '__main__':
    asyncio.run(main())
