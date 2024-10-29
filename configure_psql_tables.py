from connection_info import DBConnection
from asyncpg import Connection
import asyncio
from fields import FieldEnum, DTypeEnum, dtype_wiki2psql, SPFields, SPDTypes, SGFields

"""
GRANT SCHEMA fandom_schema;
GRANT USAGE, CREATE ON SCHEMA fandom_schema to base_user;
"""

class TableCreator:
    def __init__(
            self,
            schema_name,
            table_name: str,
            fields: FieldEnum,
            field_dtypes: DTypeEnum 
    ) -> None:
        self.table_name = table_name
        self.qc = QueryCreator(schema_name, table_name, fields, field_dtypes)


    async def __call__(self) -> str:
        """
        Creates table of name self.table_name with columns self.fields
        of datatypes self
        """
        conn = await DBConnection.get_async_con()
        table_exists = await self.get_exists(conn)

        if not table_exists:
            create_query = self.qc.get_create_query()
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

    
class QueryCreator:
    def __init__(
        self,
        schema_name: str,
        table_name: str,
        fields: FieldEnum,
        field_dtypes: DTypeEnum
    ) -> None:
        self.table_name = table_name
        self.fields = fields
        self.field_wikidtypes = field_dtypes
        self.schema_name = schema_name

    def get_field_query(self) -> str:
        res = []

        for name, value in self.fields.__members__.items():
            res.append(f'{name} {dtype_wiki2psql[self.field_wikidtypes[value]]}')

        res = ', \n'.join(res)
        return res

    def get_create_query(self) -> str:
        col_fields = self.get_field_query()
        id_str = 'id SERIAL PRIMARY KEY, \n'
        created_str = 'created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n'
        
        col_fields = id_str + col_fields + created_str

        create_query = f"""
            CREATE TABLE {self.schema_name}.{self.table_name} (
            {col_fields});
        """

        return create_query

    def get_create_temp_query(self) -> str:
        col_fields = self.get_field_query()
        create_temp_query = f"""
            CREATE TEMPORARY TABLE {self.table_name} (
            {col_fields});
        """ 
        return create_temp_query

async def main() -> None:
    SGCreator = TableCreator('fandom_schema', 'test', SPFields, SPDTypes)
    print(await SGCreator())


if __name__ == '__main__':
    asyncio.run(main())
