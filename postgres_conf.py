from connections import DBConnection
from asyncpg import Connection
import asyncio
from fields import FieldEnum, DTypeEnum, dtype_wiki2psql, SPFields, SPDTypes, SGFields, SGDTypes

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
            field_dtypes: DTypeEnum,
            conflicts: list[str] = None
    ) -> None:
        self.table_name = table_name
        self.schema_name = schema_name
        conflicts = [] if conflicts is None else conflicts
        self.qc = QueryCreator(schema_name, table_name, fields, field_dtypes, conflicts)


    async def __call__(self, force=False) -> str:
        """
        Creates table of name self.table_name with columns self.fields
        of datatypes self
        """
        conn = await DBConnection.get_async_con()
        table_exists = await self.get_exists(conn)

        if (not table_exists) or force:
            if table_exists: 
                await conn.execute(f"""
                    DELETE FROM {self.schema_name}.{self.table_name}; 
                    DROP TABLE {self.schema_name}.{self.table_name};
                """)

            create_query = self.qc.get_create_query()
            conflict_query = self.qc.get_conflict_query()
            await conn.execute(create_query)
            await conn.execute(conflict_query)
            return 'Table Created'
        
        return 'Table already found in database'


    async def get_exists(self, conn: Connection) -> bool:
        """
        Returns Bool as to whether table already exists
        """
        fandom_tables = await conn.execute(f"""
            select table_name from information_schema.tables 
            where table_schema = '{self.schema_name}' and table_name = '{self.table_name}';
        """)

        return fandom_tables == 'SELECT 1'

    
class QueryCreator:
    def __init__(
        self,
        schema_name: str,
        table_name: str,
        fields: FieldEnum,
        field_dtypes: DTypeEnum,
        conflicts: list[str]
    ) -> None:
        self.table_name = table_name
        self.fields = fields
        self.field_wikidtypes = field_dtypes
        self.schema_name = schema_name
        self.conflicts = conflicts

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
        
        col_fields = id_str + col_fields + ',\n' + created_str

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

    def get_conflict_query(self) -> str:
        field_join = ', '.join(self.conflicts)
        return f"""
            ALTER TABLE {self.schema_name}.{self.table_name} ADD CONSTRAINT 
             {self.table_name}_unique UNIQUE ({field_join});
        """

async def main() -> None:
    sg_table_maker = TableCreator(
        'fandom_schema', 'scoreboard_games', SGFields, SGDTypes, ['game_id', 'match_id'])
    sp_table_maker = TableCreator(
        'fandom_schema', 'scoreboard_players', SPFields, SPDTypes, ['name', 'game_id', 'match_id']
    )
    print(await sg_table_maker(True))
    print(await sp_table_maker(True))

if __name__ == '__main__':
    asyncio.run(main())
