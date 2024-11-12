import pandas as pd
from fields import FieldEnum, SPFields, SGFields, DTypeEnum, SPDTypes, SGDTypes
from connections import ApiConnection, DBConnection
from postgres_conf import QueryCreator


class APIInterface():
    """Base class for interfacing with tables from the lol_fandom database api"""
    def __init__(
            self,
            fields: FieldEnum,
            field_dtypes: DTypeEnum,
            cargo_suffix: str,
            cargotable_name: str,
        ) -> None:
        self.fields = fields
        self.field_dtypes = field_dtypes
        self.cargo_suffix = cargo_suffix
        self.cargotable_name = cargotable_name


    def query_api(self, offset: int, start_date: str, end_date: str, limit: int = 50) -> pd.DataFrame:
        """
        Query the lolfandom api with provided offset, dates and limit
        Return cleaned query will all specified fields
        """
        raw_query = self.raw_query_api(offset, start_date, end_date, limit)
        clean_query = self.clean_raw_query(raw_query)

        return clean_query


    def raw_query_api(self, offset: int, start_date: str, end_date: str, limit: int = 50) -> pd.DataFrame:
        """
        Query API with fields specified in self.fields
        Return raw Dataframe of query made to lolfandom api
        """
        client = ApiConnection.get_client()

        query_result = client.cargo_client.query(
            tables=f'{self.cargotable_name}={self.cargo_suffix}',
            fields=self.fields.get_suffixed(self.cargo_suffix),
            limit=limit,
            offset=offset,
            where=(
                f"{self.cargo_suffix}.DateTime_UTC >= '{start_date} 00:00:00' AND "
                f"{self.cargo_suffix}.DateTime_UTC <= '{end_date} 00:00:00'"
            ),
            order_by=f"{self.cargo_suffix}.DateTime_UTC"
        )
        query_result = pd.DataFrame(query_result)
        query_result.columns = [c.replace(' ', '_') for c in query_result.columns]
     
        return query_result


    def clean_raw_query(self, query: pd.DataFrame) -> None:
        """
        Clean query, fill in missing columns, coerce datatypes, unpack delimited strings
        """
        queried_fields = self.fields.list_values()
        found, missing = get_found_and_missing(queried_fields, query.columns)
        
        for m in missing:
            query[m] = pd.NA
        query = query[queried_fields]

        coercible_dtype_map, special_dtypes = self.field_dtypes.split_dtypes()
        query = query.astype(coercible_dtype_map)

        for col, dtype in special_dtypes.items():
            if 'delimiter:' in dtype.split(' '):
                query[col] = query[col].str.split(dtype[-1])

        query = query.rename(self.fields.invert(), axis=1)
        return query

    
class LocalInterface():
    """Interface for inserting into the local postgreSQL database"""
    def __init__(self, table_name: str, fields: FieldEnum, 
                 field_wikidtypes: DTypeEnum, schema_name: str, on_conflict: str = ''):
        self.table_name = table_name
        self.temp_table_name = 'temp_' + self.table_name
        self.fields = fields
        self.field_wikidtypes = field_wikidtypes
        self.schema_name = schema_name


    async def insert_new(self, entries: pd.DataFrame) -> None:
        table_str = ', '.join(entries.columns)
        entries = entries.replace({pd.NA: None})

        conn = await DBConnection.get_async_con()
        create_temp_query = self.get_create_temp_query()

        await conn.execute(create_temp_query)
        
        await conn.copy_records_to_table(
            self.temp_table_name, 
            records=entries.itertuples(index=False)
        )
        on_conflict = f"ON CONFLICT ON CONSTRAINT {self.table_name}_unique DO NOTHING"
        await conn.execute(
            f"INSERT INTO {self.schema_name}.{self.table_name} ({table_str}) "
            f"SELECT {table_str} FROM {self.temp_table_name} {on_conflict};"
        )


    def get_create_temp_query(self):
        tc = QueryCreator('', self.temp_table_name, self.fields, self.field_wikidtypes, [])
        return tc.get_create_temp_query()


class Interface():
    def __init__(
            self, schema_name: str, table_name: str, fields: FieldEnum, 
            field_dtypes: DTypeEnum, cargotable_name: str, cargo_suffix: str, on_conflict: str = ''
    ) -> None:
        self.api_interface = APIInterface(
            fields, field_dtypes, cargo_suffix, cargotable_name
        )
        self.local_interface = LocalInterface(
            table_name, fields, field_dtypes, schema_name, on_conflict
        )


class SGInterface(Interface):
    def __init__(self):
        on_conflict = """"""
        super().__init__(
            schema_name='fandom_schema', table_name='scoreboard_games', fields=SGFields, field_dtypes=SGDTypes,
            cargotable_name='ScoreboardGames', cargo_suffix='SG'
    )


class SPInterface(Interface):
    def __init__(self):
        super().__init__(
            schema_name='fandom_schema', table_name='scoreboard_players', fields=SPFields, field_dtypes=SPDTypes,
            cargotable_name='ScoreboardPlayers', cargo_suffix='SP', 
    )


def get_found_and_missing(queried, returned):
    """
    Returns:
    Intersection of queried and returned, and
    elements in queried but not in returned
    """
    queried, returned = set(queried), set(returned)
    found = queried.intersection(returned)
    missing = queried.difference(returned)
    return list(found), list(missing)


def main():
    pass

if __name__ == '__main__':
    main()
