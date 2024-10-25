from abc import ABC, abstractmethod
import asyncpg
from pandas import DataFrame
from fields import SPFields, SGFields
from connection_info import ApiConnection, DBConnection


class APIInterface(ABC):
    """Abstract base class for interfacing with the lol_fandom database api"""
    def __init__(self) -> None:
        pass

    @abstractmethod
    def query_api(
        self, 
        offset: int,
        start_date: str,
        end_date: str
    ) -> DataFrame:
        pass

    @abstractmethod
    def insert_new(self, entries: DataFrame) -> None:
        pass


class ScoreboardGamesInterface(APIInterface):
    """Interface for the Scoreboard Games table"""
    def __init__(self):
        super().__init__()


    def query_api(self, offset, start_date, end_date, limit=50):
        client = ApiConnection.get_client()

        query_result = client.cargo_client.query(
            tables='ScoreboardGames=SG',
            fields=SGFields.get_suffixed(),
            limit=limit,
            offset=offset,
            where=(
                f"""
                    SG.DateTime_UTC >= '{start_date} 00:00:00' AND
                    SG.DateTime_UTC <= '{end_date} 00:00:00'
                """
            )
        )
        return query_result

    async def insert_new(self, entries):
        conn = await DBConnection.get_async_con()

        return super().insert_new(entries)
 


class ScoreboardPlayersInterface(APIInterface):
    """Interface for the Scoreboard Players table"""
    def __init__(self):
        super().__init__()


    def query_api(self, offset, start_date, end_date, limit=50):
        client = ApiConnection.get_client()

        query_result = client.cargo_client.query(
            tables='ScoreboardPlayers=SP',
            fields=SPFields.get_suffixed(),
            limit=limit,
            offset=offset,
            where=(
                f"""
                    SP.DateTime_UTC >= '{start_date} 00:00:00' AND
                    SP.DateTime_UTC <= '{end_date} 00:00:00'
                """
            )
        )
        return query_result


    def insert_new(self, entries):
        return super().insert_new(entries)


def main():
    sgi = ScoreboardGamesInterface()
    print(sgi.query_api(0, '2024-01-01', '2024-01-02'))

if __name__ == '__main__':
    main()
