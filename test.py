import asyncio
from connection_info import DBConnection
import pandas as pd

async def main():
    conn = await DBConnection.get_async_con()
    result = await conn.fetchrow("select * from fandom_schema.scoreboard_players")
    print(pd.DataFrame(result))

if __name__ == '__main__':
    asyncio.run(main())