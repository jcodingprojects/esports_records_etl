from connection_info import ApiConnection, DBConnection
from fields import SPFields
import time
import asyncio
import pandas as pd


async def insert_test():
    conn = await DBConnection.get_async_con()
    test_date = pd.DataFrame([['j', 'af']], columns=['name', 'mail'])
    await conn.copy_records_to_table('_data', test_date)

async def main():
    await asyncio.create_task(insert_test())

if __name__ == '__main__':
    asyncio.run(main())