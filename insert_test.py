from connection_info import ApiConnection, DBConnection
from fields import SPFields
import time
import asyncio
import pandas as pd


async def insert_test():
    conn = await DBConnection.get_async_con()
    test_date = pd.DataFrame([['j', 'af']], columns=['mail', 'name'])
    await conn.execute("""create temporary table temp_data (name varchar(256), mail varchar(256))""")
    await conn.copy_records_to_table(table_name='temp_data', records=test_date.itertuples(False))
    await conn.execute("""
        INSERT INTO insert_test (name, mail)
        SELECT name, mail
        from temp_data 
        ON CONFLICT (mail)
        DO UPDATE SET name = EXCLUDED.name || 'aa'""")

async def main():
    await asyncio.create_task(insert_test())

if __name__ == '__main__':
    asyncio.run(main())