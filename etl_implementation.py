import asyncio
import time
from collections.abc import Iterable, Awaitable
import concurrent.futures
import random
from api_base import Interface, SPInterface, SGInterface
from pandas import DataFrame, date_range, Timedelta


class APIETL:
    def __init__( self, rate: float, n_workers: int, start_date: str, end_date: str, interface):
        self.n_workers = n_workers
        self.rate = rate
        self.start_date = start_date
        self.end_date = end_date
        self.interface = interface
        self.query_limit = 500


    async def rate_limited_dates(self, queue):
        """Generates offsets at a controlled rate and inserts them into the queue"""
        for i in date_range(self.start_date, self.end_date):
            date_tuple = tuple( 
                i.strftime('%Y-%m-%d') for i in (i, i + Timedelta(days=1))
            )
            await queue.put(date_tuple)
            await asyncio.sleep(1 / self.rate)


    async def run(self):
        """Runs ETL process, starts rate limited queue and create pool of workers"""
        self.queue = asyncio.Queue(maxsize=self.n_workers)
        queue_input = asyncio.create_task(self.rate_limited_dates(self.queue))

        workers = [
            asyncio.create_task(self.worker(i)) 
            for i in range(self.n_workers)
        ]

        await asyncio.gather(*workers)
        queue_input.cancel()


    async def worker(self, worker_i):
        """
        Worker in pool which retrieves date from async queue and performs etl process for that date in the api database.
        """
        worker_interface = self.interface()
        while True:
            date_tuple = await self.queue.get()
            for i in range(30):
                offset = i * self.query_limit

                print(f"{worker_i = :<10} on date ={ date_tuple[0]}")
                query_result = await self.worker_api_call(worker_interface, date_tuple, offset)
                await self.worker_insert_entries(query_result, worker_interface)

                if not len(query_result):
                    break


    async def worker_api_call(self, worker_interface: Interface, date_tuple: tuple[str], offset: int):
        """Extract data from api"""
        loop = asyncio.get_running_loop()

        with concurrent.futures.ThreadPoolExecutor() as pool:
            query_result = await loop.run_in_executor(
                pool,
                worker_interface.api_interface.query_api,
                offset, date_tuple[0], date_tuple[1], self.query_limit
            )

        return query_result


    async def worker_insert_entries(self, query_result: DataFrame, worker_interface: Interface):
        await worker_interface.local_interface.insert_new(query_result)
        return None


async def main():
    sp_etl = APIETL(2, 5, '2016-01-01', '2025-01-01', SPInterface)
    await sp_etl.run()
    
    sg_etl = APIETL(2, 5, '2016-01-01', '2025-01-01', SGInterface)
    await sg_etl.run()
    

if __name__ == '__main__':
    asyncio.run(main())
