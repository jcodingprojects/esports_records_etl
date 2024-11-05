import asyncio
import time
from collections.abc import Iterable, Awaitable
import concurrent.futures
import random
from api_base import Interface, SPInterface
from pandas import DataFrame


class ETLManager:
    def __init__( self, rate: float, n_workers: int, start_date: str, end_date: str, interface):
        self.n_workers = n_workers
        self.rate = rate
        self.start_date = start_date
        self.end_date = end_date
        self.interface = interface


    async def rate_limited_offset(self, queue):
        """Generates offsets at a controlled rate and inserts them into the queue"""
        offset = 0
        while True:
            await queue.put(offset)  
            offset += 1
            await asyncio.sleep(1 / self.rate)


    async def run(self):
        """Runs ETL process, starts rate limited queue and create pool of workers"""

        self.queue = asyncio.Queue(maxsize=self.n_workers)
        queue_input = asyncio.create_task(self.rate_limited_offset(self.queue))

        workers = [
            asyncio.create_task(self.worker(i)) 
            for i in range(self.n_workers)
        ]

        await asyncio.gather(*workers)
        queue_input.cancel()


    async def worker(self, worker_i):
        """
        Worker in pool which retrieves offset from async queue and performs etl
        process for that offset in the api database.
        Cancel worker once dt has passed today
        """
        worker_interface = self.interface()
        for i in range(2):
            print(f'{worker_i = }, {i = }')
            offset = await self.queue.get()

            print(f"Worker for {offset = } started")
            query_result = await self.worker_api_call(offset, worker_interface)
            await self.worker_insert_entries(query_result, worker_interface)

            print(f"{worker_i} received result of length {len(query_result)} for {offset = }")
            if query_result is None:
                return None


    async def worker_api_call(self, offset: int, worker_interface: Interface):
        """
        Extract data from api
        """
        loop = asyncio.get_running_loop()

        with concurrent.futures.ThreadPoolExecutor() as pool:
            query_result = await loop.run_in_executor(
                pool,
                worker_interface.api_interface.query_api,
                offset, self.start_date, self.end_date
            )
        return query_result


    async def worker_insert_entries(self, query_result: DataFrame, worker_interface: Interface):
        await worker_interface.local_interface.insert_new(query_result)
        return None


async def main():
    etler = ETLManager(2, 3, '2024-01-01', '2024-02-01', SPInterface)
    await etler.run()
        

if __name__ == '__main__':
    asyncio.run(main())
