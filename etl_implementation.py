import asyncio
import time
from collections.abc import Iterable, Awaitable
import concurrent.futures
import random
from api_base import APIInterface

api_returns = [1, 2, 3, 4, 6, 5]



class ETLManager:
    def __init__(
            self,
            rate: float,
            n_workers: int,
            start_date: str,
            end_date: str,
            table : APIInterface
    ):
        self.n_workers = n_workers
        self.rate = rate
        self.start_date = start_date
        self.end_date = end_date
        self.table = table
        pass


    async def rate_limited_offset(self, queue):
        """Generates offsets at a controlled rate and inserts them into the queue"""
        offset = 0
        while True:
            await queue.put(offset)  # Add offset to the queue
            offset += 1
            await asyncio.sleep(1 / self.rate)


    async def run(self):
        """Runs ETL process, starts rate limited queue and create pool of workers"""

        self.queue = asyncio.Queue(maxsize=self.n_workers)
        asyncio.create_task(self.rate_limited_offset(self.queue))

        workers = [
            asyncio.create_task(self.worker()) 
            for _ in range(self.n_workers)
        ]

        await asyncio.gather(*workers)


    async def worker(self):
        """
        Worker in pool which retrieves offset from async queue and performs etl
        process for that offset in the api database.
        Cancel worker once dt has passed today
        """
        while True:
            offset = await self.queue.get()

            print(f"Worker for {offset = } started")
            result = self.use_api(offset)

            print(f"Worker got result for offset {offset}: {result}")
            if result is None:
                return None


    async def use_api(self, offset: int):
        """
        Extract data from api
        """
        loop = asyncio.get_running_loop()

        with concurrent.futures.ThreadPoolExecutor() as pool:
            result = await loop.run_in_executor(
                pool,
                self.table.query_api,
                offset, self.start_date, self.end_date
            )


        if bool(api_returns):
            return api_returns.pop()
        else:
            return None
        
async def main():
    etler = ETLManager(5, 5)
    await etler.run()
        

if __name__ == '__main__':
    asyncio.run(main())
