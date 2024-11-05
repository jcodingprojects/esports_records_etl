from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from api_base import SPInterface
import asyncio
import time

async def main():
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:

        query_future = loop.run_in_executor(
            pool,
            query_wrapper
        )

        sleep_future = mysleep()
        results = await asyncio.gather(query_future, sleep_future)
        return results


def query_wrapper():
    interface = SPInterface()
    print(f'Query starting at time: {time.perf_counter():>20}')
    qr = interface.api_interface.query_api(0, '2024-01-01', '2024-02-01')
    print(f'Query finished at time: {time.perf_counter():>20}')
    return qr


async def mysleep():
    print(f'Sleep starting at time: {time.perf_counter():>20}')
    await asyncio.sleep(1)
    print(f'Sleep finished at time: {time.perf_counter():>20}')
    return None


if __name__ == '__main__':
    results = asyncio.run(main(), debug=True)
    print(results[0])
