import aiohttp
from asyncio import ensure_future, gather
import asyncio


async def request_worker(session: aiohttp.ClientSession, **kwargs):
    async with session.request(**kwargs) as response:
        return await response.json()


async def request_controller(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [ensure_future(request_worker(session, **url)) for url in urls]
        results = await gather(*tasks)
    return results


def multi_async_requests(urls: list[dict]):
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(request_controller(urls))

    return results
