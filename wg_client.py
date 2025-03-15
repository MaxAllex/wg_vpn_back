import logging
import os
from contextlib import asynccontextmanager

import aiohttp
from aiohttp import ClientSession, ClientResponse
from dotenv import load_dotenv

load_dotenv()
PASSWORD_DATA = {'password': os.getenv('PASSWORD', '37cIHWp8SDiL'), 'remember': 'true'}

logger = logging.getLogger(__name__)


async def get_config(session: ClientSession, endpoint: str, client_id: str) -> bytes:
    async with session.get(f"http://{endpoint}:51821/api/wireguard/client/{client_id}/configuration") as response:
        return await response.read()


async def create_client(session: ClientSession, endpoint: str, user_name: str) -> dict:
    async with session.post(f"http://{endpoint}:51821/api/wireguard/client", json={'name': user_name}) as response:
        return await response.json()


async def get_clients(session: ClientSession, endpoint: str) -> dict:
    async with session.get(f"http://{endpoint}:51821/api/wireguard/client") as response:
        return await response.json()  # JSON сразу возвращаем


@asynccontextmanager
async def create_session(endpoint: str):
    session = aiohttp.ClientSession()
    try:
        async with session.post(f"http://{endpoint}:51821/api/session", json=PASSWORD_DATA) as response:
            cookies = response.cookies

        session.cookie_jar.update_cookies({key: morsel.value for key, morsel in cookies.items()})
        yield session
    finally:
        await session.close()
