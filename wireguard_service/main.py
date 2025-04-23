import threading
import logging
import os
from contextlib import asynccontextmanager
from typing import List
import json
import aiohttp
from aiohttp import ClientSession, ClientResponse
from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer
import asyncio
import base64 as b64


class WireguardService:
    def __init__(self, endpoints: List[str], kafka_producer: KafkaProducer,  password_data: dict):
        self.kafka_producer = kafka_producer
        self.endpoints = endpoints
        self.password_data = password_data
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        if self.bootstrap_servers is None:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is not set")
        asyncio.run(self._start_kafka_consumer())
    async def get_config(self, session: ClientSession, endpoint: str, client_id: str) -> bytes:
        async with session.get(f"http://{endpoint}:51821/api/wireguard/client/{client_id}/configuration") as response:
            return await response.read()
    async def create_client(self, session: ClientSession, endpoint: str, user_name: str) -> dict:
        async with session.post(f"http://{endpoint}:51821/api/wireguard/client", json={'name': user_name}) as response:
            return await response.json()


    async def get_clients(self, session: ClientSession, endpoint: str) -> dict:
        async with session.get(f"http://{endpoint}:51821/api/wireguard/client") as response:
            return await response.json()


    async def action_with_client(self, session: ClientSession, endpoint: str, client_id: str, action: str) -> dict:
        async with session.post(f"http://{endpoint}:51821/api/wireguard/client/{client_id}/{action}") as response:
            return response.json()


    async def delete_client(self, session: ClientSession, endpoint: str, client_id: str) -> dict:
        async with session.delete(f"http://{endpoint}:51821/api/wireguard/client/{client_id}") as response:
            return response.json()


    @asynccontextmanager
    async def create_session(self, endpoint: str):
        session = aiohttp.ClientSession()
        try:
            async with session.post(f"http://{endpoint}:51821/api/session", json=self.password_data) as response:
                cookies = response.cookies

            session.cookie_jar.update_cookies({key: morsel.value for key, morsel in cookies.items()})
            yield session
        finally:
            await session.close()


    
    async def best_endpoint(self):
        pass

    async def get_config_handler(self, user_data, correlation_id):
        endpoint = await self.best_endpoint()
        session = await self.create_session(endpoint)
        result = await self.get_config(session, endpoint, user_data['wg_id'])
        await self.kafka_producer.send('config-responses', value=json.dumps({'correlation_id': correlation_id, 'config_response': {
            "status": True,
            "output": b64.b64encode(result)
        }}).encode('utf-8'))

    async def _start_kafka_consumer(self):
        """Запускает фоновый поток для получения ответов из Kafka"""
        def consume_responses():
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id='config-gateway-group',
                auto_offset_reset='earliest',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')))
            consumer.subscribe(['config-requests'])
            for msg in consumer:
                try:
                    data = msg.value
                    correlation_id = data['correlation_id']
                    user_data = data['user_data']
                    self.get_config_handler(user_data, correlation_id)

                except Exception as e:
                    logging.error(f"Error processing Kafka message: {e}")
        
        threading.Thread(target=consume_responses, daemon=True).start()

 
def main():    
    load_dotenv()
    PASSWORD_DATA = {'password': os.getenv('PASSWORD'), 'remember': 'true'}

    logger = logging.getLogger(__name__)


if __name__ == "__main__":
    main()       