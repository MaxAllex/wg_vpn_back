import threading
import logging
import os
from contextlib import asynccontextmanager
from typing import List
import json
import aiohttp
from aiohttp import ClientSession, ClientResponse, ClientError
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer
import asyncio
import base64 as b64
import qrcode
from qrcode.main import QRCode
from io import BytesIO
import datetime
import pytz
import database.postgres_client
from apscheduler.schedulers.asyncio import AsyncIOScheduler

class WireguardService:
    def __init__(self, endpoints: List[str], kafka_producer: KafkaProducer,  password_data: dict, logger, client_repository: database.postgres_client.ClientRepository):
        self.logger = logger
        self.kafka_producer = kafka_producer
        self.endpoints = endpoints
        self.password_data = password_data
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.client_repository = client_repository
        if self.bootstrap_servers is None:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is not set")
    
    async def second_step_check_traffic(self):
        db_clients = await self.client_repository.get_all_clients()
        for client in db_clients:
            if client.last_used_gigabytes + client.used_gigabytes > client.max_gigabytes and not client.has_premium_status and client.config_file is not None and client.config_file != "":
                self.kafka_producer.send("disable-client", json.dumps({"telegram_id": client.telegram_id, "wg_id": client.wg_id}).encode('utf-8'))
                await self.client_repository.update_user_data(client.id, 0, config_file=None, qr_code=None, enabled_status=False)
                async with self.create_session(client.wg_server) as session:
                    await self.delete_client(session, client.wg_server, client.wg_id)
    
    async def scheduler_check_traffic(self):
        db_clients = await self.client_repository.get_all_clients()
        wg_clients = {}
        for client in db_clients:
            if client.wg_server is not None and client.wg_server != "" and client.wg_server not in wg_clients.keys():
                async with self.create_session(client.wg_server) as session:
                    wg_clients[client.wg_server] = await self.get_clients(session, client.wg_server)
        
        for clients in wg_clients.values():
            for client in clients:
                for db_client in db_clients:
                    if str(db_client.wg_id) == str(client['id']):
                        transfer_tx = client.get("transferTx", 0)
                        gigabytes_value = self.bytes_to_gb(transfer_tx) if transfer_tx else 0
                        self.client_repository.update_single_field(str(db_client.id), 0, "gigabytes", gigabytes_value)
                        self.client_repository.update_single_field(str(db_client.id), 0, "latest_handshake", client['latestHandshakeAt'])
                        break


    async def scheduler_upload_traffic_for_users(self):
        db_clients = await self.client_repository.get_all_clients()
        for client in db_clients:
            max_gigabytes = 10
            if client.last_used_gigabytes+client.used_gigabytes < client.max_gigabytes and client.max_gigabytes != 10:
                max_gigabytes = 10+client.max_gigabytes-client.last_used_gigabytes-client.used_gigabytes
            await self.client_repository.update_user_data(str(client.id), 0, last_used_gigabytes=0, used_gigabytes=0,max_gigabytes=max_gigabytes, enabled_status=True)
        self.kafka_producer.send("upload-traffic", json.dumps({"telegram_id": 0}).encode('utf-8'))

    async def scheduler_check_premium_status(self):
        db_clients = await self.client_repository.get_all_clients()
        for client in db_clients:
            if client.has_premium_status:
                today = datetime.date.today()
                reminder_date = client.premium_status_is_valid_until.date() - datetime.timedelta(days=1)
                telegram_id = client.telegram_id
                if client.premium_status_is_valid_until.date() <= today:
                    await self.client_repository.update_single_field(str(client.id), "has_premium_status", False)
                    await self.client_repository.update_single_field(str(client.id), "premium_status_is_valid_until", None)
                    self.kafka_producer.send("disable-premium", json.dumps({"telegram_id": telegram_id}).encode('utf-8'))
                elif client.premium_status_is_valid_until.date() == reminder_date:
                    self.kafka_producer.send("premium-reminder", json.dumps({"telegram_id": telegram_id}).encode('utf-8'))


    def run(self):
        scheduler = AsyncIOScheduler()

        scheduler.add_job(
            self.scheduler_check_traffic,
            CronTrigger(minute="*/1", timezone=pytz.timezone("Europe/Moscow")),
        )

        scheduler.add_job(
            self.scheduler_upload_traffic_for_users,
            CronTrigger(day=1, hour=9, minute=0, second=0, timezone=pytz.timezone("Europe/Moscow")),
        )

        scheduler.add_job(
            self.scheduler_check_premium_status,
            CronTrigger(day="*/1", hour=18, minute=0, second=0, timezone=pytz.timezone("Europe/Moscow")),
        )

        scheduler.start()
        self._start_kafka_consumer()
        try:
            asyncio.get_event_loop().run_forever()
        except KeyboardInterrupt:
            pass
        finally:
            scheduler.shutdown()

    async def _keep_alive(self):
        """Просто держит программу запущенной"""
        while True:
            await asyncio.sleep(1)



    async def get_config(self, session: ClientSession, endpoint: str, client_id: str) -> bytes:
        print(f"http://{endpoint}/api/wireguard/client")
        async with session.get(f"http://{endpoint}/api/wireguard/client/{client_id}/configuration") as response:
            return await response.read()
    
    async def create_client(self, session: ClientSession, endpoint: str, user_name: str) -> dict:
        print(f"http://{endpoint}/api/wireguard/client")
        async with session.post(f"http://{endpoint}/api/wireguard/client", json={'name': user_name}) as response:
            return await response.json()


    async def get_clients(self, session: ClientSession, endpoint: str) -> dict:
        print(f"http://{endpoint}/api/wireguard/client")
        async with session.get(f"http://{endpoint}/api/wireguard/client") as response:
            return await response.json()


    async def action_with_client(self, session: ClientSession, endpoint: str, client_id: str, action: str) -> dict:
        print(f"http://{endpoint}/api/wireguard/client")
        async with session.post(f"http://{endpoint}/api/wireguard/client/{client_id}/{action}") as response:
            return response.json()


    async def delete_client(self, session: ClientSession, endpoint: str, client_id: str) -> dict:
        print(f"http://{endpoint}/api/wireguard/client")
        async with session.delete(f"http://{endpoint}/api/wireguard/client/{client_id}") as response:
            return response.json()


    @asynccontextmanager
    async def create_session(self, endpoint: str):
        session = aiohttp.ClientSession()
        try:
            async with session.post(f"http://{endpoint}/api/session", json=self.password_data) as response:
                cookies = response.cookies
            session.cookie_jar.update_cookies({key: morsel.value for key, morsel in cookies.items()})
            yield session
        finally:
            await session.close()

    """
        Выбор лучшего сервера
    """
    async def get_clients_count(self, session: aiohttp.ClientSession, endpoint: str) -> int:
        try:
            url = f"http://{endpoint}/api/wireguard/client"
            async with session.get(url) as response:
                data = await response.json()
                return len(data) 
        except:
            return float('inf')
                    
    async def evaluate_endpoint(self, endpoint: str, timeout: float = 2.0):
        print("Evaluating endpoint:", endpoint)
        result = {
            "endpoint": endpoint,
            "alive": False,
            "latency": float('inf'),
            "clients": float('inf'),
            "score": 0
        }

        try:
            start = datetime.datetime.now()
            async with self.create_session(endpoint) as session:
                health_url = f"http://{endpoint}/"
                async with session.get(health_url) as health_response:
                    if health_response.status >= 500:
                        return result
                result["latency"] = (datetime.datetime.now() - start).total_seconds()
                
                result["clients"] = await self.get_clients_count(session, endpoint)

                result["score"] = result["latency"] * 0.7 + result["clients"] * 0.3
                result["alive"] = True
            return result
        except Exception as e:
            print(e)
            
        

    async def best_endpoint(self, timeout: float = 2.0) -> str:
        try:
            tasks = [self.evaluate_endpoint(ep, timeout) for ep in self.endpoints]
            results = await asyncio.gather(*tasks)

            alive = [r for r in results if r["alive"]]
            
            if not alive:
                return "Failed"
            
            alive.sort(key=lambda x: x["score"])
            return alive[0]["endpoint"]
        except Exception as e:
            self.logger.error(f"Ошибка получения лучшего сервера:{e}")
            return "Failed"

    """
        Конец функций для выбора эндпоинта
    """


    async def get_config_handler(self, user_data, correlation_id):
        client_data = await self.client_repository.get_client_by_user_id(user_data['id'])
        if client_data.last_used_gigabytes + client_data.used_gigabytes > client_data.max_gigabytes and not client_data.has_premium_status:
            self.kafka_producer.send('config-responses', value=json.dumps({'correlation_id': correlation_id, 'config_response': {"status": False}}).encode("utf-8"))
            return
        endpoint = client_data.wg_server
        print(endpoint)
        if not await self.check_alive(endpoint):
            start_endpoint = endpoint
            endpoint = await self.best_endpoint()
            if endpoint == "Failed":
                self.kafka_producer.send('config-responses', value=json.dumps({'correlation_id': correlation_id, 'qr_response': {"status": False}}).encode("utf-8"))
                return
            print("WHAT")
            temp_wg = await self.create_client_handler(user_data, "changed server")
            async with self.create_session(endpoint) as session:
                self.delete_client(session, start_endpoint, client_data.wg_id)
            client_data.wg_server = endpoint
            client_data.wg_id = temp_wg
            await self.client_repository.update_single_field(user_data['id'], "wg_server", endpoint)
            await self.client_repository.update_single_field(user_data['id'], "wg_id", temp_wg)

        
        async with self.create_session(endpoint) as session:
            print("JFJIFJSKFJKLSJFKSJ")
            result = await self.get_config(session, endpoint, client_data.wg_id)
            print("JFJIFJSKFJKLSJFKSJ")
            await self.client_repository.update_single_field(user_data['id'], "config_file", b64.b64encode(result).decode("utf-8"))
            print("JFJIFJSKFJKLSJFKSJ")
            self.kafka_producer.send('config-responses', value=json.dumps({'correlation_id': correlation_id, 'config_response': {
                "status": True
            }}).encode('utf-8'))

    
    async def get_qr_handler(self, user_data, correlation_id):
        
        client_data = await self.client_repository.get_client_by_user_id(user_data['id'])
        if client_data.last_used_gigabytes + client_data.used_gigabytes > client_data.max_gigabytes and not client_data.has_premium_status:
            self.kafka_producer.send('qr-responses', value=json.dumps({'correlation_id': correlation_id, 'config_response': {"status": False}}).encode("utf-8"))
            return
        endpoint = client_data.wg_server
        if not await self.check_alive(endpoint):
            start_endpoint = endpoint
            endpoint = await self.best_endpoint()
            if endpoint == "Failed":
                self.kafka_producer.send('qr-responses', value=json.dumps({'correlation_id': correlation_id, 'qr_response': {"status": False}}).encode("utf-8"))
                return
            temp_wg = await self.create_client_handler(user_data, "changed server")
            async with self.create_session(endpoint) as session:
                self.delete_client(session, start_endpoint, client_data.wg_id)
            client_data.wg_server = endpoint
            client_data.wg_id = temp_wg
            await self.client_repository.update_single_field(user_data['id'], "wg_server", endpoint)
            await self.client_repository.update_single_field(user_data['id'], "wg_id", temp_wg)

        async with self.create_session(endpoint) as session:
            result = await self.get_config(session, endpoint, client_data.wg_id)
            await self.client_repository.update_single_field(user_data['id'], "config_file", b64.b64encode(self.get_qr_code(result)).decode("utf-8"))
            self.kafka_producer.send('qr-responses', value=json.dumps({'correlation_id': correlation_id, 'qr_response': {
                "status": True,
            }}))

    async def create_client_handler(self, user_data, correlation_id):
        endpoint = await self.best_endpoint()
        if endpoint == "Failed":
            self.kafka_producer.send('connect-responses', value=json.dumps({'correlation_id': correlation_id, 'connect_response': {"status": False}}).encode("utf-8"))
            return
        async with self.create_session(endpoint) as session:
            result = await self.create_client(session, endpoint, str(user_data['id']))
            clients = await self.get_clients(session, endpoint)
            clients_with_name = [d for d in clients if d['name'] == str(user_data['id'])]
            wg_user_id = clients_with_name[0]["id"]
            if correlation_id == "changed server":
                await self.client_repository.update_user_data(user_data['id'], 0, wg_id=result["id"], wg_server=endpoint, last_used_gigabytes=user_data['used_gigabytes'], used_gigabytes=0)
                return result['id']
            if 'error' in result.keys() and result['error'] != '':
                self.kafka_producer.send('connect-responses', value=json.dumps({'correlation_id': correlation_id, 'connect_response': {
                    "status": False,
                }}))
            print(user_data)
            print(result)
            self.kafka_producer.send('connect-responses', value=json.dumps({'correlation_id': correlation_id, 'connect_response': {
                "status": True,
                "id": str(user_data['id']),
                "wg_id": str(wg_user_id),
                "wg_server": endpoint,
            }}))

    def get_qr_code(self, configuration):
        """Генерация QR-кода для конфигурации."""
        try:
            qr = QRCode(
                version=1,
                error_correction=qrcode.constants.ERROR_CORRECT_L,
                box_size=10,
                border=2,
            )

            qr.add_data(configuration)
            qr.make(fit=True)

            img = qr.make_image(fill_color="black", back_color="white")
            img_io = BytesIO()
            img.save(img_io, "PNG")
            img_io.seek(0)
            return str(img_io)
        except Exception as e:
            self.logger.error(f"Ошибка при генерации QR-кода: {e}")

    
    async def check_alive(self, endpoint):
        url = f"http://{endpoint}/api/wireguard/client"
        try:
            async with ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
                async with session.get(url) as response:
                    return response.status < 500
        except (ClientError, asyncio.TimeoutError):
            return False


    def bytes_to_db(self, bytes_value):
        gb_value = bytes_value / 1_000_000_000  # 1 ГБ = 10^9 байт
        return round(gb_value, 2)
    

    async def get_user_handler(self, user_data, correlation_id):
        client_data = await self.client_repository.get_client_by_user_id(user_data['id'])
        endpoint = client_data.wg_server
        wg_id = client_data.wg_id
        async with self.create_session(endpoint) as session:
            clients = await self.get_clients(session, endpoint)
            client_data = next((c for c in clients if str(c["id"]) == str(wg_id)), None)
            if not client_data:
                self.kafka_producer.send('info-responses', value=json.dumps({'correlation_id': correlation_id, 'status_response': {
                    "status": False,
                    "output": "Client not found"
                }}))
                return
            latest_handshake_at = client_data.get("latestHandshakeAt")
            if latest_handshake_at:
                latest_handshake_at = datetime.datetime.strptime(latest_handshake_at, "%Y-%m-%dT%H:%M:%S.%fZ")
            else:
                latest_handshake_at = datetime.datetime.strptime("1970-01-01T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")
            transfer_tx = client_data.get("transferTx", 0)
            await self.client_repository.update_single_field(user_data['id'], 0, "latest_handshake", latest_handshake_at)
            self.kafka_producer.send('info-responses', value=json.dumps({'correlation_id': correlation_id, 'status_response': {
                "status": True,
            }}))
        
        
    async def _process_message(self, topic: str, user_data: dict, correlation_id: str):
        if topic == 'config-requests':
            await self.get_config_handler(user_data, correlation_id)
        elif topic == 'connect-requests':
            await self.create_client_handler(user_data, correlation_id)
        elif topic == 'info-requests':
            await self.get_user_handler(user_data, correlation_id)
        elif topic == 'qr-requests':
            await self.get_qr_handler(user_data, correlation_id)

    def _start_kafka_consumer(self):
        loop = asyncio.get_event_loop()
        def consume_responses():
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id='config-gateway-group',
                auto_offset_reset='earliest',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')))
            consumer.subscribe(['config-requests','connect-requests', "info-requests", "qr-requests"])
            for msg in consumer:
                try:
                    data = msg.value
                    correlation_id = data['correlation_id']
                    user_data = data['user_data']
                    task = asyncio.run_coroutine_threadsafe(
                        self._process_message(msg.topic, user_data, correlation_id),
                        loop
                    )
                    task.result() 

                except Exception as e:
                    self.logger.error(f"Error processing Kafka message: {e}")
        
        threading.Thread(target=consume_responses, daemon=True).start()


def main():    
    load_dotenv()
    PASSWORD_DATA = {'password': os.getenv('PASSWORD'), 'remember': 'true'}
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if kafka_bootstrap_servers is None:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is not set")
    kafka_producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    endpoints = os.getenv("ENDPOINTS")
    if endpoints is None:
        raise ValueError("ENDPOINTS environment variable is not set")
    endpoints = endpoints.split(",")
    logger = logging.getLogger(__name__)
    client_repository = database.postgres_client.ClientRepository()
    wireguard_service = WireguardService(
        endpoints=endpoints,
        kafka_producer=kafka_producer,
        password_data=PASSWORD_DATA,
        logger=logger,
        client_repository=client_repository
    )
    wireguard_service.run()
    
    


if __name__ == "__main__":
    main()