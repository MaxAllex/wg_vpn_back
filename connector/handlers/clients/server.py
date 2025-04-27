import grpc
from concurrent import futures
import time
from kafka import KafkaProducer, KafkaConsumer

import database.entities
import database.entities.client
import database.postgres_client
from . import client_handler_pb2
from . import client_handler_pb2_grpc
from dotenv import load_dotenv
import os
import logging
import json
from services.jwt import JWTService
import threading
from queue import Queue, Empty
from datetime import datetime
from google.protobuf.timestamp_pb2 import Timestamp

import asyncio
import uuid
import database


class ClientHandlerService(client_handler_pb2_grpc.ClientHandlerServicer):
    def GetDbUserData(self, request, context, user_data) -> database.entities.client.Client:
        try:
            if "telegram_id" in user_data.keys():
                user = asyncio.run(self.client_repo.get_client_by_telegram_id(user_data["telegram_id"]))
                if user != None:
                    return user
            if "app_token" in user_data.keys():
                user = asyncio.run(self.client_repo.get_client_by_app_token(user_data["telegram_id"]))
                if user != None:
                    return user
            return None
        except Exception as e:
            self.logger.error(f"Error getting user from database: {e}")
            return None
    
    def GetStatus(self, request, context):
        print("here1")
        ack_response = client_handler_pb2.StatusResponse()
        print("here2")
        user_data = self.jwt_service.verify_token(request.access_token)
        print(user_data)
        
        if user_data == "Token expired":
            ack_response.ack.message = "Token expired"
            yield ack_response
            return
            
        if user_data == "Invalid token":
            ack_response.ack.message = "Invalid token"
            yield ack_response
            return
        db_user_data = self.GetDbUserData(request, context, user_data)
        print("here3")
        print({"id": str(db_user_data.id), "used_gigabytes": db_user_data.used_gigabytes,"wg_id": str(db_user_data.wg_id), "wg_server": db_user_data.wg_server, "max_gigabytes": db_user_data.max_gigabytes, "last_used_gigabytes": db_user_data.last_used_gigabytes})
        
        print(db_user_data)
        if db_user_data is None:
            ack_response.ack.message = "User not found"
            yield ack_response
            return
        
        ack_response.ack.message = "Request received"
        yield ack_response
        
        response_queue = Queue()
        correlation_id = str(uuid.uuid4())
        self.active_requests[correlation_id] = response_queue
        self.kafka_producer.send('info-requests', value={
            'correlation_id': correlation_id,
            'user_data': {"id": str(db_user_data.id), "used_gigabytes": db_user_data.used_gigabytes,"wg_id": str(db_user_data.wg_id), "wg_server": db_user_data.wg_server, "max_gigabytes": db_user_data.max_gigabytes, "last_used_gigabytes": db_user_data.last_used_gigabytes}    
        })

        try:
            start_time = time.time()
            while True:
                try:
                    response = response_queue.get(timeout=3)
                    if not response['status']:
                        if response['output'] == "Client not found":
                            ack_response.ack.message = "User not found"
                            yield ack_response
                            return
                        ack_response.ack.message = "Request failed"
                        yield ack_response
                        return
                    
                    ack_response.info.status = True
                    yield ack_response
                    return
                except Empty:
                    if time.time() - start_time > self.kafka_timeout_seconds:
                        ack_response.ack.message = "Timeout"
                        yield ack_response
                        return
                    continue
        finally:
            if correlation_id in self.active_requests:
                del self.active_requests[correlation_id]

    def GetConnectConfig(self, request, context):
        ack_response = client_handler_pb2.ConfigResponse()
        user_data = self.jwt_service.verify_token(request.access_token)
        if user_data == "Token expired":
            ack_response.ack.message = "Token expired"
            yield ack_response
            return
        if user_data == "Invalid token":
            ack_response.ack.message = "Invalid token"
            yield ack_response
            return
        db_user_data = self.GetDbUserData(request, context, user_data)
        if db_user_data is None:
            ack_response.ack.message = "User not found"
            yield ack_response
            return
        
        ack_response.ack.message = "Request received"
        yield ack_response
        
        response_queue = Queue()
        correlation_id = str(uuid.uuid4())
        self.active_requests[correlation_id] = response_queue
        self.kafka_producer.send('config-requests', value={
            'correlation_id': correlation_id,
            'user_data': {"id": str(db_user_data.id), "used_gigabytes": db_user_data.used_gigabytes,"wg_id": str(db_user_data.wg_id), "wg_server": db_user_data.wg_server, "max_gigabytes": db_user_data.max_gigabytes, "last_used_gigabytes": db_user_data.last_used_gigabytes}
        })

        try:
            start_time = time.time()
            while True:
                try:
                    response = response_queue.get(timeout=3)
                    if not response['status']:
                        ack_response.ack.message = "Request failed"
                        yield ack_response
                        return
                    ack_response.config.status = True
                    yield ack_response
                    return
                except Empty:
                    if time.time() - start_time > self.kafka_timeout_seconds:
                        ack_response.ack.message = "Timeout"
                        yield ack_response
                        return
                    continue
        finally:
            if correlation_id in self.active_requests:
                del self.active_requests[correlation_id]

    def GetConnectQR(self, request, context):
        ack_response = client_handler_pb2.ConfigResponse()
        user_data = self.jwt_service.verify_token(request.access_token)
        if user_data == "Token expired":
            ack_response.ack.message = "Token expired"
            yield ack_response
            return
        if user_data == "Invalid token":
            ack_response.ack.message = "Invalid token"
            yield ack_response
            return
        db_user_data = self.GetDbUserData(request, context, user_data)
        if db_user_data is None:
            ack_response.ack.message = "User not found"
            yield ack_response
            return
        
        ack_response.ack.message = "Request received"
        yield ack_response
        
        response_queue = Queue()
        correlation_id = str(uuid.uuid4())
        self.active_requests[correlation_id] = response_queue
        self.kafka_producer.send('qr-requests', value={
            'correlation_id': correlation_id,
            'user_data': {"id": str(db_user_data.id), "used_gigabytes": db_user_data.used_gigabytes,"wg_id": str(db_user_data.wg_id), "wg_server": db_user_data.wg_server, "max_gigabytes": db_user_data.max_gigabytes, "last_used_gigabytes": db_user_data.last_used_gigabytes}
        })

        try:
            start_time = time.time()
            while True:
                try:
                    response = response_queue.get(timeout=3)
                    if not response['status']:
                        ack_response.ack.message = "Request failed"
                        yield ack_response
                        return
                    ack_response.image.status = True
                    yield ack_response
                    return
                except Empty:
                    if time.time() - start_time > self.kafka_timeout_seconds:
                        ack_response.ack.message = "Timeout"
                        yield ack_response
                        return
                    continue
        finally:
            if correlation_id in self.active_requests:
                del self.active_requests[correlation_id]

    def HandleConnect(self, request, context):
        ack_response = client_handler_pb2.ConnectResponse()
        user_data = self.jwt_service.verify_token(request.access_token)
        if user_data == "Token expired":
            ack_response.ack.message = "Token expired"
            yield ack_response
            return
        if user_data == "Invalid token":
            ack_response.ack.message = "Invalid token"
            yield ack_response
            return
        db_user_data = self.GetDbUserData(request, context, user_data)
        if db_user_data is not None:
            ack_response.ack.message = "User already exists"
            yield ack_response
            return
        new_client_data = {}
        if user_data.get("telegram_id") is not None:
            new_client_data["telegram_id"] = user_data["telegram_id"]
        if user_data.get("app_token") is not None:
            new_client_data["app_token"] = user_data["app_token"]
        if new_client_data == {}:
            ack_response.ack.message = "Invalid token"
            yield ack_response
            return
        asyncio.run(self.client_repo.save_client(new_client_data))

        db_user_data = self.GetDbUserData(request, context, user_data)
        
        ack_response.ack.message = "Request received"
        yield ack_response
        
        
        response_queue = Queue()
        correlation_id = str(uuid.uuid4())
        self.active_requests[correlation_id] = response_queue
        self.kafka_producer.send('connect-requests', value={
            'correlation_id': correlation_id,
            'user_data': {"id":str(db_user_data.id)}
        })
        try:
            start_time = time.time()
            while True:
                try:
                    print("here")
                    response = response_queue.get(timeout=3)
                    if not response['status']:
                        print("here2")
                        ack_response.ack.message = "Request failed"
                        yield ack_response
                        return
                    print("here3")
                    asyncio.run(self.client_repo.update_single_field(db_user_data.id, 0, "wg_id", response['wg_id']))
                    asyncio.run(self.client_repo.update_single_field(db_user_data.id, 0, "wg_server", response['wg_server']))
                    ack_response.info.status = True
                    
                    
                    yield ack_response

                    return
                except Empty:
                    if time.time() - start_time > self.kafka_timeout_seconds:
                        ack_response.ack.message = "Timeout"
                        yield ack_response
                        return
                    continue
        finally:
            if correlation_id in self.active_requests:
                del self.active_requests[correlation_id]

    def _start_kafka_consumer(self, bootstrap_servers):
        """Запускает фоновый поток для получения ответов из Kafka"""
        def consume_responses():
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                group_id='config-gateway-group',
                auto_offset_reset='earliest',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')))
            consumer.subscribe(['config-responses', 'qr-responses', 'info-responses', 'connect-responses'])
            for msg in consumer:
                try:
                    data = json.loads(msg.value)
                    print(data)
                    correlation_id = data['correlation_id']
                    if correlation_id == "changed server":
                        continue


                    elif correlation_id in self.active_requests:
                        print(correlation_id)
                        response_queue = self.active_requests[correlation_id]
                        print(response_queue)
                        if correlation_id not in self.active_requests:
                            print("hehehehe")
                        if 'status_response' in data.keys():
                            response_queue.put(
                                data['status_response']
                            )
                        elif 'config_response' in data.keys():
                            print("config_response")
                            response_queue.put(
                                data['config_response']
                            )
                        elif 'connect_response' in data.keys():
                            response_queue.put(
                                data['connect_response']
                            )
                        elif 'qr_response' in data.keys():
                            response_queue.put(
                                data['qr_response']
                            )
                            
                        if correlation_id in self.active_requests:
                            del self.active_requests[correlation_id]
                
                except Exception as e:
                    logging.error(f"Error processing Kafka message: {e}")
        
        threading.Thread(target=consume_responses, daemon=True).start()

    def __init__(self, jwt_service, client_repository: database.postgres_client.ClientRepository,kafka_timeout_seconds=60):
        kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        if kafka_bootstrap_servers is None:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is not set")
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.client_repo = client_repository
        self.active_requests = {}
        self.kafka_timeout_seconds = kafka_timeout_seconds
        self._start_kafka_consumer(kafka_bootstrap_servers) 
        self.logger = logging.getLogger(__name__)
        self.grpc_client_port = int(os.getenv("GRPC_CLIENT_PORT"))
        self.jwt_service = jwt_service
        
    def serve(self):
        try:
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            client_handler_pb2_grpc.add_ClientHandlerServicer_to_server(self, server)
            server.add_insecure_port(f'[::]:{self.grpc_client_port}')
            self.logger.info(f"serving client_grpc server started at {self.grpc_client_port}")
            server.start()
            server.wait_for_termination()
        except Exception as e:
            self.logger.error(f"Error serving client_grpc server:{e}")
