import grpc
from concurrent import futures
import time
from kafka import KafkaProducer, KafkaConsumer
from . import client_handler_pb2
from . import client_handler_pb2_grpc
from dotenv import load_dotenv
import os
import logging
import json
from services.jwt import JWTService
import threading
from queue import Queue
import uuid

class ClientHandlerService(client_handler_pb2_grpc.ClientHandlerServicer):
    def GetDbUserData(self, request, context, user_data):
        if "telegram_id" in user_data.items():
            pass
        return "User Not Found"
    
    def GetStatus(self, request, context):
        ack_response = client_handler_pb2.StatusResponse()
        user_data = self.jwt_service.verify_token(request.access_token)
        if user_data == "Token expired":
            ack_response.ack.message = "Token expired"
            return ack_response
        if user_data == "Invalid token":
            ack_response.ack.message = "Invalid token"
            return ack_response
        db_user_data = self.GetDbUserData(request, context, user_data)
        if db_user_data == "User Not Found":
            ack_response.ack.message = "User Not Found"
            return ack_response
        
        ack_response.ack.message = "Request received"
        yield ack_response
        
        response_queue = Queue()
        correlation_id = str(uuid.uuid4())
        self.active_requests[correlation_id] = response_queue
        self.kafka_producer.send('info-requests', value={
            'correlation_id': correlation_id,
            'user_data': db_user_data
        })

        try:
            start_time = time.time()
            while True:
                try:
                    response = response_queue.get(timeout=3)
                    if not response['status']:
                        ack_response.ack.message = "Request failed"
                        return ack_response
                    ack_response.info.status = response['status']
                    ack_response.info.output = response['output']
                    ack_response.info.connection_status = response['connection_status']
                    ack_response.info.created_at = response['created_at']
                    ack_response.info.gigabytes = response['gigabytes']
                    ack_response.info.last_connection = response['last_connection']
                    ack_response.info.premium_status = response['premium_status']
                    ack_response.info.premium_until = response['premium_until']
                    return ack_response
                except Queue.Empty:
                    if time.time() - start_time > self.kafka_timeout_seconds:
                        ack_response.ack.message = "Timeout"
                        return ack_response
                    continue
        finally:
            if correlation_id in self.active_requests:
                del self.active_requests[correlation_id]

    def GetConnectConfig(self, request, context):
        ack_response = client_handler_pb2.ConfigResponse()
        user_data = self.jwt_service.verify_token(request.access_token)
        if user_data == "Token expired":
            ack_response.ack.message = "Token expired"
            return ack_response
        if user_data == "Invalid token":
            ack_response.ack.message = "Invalid token"
            return ack_response
        db_user_data = self.GetDbUserData(request, context, user_data)
        if db_user_data == "User Not Found":
            ack_response.ack.message = "User Not Found"
            return ack_response
        
        ack_response.ack.message = "Request received"
        yield ack_response
        
        response_queue = Queue()
        correlation_id = str(uuid.uuid4())
        self.active_requests[correlation_id] = response_queue
        self.kafka_producer.send('config-requests', value={
            'correlation_id': correlation_id,
            'user_data': db_user_data
        })

    def GetConnectQR(self, request, context):
        ack_response = client_handler_pb2.ConfigResponse()
        user_data = self.jwt_service.verify_token(request.access_token)
        if user_data == "Token expired":
            ack_response.ack.message = "Token expired"
            return ack_response
        if user_data == "Invalid token":
            ack_response.ack.message = "Invalid token"
            return ack_response
        db_user_data = self.GetDbUserData(request, context, user_data)
        if db_user_data == "User Not Found":
            ack_response.ack.message = "User Not Found"
            return ack_response
        
        ack_response.ack.message = "Request received"
        yield ack_response
        
        response_queue = Queue()
        correlation_id = str(uuid.uuid4())
        self.active_requests[correlation_id] = response_queue
        self.kafka_producer.send('qr-requests', value={
            'correlation_id': correlation_id,
            'user_data': db_user_data
        })

    def HandleConnect(self, request, context):
        ack_response = client_handler_pb2.ConnectResponse()
        user_data = self.jwt_service.verify_token(request.access_token)
        if user_data == "Token expired":
            ack_response.ack.message = "Token expired"
            return ack_response
        if user_data == "Invalid token":
            ack_response.ack.message = "Invalid token"
            return ack_response
        db_user_data = self.GetDbUserData(request, context, user_data)
        if db_user_data == "User Not Found":
            ack_response.ack.message = "User Not Found"
            return ack_response
        
        ack_response.ack.message = "Request received"
        yield ack_response
        
        response_queue = Queue()
        correlation_id = str(uuid.uuid4())
        self.active_requests[correlation_id] = response_queue
        self.kafka_producer.send('connect-requests', value={
            'correlation_id': correlation_id,
            'user_data': db_user_data
        })

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
                    data = msg.value
                    correlation_id = data['correlation_id']
                    
                    if correlation_id in self.active_requests:
                        response_queue = self.active_requests[correlation_id]
                        if 'status_response' in data.keys():
                            response_queue.put(
                                data['status_response']
                            )
                        elif 'config_response' in data.keys():
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

    def __init__(self, jwt_service, kafka_timeout_seconds=60):
        kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        if kafka_bootstrap_servers is None:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is not set")
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
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
