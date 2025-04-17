import grpc
from concurrent import futures
import time
import client_handler_pb2
import client_handler_pb2_grpc
from dotenv import load_dotenv
import os
import logging
from services.jwt import JWTService


class ClientHandlerService(client_handler_pb2_grpc.ClientHandlerServicer):
    def GetStatus(self, request, context):
        ack_response = client_handler_pb2.StatusResponse()
        if self.jwt_service.verify_token(request.access_token) == "Token expired":
            ack_response.ack.message = "Token expired"
            return ack_response
        if self.jwt_service.verify_token(request.access_token) == "Invalid token":
            ack_response.ack.message = "Invalid token"
            return ack_response
        ack_response.ack.message = "Request received"
        yield ack_response
        #TODO вставить обработку status
        info_response = client_handler_pb2.StatusResponse()
        info_response.info.status = True
        info_response.info.output = "Status check completed successfully"
        
        yield info_response

    def GetConnectConfig(self, request, context):
        ack_response = client_handler_pb2.ConfigResponse()
        if self.jwt_service.verify_token(request.access_token) == "Token expired":
            ack_response.status = False
            ack_response.ack.message = "Token expired"
            return ack_response
        if self.jwt_service.verify_token(request.access_token) == "Invalid token":
            ack_response.status = False
            ack_response.ack.message = "Invalid token"
            return ack_response
        
        ack_response.ack.message = "Config request received"
        yield ack_response

        #TODO вставить обработку config
        config_response = client_handler_pb2.ConfigResponse()
        config_response.status = True
        config_response.output = "Configuration data"
        yield config_response

    def GetConnectQR(self, request, context):
        ack_response = client_handler_pb2.ConfigResponse()
        if self.jwt_service.verify_token(request.access_token) == "Token expired":
            ack_response.status = False
            ack_response.ack.message = "Token expired"
            return ack_response
        if self.jwt_service.verify_token(request.access_token) == "Invalid token":
            ack_response.status = False
            ack_response.ack.message = "Invalid token"
            return ack_response
        
        ack_response.status = True
        ack_response.ack.message = "QR request received"
        yield ack_response

        #TODO вставить обработку qr
        qr_response = client_handler_pb2.ConfigResponse()
        
        qr_response.status = True
        try:
            with open("sample_qr.png", "rb") as f:
                qr_response.image.image_data = f.read()
        except FileNotFoundError:
            qr_response.status = False
            qr_response.output = "QR image not found"

        yield qr_response

    def __init__(self, jwt_service: JWTService):
        load_dotenv()
        self.logger = logging.getLogger(__name__)
        self.grpc_client_port = int(os.getenv("GRPC_CLIENT_PORT"))
        self.jwt_service = jwt_service
        
    def serve(self):
        try:
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            client_handler_pb2_grpc.add_ClientHandlerServicer_to_server(ClientHandlerService(), server)
            server.add_insecure_port(f'[::]:{self.grpc_client_port}')
            self.logger.info(f"serving client_grpc server started at {self.grpc_client_port}")
            server.start()
            server.wait_for_termination()
        except Exception as e:
            self.logger.error(f"Error serving client_grpc server:{e}")
