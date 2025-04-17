import grpc
from concurrent import futures
import time
import client_handler_pb2
import client_handler_pb2_grpc
from dotenv import load_dotenv
import os
import logging

class ClientHandlerService(client_handler_pb2_grpc.ClientHandlerServicer):
    def GetStatus(self, request, context):
        ack_response = client_handler_pb2.StatusResponse()
        ack_response.ack.message = "Request received"
        yield ack_response
        #TODO вставить обработку status
        info_response = client_handler_pb2.StatusResponse()
        if request.access_token == "valid_token":
            info_response.info.status = True
            info_response.info.output = "Status check completed successfully"
        else:
            info_response.info.status = False
            info_response.info.output = "Invalid access token"
        yield info_response

    def GetConnectConfig(self, request, context):
        ack_response = client_handler_pb2.ConfigResponse()
        ack_response.status = True
        ack_response.output = "Config request received"
        yield ack_response
        #TODO вставить обработку config
        config_response = client_handler_pb2.ConfigResponse()
        if request.access_token == "valid_token":
            config_response.status = True
            config_response.output = "Configuration data"
        else:
            config_response.status = False
            config_response.output = "Unauthorized"
        yield config_response

    def GetConnectQR(self, request, context):
        ack_response = client_handler_pb2.ConfigResponse()
        ack_response.status = True
        ack_response.output = "QR request received"
        yield ack_response

        #TODO вставить обработку qr
        qr_response = client_handler_pb2.ConfigResponse()
        if request.access_token == "valid_token":
            qr_response.status = True
            try:
                with open("sample_qr.png", "rb") as f:
                    qr_response.image.image_data = f.read()
            except FileNotFoundError:
                qr_response.status = False
                qr_response.output = "QR image not found"
        else:
            qr_response.status = False
            qr_response.output = "Invalid token"
        yield qr_response

    def __init__(self):
        load_dotenv()
        self.logger = logging.getLogger(__name__)
        self.grpc_client_port = int(os.getenv("GRPC_CLIENT_PORT"))

        
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
