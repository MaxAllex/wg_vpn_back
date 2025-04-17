import grpc
from concurrent import futures
import client_handler_pb2
import client_handler_pb2_grpc

class ClientHandlerServicer(client_handler_pb2_grpc.ClientHandlerServicer):
    def GetStatus(self, request, context):
        response = client_handler_pb2.StatusResponse()
        if request.access_token == "valid_token":
            response.ack.message = "Status OK"
        else:
            response.info.status = False
            response.info.output = "Invalid access token"
        return response

    def GetConnectConfig(self, request, context):
        response = client_handler_pb2.ConfigResponse()
        if request.access_token == "valid_token":
            response.status = True
            response.output = "Configuration data"
        else:
            response.status = False
            response.output = "Unauthorized"
        return response

    def GetConnectQR(self, request, context):
        response = client_handler_pb2.ConfigResponse()
        if request.access_token == "valid_token":
            response.status = True
            with open("sample_qr.png", "rb") as f:
                response.image.image_data = f.read()
        else:
            response.status = False
            response.output = "Invalid token"
        return response

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    client_handler_pb2_grpc.add_ClientHandlerServicer_to_server(ClientHandlerServicer(), server)
    
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()