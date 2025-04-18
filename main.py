from handlers.clients.server import ClientHandlerService
from services.jwt import JWTService
import threading
from dotenv import load_dotenv

def main():
    load_dotenv()
    jwt_service = JWTService()
    client_handler_service = ClientHandlerService(jwt_service)
    client_handler_service.serve()


if __name__ == "__main__":
    server_thread = threading.Thread(target=main)
    server_thread.start()
    server_thread.join()