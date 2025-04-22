import database.postgres_client
from handlers.clients.server import ClientHandlerService
from services.jwt import JWTService
import threading
from dotenv import load_dotenv
import database
def main():
    load_dotenv()
    jwt_service = JWTService()
    client_repository = database.postgres_client.ClientRepository()
    client_handler_service = ClientHandlerService(jwt_service, client_repository,kafka_timeout_seconds=60)
    client_handler_service.serve()


if __name__ == "__main__":
    server_thread = threading.Thread(target=main)
    server_thread.start()
    server_thread.join()