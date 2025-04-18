from handlers.clients.server import ClientHandlerService
from services.jwt import JWTService


def main():
    jwt_service = JWTService()
    client_handler_service = ClientHandlerService(jwt_service)
    client_handler_service.serve()


if __name__ == "__main__":
    main()