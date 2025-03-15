import asyncio
import aiohttp
from dataclasses import dataclass
from typing import List, Optional

import wg_client


@dataclass
class Server:
    """Класс для хранения информации о сервере"""
    host: str
    port: int
    name: str
    max_users: int
    weight: float = 1.0
    current_users: int = 0
    is_active: bool = False


class ServerSelector:
    def __init__(self, servers: List[Server]):
        self.servers = servers
        self.timeout = 5

    async def get_server_status(self, server: Server) -> None:
        """Получение текущего статуса сервера"""
        try:
            async with wg_client.create_session(server.host) as session:
                if session is None:  # Проверяем, что сессия создана
                    server.is_active = False
                    return

                response = await wg_client.get_clients(session, server.host)

                server.is_active = True
                server.current_users = len(response)

        except aiohttp.ClientError as e:
            print(f"Ошибка сети при подключении к {server.host}: {e}")
            server.is_active = False
        except Exception as e:
            print(f"Ошибка получения статуса сервера {server.host}: {e}")
            server.is_active = False

    async def update_servers_status(self):
        """Обновление статуса всех серверов"""
        tasks = []
        for server in self.servers:
            tasks.append(self.get_server_status(server))

        await asyncio.gather(*tasks, return_exceptions=True)

    async def get_best_server(self) -> Optional[Server]:
        """Получение лучшего сервера"""
        await self.update_servers_status()

        best_server = None
        best_score = float('-inf')

        for server in self.servers:
            score = self.calculate_score(server)
            if score > best_score:
                best_score = score
                best_server = server

        return best_server

    @staticmethod
    def calculate_score(server: Server) -> float:
        """Расчет рейтинга сервера"""
        if not server.is_active:
            return float('-inf')

        user_load = server.current_users / server.max_users
        score = ((1 - user_load) * 0.4) * server.weight
        return score
