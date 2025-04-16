import logging

import asyncpg
import os

from dotenv import load_dotenv

from database.entities.client import Client

load_dotenv()
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASS')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB_NAME = os.getenv('POSTGRES_DB_NAME')
POSTGRES_HOST_NAME = os.getenv('POSTGRES_HOST_NAME')


logger = logging.getLogger(__name__)


class ClientRepository:
    def __init__(self, db_user: str, db_password: str, db_host: str, db_port: int, db_name: str, max_retries: int = 3):
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.max_retries = max_retries

    async def connect(self):
        try:
            conn = await asyncpg.connect(f'{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')
            return conn
        except Exception as e:
            logger.error(f"Error connecting to the database: {e}")
            return None
    
    async def close(self, conn) -> str:
        if conn is not None:
            try:
                await conn.close()
            except Exception as e:
                logger.error(f"Error closing the database connection: {e}")
                return f"Error closing the database connection: {e}"
            else:
                return None
        return None
    
    async def save_client(self, client_data: dict, retry_count: int = 0) -> str:
        try:
            conn = await self.connect()
            
            query = """
                    INSERT INTO users (id, telegram_id, wg_id, has_premium_status, premium_status_is_valid_until, 
                            config_file, qr_code, enabled_status, created_at, need_to_disable, jwt_version)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 0);
                    """
            await conn.execute(
                                query,
                                client_data["id"],
                                client_data["telegram_id"],
                                client_data["wg_id"],
                                client_data["has_premium_status"],
                                client_data["premium_status_is_valid_until"],
                                client_data["config_file"],
                                client_data["qr_code"],
                                client_data["enabled_status"],
                                client_data["created_at"],
                                client_data["need_to_disable"]
                                )
            logger.info(f"Клиент {client_data['telegram_id']} сохранен в базе данных")
            return None
        
        except Exception as e:
            if retry_count < self.max_retries:
                return await self.save_client(client_data, retry_count + 1)
            else:
                logger.error(f"Error saving client: {e}")
                return f"Error saving client: {e}"
        finally:
            await self.close(conn)
        
    async def get_all_clients(self, retry_count: int = 0) -> list[Client]:
        try:
            conn = await self.connect()
            clients = await conn.fetch("SELECT * FROM users")
            return [Client(**dict(client)) for client in clients]  # Преобразуем в объекты Client
        except Exception as e:
            if retry_count < self.max_retries:
                return await self.get_all_clients(retry_count + 1)
            else:
                logger.error(f"Error getting client: {e}")
                return []
        finally:
            await self.close(conn)

    async def get_client_by_telegram_id(self, telegram_id:int, retry_count: int = 0) -> Client:
        conn = await self.connect()
        try:
            client = await conn.fetch("SELECT * FROM users WHERE telegram_id = $1", telegram_id)
            if len(client) == 0:
                return None
            client_data = client[0]
            return Client(
                id=client_data['id'],
                telegram_id=client_data['telegram_id'],
                wg_id=client_data['wg_id'],
                has_premium_status=client_data['has_premium_status'],
                premium_status_is_valid_until=client_data['premium_status_is_valid_until'],
                config_file=client_data['config_file'],
                qr_code=client_data['qr_code'],
                enabled_status=client_data['enabled_status'],
                created_at=client_data['created_at'],
                need_to_disable=client_data['need_to_disable'],
                jwt_version=client_data['jwt_version']
            )
        except Exception as e:
            if retry_count < self.max_retries:
                return await self.get_client_by_telegram_id(telegram_id, retry_count + 1)
            logger.error(f"Error getting client by telegram_id: {e}")
            return None
        finally:
            await conn.close()     

    async def update_user_data(self, telegram_id, retry_count , **kwargs):
        if not kwargs:
            logger.error("Нет данных для обновления")
            return
        try:
            conn = await self.connect()

            set_clause = ", ".join([f"{key} = ${i + 1}" for i, key in enumerate(kwargs.keys())])
            values = list(kwargs.values()) + [telegram_id]

            query = f"UPDATE users SET {set_clause} WHERE telegram_id = ${len(values)};"
            await conn.execute(query, *values)
            logger.info(f"Обновлены поля {list(kwargs.keys())} у пользователя {telegram_id}")
        except Exception as e:
            if retry_count < self.max_retries:
                return await self.update_user_data(telegram_id, retry_count + 1, **kwargs)
            logger.error(f"Error updating user data: {e}")
        finally:
            await conn.close()




    async def update_single_field(self, telegram_id,retry_count, field, value):
        try:
            conn = await self.connect()
        
            query = f"UPDATE users SET {field} = $1 WHERE telegram_id = $2;"
            await conn.execute(query, value, telegram_id)
            logger.info(f"Поле {field} у пользователя {telegram_id} обновлено!")
        except Exception as e:
            if retry_count < self.max_retries:
                return await self.update_single_field(telegram_id, retry_count + 1, field, value)
            logger.error(f"Error updating single field: {e}")
        finally:
            await conn.close()
            
            


#async def get_db_connection():
#    """Устанавливаем соединение с PostgreSQL"""
#    return await asyncpg.connect(f'postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST_NAME}:{int(POSTGRES_PORT)}/{POSTGRES_DB_NAME}')
#
#
#async def save_client(client_data: dict):
#    conn = await get_db_connection()
#
#    try:
#        query = """
#        INSERT INTO users (id, telegram_id, wg_id, has_premium_status, premium_status_is_valid_until, 
#                             config_file, qr_code, enabled_status, created_at, need_to_disable)
#        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);
#        """
#
#        # Извлекаем данные из client_data
#        await conn.execute(query,
#                           client_data["id"],
#                           client_data["telegram_id"],
#                           client_data["wg_id"],
#                           client_data["has_premium_status"],
#                           client_data["premium_status_is_valid_until"],
#                           client_data["config_file"],
#                           client_data["qr_code"],
#                           client_data["enabled_status"],
#                           client_data["created_at"],
#                           client_data["need_to_disable"])
#        logger.info(f"Client with wg_id {client_data['telegram_id']} saved successfully.")
#
#    except Exception as e:
#        logger.error(f"Error saving client: {e}")
#
#    finally:
#        await conn.close()
#
#
#async def get_all_clients():
#    """Получает список всех клиентов"""
#    conn = await get_db_connection()
#    try:
#        clients = await conn.fetch("SELECT * FROM users")
#        return [Client(**dict(client)) for client in clients]  # Преобразуем в объекты Client
#    except Exception as e:
#        logger.error(f"Ошибка при получении клиентов: {e}")
#        return []
#    finally:
#        await conn.close()
#
#
#async def get_client_by_telegram_id(telegram_id):
#    conn = await get_db_connection()
#    try:
#        client = await conn.fetch("SELECT * FROM users WHERE telegram_id = $1", telegram_id)
#        client_data = client[0]
#        return Client(
#            id=client_data['id'],
#            telegram_id=client_data['telegram_id'],
#            wg_id=client_data['wg_id'],
#            has_premium_status=client_data['has_premium_status'],
#            premium_status_is_valid_until=client_data['premium_status_is_valid_until'],
#            config_file=client_data['config_file'],
#            qr_code=client_data['qr_code'],
#            enabled_status=client_data['enabled_status'],
#            created_at=client_data['created_at'],
#            need_to_disable=client_data['need_to_disable']
#        )
#    except Exception as e:
#        logger.error(f"Ошибка при получении клиентов: {e}")
#        return []
#    finally:
#        await conn.close()
#
#
#async def update_user_data(telegram_id, **kwargs):
#    if not kwargs:
#        logger.error("Нет данных для обновления")
#        return
#
#    conn = await get_db_connection()
#
#    set_clause = ", ".join([f"{key} = ${i + 1}" for i, key in enumerate(kwargs.keys())])
#    values = list(kwargs.values()) + [telegram_id]
#
#    query = f"UPDATE users SET {set_clause} WHERE telegram_id = ${len(values)};"
#    await conn.execute(query, *values)
#
#    await conn.close()
#    logger.info(f"Обновлены поля {list(kwargs.keys())} у пользователя {telegram_id}")
#
#
#async def update_single_field(telegram_id, field, value):
#    conn = await get_db_connection()
#
#    query = f"UPDATE users SET {field} = $1 WHERE telegram_id = $2;"
#    await conn.execute(query, value, telegram_id)
#
#    await conn.close()
#    print(f"Поле {field} у пользователя {telegram_id} обновлено!")
#