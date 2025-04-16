import wg_client


class Utils:

    def __init__(self):
        pass

    @staticmethod
    async def get_client_id(session, best_server):
        clients = await wg_client.get_clients(session, best_server.host)
        client = [c for c in clients if c['name'] == user_name]
        client_id = client[0]["id"]
        return client_id

    @staticmethod
    async def save_client_to_db(session, best_server, user_name: str):
        clients = await wg_client.get_clients(session, best_server.host)

        clients_with_name = [d for d in clients if d['name'] == user_name]
        if len(clients_with_name) > 0:
            raise ValueError(f"Создано больше 1 клиента для {user_name}")

        wg_user_id = clients_with_name[0]["id"]
        config_response = await wg_client.get_config(session, wg_user_id, best_server.host)

        client_data = {
            "id": uuid.uuid4(),
            "telegram_id": user_name,
            "wg_id": uuid.UUID(wg_user_id),
            "has_premium_status": False,
            "premium_status_is_valid_until": None,
            "config_file": base64.b64encode(config_response).decode("utf-8"),
            "qr_code": base64.b64encode(Utils.get_qr_code(config_response.decode("utf-8")).read()).decode("utf-8"),
            "enabled_status": clients_with_name[0]["enabled"],
            "created_at": datetime.strptime(clients_with_name[0]["createdAt"], "%Y-%m-%dT%H:%M:%S.%fZ"),
            "need_to_disable": False
        }

        await save_client(client_data)

    @staticmethod
    async def check_client_traffic(self, session, best_server):
            clients_from_wg = await wg_client.get_clients(session, best_server.host)
            clients_from_db = await get_all_clients()
            clients_will_be_disable = []

            for client in clients_from_wg:
                transfer_tx = client.get("transferTx", 0)
                telegram_id = client.get("name")
                gigabytes_value = Utils.bytes_to_gb(transfer_tx) if transfer_tx else 0

                if gigabytes_value >= 10 and client.get("enabled", False):
                    clients_will_be_disable.append(telegram_id)

            for client_from_db in clients_from_db:
                telegram_id = f'{client_from_db.telegram_id}'

                if telegram_id in clients_will_be_disable:
                    if not client_from_db.has_premium_status:
                        if client_from_db.enabled_status:
                            await Utils.disable_client(client_from_db)