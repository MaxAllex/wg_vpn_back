from ServerSelector import Server, ServerSelector
import wg_client


async def get_wireguard_config(user_name: str):
    servers = [
        Server(host="192.124.209.125", port=51820, name="Latvia", max_users=50),
        Server(host="147.45.60.75", port=51820, name="Estonia", max_users=50),
        Server(host="91.184.252.165", port=51820, name="Nederland", max_users=25),
    ]

    selector = ServerSelector(servers)
    best_server = await selector.get_best_server()

    if best_server:
        print(f"Лучший сервер: {best_server.name}")
        print(f"Пользователей: {best_server.current_users}/{best_server.max_users}")
    else:
        print("Нет доступных серверов")
        return None

    async with wg_client.create_session(best_server.host) as session:
        await wg_client.create_client(session, best_server.host, user_name)
        clients = await wg_client.get_clients(session, best_server.host)
        client = [c for c in clients if c['name'] == user_name]
        client_id = client[0]["id"]
        config_as_bytes = await wg_client.get_config(session, best_server.host, client_id)
    return config_as_bytes
