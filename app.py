import requests
import json
import argparse
import logging
from py2neo import Graph, Node, Relationship
from dotenv import load_dotenv
import os
import time
import logging.handlers


# Загрузка переменных окружения из файла .env
load_dotenv()

# Ваш токен доступа
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
API_URL = os.getenv('API_URL', 'https://api.vk.com/method/')

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Подключение к базе данных Neo4j
graph = Graph(os.getenv('NEO4J_URL'), auth=(os.getenv('NEO4J_USER'), os.getenv('NEO4J_PASSWORD')))

def setup_logging(log_file=None, log_level=logging.INFO):
    handlers = []
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    handlers.append(logging.StreamHandler())
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )

def clear_database():
    """Очистка базы данных перед новым запуском."""
    graph.run("MATCH (n) DETACH DELETE n")
    logging.info("База данных очищена.")

def request_vk_api(method, params):
    """Универсальная функция для выполнения запросов к VK API."""
    params['access_token'] = ACCESS_TOKEN
    params['v'] = '5.131'

    while True:
        response = requests.get(f"{API_URL}{method}", params=params).json()
        if 'error' in response:
            error_code = response['error'].get('error_code')
            if error_code == 6:  # Too many requests per second
                logging.warning("Превышен лимит запросов. Ожидание 1 секунду.")
                time.sleep(1)
                continue
            else:
                logging.error(f"Ошибка VK API: {response['error']}")
                return None
        return response

def get_user_info(user_id):
    url = f'{API_URL}users.get?user_ids={user_id}&access_token={ACCESS_TOKEN}&v=5.131'
    response = requests.get(url)
    return response.json()

def get_followers(user_id):
    url = f'{API_URL}users.getFollowers?user_id={user_id}&access_token={ACCESS_TOKEN}&v=5.131&extended=1&fields=screen_name'
    response = requests.get(url)
    return response.json()

def get_subscriptions(user_id):
    url = f'{API_URL}users.getSubscriptions?user_id={user_id}&access_token={ACCESS_TOKEN}&v=5.131&extended=1&fields=screen_name'
    response = requests.get(url)
    return response.json()

def get_groups(user_id):
    url = f'{API_URL}groups.get?user_id={user_id}&access_token={ACCESS_TOKEN}&v=5.131&extended=1&fields=screen_name'
    response = requests.get(url)
    return response.json()

def create_user_node(user):
    user_node = Node(
        "User",
        id=user['id'],
        screen_name=user.get('screen_name', ''),
        name=f"{user.get('first_name', '')} {user.get('last_name', '')}",
        city=user.get('city', {}).get('title', '')
    )
    graph.merge(user_node, "User", "id")
    return user_node

def process_user(user_id, depth=2):
    if depth <= 0:
        return

    user_info = get_user_info(user_id)
    if not user_info or 'response' not in user_info or not user_info['response']:
        logging.error(f"Не удалось получить данные пользователя с ID {user_id}")
        return

    user_data = user_info['response'][0]
    user_node = create_user_node(user_data)

    followers = get_followers(user_id)
    if followers and 'response' in followers and followers['response'].get('items'):
        for follower in followers['response']['items']:
            follower_node = create_user_node(follower)
            graph.merge(Relationship(follower_node, "Follow", user_node))
            process_user(follower['id'], depth - 1)

    subscriptions = get_subscriptions(user_id)
    if subscriptions and 'response' in subscriptions and subscriptions['response'].get('items'):
        for subscription in subscriptions['response']['items']:
            subscription_node = create_user_node(subscription)
            graph.merge(Relationship(user_node, "Subscribe", subscription_node))
            process_user(subscription['id'], depth - 1)

    groups = get_groups(user_id)
    if groups and 'response' in groups and groups['response'].get('items'):
        for group in groups['response']['items']:
            group_node = Node("Group", id=group['id'], screen_name=group['screen_name'], name=group['name'])
            graph.merge(group_node, "Group", "id")
            graph.merge(Relationship(user_node, "Subscribe", group_node))

def query_all_users():
    return graph.run("MATCH (n:User) RETURN n").data()

def query_all_groups():
    return graph.run("MATCH (n:Group) RETURN n").data()

def query_top_5_followers():
    query = (
        "MATCH (n:User)-[r:Follow]->(m:User) "
        "RETURN m.id AS id, m.name AS name, COUNT(r) AS followers "
        "ORDER BY followers DESC LIMIT 5"
    )
    return graph.run(query).data()

def query_top_5_popular_groups():
    query = (
        "MATCH (n:User)-[r:Subscribe]->(m:Group) "
        "RETURN m.id AS id, m.name AS name, COUNT(r) AS subscribers "
        "ORDER BY subscribers DESC LIMIT 5"
    )
    return graph.run(query).data()

def query_mutual_followers():
    query = (
        "MATCH (n:User)-[:Follow]->(m:User), (m)-[:Follow]->(n) "
        "RETURN n.id AS user1_id, n.name AS user1_name, m.id AS user2_id, m.name AS user2_name"
    )
    return graph.run(query).data()

#Дополнительный запросы

def query_common_subscriptions():
    query = (
        "MATCH (n:User)-[:Subscribe]->(g:Group)<-[:Subscribe]-(m:User) "
        "WHERE n <> m "
        "RETURN n.id AS user1_id, n.name AS user1_name, m.id AS user2_id, m.name AS user2_name, g.id AS group_id, g.name AS group_name"
    )
    return graph.run(query).data()

def query_inactive_users():
    query = (
        "MATCH (n:User) "
        "WHERE NOT (n)-[:Subscribe]->() AND NOT (n)-[:Follow]->() "
        "RETURN n.id AS user_id, n.name AS user_name"
    )
    return graph.run(query).data()

def query_mutual_follow_and_subscribe():
    query = (
        "MATCH (n:User)-[:Follow]->(m:User), (m)-[:Follow]->(n), "
        "(n)-[:Subscribe]->(g:Group), (m)-[:Subscribe]->(g) "
        "RETURN n.id AS user1_id, n.name AS user1_name, m.id AS user2_id, m.name AS user2_name, g.id AS group_id, g.name AS group_name"
    )
    return graph.run(query).data()


def main(user_id):
    clear_database()
    process_user(user_id)
    logging.info("Данные сохранены в базу данных Neo4j")

    logging.info(f"Всего пользователей: {len(query_all_users())}")
    logging.info(f"Всего групп: {len(query_all_groups())}")
    logging.info(f"Топ 5 пользователей по количеству фолловеров: {query_top_5_followers()}")
    logging.info(f"Топ 5 самых популярных групп: {query_top_5_popular_groups()}")
    logging.info(f"Все пользователи, которые фолловеры друг друга: {query_mutual_followers()}")
    
    logging.info(f"Пользователи, которые подписаны на одинаковые группы: {query_common_subscriptions()}")
    logging.info(f"Пассивные пользователи (без подписок и фолловеров): {query_inactive_users()}")
    logging.info(f"Пользователи, которые одновременно фолловеры и подписаны друг на друга: {query_mutual_follow_and_subscribe()}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='VK API User Info')
    parser.add_argument('--user_id', type=int, default=274881868, help='ID пользователя ВК')
    parser.add_argument('--log_level', type=str, default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Уровень логирования')
    parser.add_argument('--log_file', type=str, help='Файл для сохранения логов')
    
    args = parser.parse_args()
    
    # Настройка логирования
    setup_logging(log_file=args.log_file, log_level=getattr(logging, args.log_level.upper()))
    
    main(args.user_id)
