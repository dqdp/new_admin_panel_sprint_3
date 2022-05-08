import json
from contextlib import contextmanager
from datetime import datetime

import psycopg2
import requests
from psycopg2.extras import DictCursor

from config.logger_settings import etl_logger
from config.settings import DATABASES, HEADER_JSON


def post_bulk_data(body: str) -> requests.Response:
    '''
    Отправка bulk-запроса с данными в elasticserch
    '''
    host = DATABASES['es']['host']
    port = DATABASES['es']['port']
    url = f'http://{host}:{port}/_bulk'
    return requests.post(url, headers=HEADER_JSON, data=body)


def get_curr_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f%z") + '00'


def to_es_bulk_format(data: list, index: str) -> str:
    '''
    Формирование тела bulk-запроса для внесения данных в
    elasticserch пачкой
    '''
    result = []
    for item in data:
        index_item = {
            'index': {
                '_index': index,
                '_id': item['id']
            }
        }
        del item['modified']
        result.append(json.dumps(index_item))
        result.append(json.dumps(item))

    return '\n'.join(result) + '\n'


@contextmanager
def open_postgres():
    '''
    Contextmanager for posgres db
    '''
    DB = DATABASES['pg']
    conn = psycopg2.connect(**DB, cursor_factory=DictCursor)
    try:
        etl_logger.info("Creating connection to postgres db host "
                        "{host}:{port}".format(host=DB['host'], port=DB['port']))
        yield conn
    finally:
        etl_logger.info("Closing connection to postgres db")
        conn.close()
