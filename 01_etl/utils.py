from contextlib import contextmanager
from datetime import datetime

import psycopg2
import json
from config.logger_settings import logger
from config.settings import DATABASES
from psycopg2.extras import DictCursor


def get_curr_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f%z") + '00'


def to_es_bulk_format(data: list) -> str:
    result = []
    for item in data:
        index_row = {
            'index': {
                '_index': 'movies',
                '_id': item['id']
            }
        }

        del item['modified']

        result.append(json.dumps(index_row))
        result.append(json.dumps(item))

    return '\n'.join(result) + '\n'


@contextmanager
def open_postgres():
    '''
    Contextmanager for posgres db
    '''
    DB = DATABASES['postgres']
    conn = psycopg2.connect(**DB, cursor_factory=DictCursor)
    try:
        logger.info("Creating connection to postgres db host "
                    "{host}:{port}".format(host=DB['host'], port=DB['port']))
        yield conn
    finally:
        logger.info("Closing connection to postgres db")
        conn.close()
