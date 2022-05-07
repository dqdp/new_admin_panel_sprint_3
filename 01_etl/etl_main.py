from time import sleep
from typing import Any

import backoff
import psycopg2
import requests
from config.logger_settings import logger
from config.settings import (BACKOFF_MAX_VALUE, BATCH_SIZE, CONTENT_TYPES,
                             INITIAL_STATE, QUERIES, STATE_FILEPATH)
from postrges_extractor import PostrgesExtractor
from state_saver import JsonFileStorage, State
from utils import get_curr_time, open_postgres, to_es_bulk_format, post_bulk_data


def process_update(content_type: str,
                   state: str,
                   postgres_extractor: Any) -> str:
    '''
    Функция реализующая пайплайн обработки данных - вычитывает порцию данных
    по изменению в конкретной таблице, преобразует их в нужный формат и отправляет 
    bulk-запрос в elasticserch
    '''
    state = state[content_type]
    query = QUERIES[content_type].format(modified=state, count=BATCH_SIZE)
    data = postgres_extractor.execute(query)
    if not data:
        return get_curr_time()

    new_state = data[-1]['modified']

    to_es = to_es_bulk_format(data)
    response = post_bulk_data(to_es)

    if response.ok:
        logger.info('Successfully transferred {count} records '
                    'to elasticsearch'.format(count=len(data)))
        print(response.text)
    else:
        logger.info('Error transfer data to elasticsearch '
                    '{ec}'.format(ec=response.reason))
    return new_state


@backoff.on_exception(backoff.expo,
                      (psycopg2.DatabaseError,
                       requests.ConnectionError),
                      logger=logger,
                      max_value=BACKOFF_MAX_VALUE)
def etl_main_loop():
    '''
    Функция с бесконечным циклом, вычитывающая изменения из базы postgres и
    записывающая их в elasticsearch. При недоступности сервисов включается 
    механизм backoff. Состояние сохраняется на диск на каждой итерации цикла
    '''
    with open_postgres() as pg_conn:

        postgres_extractor = PostrgesExtractor(pg_conn)
        state_loader = State(JsonFileStorage(STATE_FILEPATH))
        state = state_loader.get_state() or {sk: INITIAL_STATE for sk in CONTENT_TYPES}

        while True:
            for content_type in ['filmwork']: #CONTENT_TYPES:
                state[content_type] = process_update(content_type, state, postgres_extractor)
                state_loader.set_state(content_type, state[content_type])

            sleep(5)


if __name__ == '__main__':
    etl_main_loop()
