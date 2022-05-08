from time import sleep
import backoff
import psycopg2
import requests
from config.logger_settings import logger
from config.settings import (BACKOFF_MAX_VALUE, BATCH_SIZE, TABLES,
                             INITIAL_STATE, QUERIES, STATE_FILEPATH)
from postrges_extractor import PostrgesExtractor
from state_saver import JsonFileStorage, State
from utils import (get_curr_time, open_postgres, post_bulk_data,
                   to_es_bulk_format)


def process_update(table: str,
                   state: str,
                   pg_extractor: PostrgesExtractor) -> str:
    '''
    Функция реализующая пайплайн обработки данных - вычитывает порцию данных
    по изменению в конкретной таблице, преобразует их в нужный формат и отправляет
    bulk-запрос в elasticserch
    '''
    state = state[table]
    query = QUERIES[table].format(modified=state, count=BATCH_SIZE)
    data = pg_extractor.execute(query)
    if not data:
        return get_curr_time()

    new_state = data[-1]['modified']

    formatted_data = to_es_bulk_format(data, 'movies')
    response = post_bulk_data(formatted_data)

    if not response.ok:
        logger.error('Error transfer data to elasticsearch '
                     '{ec}'.format(ec=response.reason))
        return state

    logger.info('Successfully transferred {count} records '
                'to elasticsearch'.format(count=len(data)))
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

        pg_extractor = PostrgesExtractor(pg_conn)
        state_loader = State(JsonFileStorage(STATE_FILEPATH))
        state = state_loader.get_state() or {tbl: INITIAL_STATE for tbl in TABLES}

        while True:
            for table in TABLES:
                state[table] = process_update(table, state, pg_extractor)
                state_loader.set_state(table, state[table])
            sleep(3)


if __name__ == '__main__':
    etl_main_loop()
