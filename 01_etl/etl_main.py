from time import sleep
from typing import Any

import backoff
import psycopg2
from config.logger_settings import logger
from config.settings import (BACKOFF_MAX_VALUE, BATCH_SIZE, INITIAL_STATE,
                             QUERIES, STATE_FILEPATH, CONTENT_TYPES)
from postrges_extractor import PostrgesExtractor
from state_saver import JsonFileStorage, State
from utils import open_postgres, get_curr_time, to_es_bulk_format


def process_update(content_type: str,
                   state: str,
                   postgres_extractor: Any) -> str:

    state = state[content_type]
    query = QUERIES[content_type].format(modified=state, count=BATCH_SIZE)
    data = postgres_extractor.execute(query)

    if not data:
        return get_curr_time()

    new_state = data[-1]['modified']
    print(new_state)

    to_es = to_es_bulk_format(data)
    print(to_es)
    print('\n-------------------------------------------------------\n')

    logger.info('Successfully transferred {count} records '
                'to elasticsearch'.format(count=len(data)))

    return new_state


@backoff.on_exception(backoff.expo,
                      psycopg2.DatabaseError,
                      logger=logger,
                      max_value=BACKOFF_MAX_VALUE)
def etl_main_loop():
    '''
    Функция с бесконечным циклом, вычитывающая изменения из базы postgres и
    записывающая их в elasticsearch. При падении базы включается механизм
    backoff. Состояние сохраняется на диск на каждой итерации цикла
    '''
    with open_postgres() as pg_conn:

        postgres_extractor = PostrgesExtractor(pg_conn)
        state_loader = State(JsonFileStorage(STATE_FILEPATH))
        state = state_loader.get_state() or {sk: INITIAL_STATE for sk in CONTENT_TYPES}

        while True:
            for content_type in CONTENT_TYPES:
                state[content_type] = process_update(content_type, state, postgres_extractor)
                state_loader.set_state(content_type, state[content_type])

            sleep(0.25)


if __name__ == '__main__':
    etl_main_loop()
