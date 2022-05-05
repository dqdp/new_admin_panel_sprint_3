import json
from time import sleep

import backoff
import psycopg2
from config.logger_settings import logger
from config.settings import (BACKOFF_MAX_VALUE, BATCH_SIZE, INITIAL_STATE,
                             PARAMS, STATE_FILEPATH)
from postrges_extractor import PostrgesExtractor
from state_saver import JsonFileStorage, State
from utils import get_recursively, open_postgres


def process_update(content_type, state, postgres_extractor, state_loader):

    params = PARAMS[content_type]

    query = params['sql_query'].format(time=state, count=BATCH_SIZE)
    data = postgres_extractor.execute(query)

    for fw in data:
        print(json.dumps(fw))
        print('--------------------------')
        print(fw[0], fw[6])
    print('------------------------------------------------------0000----------')
    sleep(1)

    # todo: format data and load to elasticsearch

    logger.info('Successfully transferred {count} records '
                'to elasticsearch'.format(count=len(data)))

    state = get_recursively(data, 'modified')[0]
    state_loader.set_state(params['state_key'], state)


@backoff.on_exception(backoff.expo,
                      psycopg2.DatabaseError,
                      logger=logger,
                      max_value=BACKOFF_MAX_VALUE)
def etl_main_loop():
    with open_postgres() as pg_conn:

        postgres_extractor = PostrgesExtractor(pg_conn)
        state_loader = State(JsonFileStorage(STATE_FILEPATH))
        filmwork_state = state_loader.get_state('filwork_modified') or INITIAL_STATE
        person_state = state_loader.get_state('person_modified') or INITIAL_STATE
        genre_state = state_loader.get_state('filwork_modified') or INITIAL_STATE

        while True:

            process_update('filmworks',
                           filmwork_state,
                           postgres_extractor,
                           state_loader)
            process_update('persons',
                           person_state,
                           postgres_extractor,
                           state_loader)
            process_update('genres',
                           genre_state,
                           postgres_extractor,
                           state_loader)


if __name__ == '__main__':
    etl_main_loop()
