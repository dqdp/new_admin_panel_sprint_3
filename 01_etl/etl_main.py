from contextlib import contextmanager
from datetime import datetime
from time import sleep

import backoff
import psycopg2
from psycopg2.extras import DictCursor

from config.logger_settings import logger
from config.settings import BATCH_SIZE, DATABASES, INITIAL_STATE, STATE_FILEPATH
from config.sql import SELECT_MODIFIED_FILMWORKS
from postrges_extractor import PostrgesExtractor
from state_saver import JsonFileStorage, State


def get_curr_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f%z") + '00'


@contextmanager
def open_postgres():
    conn = psycopg2.connect(**DATABASES['postgres'], cursor_factory=DictCursor)
    try:
        logger.info("Creating connection to postgres db")
        yield conn
    finally:
        logger.info("Closing connection to postgres db")
        conn.close()


@backoff.on_exception(backoff.expo,
                      psycopg2.DatabaseError,
                      logger=logger,
                      max_value=15)
def etl_main_loop():
    with open_postgres() as pg_conn:

        postgres_extractor = PostrgesExtractor(pg_conn)
        state_loader = State(JsonFileStorage(STATE_FILEPATH))
        state = state_loader.get_state('modified') or INITIAL_STATE

        while True:

            query = SELECT_MODIFIED_FILMWORKS.format(state, BATCH_SIZE)
            modified_filmworks = postgres_extractor.execute(query)

            for fw in modified_filmworks:
                print(fw[0], fw[6])

            sleep(2)
            # todo: format data and load to elasticsearch

            logger.info('Successfully transferred {count} records'
                        'to elasticsearch'.format(count=len(modified_filmworks)))

            state = modified_filmworks[-1][6]
            state_loader.set_state('modified', state)


if __name__ == '__main__':
    etl_main_loop()
