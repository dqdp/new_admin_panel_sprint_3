from contextlib import contextmanager
from datetime import datetime

import psycopg2
from config.logger_settings import logger
from config.settings import DATABASES
from psycopg2.extras import DictCursor


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


def get_recursively(search_dict, field):
    """
    Takes a dict with nested lists and dicts,
    and searches all dicts for a key of the field
    provided.
    """
    fields_found = []

    for key, value in search_dict.iteritems():

        if key == field:
            fields_found.append(value)

        elif isinstance(value, dict):
            results = get_recursively(value, field)
            for result in results:
                fields_found.append(result)

        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    more_results = get_recursively(item, field)
                    for another_result in more_results:
                        fields_found.append(another_result)

    return fields_found
