import os

from dotenv import load_dotenv
from config.sql import SELECT_MODIFIED_FILMWORK, SELECT_MODIFIED_GENRE, SELECT_MODIFIED_PERSON

load_dotenv()

BATCH_SIZE = 20

STATE_FILEPATH = 'state.txt'

CONTENT_TYPES = ('filmwork', 'person', 'genre')

INITIAL_STATE = '1970-01-01 00:00:00.000000+00'

BACKOFF_MAX_VALUE = 20  # seconds

DATABASES = {
    'postgres': {
        'dbname':   os.environ.get('PG_DB_NAME'),
        'user':     os.environ.get('PG_DB_USER'),
        'password': os.environ.get('PG_DB_PASSWORD'),
        'host':     os.environ.get('PG_DB_HOST', '127.0.0.1'),
        'port':     os.environ.get('PG_DB_PORT', 5432),
    },
    'elasticsearch': {
        'host':     os.environ.get('ES_HOST', '127.0.0.1'),
        'port':     os.environ.get('ES_PORT', 6543),
    }
}

QUERIES = {
    'filmwork': SELECT_MODIFIED_FILMWORK,
    'person': SELECT_MODIFIED_PERSON,
    'genre': SELECT_MODIFIED_GENRE
}
