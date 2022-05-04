import os

from dotenv import load_dotenv

load_dotenv()

BATCH_SIZE = 5

STATE_FILEPATH = 'state.txt'

INITIAL_STATE = '1970-01-01 00:00:00.000000+00'

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
