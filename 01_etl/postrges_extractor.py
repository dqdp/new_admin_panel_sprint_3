from psycopg2.extensions import connection as _connection
from typing import List


class PostrgesExtractor:
    '''
    Класс для извлечения данных из postgres db
    '''
    def __init__(self, connection: _connection):
        self.connection = connection

    def execute(self, query: str) -> List[dict]:
        '''
        Выполняет запрос и возвращает результат в
        виде списка словарей сразу с именами полей
        '''
        cursor = self.connection.cursor()
        cursor.execute(query)

        desc = cursor.description
        column_names = [col[0] for col in desc]
        data = [dict(zip(column_names, row))
                for row in cursor.fetchall()]
        return data
