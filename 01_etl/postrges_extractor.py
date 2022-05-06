from psycopg2.extensions import connection as _connection


class PostrgesExtractor:
    def __init__(self, connection: _connection):
        self.connection = connection

    def execute(self, query: str):
        cursor = self.connection.cursor()
        cursor.execute(query)
        # return cursor.fetchall()

        desc = cursor.description
        column_names = [col[0] for col in desc]
        data = [dict(zip(column_names, row))
                for row in cursor.fetchall()]
        return data
