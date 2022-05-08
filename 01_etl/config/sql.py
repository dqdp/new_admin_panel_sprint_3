'''
Одним запросом у меня не получилось сделать, т.к. по той схеме которую вы предложили,
будут теряться данные - мы сортируем по максимальному из всех времен обновлений, и следующим запросом выбираем
по такому общему  состоянию, но в конкретной таблице состояние может быть меньше - соответственно теряются данные.

Пытался записывать состояние  по каждой таблице отдельно, типа того


to_char(fw.modified,
        'YYYY-MM-DD HH24:MI:SS.FF6TZH'
) AS filmwork_modified,
COALESCE (
    to_char(MAX(p.modified),
    'YYYY-MM-DD HH24:MI:SS.FF6TZH'),
    '1970-01-01 00:00:00.000000+00'
) AS person_modified,
COALESCE (
    to_char(MAX(g.modified),
    'YYYY-MM-DD HH24:MI:SS.FF6TZH'),
    '1970-01-01 00:00:00.000000+00'
) AS genre_modified,
to_char(
    Greatest(fw.modified, MAX(p.modified), MAX(g.modified)),
    'YYYY-MM-DD HH24:MI:SS.FF6TZH'
) AS modified,


но и так не получается - непонятно как сортировать и если сортируем по
modified = Greatest(fw.modified, MAX(p.modified), MAX(g.modified)), уходим в бесконечный цикл
по той же самой причине но в обратную сторону - выбираются раз за разом одни и те же данные
'''

SELECT_MODIFIED_FILMWORK = '''SELECT
        fw.id,
        fw.rating AS imdb_rating,
        fw.title,
        fw.description,
        to_char(fw.modified, 'YYYY-MM-DD HH24:MI:SS.FF6TZH') AS modified,
        COALESCE (
            string_agg(DISTINCT p.full_name, '')
            FILTER(WHERE p.id is not null AND pfw.role = 'director'),
            ''
            ) AS director,
        array_agg(DISTINCT p.full_name) FILTER(WHERE p.id is not null AND pfw.role = 'actor') AS actors_names,
        array_agg(DISTINCT p.full_name) FILTER(WHERE p.id is not null AND pfw.role = 'writer') AS writers_names,
        COALESCE (
            json_agg(
                DISTINCT jsonb_build_object(
                    'id', p.id,
                    'name', p.full_name
                )
            ) FILTER (WHERE p.id is not null AND pfw.role = 'actor'),
            '[]'
        ) as actors,
        COALESCE (
            json_agg(
                DISTINCT jsonb_build_object(
                    'id', p.id,
                    'name', p.full_name
                )
            ) FILTER (WHERE p.id is not null AND pfw.role = 'writer'),
            '[]'
        ) as writers,
        array_agg(DISTINCT g.name) as genre
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    WHERE fw.modified > '{modified}'
    GROUP BY fw.id
    ORDER BY modified
    LIMIT {count}; '''


SELECT_MODIFIED_PERSON = '''SELECT
        fw.id,
        fw.title,
        fw.description,
        fw.rating AS imdb_rating,
        to_char(MAX(p.modified), 'YYYY-MM-DD HH24:MI:SS.FF6TZH') AS modified,
        COALESCE (
            string_agg(DISTINCT p.full_name, '')
            FILTER(WHERE p.id is not null AND pfw.role = 'director'),
            ''
            ) AS director,
        array_agg(DISTINCT p.full_name) FILTER(WHERE p.id is not null AND pfw.role = 'actor') AS actors_names,
        array_agg(DISTINCT p.full_name) FILTER(WHERE p.id is not null AND pfw.role = 'writer') AS writers_names,
        COALESCE (
            json_agg(
                DISTINCT jsonb_build_object(
                    'id', p.id,
                    'name', p.full_name
                )
            ) FILTER (WHERE p.id is not null AND pfw.role = 'actor'),
            '[]'
        ) as actors,
        COALESCE (
            json_agg(
                DISTINCT jsonb_build_object(
                    'id', p.id,
                    'name', p.full_name
                )
            ) FILTER (WHERE p.id is not null AND pfw.role = 'writer'),
            '[]'
        ) as writers,
        array_agg(DISTINCT g.name) as genre
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    WHERE p.modified > '{modified}'
    GROUP BY fw.id
    ORDER BY modified
    LIMIT {count}; '''

SELECT_MODIFIED_GENRE = '''SELECT
        fw.id,
        fw.title,
        fw.description,
        fw.rating AS imdb_rating,
        to_char(MAX(g.modified), 'YYYY-MM-DD HH24:MI:SS.FF6TZH') AS modified,
        COALESCE (
            string_agg(DISTINCT p.full_name, '')
            FILTER(WHERE p.id is not null AND pfw.role = 'director'),
            ''
            ) AS director,
        array_agg(DISTINCT p.full_name) FILTER(WHERE p.id is not null AND pfw.role = 'actor') AS actors_names,
        array_agg(DISTINCT p.full_name) FILTER(WHERE p.id is not null AND pfw.role = 'writer') AS writers_names,
        COALESCE (
            json_agg(
                DISTINCT jsonb_build_object(
                    'id', p.id,
                    'name', p.full_name
                )
            ) FILTER (WHERE p.id is not null AND pfw.role = 'actor'),
            '[]'
        ) as actors,
        COALESCE (
            json_agg(
                DISTINCT jsonb_build_object(
                    'id', p.id,
                    'name', p.full_name
                )
            ) FILTER (WHERE p.id is not null AND pfw.role = 'writer'),
            '[]'
        ) as writers,
        array_agg(DISTINCT g.name) as genre
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    WHERE  g.modified > '{modified}'
    GROUP BY fw.id
    ORDER BY modified
    LIMIT {count}; '''
