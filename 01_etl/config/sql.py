SELECT_MODIFIED_FILMWORKS = '''SELECT
        fw.id,
        fw.title,
        fw.description,
        fw.rating,
        fw.type,
        to_char(fw.created, 'YYYY-MM-DD HH24:MI:SS.FF6TZH'),
        to_char(fw.modified, 'YYYY-MM-DD HH24:MI:SS.FF6TZH'),
        COALESCE (
            json_agg(
                DISTINCT jsonb_build_object(
                    'person_role', pfw.role,
                    'person_id', p.id,
                    'person_name', p.full_name
                )
            ) FILTER (WHERE p.id is not null),
            '[]'
        ) as persons,
        COALESCE (
            json_agg(
                DISTINCT jsonb_build_object(
                    'genre_id', g.id,
                    'genre', g.name,
                )
            ) FILTER (WHERE g.id is not null),
            '[]'
        ) as genres
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    WHERE fw.modified > '{time}'
    GROUP BY fw.id
    ORDER BY fw.modified
    LIMIT {count}; '''


SELECT_MODIFIED_PERSONS = '''SELECT
        fw.id,
        fw.title,
        COALESCE (
            json_agg(
                DISTINCT jsonb_build_object(
                    'person_role', pfw.role,
                    'person_id', p.id,
                    'person_name', p.full_name,
                    'modified', p.modified
                )
            ) FILTER (WHERE p.id is not null),
            '[]'
        ) as persons,
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    WHERE p.modified > '{time}'
    GROUP BY fw.id, p.id
    ORDER BY p.modified
    LIMIT {count}; '''

SELECT_MODIFIED_GENRES = '''SELECT
        fw.id,
        fw.title,
        COALESCE (
            json_agg(
                DISTINCT jsonb_build_object(
                    'genre_id', g.id,
                    'genre', g.name,
                    'modified', g.modified
                )
            ) FILTER (WHERE g.id is not null),
            '[]'
        ) as genres
    FROM content.film_work fw
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    WHERE fw.modified > '{time}' OR g.modified > '{time}' OR p.modified > '{time}'
    GROUP BY fw.id, g.id
    ORDER BY g.modified
    LIMIT {count}; '''
