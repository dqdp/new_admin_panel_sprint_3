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
