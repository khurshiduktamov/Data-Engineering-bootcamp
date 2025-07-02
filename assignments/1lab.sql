-- creating array of struct
CREATE TYPE films AS (
        year INT,
        film TEXT,
        votes INT,
        rating REAL,
        filmid BPCHAR(9)
);

--quality class: This field represents an actor's performance quality
CREATE TYPE quality_class AS ENUM('star', 'good', 'average', 'bad')


--DDL for actors table
CREATE TABLE actors(
        actor TEXT,
        actorid TEXT,
        current_year INT,
        films films[],
        quality_class quality_class,
        is_active BOOLEAN,
        PRIMARY KEY(actor, current_year)
);

-- populating the actors table one year at a time.

INSERT INTO actors
WITH 
        years AS (
                SELECT *
                FROM GENERATE_SERIES(1970, 2021) as year
        ),     
        p AS (
                SELECT
                        actor,
                        actorid,
                        MIN(year) AS first_year
                FROM actor_films
                GROUP BY actor, actorid
        ),
        actors_and_years AS (
                SELECT *
                FROM p
                JOIN years y
                        ON p.first_year <= y.year
        ),
        windowed AS (
                SELECT
                        acny.actor,
                        acny.actorid,
                        acny.year,
                        ARRAY_REMOVE(
                                ARRAY_AGG(
                                        CASE 
                                                WHEN af.year IS NOT NULL
                                                        THEN ROW(
                                                                af.year,
                                                                af.film,
                                                                af.votes,
                                                                af.rating,
                                                                af.filmid
                                                        )::films
                                        END)
                                        OVER (PARTITION BY acny.actor ORDER BY COALESCE(acny.year, af.year)),
                                NULL
                        ) as films
                FROM actors_and_years acny
                LEFT JOIN actor_films af
                        ON acny.actor = af.actor
                        AND acny.year = af.year
                ORDER BY acny.actor, acny.year
                        
        ),
        deduplicated AS (
                SELECT DISTINCT ON (actor, year)
                    actor,
                    actorid,
                    year,
                    films
                FROM windowed
                ORDER BY actor, year
        )
        SELECT
                actor,
                actorid,
                year as current_year,
                films,
                CASE
                        WHEN (films[CARDINALITY(films)]::films).rating > 8 THEN 'star'
                        WHEN (films[CARDINALITY(films)]::films).rating > 7 THEN 'good' 
                        WHEN (films[CARDINALITY(films)]::films).rating > 6 THEN 'average'
                        ELSE 'bad' 
                END::quality_class AS quality_class,
                (films[CARDINALITY(films)]::films).year = year AS is_active 
        FROM deduplicated
        ORDER BY actor, current_year;  -- Added ORDER BY in the final SELECT
        

-- backfill query

CREATE TABLE actors_history_scd (
        actor TEXT,
        quality_class quality_class,
        is_active BOOLEAN,
        start_year INTEGER,
        end_year INTEGER,
        current_year INTEGER,
        PRIMARY KEY (actor, start_year)
)


INSERT INTO actors_history_scd
WITH 
        with_previous AS (
                SELECT 
                        actor,
                        current_year,
                        quality_class,
                        LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY current_year) AS previous_quality_class,
                        is_active,
                        LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) AS previous_is_active
                        
                FROM actors
                WHERE current_year <= 2020
        ),
        
        with_indicators AS (
                SELECT *,
                        CASE
                                WHEN quality_class <> previous_quality_class THEN 1
                                WHEN is_active <> previous_is_active THEN 1
                                ELSE 0
                        END AS change_indicator
                FROM with_previous
        ),
        
        with_streaks AS (
                SELECT *,
                        SUM(change_indicator) 
                                OVER (PARTITION BY actor ORDER BY current_year) AS streak_identifier
                FROM with_indicators
        )
        
        SELECT
                actor,
                quality_class,
                is_active,
                MIN(current_year) AS start_year,
                MAX(current_year) AS end_year,
                2020 AS current_year
        FROM with_streaks
        GROUP BY actor, streak_identifier, is_active, quality_class;
        


-- incremental query
       
CREATE TYPE scd_type AS (
        quality_class quality_class,
        is_active BOOLEAN,
        start_year INTEGER,
        end_year INTEGER
)        

        
WITH 
        last_year_scd AS (
                SELECT * FROM actors_history_scd
                WHERE current_year = 2020
                AND end_year = 2020
        ),
        
        historical_scd AS (
                SELECT
                        actor,
                        quality_class,
                        is_active,
                        start_year,
                        end_year
                FROM actors_history_scd
                WHERE current_year = 2020
                AND end_year < 2020
        ),
        
        this_year_data AS (
                SELECT * FROM actors
                WHERE current_year = 2021
        ),
        
        unchanged_records AS (
                SELECT 
                        ty.actor,
                        ty.quality_class, 
                        ty.is_active,
                        ly.start_year, 
                        ty.current_year AS end_year
                
                FROM this_year_data ty
                        JOIN last_year_scd ly
                        ON ty.actor = ly.actor
                WHERE
                        ty.quality_class = ly.quality_class
                        AND
                        ty.is_active = ly.is_active
        ),
        
        changed_records AS (
                SELECT 
                        ty.actor,
                        UNNEST(ARRAY[
                                ROW(
                                        ly.quality_class,
                                        ly.is_active,
                                        ly.start_year,
                                        ly.end_year
                                )::scd_type,
                                ROW(
                                        ty.quality_class,
                                        ty.is_active,
                                        ty.current_year,
                                        ty.current_year
                                )::scd_type
                        ]) AS records
                FROM this_year_data ty
                        LEFT JOIN last_year_scd ly
                        ON ty.actor = ly.actor
                WHERE
                        (ty.quality_class <> ly.quality_class
                        OR
                        ty.is_active <> ly.is_active)
        ),
        
        unnested_changed_records AS (
                SELECT
                        actor,
                        (records::scd_type).quality_class,
                        (records::scd_type).is_active,
                        (records::scd_type).start_year,
                        (records::scd_type).end_year
                FROM changed_records
        ),
        
        new_records AS (
                SELECT
                        ty.actor,
                        ty.quality_class,
                        ty.is_active,
                        ty.current_year AS start_year,
                        ty.current_year AS end_year
                FROM this_year_data ty
                LEFT JOIN last_year_scd ly
                ON ty.actor = ly.actor
                WHERE ly.actor IS NULL
        )

        SELECT *, 2021 AS current_year FROM (
        
                SELECT * FROM historical_scd
                
                UNION ALL
                        
                SELECT * FROM unchanged_records
                
                UNION ALL
                
                SELECT * FROM unnested_changed_records
                
                UNION ALL
                
                SELECT * FROM new_records
        ) a
        WHERE actor = '50 Cent'
        
        
        
        
SELECT * FROM actors;



