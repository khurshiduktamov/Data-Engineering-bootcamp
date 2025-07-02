




/*
SELECT
        game_id, team_id, player_id, COUNt(1)  -- identifying grain of the game_details table and checking if there are duplicates
FROM game_details
GROUP BY 1,2,3
HAVING COUNT(1) > 1;*/


/*WITH deduped AS (
        SELECT
                g.game_date_est,
                gd.*, ROW_NUMBER() OVER(PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
        FROM game_details gd 
        JOIN games g 
        ON gd.game_id = g.game_id
)

SELECT * 
FROM deduped
WHERE row_num = 1;*/


/*WITH deduped AS (
        SELECT
                *, ROW_NUMBER() OVER(PARTITION BY user_id, event_time) AS row_num
        FROM events
        WHERE user_id IS NOT NULL
)

SELECT *
FROM deduped
WHERE row_num = 1;*/


/*CREATE TABLE user_devices_cumulated (
        user_id TEXT,
        -- which tracks a users active days by browser_type
        device_activity_datelist JSONB,
        date DATE,
        PRIMARY KEY (user_id, date)
)*/

/*{
  "chrome": ["2024-06-20", "2024-06-21"],
  "firefox": ["2024-06-19"]
}*/

/*
INSERT INTO user_devices_cumulated
WITH 
        deduped AS (
                SELECT
                        *, ROW_NUMBER() OVER(PARTITION BY user_id, device_id, DATE(CAST(event_time AS TIMESTAMP))) AS row_num
                FROM events
                WHERE user_id IS NOT NULL
        ),
        
        deduped_devices AS (
                SELECT
                        device_id AS device_id, 
                        browser_type,
                        ROW_NUMBER() OVER(PARTITION BY device_id, browser_type) AS row_num  -- Fixed: device_id not device
                FROM devices
                WHERE device_id IS NOT NULL
        ),
        
        yesterday AS(
                SELECT 
                        *
                FROM user_devices_cumulated
                WHERE date = DATE('2023-01-30')
        ),
        
        today_raw AS (
                SELECT 
                        user_id,
                        device_id,
                        DATE(CAST(event_time AS TIMESTAMP)) AS date_active
                FROM deduped
                WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
                AND user_id IS NOT NULL
                AND row_num = 1
        ),
        
        device_type_joined AS (
                SELECT
                        tr.user_id,
                        COALESCE(dd.browser_type, 'UNKNOWN_DEVICE_' || tr.device_id) AS browser_type,
                        tr.date_active
                FROM today_raw tr
                LEFT JOIN deduped_devices dd
                ON tr.device_id = dd.device_id AND row_num = 1
        ),
        
        today_agg AS (
            SELECT 
                user_id,
                browser_type,
                array_agg(DISTINCT date_active) AS active_dates,
                date_active
            FROM device_type_joined
            WHERE browser_type IS NOT NULL
            GROUP BY user_id, browser_type, date_active
        ),
        
        
        today AS (
                SELECT 
                        CAST(user_id AS TEXT) AS user_id,
                        jsonb_object_agg(browser_type, to_jsonb(active_dates)
                        ) AS device_activity_datelist,
                        date_active
                FROM today_agg
                
                GROUP BY user_id, date_active
        ),
        
        joined AS (
                SELECT
                        COALESCE(t.user_id, y.user_id) AS user_id,
                        t.device_activity_datelist AS t_device_activity_datelist,
                        y.device_activity_datelist AS y_device_activity_datelist,
                        t.date_active AS t_date_active,
                        y.date AS y_date
                FROM today t
                FULL OUTER JOIN yesterday y
                ON t.user_id = y.user_id
        ),
        
        merged AS (
                SELECT
                        user_id,
                        jsonb_object_agg(
                        COALESCE(y_vals.key, t_vals.key),
                        (
                        COALESCE(y_vals.value, '[]'::jsonb) || COALESCE(t_vals.value, '[]'::jsonb)
                        )
                        ) AS device_activity_datelist,
                        COALESCE(t_date_active, y_date + INTERVAL '1 day') AS date
                FROM joined
                LEFT JOIN LATERAL jsonb_each(y_device_activity_datelist) y_vals ON y_device_activity_datelist IS NOT NULL
                LEFT JOIN LATERAL jsonb_each(t_device_activity_datelist) t_vals ON t_device_activity_datelist IS NOT NULL AND (y_vals.key = t_vals.key OR y_vals.key IS NULL OR t_vals.key IS NULL)
                GROUP BY user_id, COALESCE(t_date_active, y_date + INTERVAL '1 day')
        )
        
        SELECT *
        FROM merged;
        */


/*SELECT
        COALESCE(t.user_id, y.user_id) AS user_id,
        
        CASE 
                WHEN y.device_activity_datelist IS NULL THEN t.device_activity_datelist
                WHEN t.device_activity_datelist IS NULL THEN y.device_activity_datelist
                ELSE y.device_activity_datelist || t.device_activity_datelist
        END AS device_activity_datelist,
                
        COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
        
FROM today t
        FULL OUTER JOIN yesterday y
        ON t.user_id = y.user_id;
        */


/*
SELECT * FROM user_devices_cumulated
WHERE date = '2023-01-31';
*/


/*
WITH deduped_devices AS (
        SELECT
                device_id, 
                browser_type,
                ROW_NUMBER() OVER(PARTITION BY device_id, browser_type) AS row_num
        FROM devices
        WHERE device_id IS NOT NULL
)

SELECT *
FROM deduped_devices
WHERE row_num = 1;
*/


-- datelist_int
/*WITH
        users AS (
                SELECT * 
                FROM user_devices_cumulated
                WHERE date = '2023-01-31'
        ),
        
        series AS (
                SELECT * FROM generate_series('2023-01-01'::date, '2023-01-31'::date, INTERVAL '1 day') AS series_date
        ),
        
        expanded AS (
                SELECT 
                        u.user_id,
                        b.browser_type,
                        s.series_date,
                        CASE 
                            WHEN b.dates @> to_jsonb(ARRAY[s.series_date]::date[]) THEN 1
                            ELSE 0
                        END AS datelist_int
                FROM users u
                CROSS JOIN LATERAL jsonb_each(u.device_activity_datelist) AS b(browser_type, dates)
                CROSS JOIN series s
                -- WHERE u.user_id = '444502572952128450'
        )

        SELECT
            user_id,
            browser_type,
            array_agg(datelist_int ORDER BY series_date) AS datelist_int_array,
            SUM(datelist_int) AS bit_count
        FROM expanded
        GROUP BY user_id, browser_type
        ORDER BY user_id, browser_type;*/


--#####################################################

/*  -- It is showing how many days user was active. 
WITH
users AS (
    SELECT * 
    FROM user_devices_cumulated
    WHERE date = '2023-01-31'
),
series AS (
    SELECT
        generate_series('2023-01-01'::date, '2023-01-31'::date, INTERVAL '1 day') AS series_date,
        row_number() OVER () - 1 AS bit_pos  -- 0-based index for POW
),
expanded AS (
    SELECT
        u.user_id,
        s.series_date,
        s.bit_pos,
        COALESCE(
            (
                SELECT 1
                FROM jsonb_each(u.device_activity_datelist) AS browser(browser_type, dates)
                WHERE dates @> to_jsonb(ARRAY[s.series_date]::date[])
                LIMIT 1
            ), 
            0
        ) AS datelist_int
    FROM users u
    CROSS JOIN series s
    WHERE u.user_id = '444502572952128450'
),
bit_encoded AS (
    SELECT
        user_id,
        SUM(
            CASE WHEN datelist_int = 1 THEN POW(2::bigint, bit_pos)::bigint ELSE 0 END
        ) AS datelist_bits
    FROM expanded
    GROUP BY user_id
)
SELECT
        user_id,
        datelist_bits -- user was active days
FROM bit_encoded;
*/
--#####################################################



-- DDL for hosts_cumulated
/*CREATE TABLE hosts_cumulated (
        host TEXT,
        -- which tracks hosts how many days haing activity
        host_activity_datelist DATE[],
        date DATE,
        PRIMARY KEY (host, date)
)*/


-- The incremental query to generate host_activity_datelist
/*
INSERT INTO hosts_cumulated
WITH 
        deduped AS (
                SELECT
                        *, ROW_NUMBER() OVER(PARTITION BY host, event_time) AS row_num
                FROM events
                WHERE host IS NOT NULL
        ),
        
        yesterday AS (
                SELECT
                        *
                FROM hosts_cumulated
                WHERE date = DATE('2023-01-09')
        ),
        
        today AS (
                SELECT
                      host,
                      DATE(CAST(event_time AS TIMESTAMP)) AS date_active 
                FROM deduped
                WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-10')
                AND host IS NOT NULL
                AND row_num = 1
                GROUP BY host, DATE(CAST(event_time AS TIMESTAMP))
        )

                        

        SELECT
                COALESCE(t.host, y.host) AS host,
                CASE
                        WHEN y.host_activity_datelist IS NULL THEN ARRAY_AGG(t.date_active)
                        WHEN ARRAY_AGG(t.date_active) IS NULL THEN y.host_activity_datelist
                        ELSE y.host_activity_datelist || ARRAY_AGG(t.date_active)      
                END AS host_activity_datelist,
                COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
        FROM today t
        FULL OUTER JOIN yesterday y
        ON t.host = y.host
        GROUP BY t.host, y.host, y.host_activity_datelist, t.date_active, y.date

*/

/*
SELECT * FROM hosts_cumulated
WHERE date = '2023-01-10'*/


-- DDLA monthly, reduced fact table DDL host_activity_reduced
/*CREATE TABLE host_activity_reduced (
        host TEXT,
        month_start DATE,
        hit_array INTEGER[],
        unique_visitors INTEGER[],
        PRIMARY KEY(host, month_start)
)*/

/*
TRUNCATE TABLE host_activity_reduced
*/


-- An incremental query that loads host_activity_reduced day-by-day

/*INSERT INTO host_activity_reduced
WITH
        daily_agg AS (
                SELECT 
                        host,
                        DATE(CAST(event_time AS TIMESTAMP)) AS date,
                        COUNT(user_id) AS hit_array,
                        COUNT(DISTINCT user_id) AS unique_visitors
                        
                FROM events
                WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-05')
                AND user_id IS NOT NULL
                GROUP BY host, DATE(CAST(event_time AS TIMESTAMP))
        ),
        
        yesterday_array AS (
                SELECT 
                        *
                FROM host_activity_reduced
                WHERE month_start = DATE('2023-01-01')
        )
        -- DATE_TRUNC('month', da.date)
        SELECT
                COALESCE(da.host, ya.host) AS host,
                COALESCE(ya.month_start, da.date) AS month_start,
                CASE
                        WHEN ya.hit_array IS NOT NULL THEN
                                ya.hit_array || ARRAY_AGG(COALESCE(da.hit_array, 0))
                        WHEN ya.hit_array IS NULL THEN
                                ARRAY_FILL(0, ARRAY_AGG(COALESCE (date - DATE(DATE_TRUNC('month', date)), 0)))
                                        || ARRAY_AGG(COALESCE(da.hit_array, 0))
                END AS hit_array,
                CASE
                        WHEN ya.unique_visitors IS NOT NULL THEN
                                ya.unique_visitors || ARRAY_AGG(COALESCE(da.unique_visitors, 0))
                        WHEN ya.unique_visitors IS NULL THEN
                                ARRAY_FILL(0, ARRAY_AGG(COALESCE (date - DATE(DATE_TRUNC('month', date)), 0))) 
                                        || ARRAY_AGG(COALESCE(da.unique_visitors, 0))
                END AS unique_visitors
                
FROM daily_agg da
FULL OUTER JOIN yesterday_array ya
        ON da.host = ya.host
GROUP BY da.host, ya.host, ya.month_start, da.date, ya.hit_array, ya.unique_visitors
ON CONFLICT (host, month_start)
DO 
        UPDATE SET hit_array = EXCLUDED.hit_array,
        unique_visitors = EXCLUDED.unique_visitors;*/
        
/*
SELECT * FROM host_activity_reduced
*/


/*
SELECT cardinality(hit_array), COUNT(1)
FROM host_activity_reduced
GROUP BY 1;

*/

/*-- Aggregate metrics by summing specific elements in the metric_array
WITH agg AS (
    SELECT host, month_start, ARRAY[SUM(hit_array[1]), 
                                                SUM(hit_array[2]), 
                                                SUM(hit_array[3]),
                                                SUM(hit_array[4]),
                                                SUM(hit_array[5])] AS summed_array
    FROM host_activity_reduced
    GROUP BY host, month_start
)
-- Select and display the metric_name, date (adjusted by index), and summed value
SELECT 
    host, 
    month_start + CAST(CAST(index - 1 AS TEXT) || ' day' AS INTERVAL) AS adjusted_date,
    elem AS hits
FROM agg
CROSS JOIN UNNEST(agg.summed_array) WITH ORDINALITY AS a(elem, index);*/