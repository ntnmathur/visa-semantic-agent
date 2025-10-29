{{
    config(
        materialized='table',
        meta={
            'metricflow': {
                'time_spine': {
                    'standard_granularity_column': 'date_day'
                }
            }
        }
    )
}}

WITH day_spine AS (
    SELECT
        DATE_ADD(DATE('2020-01-01'), INTERVAL n DAY) AS date_day
    FROM 
        UNNEST(GENERATE_ARRAY(0, 3650)) AS n
)

SELECT
    date_day AS date_day
FROM day_spine
