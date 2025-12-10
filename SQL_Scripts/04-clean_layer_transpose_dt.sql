USE ROLE sysadmin;
USE SCHEMA dev_db.clean_sch;
USE WAREHOUSE adhoc_wh;

-- create dynamic table `clean_flatten_aqi_dt` to flatten the cleaned raw data in `clean_aqi_dt`
CREATE OR REPLACE DYNAMIC TABLE clean_flatten_aqi_dt 
    TARGET_LAG = '30 min' 
    WAREHOUSE = transform_wh 
AS
WITH
    step01_combine_pollutant_cte AS (
        SELECT
            index_record_ts
            ,country
            ,state
            ,city
            ,station
            ,latitude
            ,longitude
            ,MAX(
                CASE
                    WHEN LOWER(pollutant_id) = 'pm10' THEN pollutant_avg
                END
            ) AS pm10_avg
            ,MAX(
                CASE
                    WHEN LOWER(pollutant_id) = 'pm2.5' THEN pollutant_avg
                END
            ) AS pm25_avg
            ,MAX(
                CASE
                    WHEN LOWER(pollutant_id) = 'so2' THEN pollutant_avg
                END
            ) AS so2_avg
            ,MAX(
                CASE
                    WHEN LOWER(pollutant_id) = 'no2' THEN pollutant_avg
                END
            ) AS no2_avg
            ,MAX(
                CASE
                    WHEN LOWER(pollutant_id) = 'nh3' THEN pollutant_avg
                END
            ) AS nh3_avg
            ,MAX(
                CASE
                    WHEN LOWER(pollutant_id) = 'co' THEN pollutant_avg
                END
            ) AS co_avg
            ,MAX(
                CASE
                    WHEN LOWER(pollutant_id) = 'ozone' THEN pollutant_avg
                END
            ) AS o3_avg
        FROM
            clean_aqi_dt
        GROUP BY
            index_record_ts
            ,country
            ,state
            ,city
            ,station
            ,latitude
            ,longitude
        ORDER BY
            country
            ,state
            ,city
            ,station
    ),
    step02_replace_NA_cte AS (
        SELECT
            index_record_ts
            ,country
            ,replace (state, '_', ' ') AS state
            ,city
            ,station
            ,latitude
            ,longitude
            ,CASE
                WHEN pm25_avg = 'NA' THEN 0
                WHEN pm25_avg IS NULL THEN 0
                ELSE round(pm25_avg)
            END AS pm25_avg
            ,CASE
                WHEN pm10_avg = 'NA' THEN 0
                WHEN pm10_avg IS NULL THEN 0
                ELSE round(pm10_avg)
            END AS pm10_avg
            ,CASE
                WHEN so2_avg = 'NA' THEN 0
                WHEN so2_avg IS NULL THEN 0
                ELSE round(so2_avg)
            END AS so2_avg
            ,CASE
                WHEN no2_avg = 'NA' THEN 0
                WHEN no2_avg IS NULL THEN 0
                ELSE round(no2_avg)
            END AS no2_avg
            ,CASE
                WHEN nh3_avg = 'NA' THEN 0
                WHEN nh3_avg IS NULL THEN 0
                ELSE round(nh3_avg)
            END AS nh3_avg
            ,CASE
                WHEN co_avg = 'NA' THEN 0
                WHEN co_avg IS NULL THEN 0
                ELSE round(co_avg)
            END AS co_avg
            ,CASE
                WHEN o3_avg = 'NA' THEN 0
                WHEN o3_avg IS NULL THEN 0
                ELSE round(o3_avg)
            END AS o3_avg
        FROM
            step01_combine_pollutant_cte
    )
    SELECT
        *
    FROM
        step02_replace_NA_cte;

--check data
SELECT
    *
FROM
    clean_flatten_aqi_dt
LIMIT
    10;
