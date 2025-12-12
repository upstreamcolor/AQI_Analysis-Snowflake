USE ROLE sysadmin;
USE SCHEMA dev_db.consumption_sch;
USE WAREHOUSE adhoc_wh;

-- create dynamic table `agg_city_fact_hour_level` to store agg. data per hour/city level
CREATE OR REPLACE DYNAMIC TABLE agg_city_fact_hour_level 
    TARGET_LAG = '30 min' 
    WAREHOUSE = transform_wh 
AS
    WITH
        step01_city_level_data AS (
            SELECT
                d.measurement_time
                ,l.country AS country
                ,l.state AS state
                ,l.city AS city
                ,AVG(pm10_avg) AS pm10_avg
                ,AVG(pm25_avg) AS pm25_avg
                ,AVG(so2_avg) AS so2_avg
                ,AVG(no2_avg) AS no2_avg
                ,AVG(nh3_avg) AS nh3_avg
                ,AVG(co_avg) AS co_avg
                ,AVG(o3_avg) AS o3_avg
            FROM
                air_quality_fact f
                JOIN dim_date d ON f.date_fk = d.date_pk
                JOIN dim_location l ON f.location_fk = l.location_pk
            GROUP BY
                1
                ,2
                ,3
                ,4
    )
    SELECT
        *,
        prominent_index (pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) AS prominent_pollutant
        ,CASE
            WHEN three_sub_index_criteria (pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) > 2 
            THEN GREATEST(
                pm25_avg
                ,pm10_avg
                ,so2_avg
                ,no2_avg
                ,nh3_avg
                ,co_avg
                ,o3_avg
            )
            ELSE 0
        END AS aqi
    FROM
        step01_city_level_data;

-- check data
SELECT
    *
FROM
    agg_city_fact_hour_level
ORDER BY
    country
    ,state
    ,city
    ,measurement_time
LIMIT
    100;

-- create dynamic table `agg_city_fact_hour_level` to store agg. data per hour/day level
CREATE OR REPLACE DYNAMIC TABLE agg_city_fact_day_level 
    TARGET_LAG = '30 min' 
    WAREHOUSE = transform_wh 
AS
    WITH
        step01_city_day_level_data AS (
            SELECT
                DATE(measurement_time) AS measurement_date
                ,country AS country
                ,state AS state
                ,city AS city
                ,ROUND(AVG(pm10_avg)) AS pm10_avg
                ,ROUND(AVG(pm25_avg)) AS pm25_avg
                ,ROUND(AVG(so2_avg)) AS so2_avg
                ,ROUND(AVG(no2_avg)) AS no2_avg
                ,ROUND(AVG(nh3_avg)) AS nh3_avg
                ,ROUND(AVG(co_avg)) AS co_avg
                ,ROUND(AVG(o3_avg)) AS o3_avg
            FROM
                agg_city_fact_hour_level
            GROUP BY
                1
                ,2
                ,3
                ,4
    )
    SELECT
        *
        ,prominent_index (pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) AS prominent_pollutant
        ,CASE
            WHEN three_sub_index_criteria (pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) > 2 
            THEN GREATEST(
                pm25_avg
                ,pm10_avg
                ,so2_avg
                ,no2_avg
                ,nh3_avg
                ,co_avg
                ,o3_avg
            )
            ELSE 0
        END AS aqi
    FROM
        step01_city_day_level_data;

-- check data
SELECT
    *
FROM
    agg_city_fact_day_level
ORDER BY
    country
    ,state
    ,city
    ,measurement_date
LIMIT
    100;