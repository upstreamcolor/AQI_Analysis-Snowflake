USE ROLE sysadmin;
USE SCHEMA dev_db.consumption_sch;
USE WAREHOUSE adhoc_wh;

-- create dimension table `dim_date` to store date data
CREATE OR REPLACE DYNAMIC TABLE dim_date 
    TARGET_LAG = 'downstream' 
    WAREHOUSE = transform_wh 
AS
    WITH step01_hr_data AS (
            SELECT
                index_record_ts AS measurement_time
                ,YEAR(index_record_ts) AS aqi_year
                ,MONTH(index_record_ts) AS aqi_month
                ,QUARTER(index_record_ts) AS aqi_quarter
                ,DAY(index_record_ts) aqi_day
                ,HOUR(index_record_ts) + 1 aqi_hour
            FROM
                dev_db.clean_sch.clean_flatten_aqi_dt
            GROUP BY
                1
                ,2
                ,3
                ,4
                ,5
                ,6
    )
    SELECT
        HASH(measurement_time) AS date_pk
        ,*
    FROM
        step01_hr_data
    ORDER BY
        aqi_year
        ,aqi_month
        ,aqi_day
        ,aqi_hour;

-- check data
SELECT
    *
FROM
    dim_date;

-- create dimension table `dim_location` to store location data
CREATE OR REPLACE DYNAMIC TABLE dim_location
    TARGET_LAG = 'DOWNSTREAM' 
    WAREHOUSE = transform_wh 
AS
    WITH step01_unique_data AS (
        SELECT
            latitude
            ,longitude
            ,country
            ,state
            ,city
            ,station
        FROM
            dev_db.clean_sch.clean_flatten_aqi_dt
        GROUP BY
            1
            ,2
            ,3
            ,4
            ,5
            ,6
    )
    SELECT
        HASH(LATITUDE, LONGITUDE) AS location_pk
        ,*
    FROM
        step01_unique_data
    ORDER BY
        country
        ,STATE
        ,city
        ,station;

-- check data
SELECT
    *
FROM
    dim_location;

-- create fact table `air_quality_fact` to store AQI data
CREATE OR REPLACE DYNAMIC TABLE air_quality_fact 
    TARGET_LAG = '30 min' 
    WAREHOUSE = transform_wh 
AS
    SELECT
        HASH(index_record_ts, latitude, longitude) aqi_pk
        ,HASH(index_record_ts) AS date_fk
        ,HASH(latitude, longitude) AS location_fk
        ,pm10_avg
        ,pm25_avg
        ,so2_avg
        ,no2_avg
        ,nh3_avg
        ,co_avg
        ,o3_avg
        ,prominent_index (pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) AS prominent_pollutant,
        CASE
            WHEN three_sub_index_criteria (pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) > 2 THEN 
                GREATEST(
                    get_int (pm25_avg)
                    ,get_int (pm10_avg)
                    ,get_int (so2_avg)
                    ,get_int (no2_avg)
                    ,get_int (nh3_avg)
                    ,get_int (co_avg)
                    ,get_int (o3_avg)
                )
            ELSE 0
        END AS aqi
    FROM
        dev_db.clean_sch.clean_flatten_aqi_dt

-- check data
SELECT
    *
FROM
    air_quality_fact;
