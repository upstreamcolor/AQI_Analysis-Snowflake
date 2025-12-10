-- change the context
USE ROLE sysadmin;
USE SCHEMA dev_db.clean_sch;
USE WAREHOUSE adhoc_wh;

-- creating dynamic table `clean_aqi_dt` to store cleaned raw data, 
-- which refreshes when `raw_aqi` refreshes
CREATE OR REPLACE DYNAMIC TABLE clean_aqi_dt 
    TARGET_LAG = 'downstream' 
    WAREHOUSE = transform_wh 
AS
    WITH
        air_quality_with_rank AS (
            SELECT
                index_record_ts
                ,json_data
                ,record_count
                ,json_version
                ,_stg_file_name
                ,_stg_file_load_ts
                ,_stg_file_md5
                ,_copy_data_ts
                ,row_number() over (
                    PARTITION BY
                        index_record_ts
                    ORDER BY
                        _stg_file_load_ts desc
                ) AS latest_file_rank
            FROM
                dev_db.stage_sch.raw_aqi
            WHERE
                index_record_ts IS NOT NULL
        ),
        unique_air_quality_data AS (
            SELECT
                *
            FROM
                air_quality_with_rank
            WHERE
                latest_file_rank = 1
        )
    SELECT
        index_record_ts,
        ,hourly_rec.value:country::TEXT AS country
        ,hourly_rec.value:state::TEXT AS state
        ,hourly_rec.value:city::TEXT AS city
        ,hourly_rec.value:station::TEXT AS station
        ,hourly_rec.value:latitude::NUMBER(12, 7) AS latitude
        ,hourly_rec.value:longitude::NUMBER(12, 7) AS longitude
        ,hourly_rec.value:pollutant_id::TEXT AS pollutant_id
        ,hourly_rec.value:pollutant_max::TEXT AS pollutant_max
        ,hourly_rec.value:pollutant_min::TEXT AS pollutant_min
        ,hourly_rec.value:pollutant_avg::TEXT AS pollutant_avg
        ,_stg_file_name
        ,_stg_file_load_ts
        ,_stg_file_md5
        ,_copy_data_ts
    FROM
        unique_air_quality_data,
        LATERAL flatten(input => json_data:records) hourly_rec;

-- check data
SELECT
    *
FROM
    clean_aqi_dt
LIMIT
    10;
