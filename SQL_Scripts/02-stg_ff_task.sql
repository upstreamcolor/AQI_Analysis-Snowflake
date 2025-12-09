-- change context
USE ROLE sysadmin;
USE SCHEMA dev_db.stage_sch;
USE WAREHOUSE adhoc_wh;

-- create an internal stage, `raw_stg` and enable directory service
CREATE STAGE IF NOT EXISTS raw_stg 
DIRECTORY = (ENABLE = TRUE) 
COMMENT = 'stage to store raw data';

-- create `json_file_format` file format to process the JSON file
CREATE FILE FORMAT IF NOT EXISTS json_file_format 
TYPE = 'JSON' 
COMPRESSION = 'AUTO' 
COMMENT = 'json file format';

--check for stages
SHOW STAGES;

-- check for files in stage
LIST @raw_stg;

-- creating `raw_aqi` table to store raw air quality data
CREATE OR REPLACE TRANSIENT TABLE raw_aqi (
    id INT primary key autoincrement
    ,index_record_ts TIMESTAMP NOT NULL
    ,json_data VARIANT NOT NULL
    ,record_count NUMBER NOT NULL default 0
    ,json_version TEXT NOT NULL
    -- audit columns for debugging
    ,_stg_file_name TEXT
    ,_stg_file_load_ts TIMESTAMP
    ,_stg_file_md5 TEXT
    ,_copy_data_ts TIMESTAMP default current_timestamp()
);

-- create task
CREATE OR replace task copy_air_quality_data 
    WAREHOUSE = load_wh 
    SCHEDULE = 'USING CRON 0 * * * * Asia/Kolkata' 
AS
-- run copy command first to load data to `raw_aqi`
    COPY INTO raw_aqi (
        index_record_ts,
        json_data,
        record_count,
        json_version,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    FROM
    (
        SELECT
            Try_TO_TIMESTAMP(
                t.$1:records[0].last_update::TEXT,
                'dd-mm-yyyy hh24:mi:ss'
            ) AS index_record_ts
            ,t.$1
            ,t.$1:total::INT AS record_count
            ,t.$1:version::TEXT AS json_version
            ,metadata$FILENAME AS _stg_file_name
            ,metadata$FILE_LAST_MODIFIED AS _stg_file_load_ts
            ,metadata$FILE_CONTENT_KEY AS _stg_file_md5
            ,current_timestamp() AS _copy_data_ts
        FROM
            @dev_db.stage_sch.raw_stg AS t
    ) file_format = (format_name = 'dev_db.stage_sch.JSON_FILE_FORMAT') ON_ERROR = ABORT_STATEMENT;

-- granting exec access to tasks to `SYSADMIN` role
USE ROLE accountadmin;
GRANT EXECUTE TASK, EXECUTE MANAGED TASK ON ACCOUNT TO ROLE sysadmin;
USE ROLE sysadmin;

ALTER TASK dev_db.stage_sch.copy_air_quality_data resume;

-- check the data
SELECT
    *
FROM
    raw_aqi
