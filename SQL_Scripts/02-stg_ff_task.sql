-- change context
use role sysadmin;
use schema dev_db.stage_sch;
use warehouse adhoc_wh;

-- create an internal stage, `raw_stg` and enable directory service
create stage if not exists raw_stg
directory = (enable = true)
comment = 'stage to store raw data';

-- create `json_file_format` file format to process the JSON file
create file format if not exists json_file_format 
type = 'JSON'
compression = 'AUTO' 
comment = 'json file format';

show stages;
list @raw_stg;

-- load the data that has been downloaded manually
-- run the list command to check it
select 
    * 
from 
    @dev_db.stage_sch.raw_stg
    (file_format => JSON_FILE_FORMAT) t;

select 
    Try_TO_TIMESTAMP(t.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') as index_record_ts
    ,t.$1
    ,t.$1:total::int as record_count
    ,t.$1:version::text as json_version  
from 
    @dev_db.stage_sch.raw_stg
(file_format => JSON_FILE_FORMAT) t;

-- level3
select 
    TRY_TO_TIMESTAMP(t.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') as index_record_ts
    ,t.$1
    ,t.$1:total::int as record_count
    ,t.$1:version::text as json_version
    -- meta data information
    ,metadata$FILENAME as _stg_file_name
    ,metadata$FILE_LAST_MODIFIED as _stg_file_load_ts
    ,metadata$FILE_CONTENT_KEY as _stg_file_md5
    ,current_timestamp() as _copy_data_ts
from 
    @dev_db.stage_sch.raw_stg
(file_format => JSON_FILE_FORMAT) t;

-- creating `raw_aqi` table to store raw air quality data
create or replace transient table raw_aqi (
    id int primary key autoincrement
    ,index_record_ts timestamp not null
    ,json_data variant not null
    ,record_count number not null default 0
    ,json_version text not null
    -- audit columns for debugging
    ,_stg_file_name text
    ,_stg_file_load_ts timestamp
    ,_stg_file_md5 text
    ,_copy_data_ts timestamp default current_timestamp()
);

-- create task
create or replace task copy_air_quality_data
    warehouse = load_wh
    schedule = 'USING CRON 0 * * * * Asia/Kolkata'
as
-- run copy command first to load data to `raw_aqi`
copy into raw_aqi (index_record_ts,json_data,record_count,json_version,_stg_file_name,_stg_file_load_ts,_stg_file_md5,_copy_data_ts) 
from 
(
    select 
        Try_TO_TIMESTAMP(t.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') as index_record_ts
        ,t.$1
        ,t.$1:total::int as record_count
        ,t.$1:version::text as json_version
        ,metadata$FILENAME as _stg_file_name
        ,metadata$FILE_LAST_MODIFIED as _stg_file_load_ts
        ,metadata$FILE_CONTENT_KEY as _stg_file_md5
        ,current_timestamp() as _copy_data_ts        
    from 
        @dev_db.stage_sch.raw_stg as t
)
file_format = (format_name = 'dev_db.stage_sch.JSON_FILE_FORMAT') 
ON_ERROR = ABORT_STATEMENT; 

-- granting exec access to tasks to `SYSADMIN` role
use role accountadmin;
grant execute task, execute managed task on account to role sysadmin;
use role sysadmin;

alter task dev_db.stage_sch.copy_air_quality_data resume;

-- check the data
select 
    *
from 
    raw_aqi
limit 10;

-- select with ranking
select 
    index_record_ts
    ,record_count
    ,json_version
    ,_stg_file_name
    ,_stg_file_load_ts
    ,_stg_file_md5
    ,_copy_data_ts,
    row_number() over (partition by index_record_ts order by _stg_file_load_ts desc) as latest_file_rank
from 
    raw_aqi 
order by 
    index_record_ts desc
limit 10;
