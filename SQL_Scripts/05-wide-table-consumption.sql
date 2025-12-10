USE ROLE sysadmin;
USE SCHEMA dev_db.consumption_sch;
USE WAREHOUSE adhoc_wh;

-- create function `prominent_index` to get the pollutant with the highest value
CREATE OR REPLACE FUNCTION prominent_index (
    pm25 NUMBER
    ,pm10 NUMBER
    ,so2 NUMBER
    ,no2 NUMBER
    ,nh3 NUMBER
    ,co NUMBER
    ,o3 NUMBER
) RETURNS VARCHAR 
    LANGUAGE PYTHON 
    RUNTIME_VERSION = '3.11' 
    HANDLER = 'prominent_index' 
AS 
' 
def prominent_index(pm25, pm10, so2, no2, nh3, co, o3):
    # Handle None values by replacing them with 0
    pm25 = pm25 if pm25 is not None else 0
    pm10 = pm10 if pm10 is not None else 0
    so2 = so2 if so2 is not None else 0
    no2 = no2 if no2 is not None else 0
    nh3 = nh3 if nh3 is not None else 0
    co = co if co is not None else 0
    o3 = o3 if o3 is not None else 0

    # Create a dictionary to map variable names to their values
    variables = {''PM25'': pm25, ''PM10'': pm10, ''SO2'': so2, ''NO2'': no2, ''NH3'': nh3, ''CO'': co, ''O3'': o3}
    
    # Find the variable with the highest value
    max_variable = max(variables, key = variables.get)
    
    return max_variable
';

-- create function `three_sub_index_criteria` to check where 3 or more pollutant values
-- are avl. per record for AQI calculation 
CREATE OR REPLACE FUNCTION three_sub_index_criteria (
    pm25 NUMBER
    ,pm10 NUMBER
    ,so2 NUMBER
    ,no2 NUMBER
    ,nh3 NUMBER
    ,co NUMBER
    ,o3 NUMBER
) RETURNS NUMBER(38, 0) 
    LANGUAGE PYTHON 
    RUNTIME_VERSION = '3.11' 
    HANDLER = 'three_sub_index_criteria' 
AS 
'
def three_sub_index_criteria(pm25, pm10, so2, no2, nh3, co, o3):
    pm_count = 0
    non_pm_count = 0

    if pm25 is not None and pm25 > 0:
        pm_count = 1
    elif pm10 is not None and pm10 > 0:
        pm_count = 1

    non_pm_count = min(2, sum(p is not None and p != 0 for p in [so2, no2, nh3, co, o3]))

    return pm_count + non_pm_count
';

-- create function `get_int` get the integer value for each pollutant value
CREATE OR REPLACE FUNCTION get_int (input_value FLOAT) 
    RETURNS NUMBER(38, 0) 
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11' 
    HANDLER = 'get_int' 
AS 
'
def get_int (input: float):
    if input is not None:
        return int(input)
    return 0
';

-- create dynamic table `aqi_final_wide_dt` to store prominent pollutant & AQI
CREATE OR REPLACE DYNAMIC TABLE aqi_final_wide_dt 
    TARGET_LAG = '30 min' 
    WAREHOUSE = transform_wh 
AS
    SELECT
        index_record_ts
        ,YEAR(index_record_ts) AS aqi_year
        ,MONTH(index_record_ts) AS aqi_month
        ,QUARTER(index_record_ts) AS aqi_quarter
        ,DAY(index_record_ts) aqi_day
        ,HOUR(index_record_ts) aqi_hour
        ,country
        ,state
        ,city
        ,station
        ,latitude
        ,longitude
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
        dev_db.clean_sch.clean_flatten_aqi_dt;

-- check data
SELECT
    *
FROM
    dev_db.consumption_sch.aqi_final_wide_dt
LIMIT 
    10;