-- use sysadmin role
USE ROLE sysadmin;

-- create dev development database
CREATE DATABASE IF NOT EXISTS dev_db;

-- create schemas - for landing, cleaing, storing & publishing
CREATE SCHEMA IF NOT EXISTS dev_db.stage_sch;
CREATE SCHEMA IF NOT EXISTS dev_db.clean_sch;
CREATE SCHEMA IF NOT EXISTS dev_db.consumption_sch;
CREATE SCHEMA IF NOT EXISTS dev_db.publish_sch;

-- check for all created schemas
SHOW SCHEMAS IN DATABASE dev_db;

-- create `load_wh` for ingestion purposes
CREATE WAREHOUSE IF NOT EXISTS load_wh
     comment = 'this is load warehosue for loading all the JSON files'
     warehouse_size = 'medium' 
     auto_resume = true 
     auto_suspend = 60 
     enable_query_acceleration = false 
     warehouse_type = 'standard' 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     initially_suspended = true;

-- create `transform_wh` to manage ELT activities
CREATE WAREHOUSE IF NOT EXISTS transform_wh
     comment = 'this is ETL warehosue for all loading activity' 
     warehouse_size = 'x-small' 
     auto_resume = true 
     auto_suspend = 60 
     enable_query_acceleration = false 
     warehouse_type = 'standard' 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     initially_suspended = true;

-- create `streamlit_wh` for reporting
CREATE WAREHOUSE IF NOT EXISTS streamlit_wh
     comment = 'this is streamlit virtua warehouse' 
     warehouse_size = 'x-small' 
     auto_resume = true
     auto_suspend = 60 
     enable_query_acceleration = false 
     warehouse_type = 'standard' 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     initially_suspended = true;

-- create `adhoc_wh` for extra tasks
CREATE WAREHOUSE IF NOT EXISTS adhoc_wh
     comment = 'this is adhoc warehosue for all adhoc & development activities' 
     warehouse_size = 'x-small' 
     auto_resume = true 
     auto_suspend = 60 
     enable_query_acceleration = false 
     warehouse_type = 'standard' 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     initially_suspended = true;

-- check for all created warehosues
SHOW WAREHOUSES;
