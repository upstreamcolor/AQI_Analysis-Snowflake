## AQI Analysis | <img src="https://raw.githubusercontent.com/upstreamcolor/AQI_Analysis-Snowflake/refs/heads/main/icons/snowflake.svg" width="20" /> <img src="https://raw.githubusercontent.com/upstreamcolor/AQI_Analysis-Snowflake/refs/heads/main/icons/streamlit.svg" width="20" /> <img src="https://raw.githubusercontent.com/upstreamcolor/AQI_Analysis-Snowflake/refs/heads/main/icons/githubactions.svg" width="20" />

An end-to-end data pipeline in Snowflake to ingest, process, and visualize hourly AQI data, transforming raw JSON data into structured fact and dimension tables for real-time insights via a Streamlit dashboard.

#### 📁 Folder Structure
---
```
AQI_Analysis-Snowflake/
│
├── sql_scripts/                            
│   ├── 01-wh_db_schema.sql               # schema, sarehouses for ingestion, transformation & consumption
│   ├── 02-stg_ff_task.sql                # stage, file format, task & transient table (bronze layer)
│   ├── 03-clean_layer_dt.sql             # dynamic table to store cleaned raw data (silver layer)
│   ├── 04-clean_layer_transpose_dt.sql   # dynamic table to store flattened data (silver layer)
│   ├── 05-wide_table_consumption.sql     # python UDFs, AQI calc. & storage in a wide transformed table
│   ├── 06-fact_and_dim.sql               # location & date dimension tables, AQI fact table (gold layer)
│   └── 07-aggregated_fact_table.sql      # hourly & daily aggregated data (gold layer)
├── icons/                                  
│   ├── githubactions.svg
│   ├── snowflake.svg
│   └── streamlit.svg
└── README.md                               # project documentation
```